use std::{
  collections::LinkedList,
  sync::atomic::{AtomicU64, Ordering},
  time::Duration,
};

use agnostic::Sleep;
use futures::{future::join_all, FutureExt, Stream, StreamExt};
use smallvec::SmallVec;

use super::*;
use crate::{
  error::RaftError,
  observe,
  raft::{ApplyRequest, ApplySender},
  storage::{Log, LogKind, LogStorage},
  utils::override_notify_bool,
  Observed,
};

mod commitment;
use commitment::Commitment;
mod replication;
use replication::Replication;

const MIN_CHECK_INTERVAL: Duration = Duration::from_millis(10);
const OLDEST_LOG_GAUGE_INTERVAL: Duration = Duration::from_secs(10);
const NUM_INLINED: usize = 4;

pub(super) struct Verify<F: FinateStateMachine, S: Storage, T: Transport> {
  pub(super) id: u64,
  pub(super) quorum_size: usize,
  pub(super) votes: Arc<async_lock::Mutex<(usize, Option<async_channel::Sender<Verify<F, S, T>>>)>>,
}

impl<F: FinateStateMachine, S: Storage, T: Transport> Clone for Verify<F, S, T> {
  fn clone(&self) -> Self {
    Self {
      id: self.id,
      quorum_size: self.quorum_size,
      votes: self.votes.clone(),
    }
  }
}

impl<F: FinateStateMachine, S: Storage, T: Transport> Verify<F, S, T> {
  /// Used to respond to a verify request.
  /// This may block when responding on the `notify_tx``.
  pub(super) async fn vote(&self, leader: bool) {
    let mut votes = self.votes.lock().await;

    if let Some(tx) = votes.1.take() {
      if leader {
        votes.0 += 1;
        if votes.0 >= self.quorum_size {
          let _ = tx
            .send(Verify {
              id: self.id,
              quorum_size: self.quorum_size,
              votes: self.votes.clone(),
            })
            .await;
        }
      } else {
        let _ = tx
          .send(Verify {
            id: self.id,
            quorum_size: self.quorum_size,
            votes: self.votes.clone(),
          })
          .await;
      }
    }
  }
}

struct LeaderState<F: FinateStateMachine, S: Storage, T: Transport> {
  // indicates that a leadership transfer is in progress.
  leadership_transfer_in_progress: AtomicBool,
  commit_rx: async_channel::Receiver<()>,
  commitment: Commitment<T::Id, <T::Resolver as AddressResolver>::Address>,
  // list of log in log index order
  inflight: LinkedList<Inflight<F, Error<F, S, T>>>,
  repl_state: HashMap<T::Id, Replication<F, S, T>>,
  notify: HashMap<u64, oneshot::Sender<Result<(), Error<F, S, T>>>>,
  step_down_rx: async_channel::Receiver<()>,
}

impl<F: FinateStateMachine, S: Storage, T: Transport> LeaderState<F, S, T> {
  fn new(
    start_index: u64,
    latest: &Membership<T::Id, <T::Resolver as AddressResolver>::Address>,
    step_down_rx: async_channel::Receiver<()>,
  ) -> Self {
    let (commit_tx, commit_rx) = async_channel::bounded(1);
    let commitment = Commitment::new(commit_tx, latest, start_index);
    Self {
      leadership_transfer_in_progress: AtomicBool::new(false),
      commit_rx,
      commitment,
      inflight: LinkedList::new(),
      repl_state: HashMap::new(),
      notify: HashMap::new(),
      step_down_rx,
    }
  }
}

impl<F, S, T, SC, R> RaftRunner<F, S, T, SC, R>
where
  F: FinateStateMachine<
    Id = T::Id,
    Address = <T::Resolver as AddressResolver>::Address,
    SnapshotSink = <S::Snapshot as SnapshotStorage>::Sink,
    Runtime = R,
  >,
  S: Storage<Id = T::Id, Address = <T::Resolver as AddressResolver>::Address, Runtime = R>,
  T: Transport<Runtime = R>,
  <T::Resolver as AddressResolver>::Address: Send + Sync + 'static,
  SC: Sidecar<Runtime = R>,
  R: Runtime,
  <R::Sleep as std::future::Future>::Output: Send,
  <R::Interval as futures::Stream>::Item: Send + 'static,
{
  pub(super) async fn run_leader(
    &mut self,
    #[cfg(feature = "metrics")] saturation_metric: &mut SaturationMetric,
  ) -> Result<bool, ()> {
    let local_id = self.transport.local_id();
    let local_addr = self.transport.local_addr();

    tracing::info!(target = "ruraft", id=%local_id, addr=%local_addr, "entering leader state");

    #[cfg(feature = "metrics")]
    metrics::increment_counter!("ruraft.state.leader");

    override_notify_bool(&self.leader_tx, &self.leader_rx, true).await;

    // Push to the notify channel
    futures::select! {
      _ = self.shutdown_rx.recv().fuse() => {
        return Ok(false);
      }
      _ = self.leadership_change_tx.send(true).fuse() => {}
    }

    let (step_down_tx, step_down_rx) = async_channel::bounded(1);
    let latest = self.memberships.latest().1.clone();
    let last_index = self.state.last_index() + 1;
    let mut leader_state = LeaderState::<F, S, T>::new(last_index, &latest, step_down_rx);
    let (stop_tx, stop_rx) = async_channel::bounded(1);

    #[cfg(feature = "metrics")]
    {
      use crate::storage::LogStorageExt;
      // Run a background go-routine to emit metrics on log age
      let s = self.storage.clone();
      let stop_rx = stop_rx.clone();

      super::super::spawn_local::<R, _>(self.wg.add(1), async move {
        s.log_store()
          .emit_metrics(OLDEST_LOG_GAUGE_INTERVAL, stop_rx)
          .await
      });
    }

    // Start a replication routine for each peer
    let current_term = self.current_term();
    let last_idx = self.last_index();
    for (id, (addr, _)) in latest.iter() {
      if local_id.eq(id) {
        continue;
      }

      tracing::info!(target = "ruraft.repl", peer=%id, "added peer, starting replication");
      let n = Node::new(id.clone(), addr.clone());

      self
        .spawn_replication(
          n,
          &mut leader_state,
          current_term,
          last_idx + 1,
          step_down_tx.clone(),
        )
        .await;
    }
    #[cfg(feature = "metrics")]
    metrics::gauge!("ruraft.peers", latest.len() as f64);

    // Dispatch a no-op log entry first. This gets this leader up to the latest
    // possible commit index, even in the absence of client commands. This used
    // to append a membership entry instead of a noop. However, that permits
    // an unbounded number of uncommitted memberships in the log. We now
    // maintain that there exists at most one uncommitted membership entry in
    // any log, so we have to do proper no-ops here.
    //
    // we ignore the response here, since we are not waiting for it
    let (noop_tx, _noop_rx) = oneshot::channel();
    let noop = ApplyRequest {
      log: LogKind::Noop,
      tx: ApplySender::Noop(noop_tx),
    };
    self
      .dispatch_logs(local_id, &mut leader_state, smallvec::smallvec![noop])
      .await;

    // Sit in the leader loop until we step down
    let (leader_state, rst) = self
      .leader_loop(
        local_id,
        leader_state,
        step_down_tx,
        #[cfg(feature = "metrics")]
        saturation_metric,
      )
      .await;

    self.clean_leader_state(leader_state, stop_tx).await;
    rst
  }

  /// The hot loop for a leader. It is invoked after all the various leader setup is done.
  async fn leader_loop(
    &self,
    local_id: &T::Id,
    mut leader_state: LeaderState<F, S, T>,
    step_down_tx: async_channel::Sender<()>,
    #[cfg(feature = "metrics")] saturation_metric: &mut SaturationMetric,
  ) -> (LeaderState<F, S, T>, Result<bool, ()>) {
    // stepDown is used to track if there is an inflight log that
    // would cause us to lose leadership (specifically a RemovePeer of
    // ourselves). If this is the case, we must not allow any logs to
    // be processed in parallel, otherwise we are basing commit on
    // only a single peer (ourself) and replicating to an undefined set
    // of peers.
    let mut step_down = false;

    let lease = R::sleep(self.options.leader_lease_timeout());
    futures::pin_mut!(lease);

    let (verify_resp_tx, verify_resp_rx) = async_channel::bounded(64);
    let mut verify_id = 0u64;

    let stable_membership_consumer = StableMembershipConsumer {
      rx: self.membership_change_rx.clone(),
      memberships: self.memberships.clone(),
      commit_index: self.state.commit_index.clone(),
      commitment: leader_state.commitment.clone(),
    };
    futures::pin_mut!(stable_membership_consumer);

    while self.state.role() == Role::Leader {
      #[cfg(feature = "metrics")]
      saturation_metric.sleeping();

      futures::select! {
        rpc = self.rpc.recv().fuse() => {
          #[cfg(feature = "metrics")]
          saturation_metric.working();

          match rpc {
            Ok(rpc) => {
              let (tx, req) = rpc.into_components();
              self.handle_request(tx, req).await;
            }
            Err(e) => {
              tracing::error!(target = "ruraft.leader", err=%e, "rpc consumer closed unexpectedly, shutting down...");
              return (leader_state, Err(()));
            }
          }
        }
        _ = leader_state.step_down_rx.recv().fuse() => {
          #[cfg(feature = "metrics")]
          saturation_metric.working();
          self.state.set_role(Role::Follower, &self.observers).await;
        }
        f = self.leader_transfer_rx.recv().fuse() => {
          #[cfg(feature = "metrics")]
          saturation_metric.working();
          todo!()
        }
        _ = leader_state.commit_rx.recv().fuse() => {
          #[cfg(feature = "metrics")]
          saturation_metric.working();
          todo!()
        }
        v = self.verify_rx.recv().fuse() => {
          #[cfg(feature = "metrics")]
          saturation_metric.working();

          match v {
            Ok(v) => self.handle_verify_leader_request(v, &mut leader_state, &mut verify_id, verify_resp_tx.clone()).await,
            Err(e) => {
              tracing::error!(target = "ruraft.leader", err=%e, "verify leader sender closed unexpectedly, shutting down...");
              return (leader_state, Err(()));
            }
          }
        }
        v = verify_resp_rx.recv().fuse() => {
          #[cfg(feature = "metrics")]
          saturation_metric.working();

          let v = v.expect("verify response sender closed unexpectedly, please report this bug to https://github.com/al8n/ruraft");
          self.handle_verify_leader_response(&mut leader_state, v).await;
        }
        ur = self.user_restore_rx.recv().fuse() => {
          #[cfg(feature = "metrics")]
          saturation_metric.working();

          match ur {
            Ok((src, tx)) => {
              if leader_state.leadership_transfer_in_progress.load(Ordering::Acquire) {
                let err = Error::<F, S, T>::Raft(RaftError::LeadershipTransferInProgress);
                tracing::debug!(target = "ruraft.leader", err=%err, "restore snapshot request received, but leadership transfer in progress");
                if tx.send(Err(err)).is_err() {
                  tracing::error!(target = "ruraft.leader", "restore snapshot response receiver closed, shutting down...");
                  return (leader_state, Err(()));
                }
                continue;
              }

              self.restore_user_snapshot(src).await;
            }
            Err(e) => {
              tracing::error!(target = "ruraft.leader", err=%e, "restore snapshot response sender closed unexpectedly, shutting down...");
              return (leader_state, Err(()));
            }
          }
        }
        cm = self.committed_membership_rx.recv().fuse() => {
          #[cfg(feature = "metrics")]
          saturation_metric.working();

          match cm {
            Ok(cm) => {
              if leader_state.leadership_transfer_in_progress.load(Ordering::Acquire) {
                let err = Error::<F, S, T>::Raft(RaftError::LeadershipTransferInProgress);
                tracing::debug!(target = "ruraft.leader", err=%err, "committed membership request received, but leadership transfer in progress");
                if cm.send(Err(err)).is_err() {
                  tracing::error!(target = "ruraft.leader", "committed membership response receiver closed, shutting down...");
                  return (leader_state, Err(()));
                }
                continue;
              }

              if cm.send(Ok(self.memberships.committed().clone())).is_err() {
                tracing::error!(target = "ruraft.leader", "committed membership response receiver closed");
                return (leader_state, Err(()));
              }
            }
            Err(e) => {
              tracing::error!(target = "ruraft.leader", err=%e, "committed membership response sender closed unexpectedly, shutting down...");
              return (leader_state, Err(()));
            }
          }
        }
        m = stable_membership_consumer.next().fuse() => {
          #[cfg(feature = "metrics")]
          saturation_metric.working();

          match m {
            Some(m) => {
              if leader_state.leadership_transfer_in_progress.load(Ordering::Acquire) {
                let err = Error::<F, S, T>::Raft(RaftError::LeadershipTransferInProgress);
                tracing::debug!(target = "ruraft.leader", err=%err, "membership change request received, but leadership transfer in progress");
                if m.tx.send(Err(err)).is_err() {
                  tracing::error!(target = "ruraft.leader", "membership change response receiver closed, shutting down...");
                  return (leader_state, Err(()));
                }
                continue;
              }

              self.append_membership_entry(
                local_id,
                &mut leader_state,
                &step_down_tx,
                m,
              ).await;
            }
            None => {
              tracing::error!(target = "ruraft.leader", "stable membership change response sender closed unexpectedly, shutting down...");
              return (leader_state, Err(()));
            }
          }
        }
        new_log = self.apply_rx.recv().fuse() => {
          #[cfg(feature = "metrics")]
          saturation_metric.working();

          match new_log {
            Ok(new_log) => {
              if leader_state.leadership_transfer_in_progress.load(Ordering::Acquire) {
                let err = Error::<F, S, T>::Raft(RaftError::LeadershipTransferInProgress);
                tracing::debug!(target = "ruraft.leader", err=%err, "apply request received, but leadership transfer in progress");
                if new_log.tx.send_err(err).is_err() {
                  tracing::error!(target = "ruraft.leader", "apply response receiver closed, shutting down...");
                  return (leader_state, Err(()));
                }
                continue;
              }

              self.apply_new_log_entry(local_id, &mut leader_state, new_log, step_down).await;
            }
            Err(e) => {
              tracing::error!(target = "ruraft.leader", err=%e, "apply response sender closed unexpectedly, shutting down...");
              return (leader_state, Err(()));
            }
          }
        }
        _ = (&mut lease).fuse() => {
          #[cfg(feature = "metrics")]
          saturation_metric.working();

          // Check if we've exceeded the lease, potentially stepping down
          let max_diff = self.check_leader_lease(local_id, &mut leader_state).await;


          // Next check interval should adjust for the last node we've
          // contacted, without going negative
          let check_interval = self.options.leader_lease_timeout().saturating_sub(max_diff).max(MIN_CHECK_INTERVAL);
          // Renew the lease timer
          lease.as_mut().reset(Instant::now() + check_interval);
        }
        _ = self.leader_notify_rx.recv().fuse() => {
          join_all(leader_state.repl_state.iter().map(|(id, repl)| {
            async move {
              if let Err(e) = repl.heartbeat_notify.send(()).await {
                tracing::error!(target = "ruraft.leader", peer=%id, err=%e, "failed to send heartbeat notify to replication");
              }
            }
          })).await;
        }
        _ = self.follower_notify_rx.recv().fuse() => {
          //  Ignore since we are not a follower
        }
        _ = self.shutdown_rx.recv().fuse() => {
          return (leader_state, Ok(false));
        }
      }
    }
    (leader_state, Ok(true))
  }

  async fn clean_leader_state(
    &self,
    leader_state: LeaderState<F, S, T>,
    stop_tx: async_channel::Sender<()>,
  ) {
    stop_tx.close();

    // Since we were the leader previously, we update our
    // last contact time when we step down, so that we are not
    // reporting a last contact time from before we were the
    // leader. Otherwise, to a client it would seem our data
    // is extremely stale.
    self.set_last_contact(Instant::now());

    // Respond to all inflight operations
    leader_state.inflight.into_iter().for_each(|inf| {
      let _ = inf.tx.send_err(Error::Raft(RaftError::LeadershipLost));
    });

    // Respond to any pending verify requests
    leader_state.notify.into_iter().for_each(|(id, tx)| {
      if tx
        .send(Err(Error::Raft(RaftError::LeadershipLost)))
        .is_err()
      {
        tracing::warn!(
          target = "ruraft.leader",
          id=%id,
          "verify leader response receiver closed"
        );
      }
    });

    // If we are stepping down for some reason, no known leader.
    // We may have stepped down due to an RPC call, which would
    // provide the leader, so we cannot always blank this out.
    {
      let cleader = self.leader.load();
      let local_id = self.transport.local_id();
      let local_addr = self.transport.local_addr();
      if let Some(cleader) = cleader.as_ref() {
        if cleader.id().eq(local_id) && cleader.addr().eq(local_addr) {
          self.leader.set(None, &self.observers).await;
        }
      }
    }

    // Notify that we are not the leader
    override_notify_bool(&self.leader_tx, &self.leader_rx, false).await;

    // Push to the notify channel
    futures::select! {
      _ = self.shutdown_rx.recv().fuse() => {
        // On shutdown, make a best effort but do not block
        futures::select! {
          _ = self.leadership_change_tx.send(false).fuse() => {}
          default => {}
        }
      }
      _ = self.leadership_change_tx.send(false).fuse() => {}
    }
  }

  /// Set up state and start asynchronous replication to
  /// new peers, and stop replication to removed peers. Before removing a peer,
  /// it'll instruct the replication routines to try to replicate to the current
  /// index. This must only be called from the main thread.
  async fn start_stop_replication(
    &self,
    local_id: &T::Id,
    leader_state: &mut LeaderState<F, S, T>,
    step_down_tx: &async_channel::Sender<()>,
  ) {
    let latest = self.memberships.latest().1.clone();
    let last_idx = self.last_index();
    let mut in_membership = HashMap::with_capacity(latest.len());

    // Start replication tasks that need starting
    for (id, (addr, _)) in latest.iter() {
      if id.eq(local_id) {
        continue;
      }

      in_membership.insert(id.clone(), true);

      match leader_state.repl_state.get(id) {
        None => {
          tracing::info!(target = "ruraft.repl", peer=%id, "added peer, starting replication");

          self
            .spawn_replication(
              Node::new(id.clone(), addr.clone()),
              leader_state,
              self.current_term(),
              last_idx + 1,
              step_down_tx.clone(),
            )
            .await;
        }
        Some(r) => {
          let peer = r.peer.load_full();

          if peer.addr().ne(addr) {
            tracing::info!(target = "ruraft.repl", peer=%id, "updating peer");
            r.peer.store(Arc::new(Node::new(id.clone(), addr.clone())));
          }
        }
      }
    }

    // Stop replication tasks that need stopping
    let mut futs = futures::stream::FuturesUnordered::new();
    leader_state.repl_state.retain(|id, repl| {
      if in_membership.contains_key(id) {
        true
      } else {
        tracing::info!(target = "ruraft.repl", peer=%id, "removing peer, stopping replication");
        repl.stop_tx.close();
        let id = id.clone();
        futs.push(async move {
          observe(&self.observers, Observed::Peer { id, removed: true }).await;
        });
        false
      }
    });

    while futs.next().await.is_some() {}

    // Update peers metric
    #[cfg(feature = "metrics")]
    metrics::gauge!("ruraft.peers", latest.len() as f64);
  }

  /// Handle commit event, return `true` if we should shutdown
  async fn handle_commit(&self, local_id: &T::Id, leader_state: &mut LeaderState<F, S, T>) -> bool {
    // Process the newly committed entries
    let old_commit_index = self.current_term();
    let commit_index = leader_state.commitment.get_commit_index().await;
    self.set_commit_index(commit_index);

    // New membership has been committed, set it as the committed
		// value.
    let (latest_index, latest) = {
      let l = self.memberships.latest();
      (l.0, l.1.clone())
    };

    let has_vote = latest.is_voter(local_id);
    if latest_index > old_commit_index && latest_index <= commit_index{
      self.memberships.set_committed(latest, latest_index);
      if !has_vote {
        return true;
      }
    }

    let start = Instant::now();
    let mut last_index_in_group = 0;

    // Pull all inflight logs that are committed off the queue.
    let mut inflight = core::mem::replace(&mut leader_state.inflight, LinkedList::new()).into_iter();
    while let Some(inf) = inflight.next() {
      let idx = inf.log.index;
      if idx > commit_index {
        // Don't go past the committed index

      }

      // Measure the commit time
      
    }

    false
  }

  /// Used to manually consume an external snapshot, such as if restoring from a backup.
  /// We will use the current Raft configuration, not the one from the snapshot,
  /// so that we can restore into a new cluster.
  /// We will also use the higher of the index of the snapshot, or the current index,
  /// and then add 1 to that, so we force a new state with a hole in the Raft log,
  /// so that the snapshot will be sent to followers and used for any new joiners.
  /// This can only be run on the leader,
  /// and returns a future that can be used to block until complete.
  async fn restore_user_snapshot(&self, src: <S::Snapshot as SnapshotStorage>::Source) {
    todo!()
  }

  /// Changes the membership and adds a new membership entry to the log.
  async fn append_membership_entry(
    &self,
    local_id: &T::Id,
    leader_state: &mut LeaderState<F, S, T>,
    step_down_tx: &async_channel::Sender<()>,
    req: MembershipChangeRequest<F, S, T>,
  ) {
    let membership = {
      let latest = self.memberships.latest().clone();
      Membership::next(&latest.1, latest.0, req.cmd)
    };

    match membership {
      Err(e) => {
        let _ = req.tx.send(Err(Error::membership(e)));
      }
      Ok(membership) => {
        tracing::info!(
          target = "ruraft.leader",
          servers = %membership,
          "updating membership");

        // In pre-ID compatibility mode we translate all configuration changes
        // in to an old remove peer message, which can handle all supported
        // cases for peer changes in the pre-ID world (adding and removing
        // voters)
        let m = Arc::new(membership);
        let logs = smallvec::smallvec![ApplyRequest {
          log: LogKind::Membership(m.clone()),
          tx: ApplySender::Membership(req.tx),
        }];

        let Some(LastLog { index, .. }) = self.dispatch_logs(local_id, leader_state, logs).await
        else {
          return;
        };

        self.memberships.set_latest(m.clone(), index);
        leader_state.commitment.set_membership(&m).await;
        self
          .start_stop_replication(local_id, leader_state, step_down_tx)
          .await;
      }
    }
  }

  async fn apply_new_log_entry(
    &self,
    local_id: &T::Id,
    leader_state: &mut LeaderState<F, S, T>,
    req: ApplyRequest<F, Error<F, S, T>>,
    step_down: bool,
  ) {
    // Group commit, gather all the ready commits
    let mut reqs: SmallVec<[_; NUM_INLINED]> = smallvec::smallvec![req];
    'group: loop {
      for _ in 0..self.options.max_append_entries() {
        futures::select! {
          req = self.apply_rx.recv().fuse() => {
            if let Ok(req) = req {
              reqs.push(req);
            } else {
              break 'group;
            }
          }
          default => {
            break 'group;
          }
        }
      }
    }

    // Dispatch the logs
    if step_down {
      // we're in the process of stepping down as leader, don't process anything new
      for req in reqs {
        let _ = req.tx.send_err(Error::Raft(RaftError::LeadershipLost));
      }
    } else {
      self.dispatch_logs(local_id, leader_state, reqs).await;
    }
  }

  /// Called on the leader to push a log to disk, mark it
  /// as inflight and begin replication of it.
  async fn dispatch_logs(
    &self,
    local_id: &T::Id,
    leader_state: &mut LeaderState<F, S, T>,
    reqs: SmallVec<[ApplyRequest<F, Error<F, S, T>>; NUM_INLINED]>,
  ) -> Option<LastLog> {
    let now = Instant::now();
    #[cfg(feature = "metrics")]
    scopeguard::defer! {
      metrics::histogram!("ruraft.leader.dispatch_logs", now.elapsed().as_millis() as f64);
    }

    let term = self.current_term();
    let last_idx = self.last_index();

    let n = reqs.len();

    let mut logs = SmallVec::<[_; NUM_INLINED]>::with_capacity(n);

    #[cfg(feature = "metrics")]
    metrics::gauge!("ruraft.leader.dispatch_num_logs", n as f64);

    for req in reqs {
      let log = Log {
        term,
        index: last_idx,
        kind: req.log,
        appended_at: Some(now),
      };
      logs.push(log.clone());
      leader_state.inflight.push_back(Inflight {
        tx: req.tx,
        log
      });
    }

    // Write the log entry locally
    let ls = self.storage.log_store();
    if let Err(e) = ls.store_logs(&logs).await {
      tracing::error!(target = "ruraft.leader", err=%e, "failed to commit logs");
      let new = LinkedList::new();
      for inf in core::mem::replace(&mut leader_state.inflight, new) {
        // TODO: log the error?
        let _ = inf.tx.send_err(Error::log(e.clone()));
      }

      self.state.set_role(Role::Follower, &self.observers).await;
      return None;
    }

    leader_state
      .commitment
      .match_index(local_id, last_idx)
      .await;

    // Update the last log since it's on disk now
    self.state.set_last_log(LastLog::new(last_idx, term));

    // Notify the replicators of the new log
    join_all(leader_state.repl_state.values().map(|repl| async move {
      let _ = repl.trigger_tx.send(()).await;
    }))
    .await;
    Some(LastLog::new(last_idx, term))
  }

  /// Used to check if we can contact a quorum of nodes
  /// within the last leader lease interval. If not, we need to step down,
  /// as we may have lost connectivity. Returns the maximum duration without
  /// contact. This must only be called from the main thread.
  async fn check_leader_lease(
    &self,
    local_id: &T::Id,
    leader_state: &mut LeaderState<F, S, T>,
  ) -> Duration {
    // Track contacted nodes, we can always contact ourself
    let mut contacted = 0;

    // Store lease timeout for this one check invocation as we need to refer to it
    // in the loop and would be confusing if it ever becomes reloadable and
    // changes between iterations below.
    let lease_timeout = self.options.leader_lease_timeout();

    // Check each follower
    let mut max_diff = Duration::ZERO;

    let now = Instant::now();

    let latest = self.memberships.latest().1.clone();
    for (id, (addr, suffrage)) in latest.iter() {
      if !suffrage.is_voter() {
        continue;
      }

      if id.eq(local_id) {
        contacted += 1;
        continue;
      }

      let Some(repl) = leader_state.repl_state.get(id) else {
        continue;
      };

      let diff = now - repl.last_contact();
      if diff <= lease_timeout {
        contacted += 1;
        if diff > max_diff {
          max_diff = diff;
        }
      } else {
        // Log at least once at high value, then debug. Otherwise it gets very verbose.
        if diff <= 3 * lease_timeout {
          tracing::warn!(target = "ruraft.leader", peer=%id, "fail to contact");
        } else {
          tracing::debug!(target = "ruraft.leader", peer=%id, "fail to contact");
        }
      }

      #[cfg(feature = "metrics")]
      metrics::gauge!("ruraft.leader.last_contact", diff.as_millis() as f64);
    }

    // Verify we can contact a quorum
    if contacted < latest.quorum_size() {
      tracing::warn!(
        target = "ruraft.leader",
        "failed to contact quorum of nodes, stepping down"
      );
      self.state.set_role(Role::Follower, &self.observers).await;
      #[cfg(feature = "metrics")]
      metrics::increment_counter!("ruraft.transition.leader_lease_timeout");
    }
    max_diff
  }

  /// Causes the followers to attempt an immediate heartbeat.
  async fn handle_verify_leader_request(
    &self,
    v: oneshot::Sender<Result<(), Error<F, S, T>>>,
    leader_state: &mut LeaderState<F, S, T>,
    verify_id: &mut u64,
    verify_resp_tx: async_channel::Sender<Verify<F, S, T>>,
  ) {
    let quorum_size = self.memberships.latest().1.quorum_size();

    if quorum_size == 1 {
      // We are the only node in the cluster,
      if v.send(Ok(())).is_err() {
        tracing::error!(
          target = "ruraft.leader",
          "receive verify leader request, but fail to send response, receiver closed"
        );
      }
      return;
    }

    // Just dispatched, start the verification
    // Track this request
    let id = *verify_id;
    *verify_id = (*verify_id).wrapping_add(1u64);
    leader_state.notify.insert(id, v);
    let verify = Verify {
      id,
      quorum_size,
      votes: Arc::new(async_lock::Mutex::new((1, Some(verify_resp_tx)))),
    };

    join_all(leader_state.repl_state.iter().map(|(id, repl)| {
      repl.notify.lock().insert(verify.id, verify.clone());
      async move {
        if let Err(e) = repl.heartbeat_notify.send(()).await {
          tracing::error!(target = "ruraft.leader", peer=%id, err=%e, "failed to send heartbeat notify to replication");
        }
      }
    })).await;
  }

  async fn handle_verify_leader_response(
    &self,
    leader_state: &mut LeaderState<F, S, T>,
    v: Verify<F, S, T>,
  ) {
    let votes = v.votes.lock().await.0;
    leader_state.repl_state.iter().for_each(|(_, repl)| {
      repl.clean_notify(v.id);
    });
    if votes < v.quorum_size {
      // Early return, means there must be a new leader
      tracing::warn!(
        target = "ruraft.leader",
        "new leader elected, stepping down"
      );
      self.state.set_role(Role::Follower, &self.observers).await;

      if let Some(tx) = leader_state.notify.remove(&v.id) {
        if tx.send(Err(Error::Raft(RaftError::NotLeader))).is_err() {
          tracing::error!(
            target = "ruraft.leader",
            "receive verify leader response, but fail to send response, receiver closed"
          );
        }
      } else {
        tracing::warn!(
          target = "ruraft.leader",
          "receive verify leader response, but fail to find response sender"
        );
      }
    } else {
      // Quorum of members agree, we are still leader
      if let Some(tx) = leader_state.notify.remove(&v.id) {
        if tx.send(Ok(())).is_err() {
          tracing::error!(
            target = "ruraft.leader",
            "receive verify leader response, but fail to send response, receiver closed"
          );
        }
      } else {
        tracing::warn!(
          target = "ruraft.leader",
          "receive verify leader response, but fail to find response sender"
        );
      }
    }
  }
}

/// What is the meaning of stable membership?
///
/// Have to wait until:
/// 1. The latest configuration is committed, and
/// 2. This leader has committed some entry (the noop) in this term
///    https://groups.google.com/forum/#!msg/raft-dev/t4xj6dJTP6E/d2D9LrWRza8J
#[pin_project::pin_project]
struct StableMembershipConsumer<F: FinateStateMachine, S: Storage, T: Transport> {
  commitment: Commitment<T::Id, <T::Resolver as AddressResolver>::Address>,
  memberships: Arc<Memberships<T::Id, <T::Resolver as AddressResolver>::Address>>,
  commit_index: Arc<AtomicU64>,
  #[pin]
  rx: async_channel::Receiver<MembershipChangeRequest<F, S, T>>,
}

impl<F: FinateStateMachine, S: Storage, T: Transport> Stream for StableMembershipConsumer<F, S, T> {
  type Item = MembershipChangeRequest<F, S, T>;

  fn poll_next(
    self: std::pin::Pin<&mut Self>,
    cx: &mut std::task::Context<'_>,
  ) -> std::task::Poll<Option<Self::Item>> {
    let this = self.project();
    if this.memberships.latest().0 == this.memberships.committed().0
      && this.commit_index.load(Ordering::Acquire) >= this.commitment.start_index()
    {
      this.rx.poll_next(cx)
    } else {
      std::task::Poll::Pending
    }
  }
}

struct Inflight<F: FinateStateMachine, E> {
  log: Log<F::Id, F::Address>,
  tx: ApplySender<F, E>,
}
