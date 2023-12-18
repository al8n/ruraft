use std::{
  borrow::Cow,
  collections::LinkedList,
  sync::atomic::{AtomicU64, Ordering},
  time::Duration,
};

use agnostic::Sleep;
use futures::{future::join_all, FutureExt, Stream, StreamExt};
use smallvec::SmallVec;

use super::*;
use crate::{
  observe,
  raft::{ApplyRequest, ApplySender, LastSnapshot},
  storage::{
    remove_old_logs, Log, LogKind, LogStorage, SnapshotSink, SnapshotSource, StorageError,
  },
  transport::{TimeoutNowRequest, TransportError},
  utils::override_notify_bool,
  Observed,
};

mod commitment;
use commitment::Commitment;
mod replication;
use replication::Replication;

const MIN_CHECK_INTERVAL: Duration = Duration::from_millis(10);
#[cfg(feature = "metrics")]
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
  leadership_transfer_in_progress: Arc<AtomicBool>,
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
      leadership_transfer_in_progress: Arc::new(AtomicBool::new(false)),
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
    Data = T::Data,
    SnapshotSink = <S::Snapshot as SnapshotStorage>::Sink,
    Runtime = R,
  >,
  S: Storage<
    Id = T::Id,
    Address = <T::Resolver as AddressResolver>::Address,
    Data = T::Data,
    Runtime = R,
  >,
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
    let (stop_tx, _stop_rx) = async_channel::bounded(1);

    #[cfg(feature = "metrics")]
    {
      use crate::storage::LogStorageExt;
      // Run a background go-routine to emit metrics on log age
      let s = self.storage.clone();

      super::super::spawn_local::<R, _>(self.wg.add(1), async move {
        s.log_store()
          .emit_metrics(OLDEST_LOG_GAUGE_INTERVAL, _stop_rx)
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
              let (tx, req, conn) = rpc.into_components();
              self.handle_request(tx.into(), req, conn).await;
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
        ev = self.leader_transfer_rx.recv().fuse() => {
          #[cfg(feature = "metrics")]
          saturation_metric.working();

          match ev {
            Err(e) => {
              tracing::error!(target = "ruraft.leader", err=%e, "leader transfer sender closed unexpectedly, shutting down...");
              return (leader_state, Err(()));
            }
            Ok((target, tx)) => {
              if leader_state.leadership_transfer_in_progress.load(Ordering::Acquire) {
                let err = Error::<F, S, T>::leadership_transfer_in_progress();
                tracing::debug!(target = "ruraft.leader", err=%err, "leader transfer request received, but leadership transfer in progress");
                if tx.send(Err(err)).is_err() {
                  tracing::error!(target = "ruraft.leader", "leader transfer response receiver closed, shutting down...");
                  return (leader_state, Err(()));
                }
                continue;
              }

              self.handle_leader_transfer(local_id, &mut leader_state, target, tx).await;
            }
          }
        }
        _ = leader_state.commit_rx.recv().fuse() => {
          #[cfg(feature = "metrics")]
          saturation_metric.working();
          step_down = self.handle_commit(local_id, &mut leader_state).await;

          if step_down {
            if self.options.shutdown_on_remove {
              tracing::info!(target = "ruraft.leader", "removed ourself, shutting down...");
              self.shutdown.shutdown(&self.state, &self.observers).await;
            } else {
              tracing::info!(target = "ruraft.leader", "removed ourself, transitioning to follower");
              self.state.set_role(Role::Follower, &self.observers).await;
            }
          }
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
                let err = Error::<F, S, T>::leadership_transfer_in_progress();
                tracing::debug!(target = "ruraft.leader", err=%err, "restore snapshot request received, but leadership transfer in progress");
                if tx.send(Err(err)).is_err() {
                  tracing::error!(target = "ruraft.leader", "restore snapshot response receiver closed, shutting down...");
                  return (leader_state, Err(()));
                }
                continue;
              }

              if tx.send(self.restore_user_snapshot(&mut leader_state, src).await).is_err() {
                tracing::error!(target = "ruraft.leader", "user restore snapshot response receiver closed");
              }
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
                let err = Error::<F, S, T>::leadership_transfer_in_progress();
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
                let err = Error::<F, S, T>::leadership_transfer_in_progress();
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
                let err = Error::<F, S, T>::leadership_transfer_in_progress();
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
    self.update_last_contact();

    // Respond to all inflight operations
    leader_state.inflight.into_iter().for_each(|inf| {
      let _ = inf.tx.send_err(Error::leadership_lost());
    });

    // Respond to any pending verify requests
    leader_state.notify.into_iter().for_each(|(id, tx)| {
      if tx.send(Err(Error::leadership_lost())).is_err() {
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

  async fn handle_leader_transfer(
    &self,
    local_id: &T::Id,
    leader_state: &mut LeaderState<F, S, T>,
    target: Option<Node<T::Id, <T::Resolver as AddressResolver>::Address>>,
    tx: oneshot::Sender<Result<(), Error<F, S, T>>>,
  ) {
    if let Some(ref t) = target {
      tracing::debug!(target = "ruraft.leader", id=%t.id(), address=%t.addr(), "starting leadership transfer");
    } else {
      tracing::debug!(target = "ruraft.leader", "starting leadership transfer");
    }

    // When we are leaving leaderLoop, we are no longer
    // leader, so we should stop transferring.
    let (left_leader_loop_tx, left_leader_loop_rx) = async_channel::bounded::<()>(1);
    scopeguard::defer!(let _ = left_leader_loop_tx.close(););

    let (stop_tx, stop_rx) = async_channel::bounded(1);
    let (done_tx, done_rx) = async_channel::bounded(1);

    // This is intentionally being setup outside of the
    // leadership_transfer function. Because the timeout_now
    // call is blocking and there is no way to abort that
    // in case eg the timer expires.
    // The leadership_transfer function is controlled with
    // the stop channel and done channel.
    // No matter how this exits, have this function set
    // leadership transfer to false before we return
    //
    // Note that this leaves a window where callers of
    // leadership_transfer() and
    // may start executing after they get their future but before
    // this routine has set leadership_transfer_in_progress back to false.
    // It may be safe to modify things such that set_leadership_transfer_in_progress
    // is set to false before calling future.Respond, but that still needs
    // to be tested and this situation mirrors what callers already had to deal with.
    {
      let opts = self.reloadable_options.clone();
      let leadership_transfer_in_progress = leader_state.leadership_transfer_in_progress.clone();
      R::spawn_detach(async move {
        scopeguard::defer!(leadership_transfer_in_progress.store(false, Ordering::Release));
        futures::select! {
          _ = R::sleep(opts.load(Ordering::Acquire).election_timeout()).fuse() => {
            stop_tx.close();
            let err = Error::leadership_transfer_timeout();
            tracing::debug!(target = "ruraft.leader", err=%err, "leadership transfer timeout");
            let _ = tx.send(Err(err));
            let _ = done_rx.recv().await;
          }
          _ = left_leader_loop_rx.recv().fuse() => {
            stop_tx.close();
            let err = Error::leadership_lost_during_transfer();
            tracing::debug!(target = "ruraft.leader", err=%err, "lost leadership during transfer (expected)");
            let _ = tx.send(Err(err));
            let _ = done_rx.recv().await;
          }
          err = done_rx.recv().fuse() => {
            match err {
              Ok(Err(e)) => {
                let _ = tx.send(Err(e));
              }
              Ok(Ok(())) => {
                let _ = tx.send(Ok(()));
              }
              Err(e) => {
                tracing::error!(target = "ruraft.leader", err=%e, "done signal sender was closed unexpectedly");
              }
            }
          }
        }
      });
    }

    // leader_state.repl_state is accessed here before
    // starting leadership transfer asynchronously because
    // leader_state is only supposed to be accessed in the
    // leaderloop.

    let Some(target) = target.or_else(|| self.pick_server(local_id, leader_state)) else {
      let _ = done_tx
        .send(Err(Error::leadership_transfer_no_target()))
        .await;
      return;
    };

    let Some(repl_state) = leader_state.repl_state.get(target.id()) else {
      let _ = done_tx.send(Err(Error::no_replication_state(target))).await;
      return;
    };

    leader_state
      .leadership_transfer_in_progress
      .store(true, Ordering::Release);

    {
      let state = self.state.clone();
      let transport = self.transport.clone();
      let next_index = repl_state.next_index.clone();
      let trigger_defer_error_tx = repl_state.trigger_defer_error_tx.clone();
      R::spawn_detach(async move {
        Self::leadearship_transfer(
          target,
          state,
          transport,
          next_index,
          trigger_defer_error_tx,
          stop_rx,
          done_tx,
        )
        .await
      })
    }
  }

  async fn leadearship_transfer(
    target: Node<T::Id, <T::Resolver as AddressResolver>::Address>,
    state: Arc<State>,
    trans: Arc<T>,
    next_index: Arc<AtomicU64>,
    trigger_defer_err_tx: async_channel::Sender<oneshot::Sender<Result<(), Error<F, S, T>>>>,
    stop_rx: async_channel::Receiver<()>,
    done_tx: async_channel::Sender<Result<(), Error<F, S, T>>>,
  ) {
    // make sure we are not already stopped
    futures::select! {
      _ = stop_rx.recv().fuse() => {
        let _ = done_tx.send(Ok(())).await;
        return;
      }
      default => {}
    }

    while next_index.load(Ordering::SeqCst) <= state.last_index() {
      let (tx, rx) = oneshot::channel();
      if trigger_defer_err_tx.send(tx).await.is_err() {
        let _ = done_tx
          .send(Err(Error::leadership_transfer_target_exits()))
          .await;
        return;
      }

      futures::select! {
        _ = stop_rx.recv().fuse() => {
          let _ = done_tx.send(Ok(())).await;
          return;
        }
        err = rx.fuse() => {
          match err {
            Ok(Err(e)) => {
              let _ = done_tx.send(Err(e)).await;
              return;
            }
            Ok(Ok(())) => {}
            Err(_) => {
              let _ = done_tx.send(Err(Error::leadership_transfer_target_exits())).await;
              return;
            }
          }
        }
      }
    }

    // Step ?: the thesis describes in chap 6.4.1: Using clocks to reduce
    // messaging for read-only queries. If this is implemented, the lease
    // has to be reset as well, in case leadership is transferred. This
    // implementation also has a lease, but it serves another purpose and
    // doesn't need to be reset. The lease mechanism in our raft lib, is
    // setup in a similar way to the one in the thesis, but in practice
    // it's a timer that just tells the leader how often to check
    // heartbeats are still coming in.

    // Step 3: send timeout_now message to target server.

    if let Err(e) = trans
      .timeout_now(
        &target,
        TimeoutNowRequest {
          header: trans.header(),
        },
      )
      .await
    {
      let _ = done_tx
        .send(Err(Error::transport(e.with_message(Cow::Owned(format!(
          "failed to make TimeoutNow rpc to {target}"
        ))))))
        .await;
      return;
    }
    let _ = done_tx.send(Ok(())).await;
  }

  /// Returns the follower that is most up to date and participating in quorum.
  /// Because it accesses leaderstate, it should only be called from the leaderloop.
  fn pick_server(
    &self,
    local_id: &T::Id,
    leader_state: &LeaderState<F, S, T>,
  ) -> Option<Node<T::Id, <T::Resolver as AddressResolver>::Address>> {
    let latest = self.memberships.latest().1.clone();
    let mut current = 0;
    let mut tmp = None;
    for (id, (addr, suffrage)) in latest.iter() {
      if id.eq(local_id) || !suffrage.is_voter() {
        continue;
      }

      let Some(state) = leader_state.repl_state.get(id) else {
        continue;
      };

      let next_idx = state.next_index.load(Ordering::SeqCst);
      if next_idx > current {
        current = next_idx;
        tmp = Some((id, addr));
      }
    }

    tmp.map(|(id, addr)| Node::new(id.clone(), addr.clone()))
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
    if latest_index > old_commit_index && latest_index <= commit_index {
      self.memberships.set_committed(latest, latest_index);
      if !has_vote {
        return true;
      }
    }

    #[cfg(feature = "metrics")]
    let start = Instant::now();

    // Pull all inflight logs that are committed off the queue.
    let inflight = core::mem::take(&mut leader_state.inflight);

    let mut group_futs = HashMap::new();
    let mut last_idx_in_group = 0;
    let mut num_commits = 0;

    let remaining = inflight
      .into_iter()
      .filter_map(|inf| {
        let idx = inf.log.index;
        if idx > commit_index {
          // Don't go past the committed index
          Some(inf)
        } else {
          num_commits += 1;
          #[cfg(feature = "metrics")]
          metrics::histogram!(
            "ruraft.leader.commit_time",
            inf.dispatch.elapsed().as_millis() as f64
          );
          group_futs.insert(idx, inf);
          last_idx_in_group = idx;
          None
        }
      })
      .collect::<LinkedList<_>>();

    let _ = core::mem::replace(&mut leader_state.inflight, remaining);

    // Process the group
    if !group_futs.is_empty() {
      self.process_logs(last_idx_in_group, Some(group_futs)).await;
    }

    // Measure the time to enqueue batch of logs for FSM to apply
    #[cfg(feature = "metrics")]
    metrics::histogram!("ruraft.fsm.enqueue", start.elapsed().as_millis() as f64);

    // Count the number of logs enqueued
    #[cfg(feature = "metrics")]
    metrics::gauge!("ruraft.leader.commit_num_logs", num_commits as f64);

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
  async fn restore_user_snapshot(
    &self,
    leader_state: &mut LeaderState<F, S, T>,
    mut src: <S::Snapshot as SnapshotStorage>::Source,
  ) -> Result<(), Error<F, S, T>> {
    #[cfg(feature = "metrics")]
    let start = Instant::now();
    #[cfg(feature = "metrics")]
    scopeguard::defer!(metrics::histogram!(
      "ruraft.leader.restore_user_snapshot",
      start.elapsed().as_millis() as f64
    ));

    // We don't support snapshots while there's a config change
    // outstanding since the snapshot doesn't have a means to
    // represent this state.
    let committed_index = self.memberships.committed().0;
    let (latest_index, latest) = {
      let l = self.memberships.latest();
      (l.0, l.1.clone())
    };
    if committed_index != latest_index {
      return Err(Error::cannot_restore_snapshot(
        committed_index,
        latest_index,
      ));
    }

    // Cancel any inflight requests.
    let inflight = core::mem::take(&mut leader_state.inflight);
    for inf in inflight {
      let _ = inf.tx.send_err(Error::aborted_by_restore());
    }

    // We will overwrite the snapshot metadata with the current term,
    // an index that's greater than the current index, or the last
    // index in the snapshot. It's important that we leave a hole in
    // the index so we know there's nothing in the Raft log there and
    // replication will fault and send the snapshot.
    let term = self.current_term();
    let mut last_index = self.last_index();
    let meta = src.meta();
    let meta_index = meta.index();
    let meta_size = meta.size();
    if meta_index > last_index {
      last_index = meta_index;
    }
    last_index += 1;

    // Dump the snapshot. Note that we use the latest membership,
    // not the one that came with the snapshot.
    let mut sink = self
      .storage
      .snapshot_store()
      .create(meta.version(), term, last_index, latest, latest_index)
      .await
      .map_err(|e| {
        Error::storage(
          <S::Error as StorageError>::snapshot(e)
            .with_message(Cow::Borrowed("failed to create snapshot")),
        )
      })?;

    let n = match futures::io::copy(&mut src, &mut sink).await {
      Ok(n) => n,
      Err(e) => {
        if let Err(e) = sink.cancel().await {
          tracing::error!(target = "ruraft.leader", err=%e, "failed to cancel snapshot");
        }

        return Err(Error::storage(
          <S::Error as StorageError>::io(e).with_message(Cow::Borrowed("failed to copy snapshot")),
        ));
      }
    };

    if n != meta_size {
      if let Err(e) = sink.cancel().await {
        tracing::error!(target = "ruraft.leader", err=%e, "failed to cancel snapshot");
      }
      return Err(Error::storage(
        <S::Error as StorageError>::io(std::io::Error::new(std::io::ErrorKind::Other, ""))
          .with_message(Cow::Owned(format!(
            "failed to write snapshot, size did not match ({} != {})",
            n, meta_size
          ))),
      ));
    }

    let snapshot_id = sink.id();
    if let Err(e) = sink.close().await {
      return Err(Error::storage(
        <S::Error as StorageError>::io(e).with_message(Cow::Borrowed("failed to close snapshot")),
      ));
    }
    tracing::info!(
      target = "ruraft.leader",
      bytes = n,
      "copied to local snapshot"
    );

    // Restore the snapshot into the FSM. If this fails we are in a
    // bad state so we panic to take ourselves out.
    let (tx, rx) = oneshot::channel();
    let fsm = FSMRequest::Restore {
      id: snapshot_id,
      tx,
      shutdown_rx: self.shutdown_rx.clone(),
    };
    futures::select! {
      _ = self.fsm_mutate_tx.send(fsm).fuse() => {}
      _ = self.shutdown_rx.recv().fuse() => {
        return Err(Error::shutdown());
      }
    }
    match rx.await {
      Ok(Ok(())) => {}
      Ok(Err(e)) => {
        panic!("failed to restore snapshot: {e}");
      }
      Err(e) => {
        panic!("failed to restore snapshot: {e}");
      }
    }

    // We set the last log so it looks like we've stored the empty
    // index we burned. The last applied is set because we made the
    // FSM take the snapshot state, and we store the last snapshot
    // in the stable store since we created a snapshot as part of
    // this process.
    self.set_last_log(LastLog::new(last_index, term));
    self.set_last_applied(last_index);
    self.set_last_snapshot(LastSnapshot::new(last_index, term));

    // Remove old logs if r.logs is a MonotonicLogStore. Log any errors and continue.
    let ls = self.storage.log_store();
    if <S::Log as LogStorage>::is_monotonic() {
      if let Err(e) = remove_old_logs::<S>(ls).await {
        tracing::error!(target = "ruraft.leader", err=%e, "failed to remove old logs");
      }
    }

    tracing::info!(target = "ruraft.leader", index=%last_index, "restored user snapshot");
    Ok(())
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
        let logs = smallvec::smallvec![ApplyRequest {
          log: LogKind::Membership(membership.clone()),
          tx: ApplySender::Membership(req.tx),
        }];

        let Some(LastLog { index, .. }) = self.dispatch_logs(local_id, leader_state, logs).await
        else {
          return;
        };

        self.memberships.set_latest(membership.clone(), index);
        leader_state.commitment.set_membership(&membership).await;
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
        let _ = req.tx.send_err(Error::leadership_lost());
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
    #[cfg(feature = "metrics")]
    let now = Instant::now();
    let appended_at = std::time::SystemTime::now();
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
        appended_at: Some(appended_at),
      };
      logs.push(log.clone());
      leader_state.inflight.push_back(Inflight {
        #[cfg(feature = "metrics")]
        dispatch: now,
        tx: req.tx,
        log,
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
    for (id, (_, suffrage)) in latest.iter() {
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
      metrics::histogram!("ruraft.leader.last_contact", diff.as_millis() as f64);
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
        if tx.send(Err(Error::not_leader())).is_err() {
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
