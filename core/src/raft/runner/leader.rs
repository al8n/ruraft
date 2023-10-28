use std::{
  collections::{HashSet, LinkedList},
  sync::atomic::Ordering,
  time::Duration,
};

use futures::{future::join_all, StreamExt};
use nodecraft::{Address, Id};

use super::*;
use crate::{error::RaftError, storage::Log, utils::random_timeout};

mod commitment;
use commitment::Commitment;
mod replication;
use replication::Replication;

const OLDEST_LOG_GAUGE_INTERVAL: Duration = Duration::from_secs(10);

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

    match votes.1.take() {
      Some(tx) => {
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
      None => return,
    }
  }
}

struct LeaderState<F: FinateStateMachine, S: Storage, T: Transport> {
  // indicates that a leadership transfer is in progress.
  leadership_transfer_in_progress: AtomicBool,
  commit_rx: async_channel::Receiver<()>,
  commitment: Commitment<T::Id, <T::Resolver as AddressResolver>::Address>,
  // list of log in log index order
  inflight: LinkedList<Log<T::Id, <T::Resolver as AddressResolver>::Address>>,
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

    // override_notify_bool(self.leader_tx, true);

    // Store the notify chan. It's not reloadable so shouldn't change before the
    // defer below runs, but this makes sure we always notify the same chan if
    // ever for both gaining and loosing leadership.

    // TODO: implement
    // notify := r.config().NotifyCh

    // Push to the notify channel if given
    // if notify != nil {
    //   select {
    //   case notify <- true:
    //   case <-r.shutdownCh:
    //   }
    // }

    let (step_down_tx, step_down_rx) = async_channel::bounded(1);
    let latest = self.memberships.latest().1.clone();
    let last_index = self.state.last_index() + 1;
    let leader_state = LeaderState::<F, S, T>::new(last_index, &latest, step_down_rx);
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

    let rst = self
      .leader_loop(
        leader_state,
        #[cfg(feature = "metrics")]
        saturation_metric,
      )
      .await;

    // TODO: cleanup logic

    rst
  }

  async fn leader_loop(
    &self,
    mut leader_state: LeaderState<F, S, T>,
    #[cfg(feature = "metrics")] saturation_metric: &mut SaturationMetric,
  ) -> Result<bool, ()> {
    // stepDown is used to track if there is an inflight log that
    // would cause us to lose leadership (specifically a RemovePeer of
    // ourselves). If this is the case, we must not allow any logs to
    // be processed in parallel, otherwise we are basing commit on
    // only a single peer (ourself) and replicating to an undefined set
    // of peers.
    let mut step_down = false;

    let mut lease = R::interval(self.options.leader_lease_timeout());

    let (verify_resp_tx, verify_resp_rx) = async_channel::bounded(64);
    let mut verify_id = 0u64;
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
              self.state.set_role(Role::Shutdown);
              return Err(());
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
              self.state.set_role(Role::Shutdown);
              return Err(());
            }
          }
        }
        v = verify_resp_rx.recv().fuse() => {
          #[cfg(feature = "metrics")]
          saturation_metric.working();

          let v = v.expect("verify response sender closed unexpectedly, please report this bug to https://github.com/al8n/ruraft");
          self.handle_verify_leader_response(&mut leader_state, v).await;
        }
        _ = lease.next().fuse() => {
          #[cfg(feature = "metrics")]
          saturation_metric.working();


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
          return Ok(false);
        }
      }
    }
    Ok(true)
  }

  /// Set up state and start asynchronous replication to
  /// new peers, and stop replication to removed peers. Before removing a peer,
  /// it'll instruct the replication routines to try to replicate to the current
  /// index. This must only be called from the main thread.
  async fn start_stop_replication(
    &self,
    local_id: &T::Id,
    leader_state: &mut LeaderState<F, S, T>,
    step_down_tx: async_channel::Sender<()>,
  ) {
    let latest = self.memberships.latest().1.clone();
    let last_idx = self.last_index();
    let mut in_membership = HashMap::with_capacity(latest.len());

    // Start replication goroutines that need starting
    for (id, (addr, suffrage)) in latest.iter() {
      if id.eq(local_id) {
        continue;
      }

      in_membership.insert(id.clone(), true);

      match leader_state.repl_state.get(id) {
        None => {
          tracing::info!(target = "ruraft.repl", peer=%id, "added peer, starting replication");

          let repl = Replication::<F, S, T>::new::<R>(
            &self.wg,
            Node::new(id.clone(), addr.clone()),
            leader_state.commitment.clone(),
            self.state.current_term(),
            last_idx + 1,
            step_down_tx.clone(),
          )
          .await;
          // repl.trigger_tx.send(()).await.unwrap();
          // TODO: implement
          // r.leaderState.replState[server.ID] = s
          // r.goFunc(func() { r.replicate(s) })
          // asyncNotifyCh(s.triggerCh)
          // r.observe(PeerObservation{Peer: server, Removed: false})
        }
        Some(r) => {
          let peer: Arc<_> = r.peer.load().clone();

          if peer.addr().ne(addr) {
            tracing::info!(target = "ruraft.repl", peer=%id, "updating peer");
            r.peer.store(Arc::new(Node::new(id.clone(), addr.clone())));
          }
        }
      }
    }
    // Update peers metric
    #[cfg(feature = "metrics")]
    metrics::gauge!("ruraft.peers", latest.len() as f64);
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
      self.state.set_role(Role::Follower);

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
