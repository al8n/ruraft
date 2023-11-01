use agnostic::Runtime;
use arc_swap::ArcSwap;
use atomic::Atomic;
use futures::FutureExt;
use nodecraft::resolver::AddressResolver;
use parking_lot::Mutex;
use std::{
  collections::{HashMap, HashSet},
  net::SocketAddr,
  sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
  },
  time::{Instant, Duration},
};
use wg::AsyncWaitGroup;

use super::{super::super::spawn_local, Commitment, Node, Verify};
use crate::{storage::Storage, transport::{Transport, HeartbeatRequest, Header}, FinateStateMachine, options::ReloadableOptions, utils::{random_timeout, capped_exponential_backoff}, observe, ObserverId, Observer, Observed};

const MAX_FAILURE_SCALE: u64 = 12;
const FAILURE_WAIT: Duration = Duration::from_millis(10);

pub(super) struct Replication<F: FinateStateMachine, S: Storage, T: Transport> {
  /// The index of the next log entry to send to the follower,
  /// which may fall past the end of the log.
  next_index: Arc<AtomicU64>,

  /// Contains the network address and ID of the remote follower
  pub(super) peer: Arc<ArcSwap<Node<T::Id, <T::Resolver as AddressResolver>::Address>>>,

  /// closed when this leader steps down or the follower is
  /// removed from the cluster. In the follower removed case, it carries a log
  /// index; replication should be attempted with a best effort up through that
  /// index, before exiting.
  stop_tx: async_channel::Sender<()>,

  /// Notify every time new entries are appended to the log.
  pub(super) trigger_tx: async_channel::Sender<()>,

  /// Used to provide a backchannel. By sending a
  /// deferErr, the sender can be notifed when the replication is done.
  trigger_defer_error_tx: async_channel::Sender<()>,

  /// Notify the runner to send out a heartbeat, which is used to check that
  /// this server is still leader.
  pub(super) heartbeat_notify: async_channel::Sender<()>,

  /// a map of futures to be resolved upon receipt of an
  /// acknowledgement, then cleared from this map.
  pub(super) notify: Arc<Mutex<HashMap<u64, Verify<F, S, T>>>>,

  /// Updated to the current time whenever any response is
  /// received from the follower (successful or not). This is used to check
  /// whether the leader should step down (`check_leader_lease`).
  last_contact: Arc<ArcSwap<Instant>>,
}

impl<F: FinateStateMachine, S: Storage, T: Transport> Replication<F, S, T>
where
  <T::Resolver as AddressResolver>::Address: Send + Sync + 'static,
{
  pub(super) async fn new<R: Runtime>(
    wg: &AsyncWaitGroup,
    peer: Node<T::Id, <T::Resolver as AddressResolver>::Address>,
    commitment: Commitment<T::Id, <T::Resolver as AddressResolver>::Address>,
    current_term: u64,
    next_index: u64,
    step_down_tx: async_channel::Sender<()>,
  ) -> (Self, ReplicationRunner<F, S, T>) {
    let next_index = Arc::new(AtomicU64::new(next_index));
    let peer = Arc::new(ArcSwap::from_pointee(peer));
    let notify = Arc::new(Mutex::new(HashMap::new()));

    let (stop_tx, stop_rx) = async_channel::bounded(1);
    let (trigger_tx, trigger_rx) = async_channel::bounded(1);
    let (trigger_defer_error_tx, trigger_defer_error_rx) = async_channel::bounded(1);
    let (heartbeat_notify, heartbeat_signal) = async_channel::bounded(1);

    let last_contact = Arc::new(ArcSwap::from_pointee(Instant::now()));

    let runner = ReplicationRunner::<F, S, T> {
      commitment,
      current_term,
      next_index: next_index.clone(),
      peer: peer.clone(),
      stop_rx,
      trigger_rx,
      trigger_defer_error_rx,
      last_contact: last_contact.clone(),
      failures: 0,
      heartbeat_signal,
      notify: notify.clone(),
      step_down_tx,
      allow_pipeline: false,
    };

    (Self {
      next_index,
      peer,
      stop_tx,
      trigger_tx,
      trigger_defer_error_tx,
      heartbeat_notify,
      notify,
      last_contact,
    }, runner)
  }

  /// Used to notify all the waiting verify futures
  /// if the follower believes we are still the leader.
  pub(super) async fn notify_all(notify: &Mutex<HashMap<u64, Verify<F, S, T>>>, leader: bool) {
    // Clear the waiting notifies minimizing lock time
    let n = notify.lock().drain().collect::<Vec<_>>();

    // Submit our votes
    futures::future::join_all(
      n.into_iter()
        .map(|(_, v)| async move { v.vote(leader).await }),
    )
    .await;
  }

  /// Used to delete notify
  pub(super) fn clean_notify(&self, id: u64) {
    self.notify.lock().remove(&id);
  }

  /// Returns the time of last contact.
  pub(super) fn last_contact(&self) -> Instant {
    **self.last_contact.load()
  }
}

pub(super) struct ReplicationRunner<F: FinateStateMachine, S: Storage, T: Transport> {
  /// commitment tracks the entries acknowledged by followers so that the
  /// leader's commit index can advance. It is updated on successful
  /// AppendEntries responses.
  commitment: Commitment<T::Id, <T::Resolver as AddressResolver>::Address>,

  /// The term of this leader, to be included in `append_entries`
  /// requests.
  current_term: u64,

  /// The index of the next log entry to send to the follower,
  /// which may fall past the end of the log.
  next_index: Arc<AtomicU64>,

  /// Contains the network address and ID of the remote follower
  peer: Arc<ArcSwap<Node<T::Id, <T::Resolver as AddressResolver>::Address>>>,

  /// notified when this leader steps down or the follower is
  /// removed from the cluster. In the follower removed case, it carries a log
  /// index; replication should be attempted with a best effort up through that
  /// index, before exiting.
  stop_rx: async_channel::Receiver<()>,

  /// Notified every time new entries are appended to the log.
  trigger_rx: async_channel::Receiver<()>,

  /// Used to provide a backchannel. By sending a
  /// deferErr, the sender can be notifed when the replication is done.
  trigger_defer_error_rx: async_channel::Receiver<()>,

  /// Updated to the current time whenever any response is
  /// received from the follower (successful or not). This is used to check
  /// whether the leader should step down (`check_leader_lease`).
  last_contact: Arc<ArcSwap<Instant>>,

  /// Counts the number of failed RPCs since the last success, which is
  /// used to apply backoff.
  failures: u64,

  /// Signal to send out a heartbeat, which is used to check that
  /// this server is still leader.
  heartbeat_signal: async_channel::Receiver<()>,

  /// a map of futures to be resolved upon receipt of an
  /// acknowledgement, then cleared from this map.
  notify: Arc<Mutex<HashMap<u64, Verify<F, S, T>>>>,

  /// Used to indicate to the leader that we
  /// should step down based on information from a follower.
  step_down_tx: async_channel::Sender<()>,

  /// Used to determine when to pipeline the `append_entries` RPCs.
  /// It is private to this replication task.
  allow_pipeline: bool,
}

impl<F: FinateStateMachine, S: Storage, T: Transport> ReplicationRunner<F, S, T>
where
  <T::Resolver as AddressResolver>::Address: Send + Sync + 'static,
{
  fn spawn(
    self,
    wg: &AsyncWaitGroup,
  ) {
    let runner_wg = wg.clone();
    spawn_local::<T::Runtime, _>(wg.add(1), async move {
      self.run(runner_wg).await;
    });
  }

  async fn run(
    mut self,
    wg: AsyncWaitGroup,
  ) {
    // // Start an async heartbeating routing
    // let (stop_heartbeat_tx, stop_heartbeat_rx) = async_channel::bounded(1);
    // spawn_local::<T::Runtime, _>(wg.add(1), async move {
      
    // });

  }

  fn set_last_contact(&self) {
    self.last_contact.store(Arc::new(Instant::now()));
  }
}

impl<F, S, T, SC, R> super::RaftRunner<F, S, T, SC, R>
where
  F: FinateStateMachine<
    Id = T::Id,
    Address = <T::Resolver as AddressResolver>::Address,
    SnapshotSink = <S::Snapshot as super::SnapshotStorage>::Sink,
    Runtime = R,
  >,
  S: Storage<Id = T::Id, Address = <T::Resolver as AddressResolver>::Address, Runtime = R>,
  T: Transport<Runtime = R>,
  <T::Resolver as AddressResolver>::Address: Send + Sync + 'static,
  SC: super::Sidecar<Runtime = R>,
  R: Runtime,
  <R::Sleep as std::future::Future>::Output: Send,
  <R::Interval as futures::Stream>::Item: Send + 'static,
{
  /// A long running task that replicates log entries to a single
  /// follower.
  pub(super) async fn replicate(&self, repl: ReplicationRunner<F, S, T>) {
    // let runner_wg = self.wg.clone();
    // spawn_local::<R, _>(self.wg.add(1), async move {
    //   // Start an async heartbeating routing
    //   let (stop_heartbeat_tx, stop_heartbeat_rx) = async_channel::bounded(1);
    //   spawn_local::<R, _>(runner_wg.add(1), async move {
        
    //   });

    // });
  }

  async fn heartbeat(
    current_term: u64,
    trans: Arc<T>,
    peer: Arc<ArcSwap<Node<T::Id, <T::Resolver as AddressResolver>::Address>>>,
    observers: Arc<async_lock::RwLock<HashMap<ObserverId, Observer<T::Id, <T::Resolver as AddressResolver>::Address>>>>,
    notify_all: Arc<Mutex<HashMap<u64, Verify<F, S, T>>>>,
    opts: Arc<Atomic<ReloadableOptions>>,
    last_contact: Arc<ArcSwap<Instant>>,
    heartbeat_signal: async_channel::Receiver<()>,
    stop_heartbeat_rx: async_channel::Receiver<()>,
  ) {
    let mut failures = 0;
    loop {
      // Wait for the next heartbeat interval or forced notify
      futures::select! {
        _ = heartbeat_signal.recv().fuse() => {}
        _ = stop_heartbeat_rx.recv().fuse() => return,
        _ = {
          let timeout = random_timeout(opts.load(Ordering::Acquire).heartbeat_timeout()).unwrap();
          R::sleep(timeout)
        }.fuse() => {}
      }

      let remote = peer.load_full();

      #[cfg(feature = "metrics")]
      let start = Instant::now();

      let req = HeartbeatRequest {
        header: trans.header(),
        term: current_term,
      };
      match trans.heartbeat(req).await {
        Ok(resp) => {
          if failures > 0 {
            observe(&observers, Observed::HeartbeatResumed(resp.header.id().clone())).await;
          }
          last_contact.store(Arc::new(Instant::now()));
          failures = 0;
          #[cfg(feature = "metrics")]
          metrics::gauge!(format!("ruraft.repl.heartbeat.{}", remote.id()), start.elapsed().as_millis() as f64);
          Replication::notify_all(&notify_all, resp.success).await;
        }
        Err(e) => {
          let next_backoff_time = capped_exponential_backoff(FAILURE_WAIT, failures, MAX_FAILURE_SCALE, random_timeout(opts.load(Ordering::Acquire).heartbeat_timeout()).unwrap() / 2);
          tracing::error!(target = "ruraft.repl", remote=%remote, backoff_time = %humantime::Duration::from(next_backoff_time), err=%e, "failed to heartbeat");
          observe(&observers, Observed::HeartbeatFailed {
            id: remote.id().clone(),
            last_contact: **last_contact.load(),
          }).await;
          failures += 1;
          futures::select! {
            _ = R::sleep(next_backoff_time).fuse() => {}
            _ = stop_heartbeat_rx.recv().fuse() => return,
          }
        }
      }
    }
  }
}