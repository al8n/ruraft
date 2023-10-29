use agnostic::Runtime;
use arc_swap::ArcSwap;
use nodecraft::resolver::AddressResolver;
use parking_lot::Mutex;
use std::{
  collections::{HashMap, HashSet},
  net::SocketAddr,
  sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
  },
  time::Instant,
};
use wg::AsyncWaitGroup;

use super::{super::super::spawn_local, Commitment, Node, Verify};
use crate::{storage::Storage, transport::Transport, FinateStateMachine};

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
  ) -> Self {
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

    spawn_local::<R, _>(wg.add(1), async move { runner.run().await });

    Self {
      next_index,
      peer,
      stop_tx,
      trigger_tx,
      trigger_defer_error_tx,
      heartbeat_notify,
      notify,
      last_contact,
    }
  }

  /// Used to notify all the waiting verify futures
  /// if the follower believes we are still the leader.
  pub(super) async fn notify_all(&self, leader: bool) {
    // Clear the waiting notifies minimizing lock time
    let n = self.notify.lock().drain().collect::<Vec<_>>();

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

impl<F: FinateStateMachine, S: Storage, T: Transport> ReplicationRunner<F, S, T> {
  async fn run(mut self) {}

  fn set_last_contact(&self) {
    self.last_contact.store(Arc::new(Instant::now()));
  }
}
