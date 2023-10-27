use arc_swap::ArcSwapOption;
use async_lock::{Mutex, RwLock};
use nodecraft::{Address, Id};
use std::{
  collections::HashSet,
  net::SocketAddr,
  sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
  },
  time::Instant,
};

use crate::commitment::Commitment;

use super::Verify;

pub(super) struct Peer<I: Id, A: Address> {
  pub(super) id: I,
  pub(super) addr: A,
  pub(super) advertise_addr: SocketAddr,
}

pub(super) struct Replication<I: Id, A: Address> {
  /// The index of the next log entry to send to the follower,
  /// which may fall past the end of the log.
  next_index: Arc<AtomicU64>,

  /// Contains the network address and ID of the remote follower
  peer: Arc<RwLock<Peer<I, A>>>,

  /// closed when this leader steps down or the follower is
  /// removed from the cluster. In the follower removed case, it carries a log
  /// index; replication should be attempted with a best effort up through that
  /// index, before exiting.
  stop_tx: async_channel::Sender<()>,

  /// Notified every time new entries are appended to the log.
  trigger_tx: async_channel::Sender<()>,

  /// Used to provide a backchannel. By sending a
  /// deferErr, the sender can be notifed when the replication is done.
  trigger_defer_error_tx: async_channel::Sender<()>,

  /// Notify the runner to send out a heartbeat, which is used to check that
  /// this server is still leader.
  heartbeat_notify: async_channel::Sender<()>,

  /// a map of futures to be resolved upon receipt of an
  /// acknowledgement, then cleared from this map.
  notify: Arc<Mutex<HashSet<Verify>>>,
}

pub(super) struct ReplicationRunner<I: Id, A: Address> {
  /// commitment tracks the entries acknowledged by followers so that the
  /// leader's commit index can advance. It is updated on successful
  /// AppendEntries responses.
  commitment: Commitment<I, A>,

  /// The term of this leader, to be included in `append_entries`
  /// requests.
  current_term: u64,

  /// The index of the next log entry to send to the follower,
  /// which may fall past the end of the log.
  next_index: Arc<AtomicU64>,

  /// Contains the network address and ID of the remote follower
  peer: Arc<RwLock<Peer<I, A>>>,

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
  last_contact: ArcSwapOption<Instant>,

  /// Counts the number of failed RPCs since the last success, which is
  /// used to apply backoff.
  failures: u64,

  /// Signal to send out a heartbeat, which is used to check that
  /// this server is still leader.
  heartbeat_signal: async_channel::Receiver<()>,

  /// a map of futures to be resolved upon receipt of an
  /// acknowledgement, then cleared from this map.
  notify: Arc<Mutex<HashSet<Verify>>>,

  /// Used to indicate to the leader that we
  /// should step down based on information from a follower.
  step_down_tx: async_channel::Sender<()>,

  /// Used to determine when to pipeline the `append_entries` RPCs.
  /// It is private to this replication task.
  allow_pipeline: bool,
}
