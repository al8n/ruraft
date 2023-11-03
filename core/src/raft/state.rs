use std::{
  collections::HashMap,
  sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
  },
};

use atomic::Atomic;
use nodecraft::{resolver::AddressResolver, Address, Id};
use parking_lot::Mutex;

use crate::{
  membership::Membership, observe, storage::SnapshotMeta, transport::Transport, Observed, Observer,
  ObserverId,
};

/// Captures the role of a Raft node: Follower, Candidate, Leader,
/// or Shutdown.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash, bytemuck::NoUninit)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
#[repr(u8)]
pub enum Role {
  /// The initial state of a Raft node.
  Follower,
  /// One of the valid states of a Raft node.
  Candidate,
  /// One of the valid states of a Raft node.
  Leader,
  /// The terminal state of a Raft node.
  Shutdown,
}

impl Role {
  /// Returns the str of the role.
  #[inline]
  pub const fn as_str(&self) -> &'static str {
    match self {
      Self::Follower => "follower",
      Self::Candidate => "candidate",
      Self::Leader => "leader",
      Self::Shutdown => "shutdown",
    }
  }
}

#[viewit::viewit]
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq)]
pub(crate) struct LastLog {
  index: u64,
  term: u64,
}

impl LastLog {
  pub(crate) fn new(index: u64, term: u64) -> Self {
    Self { index, term }
  }
}

#[viewit::viewit]
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq)]
pub(crate) struct LastSnapshot {
  term: u64,
  index: u64,
}

impl LastSnapshot {
  pub(crate) fn new(index: u64, term: u64) -> Self {
    Self { term, index }
  }
}

#[viewit::viewit]
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq)]
pub(crate) struct LastEntry {
  index: u64,
  term: u64,
}

#[viewit::viewit]
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq)]
pub(crate) struct Last {
  snapshot: LastSnapshot,
  log: LastLog,
}

/// Used to maintain various state variables
/// and provides an interface to set/get the variables in a
/// thread safe manner.
#[viewit::viewit(getters(skip), setters(skip))]
pub(crate) struct State {
  /// The current term, cache of StableStorage
  current_term: AtomicU64,

  /// Highest committed log entry
  commit_index: Arc<AtomicU64>,

  /// Last applied log to the FSM
  last_applied: AtomicU64,

  last: Arc<Mutex<Last>>,

  /// The current role
  role: Atomic<Role>,
}

impl State {
  pub(crate) fn role(&self) -> Role {
    self.role.load(Ordering::SeqCst)
  }

  pub(crate) async fn set_role<I: Id, A: Address>(
    &self,
    role: Role,
    observers: &async_lock::RwLock<HashMap<ObserverId, Observer<I, A>>>,
  ) {
    let old = self.role.swap(role, Ordering::Release);
    if old != role {
      observe::<I, A>(observers, Observed::Role(role)).await;
    }
  }

  pub(crate) fn current_term(&self) -> u64 {
    self.current_term.load(Ordering::Acquire)
  }

  pub(crate) fn set_current_term(&self, val: u64) {
    self.current_term.store(val, Ordering::Release)
  }

  pub(crate) fn commit_index(&self) -> u64 {
    self.commit_index.load(Ordering::Acquire)
  }

  pub(crate) fn set_commit_index(&self, val: u64) {
    self.commit_index.store(val, Ordering::Release)
  }

  pub(crate) fn last_applied(&self) -> u64 {
    self.last_applied.load(Ordering::Acquire)
  }

  pub(crate) fn set_last_applied(&self, val: u64) {
    self.last_applied.store(val, Ordering::Release)
  }

  pub(crate) fn last_log(&self) -> LastLog {
    let last = self.last.lock();
    last.log
  }

  pub(crate) fn set_last_log(&self, log: LastLog) {
    let mut last = self.last.lock();
    last.log = log;
  }

  pub(crate) fn last_snapshot(&self) -> LastSnapshot {
    let last = self.last.lock();
    last.snapshot
  }

  pub(crate) fn set_last_snapshot(&self, snapshot: LastSnapshot) {
    let mut last = self.last.lock();
    last.snapshot = snapshot;
  }

  /// Returns the last index and term in stable storage.
  /// Either from the last log or from the last snapshot.
  pub(crate) fn last_index(&self) -> u64 {
    let last = self.last.lock();
    std::cmp::max(last.log.index, last.snapshot.index)
  }

  /// Returns the last index and term in stable storage.
  /// Either from the last log or from the last snapshot.
  pub(crate) fn last_entry(&self) -> LastEntry {
    let last = self.last.lock();
    if last.log.index >= last.snapshot.index {
      return LastEntry {
        index: last.log.index,
        term: last.log.term,
      };
    }

    LastEntry {
      index: last.snapshot.index,
      term: last.snapshot.term,
    }
  }
}

pub(super) struct RestoredState<T: Transport> {
  pub(super) last_snapshot: LastSnapshot,
  pub(super) last_applied: u64,
  pub(super) membership_index: u64,
  pub(super) membership: Arc<Membership<T::Id, <T::Resolver as AddressResolver>::Address>>,
}

pub(super) struct InitialState<T: Transport> {
  pub(super) current_term: Option<u64>,
  pub(super) last_log_index: Option<u64>,
  pub(super) snapshots: Vec<SnapshotMeta<T::Id, <T::Resolver as AddressResolver>::Address>>,
}

impl<T: Transport> InitialState<T> {
  /// Returns `true` if the state is clean,
  /// i.e. there is no existing raft cluster, and we need to bootstrap
  /// from scratch.
  pub(crate) fn is_clean_state(&self) -> bool {
    self.current_term.is_none() && self.last_log_index.is_none() && self.snapshots.is_empty()
  }
}
