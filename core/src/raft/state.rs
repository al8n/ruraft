use std::sync::atomic::{AtomicU64, Ordering};

use async_lock::Mutex;
use atomic::Atomic;

/// Captures the role of a Raft node: Follower, Candidate, Leader,
/// or Shutdown.
#[derive(
  Debug, PartialEq, Eq, Clone, Copy, Hash, serde::Serialize, serde::Deserialize, bytemuck::NoUninit,
)]
#[serde(rename_all = "snake_case")]
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
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) struct LastLog {
  index: u64,
  term: u64,
}

#[viewit::viewit]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) struct LastSnapshot {
  index: u64,
  term: u64,
}

#[viewit::viewit]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) struct LastEntry {
  index: u64,
  term: u64,
}

#[viewit::viewit]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
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
  commit_index: AtomicU64,

  /// Last applied log to the FSM
  last_applied: AtomicU64,

  last: Mutex<Last>,

  /// The current role
  role: Atomic<Role>,
}

impl State {
  pub(crate) fn role(&self) -> Role {
    self.role.load(Ordering::Acquire)
  }

  pub(crate) fn set_role(&self, role: Role) {
    self.role.store(role, Ordering::Release)
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

  pub(crate) async fn last_log(&self) -> LastLog {
    let last = self.last.lock().await;
    last.log
  }

  pub(crate) async fn set_last_log(&self, log: LastLog) {
    let mut last = self.last.lock().await;
    last.log = log;
  }

  pub(crate) async fn last_snapshot(&self) -> LastSnapshot {
    let last = self.last.lock().await;
    last.snapshot
  }

  pub(crate) async fn set_last_snapshot(&self, snapshot: LastSnapshot) {
    let mut last = self.last.lock().await;
    last.snapshot = snapshot;
  }

  /// Returns the last index and term in stable storage.
  /// Either from the last log or from the last snapshot.
  pub(crate) async fn last_index(&self) -> u64 {
    let last = self.last.lock().await;
    std::cmp::max(last.log.index, last.snapshot.index)
  }

  /// Returns the last index and term in stable storage.
  /// Either from the last log or from the last snapshot.
  pub(crate) async fn last_entry(&self) -> LastEntry {
    let last = self.last.lock().await;
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
