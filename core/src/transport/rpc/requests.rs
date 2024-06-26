use super::*;

mod append_entries;
pub use append_entries::*;

mod install_snapshot;
pub use install_snapshot::*;

mod vote;
pub use vote::*;

mod timeout_now;
pub use timeout_now::*;

mod heartbeat;
pub use heartbeat::*;

enum_wrapper!(
  /// Request to be sent to the Raft node.
  #[derive(Debug, Clone, derive_more::From)]
  #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
  #[cfg_attr(
    feature = "serde",
    serde(
      rename_all = "snake_case",
      bound(
        serialize = "I: Eq + core::hash::Hash + serde::Serialize, A: serde::Serialize",
        deserialize = "I: Eq + core::hash::Hash + core::fmt::Display + serde::Deserialize<'de>, A: Eq + core::fmt::Display +  serde::Deserialize<'de>",
      )
    )
  )]
  #[non_exhaustive]
  pub enum Request<I, A> {
    /// Append entries request.
    AppendEntries(AppendEntriesRequest<I, A>) = 0,
    /// Vote request.
    Vote(VoteRequest<I, A>) = 1,
    /// Install snapshot request.
    InstallSnapshot(InstallSnapshotRequest<I, A>) = 2,
    /// Timeout now request.
    TimeoutNow(TimeoutNowRequest<I, A>) = 3,
    /// Heartbeat request.
    Heartbeat(HeartbeatRequest<I, A>) = 4,
  }
);

impl<I: core::hash::Hash + Eq, A: PartialEq> PartialEq for Request<I, A> {
  fn eq(&self, other: &Self) -> bool {
    match (self, other) {
      (Self::AppendEntries(a), Self::AppendEntries(b)) => a == b,
      (Self::Vote(a), Self::Vote(b)) => a == b,
      (Self::InstallSnapshot(a), Self::InstallSnapshot(b)) => a == b,
      (Self::TimeoutNow(a), Self::TimeoutNow(b)) => a == b,
      (Self::Heartbeat(a), Self::Heartbeat(b)) => a == b,
      _ => false,
    }
  }
}

impl<I: core::hash::Hash + Eq, A: Eq> Eq for Request<I, A> {}
