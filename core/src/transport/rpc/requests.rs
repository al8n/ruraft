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
  #[derive(Debug, Clone)]
  #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
  #[cfg_attr(feature = "serde", serde(rename_all = "snake_case", bound = "I: Id + serde::Serialize + for<'a> serde::Deserialize<'a>, A: Address + serde::Serialize + for<'a> serde::Deserialize<'a>, D: serde::Serialize + for<'a> serde::Deserialize<'a>"))]
  #[non_exhaustive]
  pub enum Request<I, A, D> {
    AppendEntries(AppendEntriesRequest<I, A, D>) = 0 => append_entries,
    Vote(VoteRequest<I, A>) = 1 => vote,
    InstallSnapshot(InstallSnapshotRequest<I, A>) = 2 => install_snapshot,
    TimeoutNow(TimeoutNowRequest<I, A>) = 3 => timeout_now,
    Heartbeat(HeartbeatRequest<I, A>) = 4 => heartbeat,
  }
);

impl<I: core::hash::Hash + Eq, A: PartialEq, D: PartialEq> PartialEq for Request<I, A, D> {
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

impl<I: core::hash::Hash + Eq, A: Eq, D: Eq> Eq for Request<I, A, D> {}
