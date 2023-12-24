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

mod error;
pub use error::*;

enum_wrapper!(
  /// Response from the Raft node
  #[derive(Debug, Clone)]
  #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
  #[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
  #[non_exhaustive]
  pub enum Response<I, A> {
    AppendEntries(AppendEntriesResponse<I, A>) = 0 => append_entries,
    Vote(VoteResponse<I, A>) = 1 => vote,
    InstallSnapshot(InstallSnapshotResponse<I, A>) = 2 => install_snapshot,
    TimeoutNow(TimeoutNowResponse<I, A>) = 3 => timeout_now,
    Heartbeat(HeartbeatResponse<I, A>) = 4 => heartbeat,
    Error(ErrorResponse<I, A>) = 255 => error,
  }
);

impl<I: core::hash::Hash + Eq, A: PartialEq> PartialEq for Response<I, A> {
  fn eq(&self, other: &Self) -> bool {
    match (self, other) {
      (Self::AppendEntries(a), Self::AppendEntries(b)) => a == b,
      (Self::Vote(a), Self::Vote(b)) => a == b,
      (Self::InstallSnapshot(a), Self::InstallSnapshot(b)) => a == b,
      (Self::TimeoutNow(a), Self::TimeoutNow(b)) => a == b,
      (Self::Heartbeat(a), Self::Heartbeat(b)) => a == b,
      (Self::Error(a), Self::Error(b)) => a == b,
      _ => false,
    }
  }
}

impl<I: core::hash::Hash + Eq, A: Eq> Eq for Response<I, A> {}
