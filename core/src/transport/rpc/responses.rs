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
    /// Append entries response.
    AppendEntries(AppendEntriesResponse<I, A>) = 0,
    /// Vote response.
    Vote(VoteResponse<I, A>) = 1,
    /// Install snapshot response.
    InstallSnapshot(InstallSnapshotResponse<I, A>) = 2,
    /// Timeout now response.
    TimeoutNow(TimeoutNowResponse<I, A>) = 3,
    /// Heartbeat response.
    Heartbeat(HeartbeatResponse<I, A>) = 4,
    /// Error response.
    Error(ErrorResponse<I, A>) = 255,
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
