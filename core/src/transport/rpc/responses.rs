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
