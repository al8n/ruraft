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
  #[non_exhaustive]
  pub enum Request<I, A, D> {
    AppendEntries(AppendEntriesRequest<I, A, D>) = 0 => append_entries,
    Vote(VoteRequest<I, A>) = 1 => vote,
    InstallSnapshot(InstallSnapshotRequest<I, A>) = 2 => install_snapshot,
    TimeoutNow(TimeoutNowRequest<I, A>) = 3 => timeout_now,
    Heartbeat(HeartbeatRequest<I, A>) = 4 => heartbeat,
  }
);
