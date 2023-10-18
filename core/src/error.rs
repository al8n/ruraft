use crate::{
  fsm::FinateStateMachine,
  options::OptionsError,
  storage::{LogStorage, SnapshotStorage, StableStorage, StorageError},
  transport::TransportError,
};

/// Raft errors.
#[derive(Debug, thiserror::Error)]
pub enum RaftError {
  /// Returned when an operation can't be completed on a
  /// leader node.
  #[error("ruraft: node is the leader")]
  Leader,

  /// Returned when an operation can't be completed on a
  /// follower or candidate node.
  #[error("ruraft: node is not the leader")]
  NotLeader,

  /// Returned when an operation can't be completed on a
  /// follower or candidate node.
  #[error("ruraft: node is not a voter")]
  NotVoter,

  /// Returned when a leader fails to commit a log entry
  /// because it's been deposed in the process.
  #[error("ruraft: leadership lost while committing log")]
  LeadershipLost,

  /// Returned when a leader fails to commit a log
  /// entry because it's been superseded by a user snapshot restore.
  #[error("ruraft: snapshot restored while committing log")]
  AbortedByRestore,

  /// Returned when operations are requested against an
  /// inactive Raft.
  #[error("ruraft: raft is already shutdown")]
  Shutdown,

  /// Returned when a command fails due to a timeout.
  #[error("ruraft: timed out enqueuing operation")]
  EnqueueTimeout,

  /// Returned when trying to create a snapshot
  /// but there's nothing new commited to the [`FinateStateMachine`] since we started.
  #[error("ruraft: nothing new to snapshot")]
  NothingNewToSnapshot,

  /// Returned when an operation is attempted
  /// that's not supported by the current protocol version.
  #[error("ruraft: operation not supported with current protocol version")]
  UpsupportedProtocol,

  /// Returned when attempt is made to bootstrap a
  /// cluster that already has state present.
  #[error("ruraft: bootstrap only works on new clusters")]
  CantBootstrap,

  /// Returned when the leader is rejecting
  /// client requests because it is attempting to transfer leadership.
  #[error("ruraft: leadership transfer in progress")]
  LeadershipTransferInProgress,
}

/// Errors implementation for the Raft.
#[derive(Debug, thiserror::Error)]
pub enum Error<F, S, T>
where
  F: FinateStateMachine,
  S: StorageError,
  T: TransportError,
{
  /// Raft errors.
  #[error("ruraft: {0}")]
  Raft(#[from] RaftError),

  #[error("ruraft: invalid options: {0}")]
  InvalidOptions(#[from] OptionsError),

  /// Returned when the finate state machine reports an error.
  #[error("ruraft: {0}")]
  FinateStateMachine(F::Error),

  /// Returned when the transport reports an error.
  #[error("ruraft: {0}")]
  Transport(T),

  /// Returned when the storage reports an error.
  #[error("ruraft: {0}")]
  Storage(S),
}

impl<F, S, T> Error<F, S, T>
where
  F: FinateStateMachine,
  S: StorageError,
  T: TransportError,
{
  /// Construct an error from the transport error.
  #[inline]
  pub const fn transport(err: T) -> Self {
    Self::Transport(err)
  }

  /// Construct an error from the storage error.
  #[inline]
  pub const fn storage(err: S) -> Self {
    Self::Storage(err)
  }

  /// Construct an error from the stable storage error.
  #[inline]
  pub const fn stable(err: <S::Stable as StableStorage>::Error) -> Self {
    Self::Storage(S::stable(err))
  }

  /// Construct an error from the snapshot storage error.
  #[inline]
  pub const fn snapshot(err: <S::Snapshot as SnapshotStorage>::Error) -> Self {
    Self::Storage(S::snapshot(err))
  }

  /// Construct an error from the log storage error.
  #[inline]
  pub const fn log(err: <S::Log as LogStorage>::Error) -> Self {
    Self::Storage(S::log(err))
  }

  /// Construct an error from the finate state machine error.
  #[inline]
  pub const fn fsm(err: F::Error) -> Self {
    Self::FinateStateMachine(err)
  }
}
