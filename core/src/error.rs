use nodecraft::resolver::AddressResolver;

use crate::{
  fsm::FinateStateMachine,
  membership::MembershipError,
  options::OptionsError,
  storage::{LogStorage, SnapshotStorage, StableStorage, Storage, StorageError},
  transport::Transport,
};

/// Raft errors.
#[derive(thiserror::Error)]
pub enum RaftError<T: Transport> {
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

  /// Returned when trying to create a snapshot, but the membership change has not been applied.
  #[error("ruraft: cannot take snapshot now, wait until the membership entry at {committed} has been applied (have applied {snapshot})")]
  CantTakeSnapshot { committed: u64, snapshot: u64 },

  /// Returned when an operation is attempted
  /// that's not supported by the current protocol version.
  #[error("ruraft: operation not supported with current protocol version")]
  UpsupportedProtocol,

  /// Returned when an operation is attempted to send/receive from closed channel
  #[error("ruraft: {0}")]
  Closed(&'static str),

  /// Returned when long running task exits unexpectedly.
  #[error("ruraft: {0}")]
  Exit(&'static str),

  /// Returned when the leader is rejecting
  /// client requests because it is attempting to transfer leadership.
  #[error("ruraft: leadership transfer in progress")]
  LeadershipTransferInProgress,

  /// Returned when there are some snapshots in the storage,
  /// but none of them can be loaded.
  #[error("ruraft: failed to load any existing snapshots")]
  FailedLoadSnapshot,

  /// Returned when failing to load current term.
  #[error("ruraft: failed to load current term")]
  FailedLoadCurrentTerm,

  /// Returned when failing to load last log index.
  #[error("ruraft: failed to load last log index")]
  FailedLoadLastLogIndex,

  /// Returned when failing to load last log entry.
  #[error("ruraft: failed to load last log")]
  FailedLoadLastLog,

  /// Returned when there is invalid membership.
  #[error("ruraft: {0}")]
  Membership(#[from] MembershipError<T::Id, <T::Resolver as AddressResolver>::Address>),

  /// Returned when the operation is canceled because of the other sender half of the channel is closed, e.g. apply, barrier and etc.
  #[error("ruraft: operation canceled, the sender half of the channel is closed")]
  Canceled,

  /// Returned when the leader transfer to self.
  #[error("ruraft: leader transfer to self")]
  TransferToSelf,
}

impl<T: Transport> core::fmt::Debug for RaftError<T> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    core::fmt::Display::fmt(self, f)
  }
}

/// Errors implementation for the Raft.
#[derive(Debug, thiserror::Error)]
pub enum Error<F, S, T>
where
  F: FinateStateMachine,
  S: Storage,
  T: Transport,
{
  /// Raft errors.
  #[error("ruraft: {0}")]
  Raft(#[from] RaftError<T>),

  #[error("ruraft: invalid options: {0}")]
  InvalidOptions(#[from] OptionsError),

  /// Returned when the finate state machine reports an error.
  #[error("ruraft: {0}")]
  FinateStateMachine(F::Error),

  /// Returned when the transport reports an error.
  #[error("ruraft: {0}")]
  Transport(T::Error),

  /// Returned when the storage reports an error.
  #[error("ruraft: {0}")]
  Storage(S::Error),
}

impl<F, S, T> Error<F, S, T>
where
  F: FinateStateMachine,
  S: Storage,
  T: Transport,
{
  /// Construct an error from the transport error.
  #[inline]
  pub const fn transport(err: T::Error) -> Self {
    Self::Transport(err)
  }

  /// Construct an error from the storage error.
  #[inline]
  pub const fn storage(err: S::Error) -> Self {
    Self::Storage(err)
  }

  /// Construct an error from the stable storage error.
  #[inline]
  pub fn stable(err: <S::Stable as StableStorage>::Error) -> Self {
    Self::Storage(<S::Error as StorageError>::stable(err))
  }

  /// Construct an error from the snapshot storage error.
  #[inline]
  pub fn snapshot(err: <S::Snapshot as SnapshotStorage>::Error) -> Self {
    Self::Storage(<S::Error as StorageError>::snapshot(err))
  }

  /// Construct an error from the log storage error.
  #[inline]
  pub fn log(err: <S::Log as LogStorage>::Error) -> Self {
    Self::Storage(<S::Error as StorageError>::log(err))
  }

  /// Construct an error from the finate state machine error.
  #[inline]
  pub const fn fsm(err: F::Error) -> Self {
    Self::FinateStateMachine(err)
  }

  /// Construct an error from the membership error.
  #[inline]
  pub const fn membership(
    err: MembershipError<T::Id, <T::Resolver as AddressResolver>::Address>,
  ) -> Self {
    Self::Raft(RaftError::Membership(err))
  }
}
