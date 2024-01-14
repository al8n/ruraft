use nodecraft::resolver::AddressResolver;

use crate::{
  fsm::FinateStateMachine,
  membership::MembershipError,
  options::OptionsError,
  storage::{LogStorage, SnapshotStorage, StableStorage, Storage, StorageError},
  transport::Transport,
  Node,
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
  CantTakeSnapshot {
    /// The committed index.
    committed: u64,
    /// The last snapshot index.
    snapshot: u64,
  },

  /// Returned when trying to create a snapshot, but the membership change has not been applied.
  #[error("ruraft: cannot restore snapshot now, wait until the membership entry at {latest} has been applied (have applied {committed})")]
  CantRestoreSnapshot {
    /// The committed index.
    committed: u64,
    /// The last snapshot index.
    latest: u64,
  },

  /// Returned when an operation is attempted
  /// that's not supported by the current protocol version.
  #[error("ruraft: operation not supported with current protocol version")]
  UpsupportedProtocol,

  /// Returned when an operation is attempted to send/receive from closed channel
  #[error("ruraft: {0}")]
  Closed(&'static str),

  /// Returned when the leader is rejecting
  /// client requests because it is attempting to transfer leadership.
  #[error("ruraft: leadership transfer in progress")]
  LeadershipTransferInProgress,

  /// Returned when the leader transfer times out.
  #[error("ruraft: leadership transfer timeout")]
  LeadershipTransferTimeout,

  /// Returned when the leadership was lost caused by leadership transfer.
  #[error("ruraft: lost leadership during transfer (expected)")]
  LeadershipLostDuringTransfer,

  /// Returned when we cannot find a target node to transfer leadership to.
  #[error("ruraft: cannot find a target node to transfer leadership to")]
  LeadershipTransferNoTarget,

  /// Returned when we are transfering the leadership to a target, but the target exits.
  #[error("ruraft: leadership transfer target exits")]
  LeadershipTransferTargetExits,

  /// Returned when failing to load current term.
  #[error("ruraft: failed to load current term")]
  FailedLoadCurrentTerm,

  /// Returned when failing to load log index.
  #[error("ruraft: failed to load first log index")]
  FailedLoadFirstIndex,

  /// Returned when failing to load last log entry.
  #[error("ruraft: failed to load last log index")]
  FailedLoadLastIndex,

  /// Returned when there is invalid membership.
  #[error("ruraft: {0}")]
  Membership(#[from] MembershipError<T::Id, <T::Resolver as AddressResolver>::Address>),

  /// Returned when the operation is canceled because of the other sender half of the channel is closed, e.g. apply, barrier and etc.
  #[error("ruraft: operation canceled, the sender half of the channel is closed")]
  Canceled,

  /// Returned when the leader transfer to self.
  #[error("ruraft: leader transfer to self")]
  TransferToSelf,

  /// Returned when trying to recover a raft cluster but cannot find any existing state.
  #[error(
    "ruraft: refused to recover cluster with no initial state, this is probably an operator error"
  )]
  NoExistingState,

  /// Indicates a given log entry is not available.
  #[error("ruraft: log(index = {0}) not found")]
  LogNotFound(u64),

  /// Returned by the transport to
  /// signal that pipeline replication is not supported in general, and that
  /// no error message should be produced.
  #[error("ruraft: pipeline replication not supported")]
  PipelineReplicationNotSupported,

  /// Returned when there are no snapshots in the storage.
  #[error("ruraft: no snapshots found")]
  NoSnapshots,

  /// Returned when failing to replicate.
  #[error("ruraft: replication failed")]
  ReplicationFailed,

  /// Returned when failing to find replication state.
  #[error("ruraft: cannot find replication state for node {0}")]
  NoReplicationState(Node<T::Id, <T::Resolver as AddressResolver>::Address>),

  /// Returned when trying to recover a raft cluster but cannot restore any of the available snapshots.
  #[error("ruraft: failed to restore any of the available snapshots")]
  FailedRestoreSnapshots,
}

impl<T: Transport> core::fmt::Debug for RaftError<T> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    core::fmt::Display::fmt(self, f)
  }
}

/// Errors implementation for the Raft.
#[derive(thiserror::Error)]
pub enum Error<F, S, T>
where
  F: FinateStateMachine,
  S: Storage,
  T: Transport,
{
  /// Raft errors.
  #[error("ruraft: {0}")]
  Raft(#[from] RaftError<T>),

  /// Returned when the options for the `Raft` are invalid.
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

impl<F, S, T> core::fmt::Debug for Error<F, S, T>
where
  F: FinateStateMachine,
  S: Storage,
  T: Transport,
{
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    core::fmt::Display::fmt(self, f)
  }
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

  #[inline]
  pub(crate) const fn log_not_found(index: u64) -> Self {
    Self::Raft(RaftError::LogNotFound(index))
  }

  #[inline]
  pub(crate) const fn no_snapshots() -> Self {
    Self::Raft(RaftError::NoSnapshots)
  }

  #[inline]
  pub(crate) const fn replication_failed() -> Self {
    Self::Raft(RaftError::ReplicationFailed)
  }

  #[inline]
  pub(crate) const fn leadership_transfer_timeout() -> Self {
    Self::Raft(RaftError::LeadershipTransferTimeout)
  }

  #[inline]
  pub(crate) const fn leadership_transfer_in_progress() -> Self {
    Self::Raft(RaftError::LeadershipTransferInProgress)
  }

  #[inline]
  pub(crate) const fn leadership_lost_during_transfer() -> Self {
    Self::Raft(RaftError::LeadershipLostDuringTransfer)
  }

  #[inline]
  pub(crate) const fn leadership_transfer_no_target() -> Self {
    Self::Raft(RaftError::LeadershipTransferNoTarget)
  }

  #[inline]
  pub(crate) const fn leadership_transfer_target_exits() -> Self {
    Self::Raft(RaftError::LeadershipTransferTargetExits)
  }

  #[inline]
  pub(crate) const fn no_replication_state(
    node: Node<T::Id, <T::Resolver as AddressResolver>::Address>,
  ) -> Self {
    Self::Raft(RaftError::NoReplicationState(node))
  }

  #[inline]
  pub(crate) const fn cannot_restore_snapshot(committed: u64, latest: u64) -> Self {
    Self::Raft(RaftError::CantRestoreSnapshot { committed, latest })
  }

  #[inline]
  pub(crate) const fn aborted_by_restore() -> Self {
    Self::Raft(RaftError::AbortedByRestore)
  }

  #[inline]
  pub(crate) const fn shutdown() -> Self {
    Self::Raft(RaftError::Shutdown)
  }

  #[inline]
  pub(crate) const fn enqueue_timeout() -> Self {
    Self::Raft(RaftError::EnqueueTimeout)
  }

  #[inline]
  pub(crate) const fn nothing_new_to_snapshot() -> Self {
    Self::Raft(RaftError::NothingNewToSnapshot)
  }

  #[inline]
  pub(crate) const fn cant_take_snapshot(committed: u64, snapshot: u64) -> Self {
    Self::Raft(RaftError::CantTakeSnapshot {
      committed,
      snapshot,
    })
  }

  #[inline]
  pub(crate) const fn closed(what: &'static str) -> Self {
    Self::Raft(RaftError::Closed(what))
  }

  #[inline]
  pub(crate) const fn transfer_to_self() -> Self {
    Self::Raft(RaftError::TransferToSelf)
  }

  #[inline]
  pub(crate) const fn no_existing_state() -> Self {
    Self::Raft(RaftError::NoExistingState)
  }

  #[inline]
  pub(crate) const fn failed_load_current_term() -> Self {
    Self::Raft(RaftError::FailedLoadCurrentTerm)
  }

  #[inline]
  pub(crate) const fn failed_load_first_index() -> Self {
    Self::Raft(RaftError::FailedLoadFirstIndex)
  }

  #[inline]
  pub(crate) const fn failed_load_last_index() -> Self {
    Self::Raft(RaftError::FailedLoadLastIndex)
  }

  #[inline]
  pub(crate) const fn failed_restore_snapshots() -> Self {
    Self::Raft(RaftError::FailedRestoreSnapshots)
  }

  #[inline]
  pub(crate) const fn canceled() -> Self {
    Self::Raft(RaftError::Canceled)
  }

  #[inline]
  pub(crate) const fn not_leader() -> Self {
    Self::Raft(RaftError::NotLeader)
  }

  #[inline]
  pub(crate) const fn leadership_lost() -> Self {
    Self::Raft(RaftError::LeadershipLost)
  }
}
