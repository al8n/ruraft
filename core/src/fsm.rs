use std::{borrow::Cow, future::Future};

// use futures::AsyncRead;
use agnostic_lite::RuntimeLite;
use nodecraft::{Address, Id};
use smallvec::SmallVec;

use crate::{
  storage::{CommittedLog, CommittedLogBatch, SnapshotSink},
  Data,
};

/// Represents a snapshot of the finate state machine.
#[auto_impl::auto_impl(Box)]
pub trait FinateStateMachineSnapshot: Send + Sync + 'static {
  /// Errors returned by the finate state machine snapshot.
  type Error: std::error::Error + Send + Sync + 'static;

  /// The async runtime used by the finate state machine snapshot.
  type Runtime: RuntimeLite;

  /// Persist should write the FSM snapshot to the given sink.
  ///
  /// **Note:**
  ///
  /// - [`SnapshotSink::cancel`](crate::storage::SnapshotSink::cancel) should be invoked on failure.
  /// - Whether the implementation return `Ok` or `Err`, at the end of the fn,
  /// [`close`](futures::AsyncWriteExt::close) on `sink` should be invoked.
  fn persist(
    &self,
    sink: impl SnapshotSink + 'static,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Release is invoked when we are finished with the snapshot.
  fn release(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Represents a comprehensive set of errors arising from operations within the [`FinateStateMachine`] trait.
///
/// This trait encapsulates a range of error types, providing a structured approach to categorizing
/// and handling fsm-level errors. Implementers can leverage this to define both generic and
/// fsm-specific error scenarios.
pub trait FinateStateMachineError: Clone + std::error::Error + Send + Sync + 'static {
  /// The snapshot of the finate state machine
  type Snapshot: FinateStateMachineSnapshot;

  /// Constructs an error associated with snapshot storage operations.
  fn snapshot(err: <Self::Snapshot as FinateStateMachineSnapshot>::Error) -> Self;

  /// With extra message to explain the error.
  fn with_message(self, msg: Cow<'static, str>) -> Self;
}

/// Response returned when the new log entries applied to the state machine
pub trait FinateStateMachineResponse: Send + Sync + 'static {
  /// Returns the index of the newly applied log entry.
  fn index(&self) -> u64;
}

const INLINED_RESPONSE: usize = 2;

/// Response returned by [`FinateStateMachine::apply_batch`].
#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct ApplyBatchResponse<R>(SmallVec<[R; INLINED_RESPONSE]>);

impl<R> Default for ApplyBatchResponse<R> {
  fn default() -> Self {
    Self::new()
  }
}

impl<R> ApplyBatchResponse<R> {
  /// Creates a new instance of `ApplyBatchResponse`.
  pub fn new() -> Self {
    Self(SmallVec::new())
  }

  /// Creates a new instance of `ApplyBatchResponse` with the given capacity.
  pub fn with_capacity(capacity: usize) -> Self {
    Self(SmallVec::with_capacity(capacity))
  }
}

impl<R> From<SmallVec<[R; INLINED_RESPONSE]>> for ApplyBatchResponse<R> {
  fn from(value: SmallVec<[R; INLINED_RESPONSE]>) -> Self {
    Self(value)
  }
}

impl<R> core::ops::Deref for ApplyBatchResponse<R> {
  type Target = SmallVec<[R; INLINED_RESPONSE]>;

  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

impl<R> core::ops::DerefMut for ApplyBatchResponse<R> {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.0
  }
}

impl<R> FromIterator<R> for ApplyBatchResponse<R> {
  fn from_iter<T: IntoIterator<Item = R>>(iter: T) -> Self {
    Self(iter.into_iter().collect())
  }
}

impl<R> IntoIterator for ApplyBatchResponse<R> {
  type Item = R;
  type IntoIter = smallvec::IntoIter<[R; INLINED_RESPONSE]>;

  fn into_iter(self) -> Self::IntoIter {
    self.0.into_iter()
  }
}

/// Implemented by clients to make use of the replicated log.
#[auto_impl::auto_impl(Box, Arc)]
pub trait FinateStateMachine: Send + Sync + 'static {
  /// Errors returned by the finate state machine.
  type Error: FinateStateMachineError<Snapshot = Self::Snapshot>;

  /// The snapshot type used by the finate state machine.
  type Snapshot: FinateStateMachineSnapshot<Runtime = Self::Runtime>;

  /// The response type returned by the finate state machine after apply.
  type Response: FinateStateMachineResponse;

  /// The id type used to identify nodes.
  type Id: Id;

  /// The address type of node.
  type Address: Address;

  /// The log entry's type-specific data, which will be applied to a user [`FinateStateMachine`].
  type Data: Data;

  /// The async runtime used by the finate state machine.
  type Runtime: RuntimeLite;

  /// Invoked once a log entry is committed by a majority of the cluster.
  ///
  /// Apply should apply the log to the FSM. Apply must be deterministic and
  /// produce the same result on all peers in the cluster.
  fn apply(
    &self,
    log: CommittedLog<Self::Id, Self::Address, Self::Data>,
  ) -> impl Future<Output = Result<Self::Response, Self::Error>> + Send;

  /// Invoked once a batch of log entries has been committed and
  /// are ready to be applied to the FSM. `apply_batch` will take in an array of
  /// log entries. These log entries will be in the order they were committed,
  /// will not have gaps, and could be of a few log types.
  ///
  /// The returned slice must be the same length as the input and each response
  /// should correlate to the log at the same index of the input. The returned
  /// values will be made available in the ApplyFuture returned by Raft.Apply
  /// method if that method was called on the same Raft node as the FSM.
  fn apply_batch(
    &self,
    logs: CommittedLogBatch<Self::Id, Self::Address, Self::Data>,
  ) -> impl Future<Output = Result<ApplyBatchResponse<Self::Response>, Self::Error>> + Send;

  /// Snapshot returns an FSMSnapshot used to: support log compaction, to
  /// restore the FSM to a previous state, or to bring out-of-date followers up
  /// to a recent log index.
  ///
  /// The Snapshot implementation should return quickly, because Apply can not
  /// be called while Snapshot is running. Generally this means Snapshot should
  /// only capture a pointer to the state, and any expensive IO should happen
  /// as part of FSMSnapshot.Persist.
  ///
  /// Apply and Snapshot are always called from the same thread, but Apply will
  /// be called concurrently with FSMSnapshot.Persist. This means the FSM should
  /// be implemented to allow for concurrent updates while a snapshot is happening.
  fn snapshot(&self) -> impl Future<Output = Result<Self::Snapshot, Self::Error>> + Send;

  /// Used to restore an FSM from a snapshot. It is not called
  /// concurrently with any other command. The FSM must discard all previous
  /// state before restoring the snapshot.
  fn restore(
    &self,
    snapshot: impl futures::AsyncRead + Unpin + Send + Sync + 'static,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
