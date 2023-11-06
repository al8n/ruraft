use std::{borrow::Cow, future::Future, sync::Arc};

use futures::AsyncRead;
use nodecraft::{Address, Id, CheapClone};

use crate::{membership::Membership, storage::SnapshotSink, Data};

pub trait FinateStateMachineSnapshot: Send + Sync + 'static {
  /// Errors returned by the finate state machine snapshot.
  type Error: std::error::Error;

  /// The sink type used by the finate state machine snapshot.
  type Sink: SnapshotSink;

  /// The async runtime used by the finate state machine snapshot.
  type Runtime: agnostic::Runtime;

  /// Persist should write the FSM snapshot to the given sink.
  fn persist(&self, sink: Self::Sink) -> impl Future<Output = Result<(), Self::Error>> + Send;

  // /// Release is invoked when we are finished with the snapshot.
  // fn release(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Represents a comprehensive set of errors arising from operations within the [`FinateStateMachine`] trait.
///
/// This trait encapsulates a range of error types, providing a structured approach to categorizing
/// and handling fsm-level errors. Implementers can leverage this to define both generic and
/// fsm-specific error scenarios.
pub trait FinateStateMachineError: std::error::Error + Send + Sync + 'static {
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

pub enum FinateStateMachineLogKind<I, A, D> {
  Log(Arc<D>),
  Membership(Membership<I, A>),
}

pub struct FinateStateMachineLog<I, A, D> {
  pub index: u64,
  pub term: u64,
  pub kind: FinateStateMachineLogKind<I, A, D>,
}

impl<I, A, D> FinateStateMachineLog<I, A, D> {
  pub fn new(term: u64, index: u64, kind: FinateStateMachineLogKind<I, A, D>) -> Self {
    Self { index, term, kind }
  }
}

/// Implemented by clients to make use of the replicated log.
pub trait FinateStateMachine: Send + Sync + 'static {
  /// Errors returned by the finate state machine.
  type Error: FinateStateMachineError<Snapshot = Self::Snapshot>;

  /// The snapshot type used by the finate state machine.
  type Snapshot: FinateStateMachineSnapshot<Sink = Self::SnapshotSink, Runtime = Self::Runtime>;

  /// The sink type used by the finate state machine snapshot.
  type SnapshotSink: SnapshotSink;

  /// The response type returned by the finate state machine after apply.
  type Response: FinateStateMachineResponse;

  /// The id type used to identify nodes.
  type Id: Id + CheapClone + Send + Sync + 'static;

  /// The address type of node.
  type Address: Address + CheapClone + Send + Sync + 'static;

  /// The log entry's type-specific data, which will be applied to a user [`FinateStateMachine`].
  type Data: Data;

  /// The async runtime used by the finate state machine.
  type Runtime: agnostic::Runtime;

  /// Invoked once a log entry is committed by a majority of the cluster.
  ///
  /// Apply should apply the log to the FSM. Apply must be deterministic and
  /// produce the same result on all peers in the cluster.
  fn apply(
    &self,
    log: FinateStateMachineLog<Self::Id, Self::Address, Self::Data>,
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
    logs: impl IntoIterator<Item = FinateStateMachineLog<Self::Id, Self::Address, Self::Data>>,
  ) -> impl Future<Output = Result<Vec<Self::Response>, Self::Error>> + Send;

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
    snapshot: impl AsyncRead + Unpin,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
