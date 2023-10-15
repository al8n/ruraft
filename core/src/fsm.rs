use std::future::Future;

use crate::storage::SnapshotSink;

pub trait FinateStateMachineSnapshot {
  /// Errors returned by the finate state machine snapshot.
  type Error: std::error::Error;

  /// The sink type used by the finate state machine snapshot.
  type Sink: SnapshotSink;

  /// The async runtime used by the finate state machine snapshot.
  type Runtime: agnostic::Runtime;

  /// Persist should write the FSM snapshot to the given sink.
  fn persist(&self, sink: &Self::Sink) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Release is invoked when we are finished with the snapshot.
  fn release(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Implemented by clients to make use of the replicated log.
pub trait FinateStateMachine: Send + Sync + 'static {
  /// Errors returned by the finate state machine.
  type Error: std::error::Error;
  /// The snapshot type used by the finate state machine.
  type Snapshot: FinateStateMachineSnapshot<Runtime = Self::Runtime>;
  /// The async runtime used by the finate state machine.
  type Runtime: agnostic::Runtime;

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
  fn snapshot(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
