use crate::storage::SnapshotSink;

#[async_trait::async_trait]
pub trait FinateStateMachineSnapshot {
  /// Errors returned by the finate state machine snapshot.
  type Error: std::error::Error;

  type Sink: SnapshotSink;

  /// Persist should write the FSM snapshot to the given sink.
  async fn persist(&self, sink: &Self::Sink) -> Result<(), Self::Error>;

  /// Release is invoked when we are finished with the snapshot.
  async fn release(&self) -> Result<(), Self::Error>;
}

/// Implemented by clients to make use of the replicated log.
#[async_trait::async_trait]
pub trait FinateStateMachine: Send + Sync + 'static {
  /// Errors returned by the finate state machine.
  type Error: std::error::Error;
  type Snapshot: FinateStateMachineSnapshot;

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
  async fn snapshot(&self) -> Result<(), Self::Error>;
}
