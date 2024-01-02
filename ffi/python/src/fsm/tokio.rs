use std::{borrow::Cow, sync::Arc};

use crate::error::IoError;

use super::*;
use agnostic::tokio::TokioRuntime;
use pyo3::{exceptions::PyIOError, types::PyString};
use smol_str::SmolStr;

#[pyclass]
pub struct FinateStateMachineSnapshot(Py<PyAny>);

impl RFinateStateMachineSnapshot for FinateStateMachineSnapshot {
  type Error = IoError;

  type Runtime = TokioRuntime;

  async fn persist(&self, sink: impl SnapshotSink) -> Result<(), Self::Error> {
    todo!()
  }

  async fn release(self) -> Result<(), Self::Error> {
    todo!()
  }
}

#[derive(Debug, Clone)]
enum FinateStateMachineErrorKind {
  Snapshot(IoError),
  Custom(SmolStr),
}

impl core::fmt::Display for FinateStateMachineErrorKind {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::Snapshot(err) => write!(f, "snapshot error: {err}"),
      Self::Custom(msg) => write!(f, "{msg}"),
    }
  }
}

#[derive(Debug, Clone)]
#[pyclass]
pub struct FinateStateMachineError {
  kind: FinateStateMachineErrorKind,
  msg: smallvec::SmallVec<[Cow<'static, str>; 4]>,
}

impl RFinateStateMachineError for FinateStateMachineError {
  type Snapshot = FinateStateMachineSnapshot;

  fn snapshot(err: <Self::Snapshot as RFinateStateMachineSnapshot>::Error) -> Self {
    Self {
      kind: FinateStateMachineErrorKind::Snapshot(err),
      msg: smallvec::SmallVec::new(),
    }
  }

  fn with_message(mut self, msg: Cow<'static, str>) -> Self {
    self.msg.push(msg);
    self
  }
}

impl core::fmt::Display for FinateStateMachineError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.kind)?;
    for (idx, msg) in self.msg.iter().enumerate() {
      write!(f, "\t{idx}: {}", msg)?;
    }
    Ok(())
  }
}

impl std::error::Error for FinateStateMachineError {}

#[pymethods]
impl FinateStateMachineError {
  #[staticmethod]
  fn snapshot(err: IoError) -> PyResult<Self> {
    Ok(Self {
      kind: FinateStateMachineErrorKind::Snapshot(err),
      msg: smallvec::SmallVec::new(),
    })
  }

  #[getter]
  fn messages(&self) -> smallvec::SmallVec<[Cow<'static, str>; 4]> {
    self.msg.clone()
  }

  fn with_message(&mut self, msg: &PyString) -> PyResult<()> {
    msg.extract::<String>().map(|s| self.msg.push(s.into()))
  }
}

#[pyclass]
pub struct FinateStateMachine {
  model: Py<PyAny>,
}

impl RFinateStateMachine for FinateStateMachine {
  /// Errors returned by the finate state machine.
  type Error = FinateStateMachineError;

  /// The snapshot type used by the finate state machine.
  type Snapshot = FinateStateMachineSnapshot;

  /// The response type returned by the finate state machine after apply.
  type Response = FinateStateMachineResponse;

  /// The id type used to identify nodes.
  type Id = NodeId;

  /// The address type of node.
  type Address = NodeAddress;

  /// The log entry's type-specific data, which will be applied to a user [`FinateStateMachine`].
  type Data = Bytes;

  /// The async runtime used by the finate state machine.
  type Runtime = agnostic::tokio::TokioRuntime;

  /// Invoked once a log entry is committed by a majority of the cluster.
  ///
  /// Apply should apply the log to the FSM. Apply must be deterministic and
  /// produce the same result on all peers in the cluster.
  async fn apply(
    &self,
    log: CommittedLog<Self::Id, Self::Address, Self::Data>,
  ) -> Result<Self::Response, Self::Error> {
    todo!()
  }

  /// Invoked once a batch of log entries has been committed and
  /// are ready to be applied to the FSM. `apply_batch` will take in an array of
  /// log entries. These log entries will be in the order they were committed,
  /// will not have gaps, and could be of a few log types.
  ///
  /// The returned slice must be the same length as the input and each response
  /// should correlate to the log at the same index of the input. The returned
  /// values will be made available in the ApplyFuture returned by Raft.Apply
  /// method if that method was called on the same Raft node as the FSM.
  async fn apply_batch(
    &self,
    logs: CommittedLogBatch<Self::Id, Self::Address, Self::Data>,
  ) -> Result<Vec<Self::Response>, Self::Error> {
    todo!()
  }

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
  async fn snapshot(&self) -> Result<Self::Snapshot, Self::Error> {
    todo!()
  }

  /// Used to restore an FSM from a snapshot. It is not called
  /// concurrently with any other command. The FSM must discard all previous
  /// state before restoring the snapshot.
  async fn restore(&self, snapshot: impl AsyncRead + Send + Unpin) -> Result<(), Self::Error> {
    todo!()
  }
}
