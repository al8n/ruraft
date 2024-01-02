use std::{borrow::Cow, sync::Arc};

use crate::{RaftData, storage::snapshot::async_std::FileSnapshotSource};

use super::*;
use crate::storage::snapshot::tokio::FileSnapshotSink;
use agnostic::tokio::TokioRuntime;
use async_lock::Mutex;
use futures::AsyncWriteExt;
use pyo3::types::PyString;
use pyo3_asyncio::tokio::into_future;
use ruraft_core::{ApplyBatchResponse, storage::SnapshotSource};
use smallvec::SmallVec;
use smol_str::SmolStr;

#[derive(Clone)]
#[pyclass]
pub struct FinateStateMachineSnapshot(Arc<Py<PyAny>>);

impl RFinateStateMachineSnapshot for FinateStateMachineSnapshot {
  type Error = FinateStateMachineSnapshotError;
  type Runtime = TokioRuntime;

  async fn persist(
    &self,
    sink: impl SnapshotSink<Runtime = Self::Runtime>,
  ) -> Result<(), Self::Error> {
    let id = sink.id();
    let sink = Arc::new(Mutex::new(sink));
    let snap = FileSnapshotSink::new(id.into(), sink.clone());
    let res = Python::with_gil(|py| {
      let arg = (snap.into_py(py),);
      into_future(self.0.as_ref().as_ref(py).call_method1("persist", arg)?)
    });
    match res {
      Ok(fut) => {
        let mut sink = sink.lock().await;
        match fut.await {
          Ok(_) => {
            if let Err(e) = sink.close().await {
              tracing::error!(target = "ruraft.python.fsm.persist", err=%e, "failed to close snapshot sink.");
            }
            Ok(())
          }
          Err(err) => {
            if let Err(e) = sink.cancel().await {
              tracing::error!(target = "ruraft.python.fsm.persist", err=%e, "failed to cancel snapshot sink.");
            }

            if let Err(e) = sink.close().await {
              tracing::error!(target = "ruraft.python.fsm.persist", err=%e, "failed to close snapshot sink.");
            }

            Err(err.into())
          }
        }
      }
      Err(err) => {
        let mut sink = sink.lock().await;
        if let Err(e) = sink.cancel().await {
          tracing::error!(target = "ruraft.python.fsm.persist", err=%e, "failed to cancel snapshot sink.");
        }

        if let Err(e) = sink.close().await {
          tracing::error!(target = "ruraft.python.fsm.persist", err=%e, "failed to close snapshot sink.");
        }

        Err(err.into())
      }
    }
  }

  async fn release(&mut self) -> Result<(), Self::Error> {
    Python::with_gil(|py| into_future(self.0.as_ref().as_ref(py).call_method0("release")?))?
      .await
      .map(|_| ())
      .map_err(Into::into)
  }
}

#[derive(Debug, Clone)]
enum FinateStateMachineErrorKind {
  Snapshot(FinateStateMachineSnapshotError),
  Fsm(Arc<PyErr>),
  Custom(SmolStr),
}

impl core::fmt::Display for FinateStateMachineErrorKind {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::Snapshot(err) => write!(f, "snapshot error: {err}"),
      Self::Custom(msg) => write!(f, "{msg}"),
      Self::Fsm(err) => write!(f, "fsm error: {err}"),
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

impl From<PyErr> for FinateStateMachineError {
  fn from(value: PyErr) -> Self {
    Self {
      kind: FinateStateMachineErrorKind::Fsm(Arc::new(value)),
      msg: smallvec::SmallVec::new(),
    }
  }
}

#[pymethods]
impl FinateStateMachineError {
  #[staticmethod]
  fn snapshot(err: FinateStateMachineSnapshotError) -> PyResult<Self> {
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
pub struct FinateStateMachine(Py<PyAny>);

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
  type Data = RaftData;

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
    Python::with_gil(|py| {
      let log = crate::types::CommittedLog::from(log);
      into_future(self.0.as_ref(py).call_method1("apply", (log,))?)
    })?
    .await
    .and_then(|res| Python::with_gil(|py| res.extract::<FinateStateMachineResponse>(py)))
    .map_err(Into::into)
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
  ) -> Result<ApplyBatchResponse<Self::Response>, Self::Error> {
    Python::with_gil(|py| {
      let logs: SmallVec<[crate::types::CommittedLog; 4]> =
        logs.into_iter().map(|log| log.into()).collect();
      into_future(self.0.as_ref(py).call_method1("apply_batch", (logs,))?)
    })?
    .await
    .and_then(|res| Python::with_gil(|py| res.extract::<super::ApplyBatchResponse>(py)))
    .map(Into::into)
    .map_err(Into::into)
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
    Python::with_gil(|py| into_future(self.0.as_ref(py).call_method0("snapshot")?))?
      .await
      .and_then(|res| Python::with_gil(|py| res.extract::<FinateStateMachineSnapshot>(py)))
      .map_err(Into::into)
  }

  /// Used to restore an FSM from a snapshot. It is not called
  /// concurrently with any other command. The FSM must discard all previous
  /// state before restoring the snapshot.
  async fn restore(&self, snapshot: impl SnapshotSource<Id = Self::Id, Address = Self::Address, Runtime = Self::Runtime>,) -> Result<(), Self::Error> {
    let source = FileSnapshotSource::new(snapshot.meta().clone(), Arc::new(Mutex::new(snapshot)));
    Python::with_gil(|py| {
      into_future(self.0.as_ref(py).call_method1("restore", (source,),)?)
    })?
    .await
    .map(|_| ())
    .map_err(Into::into)
  }
}

#[pymethods]
impl FinateStateMachine {
  #[new]
  pub fn new(fsm: Py<PyAny>) -> Self {
    Self(fsm)
  }
}

#[pymodule]
pub fn tokio(py: Python, m: &PyModule) -> PyResult<()> {
  m.add_class::<FinateStateMachineError>()?;
  m.add_class::<FinateStateMachineSnapshot>()?;
  m.add_class::<FinateStateMachine>()?;
  Ok(())
}

// This function creates and returns the sled submodule.
pub fn submodule(py: Python) -> PyResult<&PyModule> {
  let module = PyModule::new(py, "tokio")?;
  tokio(py, module)?;
  Ok(module)
}
