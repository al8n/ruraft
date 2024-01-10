use std::{borrow::Cow, marker::PhantomData, sync::Arc};

use agnostic::Runtime;
use futures::AsyncRead;
use nodecraft::{NodeAddress, NodeId};
use pyo3::{exceptions::PyIOError, prelude::*, types::PyString};
use ruraft_bindings_common::storage::SupportedSnapshot;
use ruraft_core::{
  storage::{CommittedLog, CommittedLogBatch, SnapshotSink},
  FinateStateMachine as RFinateStateMachine, FinateStateMachineError as RFinateStateMachineError,
  FinateStateMachineResponse as RFinateStateMachineResponse,
  FinateStateMachineSnapshot as RFinateStateMachineSnapshot,
};
use smallvec::SmallVec;

use crate::{types::ApplyBatchResponse, FromPython, IntoPython, IntoSupportedRuntime, RaftData};

#[derive(Clone)]
#[pyclass]
pub struct FinateStateMachineResponse(Arc<Py<PyAny>>);

impl RFinateStateMachineResponse for FinateStateMachineResponse {
  fn index(&self) -> u64 {
    Python::with_gil(|py| {
      let py_result: &PyAny = self
        .0
        .as_ref()
        .as_ref(py)
        .call_method("index", (), None)
        .unwrap();

      if py_result.get_type().name().unwrap() != "int" {
        panic!(
          "Expected an int for the index() method signature, got {}",
          py_result.get_type().name().unwrap()
        );
      }
      py_result.extract()
    })
    .unwrap()
  }
}

#[pymethods]
impl FinateStateMachineResponse {
  #[new]
  pub fn new(resp: Py<PyAny>) -> Self {
    Self(Arc::new(resp))
  }
}

#[derive(Clone)]
#[pyclass]
pub struct FinateStateMachineSnapshotError(Arc<PyErr>);

impl From<PyErr> for FinateStateMachineSnapshotError {
  fn from(value: PyErr) -> Self {
    Self(Arc::new(value))
  }
}

impl core::fmt::Debug for FinateStateMachineSnapshotError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{:?}", &self.0)
  }
}

impl core::fmt::Display for FinateStateMachineSnapshotError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", &self.0)
  }
}

impl std::error::Error for FinateStateMachineSnapshotError {}

#[pymethods]
impl FinateStateMachineSnapshotError {
  #[new]
  pub fn new(err: &str) -> Self {
    Self(Arc::new(PyErr::new::<PyIOError, _>(err.to_string())))
  }

  fn __str__(&self) -> String {
    self.0.to_string()
  }

  fn __repr__(&self) -> String {
    format!("{:?}", self)
  }
}

pub struct FinateStateMachineSnapshot<R> {
  snap: Arc<Py<PyAny>>,
  _marker: PhantomData<R>,
}

impl<R> Clone for FinateStateMachineSnapshot<R> {
  fn clone(&self) -> Self {
    Self {
      snap: self.snap.clone(),
      ..*self
    }
  }
}

impl<R: Runtime> RFinateStateMachineSnapshot for FinateStateMachineSnapshot<R>
where
  R: IntoSupportedRuntime,
{
  type Error = FinateStateMachineSnapshotError;
  type Runtime = R;

  async fn persist(&self, sink: impl SnapshotSink + 'static) -> Result<(), Self::Error> {
    let id: crate::types::SnapshotId = sink.id().into();
    let snap = R::SnapshotSink::from(ruraft_bindings_common::storage::SupportedSnapshotSink::new(
      sink,
    ));

    Python::with_gil(|py| {
      let arg = (id, snap);
      R::into_supported().into_future(self.snap.as_ref().as_ref(py).call_method1("persist", arg)?)
    })
    .map_err(FinateStateMachineSnapshotError::from)?
    .await
    .map(|_| ())
    .map_err(FinateStateMachineSnapshotError::from)
  }

  async fn release(&mut self) -> Result<(), Self::Error> {
    Python::with_gil(|py| {
      R::into_supported().into_future(self.snap.as_ref().as_ref(py).call_method0("release")?)
    })?
    .await
    .map(|_| ())
    .map_err(Into::into)
  }
}

#[derive(Debug, Clone)]
enum FinateStateMachineErrorKind {
  Snapshot(FinateStateMachineSnapshotError),
  Fsm(Arc<PyErr>),
}

impl core::fmt::Display for FinateStateMachineErrorKind {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::Snapshot(err) => write!(f, "snapshot error: {err}"),
      Self::Fsm(err) => write!(f, "fsm error: {err}"),
    }
  }
}

pub struct FinateStateMachineError<R> {
  kind: FinateStateMachineErrorKind,
  msg: smallvec::SmallVec<[Cow<'static, str>; 4]>,
  _marker: PhantomData<R>,
}

impl<R> Clone for FinateStateMachineError<R> {
  fn clone(&self) -> Self {
    Self {
      kind: self.kind.clone(),
      msg: self.msg.clone(),
      _marker: PhantomData,
    }
  }
}

impl<R: agnostic::Runtime> RFinateStateMachineError for FinateStateMachineError<R>
where
  R: IntoSupportedRuntime,
{
  type Snapshot = FinateStateMachineSnapshot<R>;

  fn snapshot(err: <Self::Snapshot as RFinateStateMachineSnapshot>::Error) -> Self {
    Self {
      kind: FinateStateMachineErrorKind::Snapshot(err),
      msg: smallvec::SmallVec::new(),
      _marker: PhantomData,
    }
  }

  fn with_message(mut self, msg: Cow<'static, str>) -> Self {
    self.msg.push(msg);
    self
  }
}

impl<R> core::fmt::Debug for FinateStateMachineError<R> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct(std::any::type_name::<Self>())
      .field("kind", &self.kind)
      .field("messages", &self.msg)
      .finish()
  }
}

impl<R> core::fmt::Display for FinateStateMachineError<R> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.kind)?;
    for (idx, msg) in self.msg.iter().enumerate() {
      write!(f, "\t{idx}: {}", msg)?;
    }
    Ok(())
  }
}

impl<R> std::error::Error for FinateStateMachineError<R> {}

impl<R> From<PyErr> for FinateStateMachineError<R> {
  fn from(value: PyErr) -> Self {
    Self {
      kind: FinateStateMachineErrorKind::Fsm(Arc::new(value)),
      msg: smallvec::SmallVec::new(),
      _marker: PhantomData,
    }
  }
}

impl<R> FinateStateMachineError<R> {
  fn snapshot(err: FinateStateMachineSnapshotError) -> PyResult<Self> {
    Ok(Self {
      kind: FinateStateMachineErrorKind::Snapshot(err),
      msg: smallvec::SmallVec::new(),
      _marker: PhantomData,
    })
  }

  fn messages(&self) -> smallvec::SmallVec<[Cow<'static, str>; 4]> {
    self.msg.clone()
  }

  fn with_message(&mut self, msg: &PyString) -> PyResult<()> {
    msg.extract::<String>().map(|s| self.msg.push(s.into()))
  }
}

pub struct FinateStateMachine<R> {
  fsm: Arc<Py<PyAny>>,
  _marker: PhantomData<R>,
}

impl<R> Clone for FinateStateMachine<R> {
  fn clone(&self) -> Self {
    Self {
      fsm: self.fsm.clone(),
      _marker: PhantomData,
    }
  }
}

impl<R> RFinateStateMachine for FinateStateMachine<R>
where
  R: IntoSupportedRuntime,
  FinateStateMachineSnapshot<R>:
    FromPython<Source = <FinateStateMachineSnapshot<R> as IntoPython>::Target> + IntoPython,
{
  /// Errors returned by the finate state machine.
  type Error = FinateStateMachineError<R>;

  /// The snapshot type used by the finate state machine.
  type Snapshot = FinateStateMachineSnapshot<R>;

  /// The response type returned by the finate state machine after apply.
  type Response = FinateStateMachineResponse;

  /// The id type used to identify nodes.
  type Id = NodeId;

  /// The address type of node.
  type Address = NodeAddress;

  /// The log entry's type-specific data, which will be applied to a user [`FinateStateMachine`].
  type Data = RaftData;

  /// The async runtime used by the finate state machine.
  type Runtime = R;

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
      R::into_supported().into_future(self.fsm.as_ref().as_ref(py).call_method1("apply", (log,))?)
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
  ) -> Result<ruraft_core::ApplyBatchResponse<Self::Response>, Self::Error> {
    Python::with_gil(|py| {
      let logs: SmallVec<[crate::types::CommittedLog; 4]> =
        logs.into_iter().map(|log| log.into()).collect();
      R::into_supported().into_future(
        self
          .fsm
          .as_ref()
          .as_ref(py)
          .call_method1("apply_batch", (logs,))?,
      )
    })?
    .await
    .and_then(|res| Python::with_gil(|py| res.extract::<ApplyBatchResponse>(py)))
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
    Python::with_gil(|py| {
      R::into_supported().into_future(self.fsm.as_ref().as_ref(py).call_method0("snapshot")?)
    })?
    .await
    .and_then(|res| {
      Python::with_gil(|py| {
        res
          .extract::<<FinateStateMachineSnapshot<R> as IntoPython>::Target>(py)
          .map(FromPython::from_python)
      })
    })
    .map_err(Into::into)
  }

  /// Used to restore an FSM from a snapshot. It is not called
  /// concurrently with any other command. The FSM must discard all previous
  /// state before restoring the snapshot.
  async fn restore(
    &self,
    snapshot: impl AsyncRead + Unpin + Send + Sync + 'static,
  ) -> Result<(), Self::Error> {
    let source = R::Snapshot::from(SupportedSnapshot::new(snapshot));

    Python::with_gil(|py| {
      R::into_supported().into_future(
        self
          .fsm
          .as_ref()
          .as_ref(py)
          .call_method1("restore", (source,))?,
      )
    })?
    .await
    .map(|_| ())
    .map_err(Into::into)
  }
}

impl<R> FinateStateMachine<R> {
  pub fn new(fsm: Py<PyAny>) -> Self {
    Self {
      fsm: Arc::new(fsm),
      _marker: PhantomData,
    }
  }
}

macro_rules! wrap_fsm {
  ($rt: ident) => {
    paste::paste! {
      #[derive(Clone)]
      #[::pyo3::pyclass(name = "FinateStateMachine")]
      pub struct [< $rt FinateStateMachine >](super::FinateStateMachine< agnostic:: [< $rt:snake >] :: [< $rt Runtime >] >);

      impl crate::IntoPython for super::FinateStateMachine< agnostic:: [< $rt:snake >] :: [< $rt Runtime >] > {
        type Target = [< $rt FinateStateMachine >];

        fn into_python(self) -> Self::Target {
          [< $rt FinateStateMachine >] (self)
        }
      }

      impl crate::FromPython for super::FinateStateMachine< agnostic:: [< $rt:snake >] :: [< $rt Runtime >] > {
        type Source = [< $rt FinateStateMachine >];

        fn from_python(slf: Self::Source,) -> super::FinateStateMachine< agnostic:: [< $rt:snake >] :: [< $rt Runtime >] > {
          slf.0
        }
      }

      #[::pyo3::pymethods]
      impl [< $rt FinateStateMachine >] {
        pub fn new(fsm: ::pyo3::Py<::pyo3::PyAny>) -> Self {
          Self(super::FinateStateMachine {
            fsm: std::sync::Arc::new(fsm),
            _marker: ::core::marker::PhantomData,
          })
        }
      }
    }
  };
}

macro_rules! wrap_fsm_snapshot {
  ($rt: ident) => {
    paste::paste! {
      #[derive(Clone)]
      #[::pyo3::pyclass(name = "FinateStateMachineSnapshot")]
      pub struct [< $rt FinateStateMachineSnapshot >](super::FinateStateMachineSnapshot< agnostic:: [< $rt:snake >] :: [< $rt Runtime >] >);

      impl crate::IntoPython for super::FinateStateMachineSnapshot< agnostic:: [< $rt:snake >] :: [< $rt Runtime >] > {
        type Target = [< $rt FinateStateMachineSnapshot >];

        fn into_python(self) -> Self::Target {
          [< $rt FinateStateMachineSnapshot >] (self)
        }
      }

      impl crate::FromPython for super::FinateStateMachineSnapshot< agnostic:: [< $rt:snake >] :: [< $rt Runtime >] > {
        type Source = [< $rt FinateStateMachineSnapshot >];

        fn from_python(slf: Self::Source,) -> super::FinateStateMachineSnapshot< agnostic:: [< $rt:snake >] :: [< $rt Runtime >] > {
          slf.0
        }
      }

      #[::pyo3::pymethods]
      impl [< $rt FinateStateMachineSnapshot >] {
        pub fn new(snap: ::pyo3::Py<::pyo3::PyAny>) -> Self {
          Self( super::FinateStateMachineSnapshot {
            snap: std::sync::Arc::new(snap),
            _marker: ::core::marker::PhantomData,
          })
        }
      }
    }
  };
}

#[cfg(feature = "tokio")]
pub mod tokio {
  wrap_fsm!(Tokio);
  wrap_fsm_snapshot!(Tokio);
}

#[cfg(feature = "async-std")]
pub mod async_std {
  wrap_fsm!(AsyncStd);
  wrap_fsm_snapshot!(AsyncStd);
}
