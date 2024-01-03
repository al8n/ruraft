use std::sync::Arc;

use nodecraft::{NodeAddress, NodeId};
use pyo3::{exceptions::PyIOError, prelude::*};
use ruraft_core::{
  storage::{CommittedLog, CommittedLogBatch, SnapshotSink},
  ApplyBatchResponse as RApplyBatchResponse, FinateStateMachine as RFinateStateMachine,
  FinateStateMachineError as RFinateStateMachineError,
  FinateStateMachineResponse as RFinateStateMachineResponse,
  FinateStateMachineSnapshot as RFinateStateMachineSnapshot,
};
use smallvec::SmallVec;

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
pub struct ApplyBatchResponse(RApplyBatchResponse<FinateStateMachineResponse>);

impl From<ApplyBatchResponse> for RApplyBatchResponse<FinateStateMachineResponse> {
  fn from(value: ApplyBatchResponse) -> Self {
    value.0
  }
}

#[pymethods]
impl ApplyBatchResponse {
  #[new]
  pub fn new(resps: SmallVec<[FinateStateMachineResponse; 2]>) -> Self {
    Self(resps.into())
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

macro_rules! state_machine {
  ($runtime:ident) => {
    #[derive(Clone)]
    #[pyclass]
    pub struct FinateStateMachineSnapshot(Arc<Py<PyAny>>);

    impl RFinateStateMachineSnapshot for FinateStateMachineSnapshot {
      type Error = FinateStateMachineSnapshotError;
      type Runtime = $runtime;

      async fn persist(
        &self,
        sink: impl SnapshotSink<Runtime = Self::Runtime>,
      ) -> Result<(), Self::Error> {
        let id = sink.id();
        let sink = FearlessCell::new(Box::new(sink));
        // Safety:
        // - SnapshotSink impl AsyncWrite
        let snap = FileSnapshotSink::new(id.into(), unsafe { core::mem::transmute(sink.clone()) });
        let res = Python::with_gil(|py| {
          let arg = (snap.into_py(py),);
          into_future(self.0.as_ref().as_ref(py).call_method1("persist", arg)?)
        });
        match res {
          Ok(fut) => {
            match fut.await {
              Ok(_) => {
                Python::with_gil(|py| {
                  future_into_py(py, async move {
                    // Safety
                    // 1. hold GIL
                    // 2. no data race in rust side
                    let sink = unsafe { sink.get_mut() };
                    if let Err(e) = sink.close().await {
                      tracing::error!(target = "ruraft.python.fsm.persist", err=%e, "failed to close snapshot sink.");
                    }
                    Ok(())
                  })
                  .map(|_| ())
                })
                .map_err(Into::into)
              }
              Err(err) => {
                Python::with_gil(|py| {
                  future_into_py(py, async move {
                    // Safety
                    // 1. hold GIL
                    // 2. no data race in rust side
                    let sink = unsafe { sink.get_mut() };
                    if let Err(e) = sink.cancel().await {
                      tracing::error!(target = "ruraft.python.fsm.persist", err=%e, "failed to cancel snapshot sink.");
                    }

                    if let Err(e) = sink.close().await {
                      tracing::error!(target = "ruraft.python.fsm.persist", err=%e, "failed to close snapshot sink.");
                    }

                    Ok(())
                  })
                  .map(|_| ())
                })
                .map_err(|_| err.into())
              }
            }
          }
          Err(err) => {
            Python::with_gil(|py| {
              future_into_py(py, async move {
                // Safety
                // 1. hold GIL
                // 2. no data race in rust side
                let sink = unsafe { sink.get_mut() };
                if let Err(e) = sink.cancel().await {
                  tracing::error!(target = "ruraft.python.fsm.persist", err=%e, "failed to cancel snapshot sink.");
                }

                if let Err(e) = sink.close().await {
                  tracing::error!(target = "ruraft.python.fsm.persist", err=%e, "failed to close snapshot sink.");
                }

                Ok(())
              })
              .map(|_| ())
            })
            .map_err(|_| err.into())
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
    }

    impl core::fmt::Display for FinateStateMachineErrorKind {
      fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
          Self::Snapshot(err) => write!(f, "snapshot error: {err}"),
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
      type Runtime = $runtime;

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
        let source = FileSnapshotSource::new(snapshot.meta().clone(), FearlessCell::new(Box::new(snapshot)));
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
  };
}

#[cfg(feature = "tokio")]
pub mod tokio;

#[cfg(feature = "async-std")]
pub mod async_std;

#[pymodule]
pub fn fsm(py: Python, m: &PyModule) -> PyResult<()> {
  m.add_class::<FinateStateMachineResponse>()?;
  m.add_class::<ApplyBatchResponse>()?;
  #[cfg(feature = "tokio")]
  m.add_submodule(self::tokio::submodule(py)?)?;

  #[cfg(feature = "async-std")]
  m.add_submodule(self::async_std::submodule(py)?)?;

  Ok(())
}

// This function creates and returns the sled submodule.
pub fn submodule(py: Python) -> PyResult<&PyModule> {
  let module = PyModule::new(py, "fsm")?;
  fsm(py, module)?;
  Ok(module)
}
