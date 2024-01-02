use std::sync::Arc;

use futures::AsyncRead;
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

pub mod tokio;

pub fn index<T: RFinateStateMachineResponse>(resp: &T) -> u64 {
  resp.index()
}

#[pyfunction]
#[pyo3(name = "index")]
pub fn index_wrapper(model: &FinateStateMachineResponse) {
  index(model);
}

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

#[pymodule]
pub fn fsm(py: Python, m: &PyModule) -> PyResult<()> {
  m.add_class::<FinateStateMachineResponse>()?;
  m.add_class::<ApplyBatchResponse>()?;
  m.add_function(wrap_pyfunction!(index_wrapper, m)?)?;
  #[cfg(feature = "tokio")]
  m.add_submodule(self::tokio::submodule(py)?)?;

  Ok(())
}

// This function creates and returns the sled submodule.
pub fn submodule(py: Python) -> PyResult<&PyModule> {
  let module = PyModule::new(py, "fsm")?;
  fsm(py, module)?;
  Ok(module)
}
