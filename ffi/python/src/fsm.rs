use bytes::Bytes;
use futures::AsyncRead;
use nodecraft::{NodeAddress, NodeId};
use pyo3::prelude::*;
use ruraft_core::{
  storage::{CommittedLog, CommittedLogBatch, SnapshotSink},
  FinateStateMachine as RFinateStateMachine, FinateStateMachineError as RFinateStateMachineError,
  FinateStateMachineResponse as RFinateStateMachineResponse,
  FinateStateMachineSnapshot as RFinateStateMachineSnapshot,
};
use ruraft_snapshot::sync::FileSnapshotSink;

mod tokio;

pub fn index<T: RFinateStateMachineResponse>(resp: &T) -> u64 {
  resp.index()
}

#[pyfunction]
#[pyo3(name = "index")]
pub fn index_wrapper(model: &FinateStateMachineResponse) {
  index(model);
}

#[pyclass]
pub struct FinateStateMachineResponse(Py<PyAny>);

impl RFinateStateMachineResponse for FinateStateMachineResponse {
  fn index(&self) -> u64 {
    Python::with_gil(|py| {
      let py_result: &PyAny = self.0.as_ref(py).call_method("index", (), None).unwrap();

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
  pub fn new(model: Py<PyAny>) -> Self {
    Self(model)
  }
}

#[pymodule]
pub fn fsm(_py: Python, m: &PyModule) -> PyResult<()> {
  m.add_class::<FinateStateMachineResponse>()?;
  m.add_function(wrap_pyfunction!(index_wrapper, m)?)?;
  Ok(())
}

// This function creates and returns the sled submodule.
pub fn submodule(py: Python) -> PyResult<&PyModule> {
  let module = PyModule::new(py, "fsm")?;
  fsm(py, module)?;
  Ok(module)
}
