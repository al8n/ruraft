use pyo3::prelude::*;
use ruraft_core::{FinateStateMachine, FinateStateMachineResponse};

#[pymodule]
pub fn fsm(_py: Python, m: &PyModule) -> PyResult<()> {
  Ok(())
}

// This function creates and returns the sled submodule.
pub fn submodule(py: Python) -> PyResult<&PyModule> {
  let module = PyModule::new(py, "fsm")?;
  fsm(py, module)?;
  Ok(module)
}
