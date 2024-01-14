use pyo3::prelude::*;

// This function creates and returns the sled submodule.
pub fn submodule(py: Python) -> PyResult<&PyModule> {
  let module = PyModule::new(py, "transport")?;
  Ok(module)
}
