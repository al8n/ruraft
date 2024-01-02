use std::sync::Arc;

use pyo3::{exceptions::PyIOError, prelude::*};

#[derive(Clone)]
#[pyclass]
pub struct IoError(Arc<std::io::Error>);

impl core::fmt::Display for IoError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.0)
  }
}

impl core::fmt::Debug for IoError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{:?}", self.0)
  }
}

impl std::error::Error for IoError {}

#[pymethods]
impl IoError {
  #[staticmethod]
  fn new(py: Python, err: &PyIOError) -> PyResult<Self> {
    let err: String = err.str()?.extract()?;

    Ok(Self(Arc::new(std::io::Error::new(
      std::io::ErrorKind::Other,
      err,
    ))))
  }
}

#[pymodule]
pub fn error(_py: Python, m: &PyModule) -> PyResult<()> {
  m.add_class::<IoError>()?;
  Ok(())
}

// This function creates and returns the sled submodule.
pub fn submodule(py: Python) -> PyResult<&PyModule> {
  let module = PyModule::new(py, "error")?;
  error(py, module)?;
  Ok(module)
}
