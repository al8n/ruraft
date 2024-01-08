use std::time::Duration;

use pyo3::prelude::*;

use ruraft_tcp::net::NetTransportOptions;

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
#[pyclass]
pub struct Options(NetTransportOptions);

#[pymethods]
impl Options {
  #[new]
  pub fn new() -> Self {
    Self(Default::default())
  }

  /// Get the maximum connections in the pool.
  #[getter]
  pub fn max_pool(&self) -> usize {
    self.0.max_pool
  }

  /// Set the maximum connections in the pool.
  #[setter]
  pub fn set_max_pool(&mut self, value: usize) {
    self.0.max_pool = value;
  }

  /// Returns the max inflight append entries requests.
  #[getter]
  pub fn max_inflight_requests(&self) -> usize {
    self.0.max_inflight_requests()
  }

  /// Sets the max inflight append entries requests.
  #[setter]
  pub fn set_max_inflight_requests(&mut self, val: usize) {
    self.0.max_inflight_requests = val;
  }

  /// Returns the timeout used to apply I/O deadlines.
  #[getter]
  pub fn timeout(&self) -> ::chrono::Duration {
    ::chrono::Duration::milliseconds(self.0.timeout().as_millis() as i64)
  }

  /// Set the timeout used to apply I/O deadlines.
  #[setter]
  pub fn set_timeout(&mut self, timeout: ::chrono::Duration) {
    self.0.timeout = Duration::from_millis(timeout.num_milliseconds() as u64);
  }
}

#[pymodule]
pub fn transport(py: Python, m: &PyModule) -> PyResult<()> {
  m.add_class::<Options>()?;
  // #[cfg(feature = "tokio")]
  // m.add_submodule(self::tokio::submodule(py)?)?;

  // #[cfg(feature = "async-std")]
  // m.add_submodule(self::async_std::submodule(py)?)?;

  Ok(())
}

// This function creates and returns the sled submodule.
pub fn submodule(py: Python) -> PyResult<&PyModule> {
  let module = PyModule::new(py, "transport")?;
  transport(py, module)?;
  Ok(module)
}
