use pyo3::prelude::*;
use ruraft_python::*;

/// Expose [`ruraft`](https://crates.io/crates/ruraft) Raft protocol implementation to a Python module.
#[pymodule]
pub fn _internal(py: Python, m: &PyModule) -> PyResult<()> {
  register::<AsyncStdRuntime>(py, m)
}
