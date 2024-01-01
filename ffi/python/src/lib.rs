#![allow(clippy::new_without_default)]

use pyo3::{types::PyModule, *};

mod storage;
mod types;

/// Expose [`ruraft`](https://crates.io/crates/ruraft) Raft protocol implementation to a Python module.
#[pymodule]
pub fn pyraft(py: Python, m: &PyModule) -> PyResult<()> {
  m.add_submodule(storage::submodule(py)?)?;
  m.add_submodule(types::submodule(py)?)?;
  Ok(())
}
