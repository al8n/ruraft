use pyo3::prelude::*;
use ruraft_python::*;

/// Expose [`ruraft`](https://crates.io/crates/ruraft) Raft protocol implementation to a Python module.
#[pymodule]
pub fn _internal(py: Python, m: &PyModule) -> PyResult<()> {
  let typem = types::register(py)?;
  types::futs::register_async_std(typem)?;
  py.import("sys")?
    .getattr("modules")?
    .set_item("async_raft.types", typem)?;

  let membershipm = types::membership::register(py)?;
  py.import("sys")?
  .getattr("modules")?
  .set_item("async_raft.membership", membershipm)?;

  let optionsm = options::register(py)?;
  py.import("sys")?
    .getattr("modules")?
    .set_item("async_raft.options", optionsm)?;

  m.add_class::<raft::AsyncStdRaft>()?;

  Ok(())
}