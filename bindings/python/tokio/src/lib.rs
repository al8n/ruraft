use pyo3::prelude::*;
use ruraft_python::*;

/// Expose [`ruraft`](https://crates.io/crates/ruraft) Raft protocol implementation to a Python module.
#[pymodule]
pub fn _internal(py: Python, m: &PyModule) -> PyResult<()> {
  let typem = types::register(py)?;
  types::register_tokio(typem)?;
  m.add_submodule(typem)?;
  py.import("sys")?
    .getattr("modules")?
    .set_item("tokio_raft.types", typem)?;

  let membershipm = types::membership::register(py)?;
  m.add_submodule(membershipm)?;
  py.import("sys")?
    .getattr("modules")?
    .set_item("tokio_raft.membership", membershipm)?;

  let optionsm = options::register(py)?;
  m.add_submodule(optionsm)?;
  py.import("sys")?
    .getattr("modules")?
    .set_item("tokio_raft.options", optionsm)?;

  m.add_class::<raft::TokioRaft>()?;
  m.add_class::<fsm::tokio::TokioFinateStateMachine>()?;
  m.add_class::<fsm::tokio::TokioFinateStateMachineSnapshot>()?;
  m.add_class::<fsm::FinateStateMachineResponse>()?;

  Ok(())
}
