use pyo3::prelude::*;
use ruraft_python::*;

/// Expose [`ruraft`](https://crates.io/crates/ruraft) Raft protocol implementation to a Python module.
#[pymodule]
pub fn _internal(py: Python, m: &PyModule) -> PyResult<()> {
  let typem = types::register(py)?;
  types::register_async_std(typem)?;
  m.add_submodule(typem)?;
  py.import("sys")?
    .getattr("modules")?
    .set_item("asyn_std_raft.types", typem)?;

  let membershipm = types::membership::register(py)?;
  m.add_submodule(membershipm)?;
  py.import("sys")?
    .getattr("modules")?
    .set_item("asyn_std_raft.membership", membershipm)?;

  let optionsm = options::register(py)?;
  m.add_submodule(optionsm)?;
  py.import("sys")?
    .getattr("modules")?
    .set_item("asyn_std_raft.options", optionsm)?;

  m.add_class::<raft::AsyncStdRaft>()?;
  m.add_class::<fsm::async_std::AsyncStdFinateStateMachine>()?;
  m.add_class::<fsm::async_std::AsyncStdFinateStateMachineSnapshot>()?;
  m.add_class::<fsm::FinateStateMachineResponse>()?;

  Ok(())
}
