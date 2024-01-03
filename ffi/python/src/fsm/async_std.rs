use std::{borrow::Cow, sync::Arc};

use crate::{storage::snapshot::async_std::FileSnapshotSource, FearlessCell, RaftData};

use super::*;
use crate::storage::snapshot::async_std::FileSnapshotSink;
use agnostic::async_std::AsyncStdRuntime;
use futures::AsyncWriteExt;
use pyo3::types::PyString;
use pyo3_asyncio::async_std::{future_into_py, into_future};
use ruraft_core::{storage::SnapshotSource, ApplyBatchResponse};
use smallvec::SmallVec;

state_machine!(AsyncStdRuntime);

#[pymodule]
pub fn async_std(_py: Python, m: &PyModule) -> PyResult<()> {
  m.add_class::<FinateStateMachineError>()?;
  m.add_class::<FinateStateMachineSnapshot>()?;
  m.add_class::<FinateStateMachine>()?;
  Ok(())
}

// This function creates and returns the sled submodule.
pub fn submodule(py: Python) -> PyResult<&PyModule> {
  let module = PyModule::new(py, "async_std")?;
  async_std(py, module)?;
  Ok(module)
}
