use std::{borrow::Cow, sync::Arc};

use crate::{storage::snapshot::tokio::FileSnapshotSource, FearlessCell, RaftData};

use super::*;
use crate::storage::snapshot::tokio::FileSnapshotSink;
use agnostic::tokio::TokioRuntime;
use futures::AsyncWriteExt;
use pyo3::types::PyString;
use pyo3_asyncio::tokio::{future_into_py, into_future};
use ruraft_core::{storage::SnapshotSource, ApplyBatchResponse};
use smallvec::SmallVec;

state_machine!(TokioRuntime);

#[pymodule]
pub fn tokio(_py: Python, m: &PyModule) -> PyResult<()> {
  m.add_class::<FinateStateMachineError>()?;
  m.add_class::<FinateStateMachineSnapshot>()?;
  m.add_class::<FinateStateMachine>()?;
  Ok(())
}

// This function creates and returns the sled submodule.
pub fn submodule(py: Python) -> PyResult<&PyModule> {
  let module = PyModule::new(py, "tokio")?;
  tokio(py, module)?;
  Ok(module)
}
