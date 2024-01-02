use std::sync::Arc;

use async_lock::RwLock;
use futures::AsyncWriteExt;
use nodecraft::{NodeAddress, NodeId};
use pyo3::{prelude::*, types::PyBytes};
use pyo3_asyncio::tokio::*;
use ruraft_core::storage::SnapshotSink;
use ruraft_snapshot::sync::FileSnapshotSink as RFileSnapshotSink;
use smallvec::SmallVec;

use super::SnapshotId;

const INLINED: usize = 64;

#[pyclass]
pub struct FileSnapshotSink {
  id: SnapshotId,
  sink: Arc<RwLock<RFileSnapshotSink<NodeId, NodeAddress, agnostic::tokio::TokioRuntime>>>,
}

#[pymethods]
impl FileSnapshotSink {
  fn write<'a>(&self, py: Python<'a>, bytes: &'a PyBytes) -> PyResult<&'a PyAny> {
    let this = self.sink.clone();
    let buf = SmallVec::<[u8; INLINED]>::from_slice(bytes.as_bytes());
    future_into_py(py, async move {
      let mut sink = this.write().await;
      let readed = sink
        .write(&buf)
        .await
        .map_err(|err| PyErr::new::<pyo3::exceptions::PyIOError, _>(err.to_string()))?;
      Ok(readed)
    })
  }

  fn write_all<'a>(&self, py: Python<'a>, bytes: &'a PyBytes) -> PyResult<&'a PyAny> {
    let this = self.sink.clone();
    let buf = SmallVec::<[u8; INLINED]>::from_slice(bytes.as_bytes());
    future_into_py(py, async move {
      let mut sink = this.write().await;
      sink
        .write_all(&buf)
        .await
        .map_err(|err| PyErr::new::<pyo3::exceptions::PyIOError, _>(err.to_string()))?;
      Ok(())
    })
  }

  fn cancel<'a>(&'a self, py: Python<'a>) -> PyResult<&'a PyAny> {
    let this = self.sink.clone();
    future_into_py(py, async move {
      let mut sink = this.write().await;
      sink
        .cancel()
        .await
        .map_err(|err| PyErr::new::<pyo3::exceptions::PyIOError, _>(err.to_string()))
    })
  }

  #[getter]
  fn id(&self) -> SnapshotId {
    self.id
  }
}

#[pymodule]
pub fn tokio(_py: Python, m: &PyModule) -> PyResult<()> {
  m.add_class::<FileSnapshotSink>()?;
  Ok(())
}

// This function creates and returns the sled submodule.
pub fn submodule(py: Python) -> PyResult<&PyModule> {
  let module = PyModule::new(py, "tokio")?;
  tokio(py, module)?;
  Ok(module)
}
