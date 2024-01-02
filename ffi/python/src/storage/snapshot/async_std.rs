use std::sync::Arc;

use async_lock::Mutex;
use futures::{AsyncWriteExt, AsyncWrite};
use pyo3::{prelude::*, types::PyBytes};
use pyo3_asyncio::async_std::*;
use smallvec::SmallVec;

use super::SnapshotId;

const INLINED: usize = 64;

#[derive(Clone)]
#[pyclass]
pub struct FileSnapshotSink {
  id: SnapshotId,
  sink: Arc<Mutex<dyn AsyncWrite + Send + Sync + Unpin + 'static>>,
}

impl FileSnapshotSink {
  pub fn new(id: SnapshotId, writer: impl AsyncWrite + Send + Sync + Unpin + 'static) -> Self {
    Self {
      id,
      sink: Arc::new(Mutex::new(writer))
    }
  }
}

#[pymethods]
impl FileSnapshotSink {
  fn write<'a>(&self, py: Python<'a>, bytes: &'a PyBytes) -> PyResult<&'a PyAny> {
    let this = self.sink.clone();
    let buf = SmallVec::<[u8; INLINED]>::from_slice(bytes.as_bytes());
    future_into_py(py, async move {
      let mut sink = this.lock().await;
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
      let mut sink = this.lock().await;
      sink
        .write_all(&buf)
        .await
        .map_err(|err| PyErr::new::<pyo3::exceptions::PyIOError, _>(err.to_string()))?;
      Ok(())
    })
  }

  #[getter]
  fn id(&self) -> SnapshotId {
    self.id
  }
}

#[pymodule]
pub fn async_std(_py: Python, m: &PyModule) -> PyResult<()> {
  m.add_class::<FileSnapshotSink>()?;
  Ok(())
}

// This function creates and returns the sled submodule.
pub fn submodule(py: Python) -> PyResult<&PyModule> {
  let module = PyModule::new(py, "async_std")?;
  async_std(py, module)?;
  Ok(module)
}
