
use std::sync::Arc;
use async_lock::Mutex;
use futures::{AsyncRead, AsyncReadExt};
use pyo3::{exceptions::PyIOError, prelude::*, types::PyByteArray, ffi::PyByteArrayObject};


#[derive(Clone)]
#[pyclass]
pub struct IoError(Arc<std::io::Error>);

impl core::fmt::Display for IoError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.0)
  }
}

impl core::fmt::Debug for IoError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{:?}", self.0)
  }
}

impl std::error::Error for IoError {}

#[pymethods]
impl IoError {
  #[staticmethod]
  fn new(err: &PyIOError) -> PyResult<Self> {
    let err: String = err.str()?.extract()?;

    Ok(Self(Arc::new(std::io::Error::new(
      std::io::ErrorKind::Other,
      err,
    ))))
  }
}

#[pyclass]
pub struct Reader(Arc<Mutex<dyn AsyncRead + Send + Unpin>>);

#[pymethods]
impl Reader {
  fn read(&self, py: Python, buf: &PyByteArray) {
    let this = self.0.clone();
    pyo3_asyncio::tokio::future_into_py(py, async move {
      let mut reader = this.lock().await;
      let readed = reader
        .read(unsafe { buf.as_bytes_mut() })
        .await
        .map_err(|err| PyErr::new::<PyIOError, _>(err.to_string()))?;

      Ok(())
    });
  }
}


#[pymodule]
pub fn io(_py: Python, m: &PyModule) -> PyResult<()> {
  m.add_class::<IoError>()?;
  Ok(())
}

// This function creates and returns the sled submodule.
pub fn submodule(py: Python) -> PyResult<&PyModule> {
  let module = PyModule::new(py, "io")?;
  io(py, module)?;
  Ok(module)
}
