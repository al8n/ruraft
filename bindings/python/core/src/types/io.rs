use std::{pin::Pin, task::{Context, Poll}, io};

use futures::{AsyncRead, Future, io::Cursor};
use pyo3::{*, types::PyBytes};

pub struct MemoryBuffer(Py<PyBytes>);

impl Clone for MemoryBuffer {
  fn clone(&self) -> Self {
    Self(self.0.clone())
  }
}

impl AsRef<[u8]> for MemoryBuffer {
  fn as_ref(&self) -> &[u8] {
    Python::with_gil(|py| self.0.as_bytes(py))
  }
}

enum AsyncReaderInner<F> {
  File(F),
  Memory(Cursor<MemoryBuffer>)
}

impl<F: AsyncRead + Unpin> AsyncRead for AsyncReaderInner<F> {
  fn poll_read(
    self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut [u8],
  ) -> Poll<io::Result<usize>> {
    match self.get_mut() {
      Self::File(f) => {
        Pin::new(f).poll_read(cx, buf)
      },
      Self::Memory(b) => {
        Pin::new(b).poll_read(cx, buf)
      }
    }
  }
}


#[cfg(feature = "tokio")]
pub use self::tokio::{SnapshotSink as TokioSnapshotSink, SnapshotSource as TokioSnapshotSource, AsyncReader as TokioAsyncReader};

#[cfg(feature = "tokio")]
mod tokio {
  use super::*;
  use pyo3_asyncio::tokio::future_into_py;
  use ::tokio::fs::File;
  use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

  pyo3io_macros::async_reader!(future_into_py("AsyncReader": AsyncReaderInner<Compat<File>> {
    #[staticmethod]
    pub fn memory(buf: Py<PyBytes>) -> Self {
      Self::new(AsyncReaderInner::Memory(Cursor::new(MemoryBuffer(buf))))
    }

    #[staticmethod]
    pub fn file<'a>(py: Python<'a>, path: &str) -> PyResult<Self> {
      std::fs::File::open(path)
        .map(|f| Self::new(AsyncReaderInner::File(File::from_std(f).compat())))
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }
  }));

  pyo3io_macros::async_reader!(future_into_py("SnapshotSource": ruraft_bindings_common::storage::SupportedSnapshotSource));

  impl From<ruraft_bindings_common::storage::SupportedSnapshotSource> for SnapshotSource {
    fn from(s: ruraft_bindings_common::storage::SupportedSnapshotSource) -> Self {
      Self::new(s)
    }
  }

  impl From<Box<dyn AsyncRead + Send + Sync + Unpin + 'static>> for SnapshotSource {
    fn from(s: Box<dyn AsyncRead + Send + Sync + Unpin + 'static>) -> Self {
      Self::new(ruraft_bindings_common::storage::SupportedSnapshotSource::from(s))
    }
  }

  pyo3io_macros::async_writer!(future_into_py("SnapshotSink": ruraft_bindings_common::storage::SupportedSnapshotSink {
    pub fn cancel<'a>(&'a self, py: Python<'a>) -> PyResult<&'a PyAny> {
      use ruraft_core::storage::SnapshotSinkExt;

      let this = self.0.clone();
      future_into_py(py, async move {
        let mut guard = this.lock().await;
        match &mut *guard {
          pyo3io::State::Ok(sink) => sink.cancel().await.map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string())),
          pyo3io::State::Closed => Err(pyo3::exceptions::PyIOError::new_err("sink is closed")),
        }
      })
    }
  }));

  impl From<ruraft_bindings_common::storage::SupportedSnapshotSink> for SnapshotSink {
    fn from(s: ruraft_bindings_common::storage::SupportedSnapshotSink) -> Self {
      Self::new(s)
    }
  }
}

#[cfg(feature = "async-std")]
pub use self::async_std::{SnapshotSink as AsyncStdSnapshotSink, SnapshotSource as AsyncStdSnapshotSource, AsyncReader as AsyncStdAsyncReader};

#[cfg(feature = "async-std")]
mod async_std {
  use ::async_std::fs::File;
  use futures::{AsyncRead, Future};
  use pyo3::*;
  use pyo3_asyncio::async_std::future_into_py;

  use super::*;

  pyo3io_macros::async_reader!(future_into_py("AsyncReader": AsyncReaderInner<File> {
    #[staticmethod]
    pub fn memory(buf: Py<PyBytes>) -> Self {
      Self::new(AsyncReaderInner::Memory(Cursor::new(MemoryBuffer(buf))))
    }

    #[staticmethod]
    pub fn file<'a>(py: Python<'a>, path: &str) -> PyResult<Self> {
      std::fs::File::open(path)
        .map(|f| Self::new(AsyncReaderInner::File(File::from(f))))
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }
  }));

  pyo3io_macros::async_reader!(future_into_py("SnapshotSource": ruraft_bindings_common::storage::SupportedSnapshotSource));

  impl From<ruraft_bindings_common::storage::SupportedSnapshotSource> for SnapshotSource {
    fn from(s: ruraft_bindings_common::storage::SupportedSnapshotSource) -> Self {
      Self::new(s)
    }
  }

  impl From<Box<dyn AsyncRead + Send + Sync + Unpin + 'static>> for SnapshotSource {
    fn from(s: Box<dyn AsyncRead + Send + Sync + Unpin + 'static>) -> Self {
      Self::new(ruraft_bindings_common::storage::SupportedSnapshotSource::from(s))
    }
  }

  pyo3io_macros::async_writer!(future_into_py("SnapshotSink": ruraft_bindings_common::storage::SupportedSnapshotSink {
    pub fn cancel<'a>(&'a self, py: Python<'a>) -> PyResult<&'a PyAny> {
      use ruraft_core::storage::SnapshotSinkExt;

      let this = self.0.clone();
      future_into_py(py, async move {
        let mut guard = this.lock().await;
        match &mut *guard {
          pyo3io::State::Ok(sink) => sink.cancel().await.map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string())),
          pyo3io::State::Closed => Err(pyo3::exceptions::PyIOError::new_err("sink is closed")),
        }
      })
    }
  }));

  impl From<ruraft_bindings_common::storage::SupportedSnapshotSink> for SnapshotSink {
    fn from(s: ruraft_bindings_common::storage::SupportedSnapshotSink) -> Self {
      Self::new(s)
    }
  }
}

