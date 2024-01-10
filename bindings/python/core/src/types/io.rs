use std::{
  io,
  pin::Pin,
  task::{Context, Poll},
};

use crate::RaftData;
use futures::{io::Cursor, AsyncRead, Future};
use pyo3::*;

enum AsyncReaderInner<F> {
  File(F),
  Memory(Cursor<RaftData>),
}

impl<F: AsyncRead + Unpin> AsyncRead for AsyncReaderInner<F> {
  fn poll_read(
    self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut [u8],
  ) -> Poll<io::Result<usize>> {
    match self.get_mut() {
      Self::File(f) => Pin::new(f).poll_read(cx, buf),
      Self::Memory(b) => Pin::new(b).poll_read(cx, buf),
    }
  }
}

#[cfg(feature = "tokio")]
pub use self::tokio::{
  AsyncReader as TokioAsyncReader, Snapshot as TokioSnapshot, SnapshotSink as TokioSnapshotSink,
};

#[cfg(feature = "tokio")]
mod tokio {
  use super::*;
  use ::tokio::fs::File;
  use pyo3_asyncio::tokio::future_into_py;
  use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

  pyo3io_macros::async_reader!(future_into_py("AsyncReader": AsyncReaderInner<Compat<File>> {
    #[staticmethod]
    pub fn memory(buf: &[u8]) -> Self {
      Self::new(AsyncReaderInner::Memory(Cursor::new(RaftData::from(buf))))
    }

    #[staticmethod]
    pub fn file<'a>(py: Python<'a>, path: &str) -> PyResult<Self> {
      std::fs::File::open(path)
        .map(|f| Self::new(AsyncReaderInner::File(File::from_std(f).compat())))
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }
  }));

  pyo3io_macros::async_reader!(future_into_py("Snapshot": ruraft_bindings_common::storage::SupportedSnapshot));

  impl From<ruraft_bindings_common::storage::SupportedSnapshot> for Snapshot {
    fn from(s: ruraft_bindings_common::storage::SupportedSnapshot) -> Self {
      Self::new(s)
    }
  }

  impl From<Box<dyn AsyncRead + Send + Sync + Unpin + 'static>> for Snapshot {
    fn from(s: Box<dyn AsyncRead + Send + Sync + Unpin + 'static>) -> Self {
      Self::new(ruraft_bindings_common::storage::SupportedSnapshot::from(s))
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
pub use self::async_std::{
  AsyncReader as AsyncStdAsyncReader, Snapshot as AsyncStdSnapshot,
  SnapshotSink as AsyncStdSnapshotSink,
};

#[cfg(feature = "async-std")]
mod async_std {
  use ::async_std::fs::File;
  use futures::{AsyncRead, Future};
  use pyo3::*;
  use pyo3_asyncio::async_std::future_into_py;

  use super::*;

  pyo3io_macros::async_reader!(future_into_py("AsyncReader": AsyncReaderInner<File> {
    #[staticmethod]
    pub fn memory(buf: &[u8]) -> Self {
      Self::new(AsyncReaderInner::Memory(Cursor::new(RaftData::from(buf))))
    }

    #[staticmethod]
    pub fn file<'a>(py: Python<'a>, path: &str) -> PyResult<Self> {
      std::fs::File::open(path)
        .map(|f| Self::new(AsyncReaderInner::File(File::from(f))))
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }
  }));

  pyo3io_macros::async_reader!(future_into_py("Snapshot": ruraft_bindings_common::storage::SupportedSnapshot));

  impl From<ruraft_bindings_common::storage::SupportedSnapshot> for Snapshot {
    fn from(s: ruraft_bindings_common::storage::SupportedSnapshot) -> Self {
      Self::new(s)
    }
  }

  impl From<Box<dyn AsyncRead + Send + Sync + Unpin + 'static>> for Snapshot {
    fn from(s: Box<dyn AsyncRead + Send + Sync + Unpin + 'static>) -> Self {
      Self::new(ruraft_bindings_common::storage::SupportedSnapshot::from(s))
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
