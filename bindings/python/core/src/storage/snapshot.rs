use std::{
  io,
  pin::Pin,
  task::{Context, Poll}, hash::{DefaultHasher, Hasher, Hash},
};

use crate::{RaftData, Pyi};
use futures::{io::Cursor, AsyncRead};
use nodecraft::{NodeId, NodeAddress};
use pyo3::{*, pyclass::CompareOp, exceptions::PyTypeError};

#[derive(Debug, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[pyclass(frozen)]
pub struct SnapshotId(ruraft_core::storage::SnapshotId);

impl From<ruraft_core::storage::SnapshotId> for SnapshotId {
  fn from(value: ruraft_core::storage::SnapshotId) -> Self {
    Self(value)
  }
}

impl From<SnapshotId> for ruraft_core::storage::SnapshotId {
  fn from(value: SnapshotId) -> Self {
    value.0
  }
}

impl Pyi for SnapshotId {
  fn pyi() -> std::borrow::Cow<'static, str> {
    r#"

class SnapshotId:
  @property
  def index(self) -> int: ...

  @property
  def term(self) -> int: ...

  @property
  def timestamp(self) -> int: ...

  def __str__(self) -> str: ...

  def __repr__(self) -> str: ...

  def __hash__(self) -> int: ...

  def __richcmp__(self, other: SnapshotId, op) -> bool: ...

"#
    .into()
  }
}

#[pymethods]
impl SnapshotId {
  #[getter]
  pub fn index(&self) -> u64 {
    self.0.index()
  }

  #[getter]
  pub fn term(&self) -> u64 {
    self.0.term()
  }

  #[getter]
  pub fn timestamp(&self) -> u64 {
    self.0.timestamp()
  }

  pub fn __str__(&self) -> String {
    if cfg!(feature = "serde") {
      serde_json::to_string(&self.0).unwrap()
    } else {
      format!("{:?}", self.0)
    }
  }

  pub fn __repr__(&self) -> String {
    format!("{:?}", self.0)
  }

  fn __hash__(&self) -> u64 {
    let mut hasher = DefaultHasher::new();
    self.0.hash(&mut hasher);
    hasher.finish()
  }

  fn __richcmp__(&self, other: &Self, op: CompareOp) -> bool {
    match op {
      CompareOp::Lt => self.0 < other.0,
      CompareOp::Le => self.0 <= other.0,
      CompareOp::Eq => self.0 == other.0,
      CompareOp::Ne => self.0 != other.0,
      CompareOp::Gt => self.0 > other.0,
      CompareOp::Ge => self.0 >= other.0,
    }
  }
}

/// The meta data for the snapshot file
#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[pyclass(frozen)]
pub struct SnapshotMeta(ruraft_core::storage::SnapshotMeta<NodeId, NodeAddress>);

impl From<ruraft_core::storage::SnapshotMeta<NodeId, NodeAddress>> for SnapshotMeta {
  fn from(s: ruraft_core::storage::SnapshotMeta<NodeId, NodeAddress>) -> Self {
    Self(s)
  }
}

impl From<SnapshotMeta> for ruraft_core::storage::SnapshotMeta<NodeId, NodeAddress> {
  fn from(s: SnapshotMeta) -> Self {
    s.0
  }
}

impl Pyi for SnapshotMeta {
  fn pyi() -> std::borrow::Cow<'static, str> {
    r#"

class SnapshotMeta:
  @property
  def index(self) -> int: ...
 
  @property
  def term(self) -> int: ...
  
  @property
  def timestamp(self) -> int: ...
  
  @property
  def size(self) -> int: ...
  
  @property
  def membership_index(self) -> int: ...

  def membership(self) -> Membership: ...
  
  @property
  def size(self) -> int: ...

  def __str__(self) -> str: ...

  def __repr__(self) -> str: ... 

  def __richcmp__(self, other: SnapshotId, op) -> bool: ...

"#
    .into()
  }
}

#[pymethods]
impl SnapshotMeta {
  /// The term when the snapshot was taken.
  #[getter]
  pub fn term(&self) -> u64 {
    self.0.term()
  }

  /// The index when the snapshot was taken.
  #[getter]
  pub fn index(&self) -> u64 {
    self.0.index()
  }

  /// The timestamp when the snapshot was taken.
  #[getter]
  pub fn timestamp(&self) -> u64 {
    self.0.timestamp()
  }

  /// The size of the snapshot, in bytes.
  #[getter]
  pub fn size(&self) -> u64 {
    self.0.size()
  }

  /// The index of the membership when the snapshot was taken.
  #[getter]
  pub fn membership_index(&self) -> u64 {
    self.0.membership_index()
  }

  /// The membership at the time when the snapshot was taken.
  pub fn membership(&self) -> crate::types::Membership {
    self.0.membership().clone().into()
  }

  #[inline]
  pub fn __str__(&self) -> PyResult<String> {
    if cfg!(feature = "serde") {
      serde_json::to_string(&self.0).map_err(|e| PyTypeError::new_err(e.to_string()))
    } else {
      Ok(format!("{:?}", self.0))
    }
  }

  #[inline]
  pub fn __repr__(&self) -> String {
    format!("{:?}", self.0)
  }

  fn __richcmp__(&self, other: &Self, op: CompareOp) -> bool {
    match op {
      CompareOp::Lt => self.0 < other.0,
      CompareOp::Le => self.0 <= other.0,
      CompareOp::Eq => self.0 == other.0,
      CompareOp::Ne => self.0 != other.0,
      CompareOp::Gt => self.0 > other.0,
      CompareOp::Ge => self.0 >= other.0,
    }
  }
}

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
  use crate::Pyi;

  use super::*;
  use ::tokio::fs::File;
  use pyo3_asyncio::tokio::future_into_py;
  use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};
  use futures::Future;

  pyo3io_macros::async_reader!(future_into_py("AsyncReader": AsyncReaderInner<Compat<File>> {
    #[staticmethod]
    pub fn memory(buf: &[u8]) -> Self {
      Self::new(AsyncReaderInner::Memory(Cursor::new(RaftData::from(buf))))
    }

    #[staticmethod]
    pub fn file(path: std::path::PathBuf) -> PyResult<Self> {
      std::fs::File::open(path)
        .map(|f| Self::new(AsyncReaderInner::File(File::from_std(f).compat())))
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }
  }));

  impl Pyi for AsyncReader {
    fn pyi() -> std::borrow::Cow<'static, str> {
      r#"

class AsyncReader(AsyncRead):
  def memory(self, src: bytes) -> AsyncReader: ...

  def file(self, path: str) -> AsyncReader: ...

  def __aenter__(self) -> AsyncReader: ...

  def __aexit__(self, exc_type, exc_value, traceback) -> None: ...

      "#
      .into()
    }
  }

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

  impl Pyi for Snapshot {
    fn pyi() -> std::borrow::Cow<'static, str> {
      r#"

class Snapshot(AsyncRead):
  def __aenter__(self) -> Snapshot: ...

  def __aexit__(self, exc_type, exc_value, traceback) -> None: ...

      "#
      .into()
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

  impl Pyi for SnapshotSink {
    fn pyi() -> std::borrow::Cow<'static, str> {
      r#"

class SnapshotSink(AsyncWrite):
  def id(self) -> SnapshotId: ...

  async def cancel(self) -> None: ...

  def __aenter__(self) -> SnapshotSink: ...

  def __aexit__(self, exc_type, exc_value, traceback) -> None: ...

      "#
      .into()
    }
  }

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

  use crate::Pyi;

  use super::*;

  pyo3io_macros::async_reader!(future_into_py("AsyncReader": AsyncReaderInner<File> {
    #[staticmethod]
    pub fn memory(buf: &[u8]) -> Self {
      Self::new(AsyncReaderInner::Memory(Cursor::new(RaftData::from(buf))))
    }

    #[staticmethod]
    pub fn file(path: std::path::PathBuf) -> PyResult<Self> {
      std::fs::File::open(path)
        .map(|f| Self::new(AsyncReaderInner::File(File::from(f))))
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }
  }));

  pyo3io_macros::async_reader!(future_into_py("Snapshot": ruraft_bindings_common::storage::SupportedSnapshot));

  impl Pyi for AsyncReader {
    fn pyi() -> std::borrow::Cow<'static, str> {
      r#"

class AsyncReader(AsyncRead):
  def memory(self, src: bytes) -> AsyncReader: ...

  def file(self, path: str) -> AsyncReader: ...

  def __aenter__(self) -> AsyncReader: ...

  def __aexit__(self, exc_type, exc_value, traceback) -> None: ...

      "#
      .into()
    }
  }

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

  impl Pyi for Snapshot {
    fn pyi() -> std::borrow::Cow<'static, str> {
      r#"

class Snapshot(AsyncRead):
  def __aenter__(self) -> Snapshot: ...

  def __aexit__(self, exc_type, exc_value, traceback) -> None: ...

      "#
      .into()
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

  impl Pyi for SnapshotSink {
    fn pyi() -> std::borrow::Cow<'static, str> {
      r#"

class SnapshotSink(AsyncWrite):
  def id(self) -> SnapshotId: ...

  async def cancel(self) -> None: ...

  def __aenter__(self) -> SnapshotSink: ...

  def __aexit__(self, exc_type, exc_value, traceback) -> None: ...

      "#
      .into()
    }
  }

  impl From<ruraft_bindings_common::storage::SupportedSnapshotSink> for SnapshotSink {
    fn from(s: ruraft_bindings_common::storage::SupportedSnapshotSink) -> Self {
      Self::new(s)
    }
  }
}
