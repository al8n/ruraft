use std::{
  hash::{DefaultHasher, Hash, Hasher},
  marker::PhantomData,
  path::PathBuf,
};

use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use nodecraft::{NodeAddress, NodeId};
use pyo3::{
  exceptions::{PyIOError, PyTypeError},
  prelude::*,
  pyclass::CompareOp,
  types::PyBytes,
};
use ruraft_core::storage::{SnapshotId as RSnapshotId, SnapshotMeta as RSnapshotMeta};
use ruraft_snapshot::sync::FileSnapshotStorageOptions as RFileSnapshotStorageOptions;
use smallvec::SmallVec;

use crate::{FearlessCell, IntoSupportedRuntime, INLINED_U8};

#[derive(Debug, Clone, Copy)]
#[pyclass]
pub struct SnapshotId(RSnapshotId);

impl From<RSnapshotId> for SnapshotId {
  fn from(value: RSnapshotId) -> Self {
    Self(value)
  }
}

impl From<SnapshotId> for RSnapshotId {
  fn from(value: SnapshotId) -> Self {
    value.0
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
    format!("{}", self.0)
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
#[pyclass]
pub struct SnapshotMeta(RSnapshotMeta<NodeId, NodeAddress>);

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

/// Configurations for `FileSnapshotStorageOptions`
#[derive(Clone)]
#[pyclass]
pub struct FileSnapshotStorageOptions(RFileSnapshotStorageOptions);

impl From<FileSnapshotStorageOptions> for RFileSnapshotStorageOptions {
  fn from(value: FileSnapshotStorageOptions) -> Self {
    value.0
  }
}

#[pymethods]
impl FileSnapshotStorageOptions {
  /// Constructor
  #[new]
  pub fn new(path: PathBuf, retain: usize) -> Self {
    Self(RFileSnapshotStorageOptions::new(path, retain))
  }

  /// Returns the the base directory for snapshots
  #[getter]
  pub fn path(&self) -> &PathBuf {
    self.0.base()
  }

  /// Get the number of snapshots should be retained
  #[getter]
  pub fn retain(&mut self) -> usize {
    self.0.retain()
  }
}

pub struct SnapshotSink<R> {
  id: SnapshotId,
  sink: FearlessCell<Box<dyn AsyncWrite + Send + Sync + Unpin + 'static>>,
  _marker: PhantomData<R>,
}

impl<R> Clone for SnapshotSink<R> {
  fn clone(&self) -> Self {
    Self {
      sink: self.sink.clone(),
      ..*self
    }
  }
}

impl<R> SnapshotSink<R> {
  pub(crate) fn new(
    id: SnapshotId,
    writer: FearlessCell<Box<dyn AsyncWrite + Send + Sync + Unpin + 'static>>,
  ) -> Self {
    Self {
      id,
      sink: writer,
      _marker: PhantomData,
    }
  }
}

impl<S: ruraft_core::storage::SnapshotSink> From<S> for SnapshotSink<S::Runtime> {
  fn from(value: S) -> Self {
    let id = value.id();
    Self {
      id: id.into(),
      sink: FearlessCell::new(Box::new(value) as Box<_>),
      _marker: PhantomData,
    }
  }
}

impl<R: IntoSupportedRuntime> SnapshotSink<R> {
  fn write<'a>(&self, py: Python<'a>, bytes: &'a PyBytes) -> PyResult<&'a PyAny> {
    let this = self.sink.clone();
    let buf = SmallVec::<[u8; INLINED_U8]>::from_slice(bytes.as_bytes());
    R::into_supported().future_into_py(py, async move {
      let sink = unsafe { this.get_mut() };
      let written = sink
        .write(&buf)
        .await
        .map_err(|err| PyErr::new::<PyIOError, _>(err.to_string()))?;
      Ok(written)
    })
  }

  fn write_all<'a>(&self, py: Python<'a>, bytes: &'a PyBytes) -> PyResult<&'a PyAny> {
    let this = self.sink.clone();
    let buf = SmallVec::<[u8; INLINED_U8]>::from_slice(bytes.as_bytes());
    R::into_supported().future_into_py(py, async move {
      let sink = unsafe { this.get_mut() };
      sink
        .write_all(&buf)
        .await
        .map_err(|err| PyErr::new::<PyIOError, _>(err.to_string()))?;
      Ok(())
    })
  }

  fn id(&self) -> SnapshotId {
    self.id
  }
}

pub struct SnapshotSource<R> {
  meta: RSnapshotMeta<NodeId, NodeAddress>,
  source: FearlessCell<Box<dyn AsyncRead + Send + Sync + Unpin + 'static>>,
  _marker: PhantomData<R>,
}

impl<S: ruraft_core::storage::SnapshotSource<Id = NodeId, Address = NodeAddress>> From<S>
  for SnapshotSource<S::Runtime>
{
  fn from(value: S) -> Self {
    let meta = value.meta().clone();
    Self {
      meta,
      source: FearlessCell::new(Box::new(value) as Box<_>),
      _marker: PhantomData,
    }
  }
}

impl<R> Clone for SnapshotSource<R> {
  fn clone(&self) -> Self {
    Self {
      meta: self.meta.clone(),
      source: self.source.clone(),
      ..*self
    }
  }
}

impl<R> SnapshotSource<R> {
  pub(crate) fn new(
    meta: RSnapshotMeta<NodeId, NodeAddress>,
    reader: FearlessCell<Box<dyn AsyncRead + Send + Sync + Unpin + 'static>>,
  ) -> Self {
    Self {
      meta,
      source: reader,
      _marker: PhantomData,
    }
  }
}

impl<R: IntoSupportedRuntime> SnapshotSource<R> {
  /// The term when the snapshot was taken.
  pub fn term(&self) -> u64 {
    self.meta.term()
  }

  /// The index when the snapshot was taken.
  pub fn index(&self) -> u64 {
    self.meta.index()
  }

  /// The timestamp when the snapshot was taken.
  pub fn timestamp(&self) -> u64 {
    self.meta.timestamp()
  }

  /// The size of the snapshot, in bytes.
  pub fn size(&self) -> u64 {
    self.meta.size()
  }

  /// The index of the membership when the snapshot was taken.
  pub fn membership_index(&self) -> u64 {
    self.meta.membership_index()
  }

  /// The membership at the time when the snapshot was taken.
  pub fn membership(&self) -> crate::types::Membership {
    self.meta.membership().clone().into()
  }

  /// Read data from the snapshot to bytes.
  // #[pyo3(signature = (chunk_size = 1024))]
  pub fn read<'a>(&'a self, py: Python<'a>, chunk_size: usize) -> PyResult<&'a PyAny> {
    let source = self.source.clone();
    R::into_supported().future_into_py(py, async move {
      let source = unsafe { source.get_mut() };
      let mut buf: SmallVec<[u8; INLINED_U8]> = ::smallvec::smallvec![0; chunk_size];
      match source.read(&mut buf).await {
        Ok(_) => {
          buf.shrink_to_fit();
          Ok(buf)
        }
        Err(e) => Err(PyErr::new::<PyIOError, _>(format!("{:?}", e))),
      }
    })
  }

  /// Read exact num of bytes from the snapshot to bytes.
  pub fn read_exact<'a>(&'a self, py: Python<'a>, size: usize) -> PyResult<&'a PyAny> {
    let source = self.source.clone();
    R::into_supported().future_into_py(py, async move {
      let mut buf = SmallVec::<[u8; INLINED_U8]>::with_capacity(size);
      unsafe {
        source
          .get_mut()
          .read_exact(&mut buf)
          .await
          .map(|_| buf)
          .map_err(|e| PyErr::new::<PyIOError, _>(format!("{:?}", e)))
      }
    })
  }

  /// Read all data from the snapshot to bytes.
  // #[pyo3(signature = (chunk_size = 1024))]
  pub fn read_all<'a>(&'a self, py: Python<'a>, chunk_size: usize) -> PyResult<&'a PyAny> {
    let source = self.source.clone();
    R::into_supported().future_into_py(py, async move {
      let source = unsafe { source.get_mut() };
      let mut buf: SmallVec<[u8; INLINED_U8]> = Default::default();

      loop {
        // Resize buffer to accommodate new chunk
        let old_len = buf.len();
        buf.resize(old_len + chunk_size, 0);

        match source.read(&mut buf[old_len..]).await {
          Ok(0) => {
            buf.truncate(old_len); // Truncate buffer to actual data length
            break; // End of file
          }
          Ok(n) => {
            buf.truncate(old_len + n); // Truncate buffer to actual data length
          }
          Err(e) => return Err(PyErr::new::<PyIOError, _>(format!("{:?}", e))),
        }
      }
      Ok(buf)
    })
  }
}

macro_rules! wrap_sink {
  ($rt:ident) => {
    paste::paste! {
      #[derive(Clone)]
      #[::pyo3::pyclass(name = "SnapshotSink")]
      pub struct [< $rt SnapshotSink >](super::SnapshotSink< agnostic:: [< $rt:snake >] :: [< $rt Runtime >] >);

      impl crate::IntoPython for super::SnapshotSink< agnostic:: [< $rt:snake >] :: [< $rt Runtime >] > {
        type Target = [< $rt SnapshotSink >];

        fn into_python(self) -> Self::Target {
          [< $rt SnapshotSink >] (self)
        }
      }

      impl crate::FromPython for [< $rt SnapshotSink >] {
        type Source = super::SnapshotSink< agnostic:: [< $rt:snake >] :: [< $rt Runtime >] >;

        fn from_python(slf: Self::Source,) -> [< $rt SnapshotSink >] {
          [< $rt SnapshotSink >] (slf)
        }
      }

      impl [< $rt SnapshotSink >] {
        pub fn new(
          id: super::SnapshotId,
          writer: crate::FearlessCell<Box<dyn ::futures::AsyncWrite + Send + Sync + Unpin + 'static>>,
        ) -> Self {
          Self(super::SnapshotSink { id, sink: writer, _marker: ::core::marker::PhantomData })
        }

        pub fn from_rust<S>(val: S) -> Self
        where
          S: ruraft_core::storage::SnapshotSink<Runtime = agnostic:: [< $rt:snake >] :: [< $rt Runtime >] >,
        {
          crate::IntoPython::into_python(super::SnapshotSink::<S::Runtime>::from(val))
        }
      }

      #[::pyo3::pymethods]
      impl [< $rt SnapshotSink >] {
        pub fn id(&self) -> super::SnapshotId {
          self.0.id()
        }

        pub fn write<'a>(&self, py: ::pyo3::Python<'a>, bytes: &'a ::pyo3::types::PyBytes) -> ::pyo3::PyResult<&'a ::pyo3::PyAny> {
          self.0.write(py, bytes)
        }

        pub fn write_all<'a>(&self, py: ::pyo3::Python<'a>, bytes: &'a ::pyo3::types::PyBytes) -> ::pyo3::PyResult<&'a ::pyo3::PyAny> {
          self.0.write_all(py, bytes)
        }
      }
    }
  };
}

macro_rules! wrap_source {
  ($rt: ident) => {
    paste::paste! {
      #[::pyo3::pyclass(name = "SnapshotSource")]
      #[derive(Clone)]
      pub struct [< $rt SnapshotSource >] (super::SnapshotSource<agnostic:: [< $rt:snake >] :: [< $rt Runtime >] >);

      impl crate::IntoPython for super::SnapshotSource<agnostic:: [< $rt:snake >] :: [< $rt Runtime >] > {
        type Target = [< $rt SnapshotSource >];

        fn into_python(self) -> Self::Target {
          [< $rt SnapshotSource >] (self)
        }
      }

      impl crate::FromPython for [< $rt SnapshotSource >] {
        type Source = super::SnapshotSource<agnostic:: [< $rt:snake >] :: [< $rt Runtime >] >;

        fn from_python(slf: Self::Source,) -> [< $rt SnapshotSource >] {
          [< $rt SnapshotSource >] (slf)
        }
      }

      impl [< $rt SnapshotSource >] {
        pub fn new(
          meta: ruraft_core::storage::SnapshotMeta<nodecraft::NodeId, nodecraft::NodeAddress>,
          reader: crate::FearlessCell<Box<dyn futures::AsyncRead + Send + Sync + Unpin + 'static>>,
        ) -> Self {
          Self(super::SnapshotSource {
            meta,
            source: reader,
            _marker: core::marker::PhantomData,
          })
        }

        pub fn from_rust<S>(val: S) -> Self
        where
          S: ruraft_core::storage::SnapshotSource<Id = nodecraft::NodeId, Address = nodecraft::NodeAddress, Runtime = agnostic:: [< $rt:snake >] :: [< $rt Runtime >] >,
        {
          crate::IntoPython::into_python(super::SnapshotSource::<S::Runtime>::from(val))
        }
      }

      #[::pyo3::pymethods]
      impl [< $rt SnapshotSource >] {
        /// The term when the snapshot was taken.
        pub fn term(&self) -> u64 {
          self.0.term()
        }

        /// The index when the snapshot was taken.
        pub fn index(&self) -> u64 {
          self.0.index()
        }

        /// The timestamp when the snapshot was taken.
        pub fn timestamp(&self) -> u64 {
          self.0.timestamp()
        }

        /// The size of the snapshot, in bytes.
        pub fn size(&self) -> u64 {
          self.0.size()
        }

        /// The index of the membership when the snapshot was taken.
        pub fn membership_index(&self) -> u64 {
          self.0.membership_index()
        }

        /// The membership at the time when the snapshot was taken.
        pub fn membership(&self) -> crate::types::Membership {
          self.0.membership().clone().into()
        }

        /// Read data from the snapshot to bytes.
        #[pyo3(signature = (chunk_size = 1024))]
        pub fn read<'a>(&'a self, py: ::pyo3::Python<'a>, chunk_size: usize) -> ::pyo3::PyResult<&'a ::pyo3::PyAny> {
          self.0.read(py, chunk_size)
        }

        /// Read exact num of bytes from the snapshot to bytes.
        pub fn read_exact<'a>(&'a self, py: ::pyo3::Python<'a>, size: usize) -> ::pyo3::PyResult<&'a ::pyo3::PyAny> {
          self.0.read_exact(py, size)
        }

        /// Read all data from the snapshot to bytes.
        #[pyo3(signature = (chunk_size = 1024))]
        pub fn read_all<'a>(&'a self, py: ::pyo3::Python<'a>, chunk_size: usize) -> ::pyo3::PyResult<&'a ::pyo3::PyAny> {
          self.0.read_all(py, chunk_size)
        }
      }
    }
  };
}

#[cfg(feature = "tokio")]
pub mod tokio {
  wrap_sink!(Tokio);
  wrap_source!(Tokio);
}

#[cfg(feature = "async-std")]
pub mod async_std {
  wrap_sink!(AsyncStd);
  wrap_source!(AsyncStd);
}
