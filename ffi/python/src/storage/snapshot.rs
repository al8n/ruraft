use std::{
  hash::{DefaultHasher, Hash, Hasher},
  path::PathBuf,
};

use nodecraft::{NodeAddress, NodeId};
use pyo3::{exceptions::PyTypeError, prelude::*, pyclass::CompareOp};
use ruraft_core::storage::{SnapshotId as RSnapshotId, SnapshotMeta as RSnapshotMeta};
use ruraft_snapshot::sync::FileSnapshotStorageOptions as RFileSnapshotStorageOptions;

#[derive(Debug, Clone, Copy)]
#[pyclass]
pub struct SnapshotId(RSnapshotId);

impl From<RSnapshotId> for SnapshotId {
  fn from(value: RSnapshotId) -> Self {
    Self(value)
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

macro_rules! impl_source_sink {
  () => {
    #[derive(Clone)]
    #[pyclass]
    pub struct FileSnapshotSink {
      id: SnapshotId,
      sink: FearlessCell<Box<dyn AsyncWrite + Send + Sync + Unpin + 'static>>,
    }

    impl FileSnapshotSink {
      pub(crate) fn new(
        id: SnapshotId,
        writer: FearlessCell<Box<dyn AsyncWrite + Send + Sync + Unpin + 'static>>,
      ) -> Self {
        Self { id, sink: writer }
      }
    }

    #[pymethods]
    impl FileSnapshotSink {
      fn write<'a>(&self, py: Python<'a>, bytes: &'a PyBytes) -> PyResult<&'a PyAny> {
        let this = self.sink.clone();
        let buf = SmallVec::<[u8; INLINED_U8]>::from_slice(bytes.as_bytes());
        future_into_py(py, async move {
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
        future_into_py(py, async move {
          let sink = unsafe { this.get_mut() };
          sink
            .write_all(&buf)
            .await
            .map_err(|err| PyErr::new::<PyIOError, _>(err.to_string()))?;
          Ok(())
        })
      }

      #[getter]
      fn id(&self) -> SnapshotId {
        self.id
      }
    }

    #[derive(Clone)]
    #[pyclass]
    pub struct FileSnapshotSource {
      meta: RSnapshotMeta<NodeId, NodeAddress>,
      source: FearlessCell<Box<dyn AsyncRead + Send + Sync + Unpin + 'static>>,
    }

    impl FileSnapshotSource {
      pub(crate) fn new(
        meta: RSnapshotMeta<NodeId, NodeAddress>,
        reader: FearlessCell<Box<dyn AsyncRead + Send + Sync + Unpin + 'static>>,
      ) -> Self {
        Self {
          meta,
          source: reader,
        }
      }
    }

    #[pymethods]
    impl FileSnapshotSource {
      /// The term when the snapshot was taken.
      #[getter]
      pub fn term(&self) -> u64 {
        self.meta.term()
      }

      /// The index when the snapshot was taken.
      #[getter]
      pub fn index(&self) -> u64 {
        self.meta.index()
      }

      /// The timestamp when the snapshot was taken.
      #[getter]
      pub fn timestamp(&self) -> u64 {
        self.meta.timestamp()
      }

      /// The size of the snapshot, in bytes.
      #[getter]
      pub fn size(&self) -> u64 {
        self.meta.size()
      }

      /// The index of the membership when the snapshot was taken.
      #[getter]
      pub fn membership_index(&self) -> u64 {
        self.meta.membership_index()
      }

      /// The membership at the time when the snapshot was taken.
      pub fn membership(&self) -> crate::types::Membership {
        self.meta.membership().clone().into()
      }

      /// Read data from the snapshot to bytes.
      #[pyo3(signature = (chunk_size = 1024))]
      pub fn read<'a>(&'a self, py: Python<'a>, chunk_size: usize) -> PyResult<&'a PyAny> {
        let source = self.source.clone();
        future_into_py(py, async move {
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
        future_into_py(py, async move {
          let mut buf = SmallVec::<[u8; INLINED_U8]>::with_capacity(size);
          unsafe { source
            .get_mut()
            .read_exact(&mut buf)
            .await
            .map(|_| buf)
            .map_err(|e| PyErr::new::<PyIOError, _>(format!("{:?}", e)))
          }
        })
      }

      /// Read all data from the snapshot to bytes.
      #[pyo3(signature = (chunk_size = 1024))]
      pub fn read_all<'a>(&'a self, py: Python<'a>, chunk_size: usize) -> PyResult<&'a PyAny> {
        let source = self.source.clone();
        future_into_py(py, async move {
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
  };
}

#[cfg(feature = "tokio")]
pub mod tokio;

#[cfg(feature = "async-std")]
pub mod async_std;
