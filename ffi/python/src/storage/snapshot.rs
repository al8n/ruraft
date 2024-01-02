use std::hash::{DefaultHasher, Hash, Hasher};

use nodecraft::{NodeAddress, NodeId};
use pyo3::{exceptions::PyTypeError, prelude::*, pyclass::CompareOp};
use ruraft_core::storage::{SnapshotId as RSnapshotId, SnapshotMeta};

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
pub struct FileSnapshotMeta(SnapshotMeta<NodeId, NodeAddress>);

#[pymethods]
impl FileSnapshotMeta {
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

macro_rules! impl_source_sink {
  () => {
    #[derive(Clone)]
    #[pyclass]
    pub struct FileSnapshotSink {
      id: SnapshotId,
      sink: Arc<Mutex<dyn AsyncWrite + Send + Sync + Unpin + 'static>>,
    }

    impl FileSnapshotSink {
      pub fn new(
        id: SnapshotId,
        writer: Arc<Mutex<impl AsyncWrite + Send + Sync + Unpin + 'static>>,
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
          let mut sink = this.lock().await;
          let readed = sink
            .write(&buf)
            .await
            .map_err(|err| PyErr::new::<PyIOError, _>(err.to_string()))?;
          Ok(readed)
        })
      }

      fn write_all<'a>(&self, py: Python<'a>, bytes: &'a PyBytes) -> PyResult<&'a PyAny> {
        let this = self.sink.clone();
        let buf = SmallVec::<[u8; INLINED_U8]>::from_slice(bytes.as_bytes());
        future_into_py(py, async move {
          let mut sink = this.lock().await;
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
      meta: SnapshotMeta<NodeId, NodeAddress>,
      source: Arc<Mutex<dyn AsyncRead + Send + Sync + Unpin + 'static>>,
    }

    impl FileSnapshotSource {
      pub fn new(
        meta: SnapshotMeta<NodeId, NodeAddress>,
        reader: Arc<Mutex<impl AsyncRead + Send + Sync + Unpin + 'static>>,
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
          let mut source = source.lock().await;
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
          source
            .lock()
            .await
            .read_exact(&mut buf)
            .await
            .map(|_| buf)
            .map_err(|e| PyErr::new::<PyIOError, _>(format!("{:?}", e)))
        })
      }

      /// Read all data from the snapshot to bytes.
      #[pyo3(signature = (chunk_size = 1024))]
      pub fn read_all<'a>(&'a self, py: Python<'a>, chunk_size: usize) -> PyResult<&'a PyAny> {
        let source = self.source.clone();
        future_into_py(py, async move {
          let mut source = source.lock().await;
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
