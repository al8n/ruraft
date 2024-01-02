use std::hash::{DefaultHasher, Hash, Hasher};

use pyo3::{prelude::*, pyclass::CompareOp};
use ruraft_core::storage::SnapshotId as RSnapshotId;

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

#[cfg(feature = "tokio")]
pub mod tokio;

#[cfg(feature = "async-std")]
pub mod async_std;

