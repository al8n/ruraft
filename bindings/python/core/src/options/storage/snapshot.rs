use std::hash::{Hash, Hasher};

use pyo3::{exceptions::PyTypeError, types::PyModule, *};
use ruraft_bindings_common::storage::SnapshotStorageOptions as SupportedSnapshotStorageOptions;

/// Configurations for a `SnapshotStorage`
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
#[pyclass(frozen)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct SnapshotStorageOptions(SupportedSnapshotStorageOptions);

impl From<SnapshotStorageOptions> for SupportedSnapshotStorageOptions {
  fn from(value: SnapshotStorageOptions) -> Self {
    value.0
  }
}

#[pymethods]
impl SnapshotStorageOptions {
  /// Constructor a file-based snapshot storage
  ///
  /// ### Example
  ///
  /// ```python
  /// from prafty.options import SnapshotStorageOptions
  ///
  /// opts = SnapshotStorageOptions.file(options.FileSnapshotStorageOptions("/path/to/directory", 5))
  /// ```
  #[staticmethod]
  pub fn file(opts: FileSnapshotStorageOptions) -> Self {
    Self(SupportedSnapshotStorageOptions::File(opts.into()))
  }

  /// Constructor a memory-based snapshot storage
  ///
  /// ### Example
  ///
  /// ```python
  /// from prafty.options import SnapshotStorageOptions
  ///
  /// opts = SnapshotStorageOptions.memory()
  /// ```
  #[staticmethod]
  pub fn memory() -> Self {
    Self(SupportedSnapshotStorageOptions::Memory)
  }

  fn __eq__(&self, other: &Self) -> bool {
    self.0 == other.0
  }

  fn __ne__(&self, other: &Self) -> bool {
    self.0 != other.0
  }

  fn __hash__(&self) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    self.0.hash(&mut hasher);
    hasher.finish()
  }

  fn __str__(&self) -> PyResult<String> {
    if cfg!(feature = "serde") {
      serde_json::to_string(&self.0).map_err(|e| PyTypeError::new_err(e.to_string()))
    } else {
      Ok(format!("{:?}", self.0))
    }
  }

  fn __repr__(&self) -> String {
    format!("{:?}", self.0)
  }
}

/// Configurations for `FileSnapshotStorageOptions`
#[derive(Clone)]
#[pyclass(frozen)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct FileSnapshotStorageOptions(ruraft_snapshot::sync::FileSnapshotStorageOptions);

impl core::fmt::Debug for FileSnapshotStorageOptions {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{:?}", self.0)
  }
}

impl From<FileSnapshotStorageOptions> for ruraft_snapshot::sync::FileSnapshotStorageOptions {
  fn from(value: FileSnapshotStorageOptions) -> Self {
    value.0
  }
}

#[pymethods]
impl FileSnapshotStorageOptions {
  /// Constructor a file system based snapshot storage
  ///
  /// ### Example
  ///
  /// ```python
  /// from prafty import options
  ///
  /// opts = options.FileSnapshotStorageOptions("/path/to/directory", 5)
  /// ```
  #[new]
  pub fn new(base: std::path::PathBuf, retain: usize) -> Self {
    Self(ruraft_snapshot::sync::FileSnapshotStorageOptions::new(
      base, retain,
    ))
  }

  /// Returns the the base directory for snapshots
  #[getter]
  pub fn base(&self) -> &std::path::PathBuf {
    self.0.base()
  }

  /// Get the number of snapshots should be retained
  #[getter]
  pub fn retain(&self) -> usize {
    self.0.retain()
  }

  fn __str__(&self) -> PyResult<String> {
    if cfg!(feature = "serde") {
      serde_json::to_string(&self.0).map_err(|e| PyTypeError::new_err(e.to_string()))
    } else {
      Ok(format!("{:?}", self.0))
    }
  }

  fn __repr__(&self) -> String {
    format!("{:?}", self.0)
  }
}

pub fn register_snapshot_storage_options(module: &PyModule) -> PyResult<()> {
  module.add_class::<SnapshotStorageOptions>()?;
  module.add_class::<FileSnapshotStorageOptions>()?;
  Ok(())
}
