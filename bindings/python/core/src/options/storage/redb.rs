use std::{
  hash::{DefaultHasher, Hash, Hasher},
  path::PathBuf,
  sync::Arc,
};

use pyo3::{exceptions::PyTypeError, prelude::*};
use ruraft_bindings_common::storage::*;

use crate::Pyi;

/// Options used to create Db.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[pyclass(name = "RedbOptions")]
pub struct PythonRedbOptions {
  /// Sets the cache size.
  cache_size: usize,
  /// Sets the path to the database.
  path: Arc<PathBuf>,
}

impl From<PythonRedbOptions> for RedbOptions {
  fn from(db_options: PythonRedbOptions) -> Self {
    let mut opts = RedbOptions::new(db_options.path.as_ref().clone());
    opts.set_cache_size(db_options.cache_size);
    opts
  }
}

impl Pyi for PythonRedbOptions {
  fn pyi() -> std::borrow::Cow<'static, str> {
r#"

class RedbOptions:
  @property
  def cache_size(self) -> int:...
  
  @cache_size.setter
  def cache_size(self, value: int) -> None:...
  
  @property
  def path(self) -> PathLike:...
  
  @path.setter
  def path(self, value: str) -> None:...

  def __eq__(self, __value: RedbOptions) -> bool: ...
  
  def __ne__(self, __value: RedbOptions) -> bool: ...
  
  def __hash__(self) -> int: ...
  
  def __str__(self) -> str: ...
  
  def __repr__(self) -> str: ...

"#.into()
  }
}

#[pymethods]
impl PythonRedbOptions {
  /// Returns the default configuration.
  #[new]
  pub fn new(path: PathBuf) -> Self {
    Self {
      cache_size: 0,
      path: Arc::new(path),
    }
  }

  /// Sets the cache size.
  #[setter]
  pub fn set_cache_size(&mut self, cache_size: usize) {
    self.cache_size = cache_size;
  }

  /// Returns the cache size.
  #[getter]
  pub fn cache_size(&self) -> usize {
    self.cache_size
  }

  /// Sets the path of the database (builder).
  #[setter]
  pub fn set_path(&mut self, path: PathBuf) {
    self.path = Arc::new(path);
  }

  /// Returns the path of the database (builder).
  #[getter]
  pub fn path(&self) -> &PathBuf {
    &self.path
  }

  #[inline]
  pub fn __str__(&self) -> PyResult<String> {
    if cfg!(feature = "serde") {
      serde_json::to_string(self).map_err(|e| PyTypeError::new_err(e.to_string()))
    } else {
      Ok(format!("{:?}", self))
    }
  }

  #[inline]
  pub fn __repr__(&self) -> String {
    format!("{:?}", self)
  }

  pub fn __eq__(&self, other: &Self) -> bool {
    self.eq(other)
  }

  pub fn __ne__(&self, other: &Self) -> bool {
    self.ne(other)
  }

  pub fn __hash__(&self) -> u64 {
    let mut hasher = DefaultHasher::new();
    self.hash(&mut hasher);
    hasher.finish()
  }
}
