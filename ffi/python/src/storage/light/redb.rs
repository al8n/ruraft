use std::{
  hash::{DefaultHasher, Hash, Hasher},
  path::PathBuf,
  sync::Arc,
};

use pyo3::prelude::*;
use ruraft_lightwal::redb::{Db as RustDb, DbOptions as RustDbOptions};

/// Options used to create Db.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[pyclass]
pub struct DbOptions {
  /// Sets the cache size.
  cache_size: usize,
  /// Sets the path to the database.
  path: Arc<PathBuf>,
}

impl From<DbOptions> for RustDbOptions {
  fn from(db_options: DbOptions) -> Self {
    RustDbOptions::new(db_options.path.as_ref()).with_cache_size(db_options.cache_size)
  }
}

#[pymethods]
impl DbOptions {
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
    Ok(format!("{:?}", self))
  }

  #[inline]
  pub fn __repr__(&self) -> PyResult<String> {
    Ok(format!("{:?}", self))
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

#[cfg(feature = "tokio")]
use agnostic::tokio::TokioRuntime as Tokio;

#[cfg(feature = "async-std")]
use agnostic::async_std::AsyncStdRuntime as AsyncStd;

#[cfg(feature = "tokio")]
wal!(Tokio);

#[cfg(feature = "async-std")]
wal!(AsyncStd);

#[pymodule]
fn redb(_py: Python, m: &PyModule) -> PyResult<()> {
  m.add_class::<DbOptions>()?;
  #[cfg(feature = "tokio")]
  m.add_class::<TokioWal>()?;
  #[cfg(feature = "async-std")]
  m.add_class::<AsyncStdWal>()?;
  Ok(())
}

// This function creates and returns the sled submodule.
pub fn submodule(py: Python) -> PyResult<&PyModule> {
  let module = PyModule::new(py, "redb")?;
  redb(py, module)?;
  Ok(module)
}
