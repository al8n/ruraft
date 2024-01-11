use std::{path::PathBuf, sync::Arc};

use ruraft_lightwal::redb::DbOptions as RustDbOptions;

/// Options used to create Db.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
pub struct RedbOptions {
  /// Sets the cache size.
  cache_size: usize,
  /// Sets the path to the database.
  path: Arc<PathBuf>,
}

impl From<RedbOptions> for RustDbOptions {
  fn from(db_options: RedbOptions) -> Self {
    RustDbOptions::new(db_options.path.as_ref()).with_cache_size(db_options.cache_size)
  }
}

impl RedbOptions {
  /// Returns the default configuration.
  pub fn new(path: PathBuf) -> Self {
    Self {
      cache_size: 0,
      path: Arc::new(path),
    }
  }

  /// Sets the cache size.
  pub fn set_cache_size(&mut self, cache_size: usize) {
    self.cache_size = cache_size;
  }

  /// Returns the cache size.
  pub fn cache_size(&self) -> usize {
    self.cache_size
  }

  /// Sets the path of the database (builder).
  pub fn set_path(&mut self, path: PathBuf) {
    self.path = Arc::new(path);
  }

  /// Returns the path of the database (builder).
  pub fn path(&self) -> &PathBuf {
    &self.path
  }
}
