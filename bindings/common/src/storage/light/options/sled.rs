use ruraft_lightwal::sled::{DbOptions as RustDbOptions, Mode as RustMode};
use std::{path::PathBuf, sync::Arc};

/// The high-level database mode, according to the trade-offs of the RUM conjecture.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
#[repr(u8)]
pub enum SledMode {
  /// In this mode, the database will make decisions that favor using less space instead of supporting the highest possible write throughput. This mode will also rewrite data more frequently as it strives to reduce fragmentation.
  #[default]
  LowSpace = 0,
  /// In this mode, the database will try to maximize write throughput while potentially using more disk space.
  HighThroughput = 1,
}

impl SledMode {
  /// Construct the default mode [`Mode::LowSpace`].
  #[inline]
  pub const fn new() -> Self {
    Self::LowSpace
  }

  /// Low space mode.
  ///
  /// In this mode, the database will make decisions that favor using less space instead of supporting the highest possible write throughput.
  /// This mode will also rewrite data more frequently as it strives to reduce fragmentation.
  #[inline]
  pub fn low_space() -> Self {
    Self::LowSpace
  }

  /// High throughput mode.
  ///
  /// In this mode, the database will try to maximize write throughput while potentially using more disk space.
  #[inline]
  pub fn high_throughput() -> Self {
    Self::HighThroughput
  }
}

/// Top-level configuration for the system.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
pub struct SledOptions {
  /// Set the path of the database (builder).
  path: Arc<PathBuf>,
  /// maximum size in bytes for the system page cache
  cache_capacity: u64,
  /// specify whether the system should run in "low_space" or "high_throughput" mode
  mode: SledMode,
  /// whether to use zstd compression
  use_compression: bool,
  /// the compression factor to use with zstd compression. Ranges from 1 up to 22. Levels >= 20 are ‘ultra’.
  compression_factor: i32,
  /// deletes the database after drop. if no path is set, uses /dev/shm on linux
  temporary: bool,
  /// attempts to exclusively open the database, failing if it already exists
  create_new: bool,
  /// print a performance profile when the Config is dropped
  print_profile_on_drop: bool,
}

impl From<RustDbOptions> for SledOptions {
  fn from(db_options: RustDbOptions) -> Self {
    Self {
      path: db_options.path.clone().into(),
      cache_capacity: db_options.cache_capacity,
      mode: match db_options.mode {
        RustMode::LowSpace => SledMode::LowSpace,
        RustMode::HighThroughput => SledMode::HighThroughput,
      },
      use_compression: db_options.use_compression,
      compression_factor: db_options.compression_factor,
      temporary: db_options.temporary,
      create_new: db_options.create_new,
      print_profile_on_drop: db_options.print_profile_on_drop,
    }
  }
}

impl From<SledOptions> for RustDbOptions {
  fn from(db_options: SledOptions) -> Self {
    RustDbOptions::new()
      .path(db_options.path.as_ref())
      .cache_capacity(db_options.cache_capacity)
      .mode(match db_options.mode {
        SledMode::LowSpace => RustMode::LowSpace,
        SledMode::HighThroughput => RustMode::HighThroughput,
      })
      .use_compression(db_options.use_compression)
      .compression_factor(db_options.compression_factor)
      .temporary(db_options.temporary)
      .create_new(db_options.create_new)
      .print_profile_on_drop(db_options.print_profile_on_drop)
  }
}

impl Default for SledOptions {
  fn default() -> Self {
    Self::new()
  }
}

impl SledOptions {
  /// Returns the default configuration.
  pub fn new() -> Self {
    Self::from(RustDbOptions::default())
  }

  /// Sets the path of the database (builder).
  pub fn set_path(&mut self, path: PathBuf) {
    self.path = Arc::new(path);
  }

  /// Returns the path of the database (builder).
  pub fn path(&self) -> &PathBuf {
    &self.path
  }

  /// Sets maximum size in bytes for the system page cache
  pub fn set_cache_capacity(&mut self, cache_capacity: u64) {
    self.cache_capacity = cache_capacity;
  }

  /// Gets maximum size in bytes for the system page cache
  pub fn cache_capacity(&self) -> u64 {
    self.cache_capacity
  }

  /// Specifies whether the system should run in "low_space" or "high_throughput" mode
  pub fn set_mode(&mut self, mode: SledMode) {
    self.mode = mode;
  }

  /// Gets the mode of the database (builder).
  pub fn mode(&self) -> SledMode {
    self.mode
  }

  /// Sets whether to use zstd compression.
  pub fn set_use_compression(&mut self, use_compression: bool) {
    self.use_compression = use_compression;
  }

  /// Gets whether to use zstd compression.
  pub fn use_compression(&self) -> bool {
    self.use_compression
  }

  /// Sets the compression factor to use with zstd compression. Ranges from 1 up to 22. Levels >= 20 are ‘ultra’.
  pub fn set_compression_factor(&mut self, compression_factor: i32) {
    self.compression_factor = compression_factor;
  }

  /// Gets the compression factor to use with zstd compression. Ranges from 1 up to 22. Levels >= 20 are ‘ultra’.
  pub fn compression_factor(&self) -> i32 {
    self.compression_factor
  }

  /// Sets whether to delete the database after drop. if no path is set, uses /dev/shm on linux
  pub fn set_temporary(&mut self, temporary: bool) {
    self.temporary = temporary;
  }

  /// Gets whether to delete the database after drop. if no path is set, uses /dev/shm on linux
  pub fn temporary(&self) -> bool {
    self.temporary
  }

  /// Sets whether to attempt to exclusively open the database, failing if it already exists
  pub fn set_create_new(&mut self, create_new: bool) {
    self.create_new = create_new;
  }

  /// Sets whether to print a performance profile when the Config is dropped
  pub fn set_print_profile_on_drop(&mut self, print_profile_on_drop: bool) {
    self.print_profile_on_drop = print_profile_on_drop;
  }
}
