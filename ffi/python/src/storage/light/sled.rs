use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use nodecraft::{NodeAddress, NodeId};
use pyo3::exceptions::PyTypeError;
use pyo3::{types::PyModule, *};
use ruraft_lightwal::sled::{Db as RustDb, DbOptions as RustDbOptions, Mode as RustMode};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// The high-level database mode, according to the trade-offs of the RUM conjecture.
#[pyclass]
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
#[repr(u8)]
pub enum Mode {
  /// In this mode, the database will make decisions that favor using less space instead of supporting the highest possible write throughput. This mode will also rewrite data more frequently as it strives to reduce fragmentation.
  #[default]
  LowSpace = 0,
  /// In this mode, the database will try to maximize write throughput while potentially using more disk space.
  HighThroughput = 1,
}

#[pymethods]
impl Mode {
  /// Construct the default mode [`Mode::LowSpace`].
  #[inline]
  #[new]
  pub const fn new() -> Self {
    Self::LowSpace
  }

  /// Low space mode.
  ///
  /// In this mode, the database will make decisions that favor using less space instead of supporting the highest possible write throughput.
  /// This mode will also rewrite data more frequently as it strives to reduce fragmentation.
  #[inline]
  #[staticmethod]
  pub fn low_space() -> Self {
    Self::LowSpace
  }

  /// High throughput mode.
  ///
  /// In this mode, the database will try to maximize write throughput while potentially using more disk space.
  #[inline]
  #[staticmethod]
  pub fn high_throughput() -> Self {
    Self::HighThroughput
  }

  /// Returns the str of the mode.
  #[inline]
  pub const fn as_str(&self) -> &'static str {
    match self {
      Self::LowSpace => "low_space",
      Self::HighThroughput => "high_throughput",
    }
  }

  #[inline]
  pub fn __str__(&self) -> PyResult<&'static str> {
    Ok(self.as_str())
  }

  #[inline]
  pub fn __repr__(&self) -> PyResult<&'static str> {
    match self {
      Self::LowSpace => Ok("Mode::LowSpace"),
      Self::HighThroughput => Ok("Mode::HighThroughput"),
    }
  }

  fn __eq__(&self, other: &Self) -> bool {
    self.eq(other)
  }

  fn __ne__(&self, other: &Self) -> bool {
    self.ne(other)
  }

  fn __hash__(&self) -> u64 {
    let mut hasher = DefaultHasher::new();
    self.hash(&mut hasher);
    hasher.finish()
  }

  fn __int__(&self) -> u8 {
    *self as u8
  }
}

/// Top-level configuration for the system.
#[pyclass]
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
pub struct DbOptions {
  /// Set the path of the database (builder).
  path: Arc<PathBuf>,
  /// maximum size in bytes for the system page cache
  cache_capacity: u64,
  /// specify whether the system should run in "low_space" or "high_throughput" mode
  mode: Mode,
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

impl From<RustDbOptions> for DbOptions {
  fn from(db_options: RustDbOptions) -> Self {
    Self {
      path: db_options.path.clone().into(),
      cache_capacity: db_options.cache_capacity,
      mode: match db_options.mode {
        RustMode::LowSpace => Mode::LowSpace,
        RustMode::HighThroughput => Mode::HighThroughput,
      },
      use_compression: db_options.use_compression,
      compression_factor: db_options.compression_factor,
      temporary: db_options.temporary,
      create_new: db_options.create_new,
      print_profile_on_drop: db_options.print_profile_on_drop,
    }
  }
}

impl From<DbOptions> for RustDbOptions {
  fn from(db_options: DbOptions) -> Self {
    RustDbOptions::new()
      .path(db_options.path.as_ref())
      .cache_capacity(db_options.cache_capacity)
      .mode(match db_options.mode {
        Mode::LowSpace => RustMode::LowSpace,
        Mode::HighThroughput => RustMode::HighThroughput,
      })
      .use_compression(db_options.use_compression)
      .compression_factor(db_options.compression_factor)
      .temporary(db_options.temporary)
      .create_new(db_options.create_new)
      .print_profile_on_drop(db_options.print_profile_on_drop)
  }
}

impl Default for DbOptions {
  fn default() -> Self {
    Self::new()
  }
}

#[pymethods]
impl DbOptions {
  /// Returns the default configuration.
  #[new]
  pub fn new() -> Self {
    Self::from(RustDbOptions::default())
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

  /// Sets maximum size in bytes for the system page cache
  #[setter]
  pub fn set_cache_capacity(&mut self, cache_capacity: u64) {
    self.cache_capacity = cache_capacity;
  }

  /// Gets maximum size in bytes for the system page cache
  #[getter]
  pub fn cache_capacity(&self) -> u64 {
    self.cache_capacity
  }

  /// Specifies whether the system should run in "low_space" or "high_throughput" mode
  #[setter]
  pub fn set_mode(&mut self, mode: Mode) {
    self.mode = mode;
  }

  /// Gets the mode of the database (builder).
  #[getter]
  pub fn mode(&self) -> Mode {
    self.mode
  }

  /// Sets whether to use zstd compression.
  #[setter]
  pub fn set_use_compression(&mut self, use_compression: bool) {
    self.use_compression = use_compression;
  }

  /// Gets whether to use zstd compression.
  #[getter]
  pub fn use_compression(&self) -> bool {
    self.use_compression
  }

  /// Sets the compression factor to use with zstd compression. Ranges from 1 up to 22. Levels >= 20 are ‘ultra’.
  #[setter]
  pub fn set_compression_factor(&mut self, compression_factor: i32) {
    self.compression_factor = compression_factor;
  }

  /// Gets the compression factor to use with zstd compression. Ranges from 1 up to 22. Levels >= 20 are ‘ultra’.
  #[getter]
  pub fn compression_factor(&self) -> i32 {
    self.compression_factor
  }

  /// Sets whether to delete the database after drop. if no path is set, uses /dev/shm on linux
  #[setter]
  pub fn set_temporary(&mut self, temporary: bool) {
    self.temporary = temporary;
  }

  /// Gets whether to delete the database after drop. if no path is set, uses /dev/shm on linux
  #[getter]
  pub fn temporary(&self) -> bool {
    self.temporary
  }

  /// Sets whether to attempt to exclusively open the database, failing if it already exists
  #[setter]
  pub fn set_create_new(&mut self, create_new: bool) {
    self.create_new = create_new;
  }

  /// Sets whether to print a performance profile when the Config is dropped
  #[setter]
  pub fn set_print_profile_on_drop(&mut self, print_profile_on_drop: bool) {
    self.print_profile_on_drop = print_profile_on_drop;
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

/// [`sled`](https://crates.io/crates/sled) database based on [`tokio`](https://tokio.rs) runtime.
#[cfg(feature = "tokio")]
#[pyclass]
pub struct TokioDb(RustDb<NodeId, NodeAddress, Bytes, agnostic::tokio::TokioRuntime>);

#[cfg(feature = "tokio")]
#[pymethods]
impl TokioDb {
  #[new]
  pub fn new(opts: DbOptions) -> PyResult<Self> {
    RustDb::new(opts.into())
      .map(Self)
      .map_err(|e| PyTypeError::new_err(e.to_string()))
  }
}

/// [`sled`](https://crates.io/crates/sled) database based on [`async-std`](https://crates.io/crates/async-std) runtime.
#[cfg(feature = "async-std")]
#[pyclass]
pub struct AsyncStdDb(RustDb<NodeId, NodeAddress, Bytes, agnostic::async_std::AsyncStdRuntime>);

#[cfg(feature = "async-std")]
#[pymethods]
impl AsyncStdDb {
  #[new]
  pub fn new(opts: DbOptions) -> PyResult<Self> {
    RustDb::new(opts.into())
      .map(Self)
      .map_err(|e| PyTypeError::new_err(e.to_string()))
  }
}

/// [`sled`](https://crates.io/crates/sled) database based on [`smol`](https://crates.io/crates/smol) runtime.
#[cfg(feature = "smol")]
#[pyclass]
pub struct SmolDb(RustDb<NodeId, NodeAddress, Bytes, agnostic::smol::SmolRuntime>);

#[cfg(feature = "smol")]
#[pymethods]
impl SmolDb {
  #[new]
  pub fn new(opts: DbOptions) -> PyResult<Self> {
    RustDb::new(opts.into())
      .map(Self)
      .map_err(|e| PyTypeError::new_err(e.to_string()))
  }
}

#[pymodule]
fn sled(_py: Python, m: &PyModule) -> PyResult<()> {
  m.add_class::<Mode>()?;
  m.add_class::<DbOptions>()?;
  #[cfg(feature = "tokio")]
  m.add_class::<TokioDb>()?;
  #[cfg(feature = "async-std")]
  m.add_class::<AsyncStdDb>()?;
  #[cfg(feature = "smol")]
  m.add_class::<SmolDb>()?;

  Ok(())
}

// This function creates and returns the sled submodule.
pub fn submodule(py: Python) -> PyResult<&PyModule> {
  let module = PyModule::new(py, "sled")?;
  sled(py, module)?;
  Ok(module)
}
