use std::path::PathBuf;
use std::sync::Arc;

use pyo3::exceptions::PyTypeError;
use pyo3::*;
use ruraft_bindings_common::storage::*;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::Pyi;

/// The high-level database mode, according to the trade-offs of the RUM conjecture.
#[pyclass(name = "SledMode")]
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
#[repr(u8)]
pub enum PythonSledMode {
  /// In this mode, the database will make decisions that favor using less space instead of supporting the highest possible write throughput. This mode will also rewrite data more frequently as it strives to reduce fragmentation.
  #[default]
  LowSpace = 0,
  /// In this mode, the database will try to maximize write throughput while potentially using more disk space.
  HighThroughput = 1,
}

impl From<SledMode> for PythonSledMode {
  fn from(mode: SledMode) -> Self {
    match mode {
      SledMode::LowSpace => Self::LowSpace,
      SledMode::HighThroughput => Self::HighThroughput,
    }
  }
}

impl From<PythonSledMode> for SledMode {
  fn from(mode: PythonSledMode) -> Self {
    match mode {
      PythonSledMode::LowSpace => Self::LowSpace,
      PythonSledMode::HighThroughput => Self::HighThroughput,
    }
  }
}

impl Pyi for PythonSledMode {
  fn pyi() -> std::borrow::Cow<'static, str> {
r#"

class SledMode:
  @staticmethod
  def low_space() -> SledMode:...
  
  @staticmethod
  def high_throughput() -> SledMode:...

  def __eq__(self, __value: SledMode) -> bool: ...
  
  def __ne__(self, __value: SledMode) -> bool: ...
  
  def __hash__(self) -> int: ...
  
  def __str__(self) -> str: ...
  
  def __repr__(self) -> str: ...

"#.into()
  }
}

#[pymethods]
impl PythonSledMode {
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

  #[inline]
  pub fn __str__(&self) -> &'static str {
    match self {
      Self::LowSpace => "low_space",
      Self::HighThroughput => "high_throughput",
    }
  }

  #[inline]
  pub fn __repr__(&self) -> PyResult<&'static str> {
    match self {
      Self::LowSpace => Ok("SledMode::LowSpace"),
      Self::HighThroughput => Ok("SledMode::HighThroughput"),
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
#[pyclass(name = "SledOptions")]
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
pub struct PythonSledOptions {
  /// Set the path of the database (builder).
  path: Arc<PathBuf>,
  /// maximum size in bytes for the system page cache
  cache_capacity: u64,
  /// specify whether the system should run in "low_space" or "high_throughput" mode
  mode: PythonSledMode,
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

impl From<SledOptions> for PythonSledOptions {
  fn from(db_options: SledOptions) -> Self {
    Self {
      path: db_options.path().clone().into(),
      cache_capacity: db_options.cache_capacity(),
      mode: db_options.mode().into(),
      use_compression: db_options.use_compression(),
      compression_factor: db_options.compression_factor(),
      temporary: db_options.temporary(),
      create_new: db_options.create_new(),
      print_profile_on_drop: db_options.print_profile_on_drop(),
    }
  }
}

impl From<PythonSledOptions> for SledOptions {
  fn from(db_options: PythonSledOptions) -> Self {
    let mut opts = SledOptions::new();
    opts.set_path(db_options.path.as_ref().clone());
    opts.set_cache_capacity(db_options.cache_capacity);
    opts.set_mode(db_options.mode.into());
    opts.set_use_compression(db_options.use_compression);
    opts.set_compression_factor(db_options.compression_factor);
    opts.set_temporary(db_options.temporary);
    opts.set_create_new(db_options.create_new);
    opts.set_print_profile_on_drop(db_options.print_profile_on_drop);
    opts
  }
}

impl Default for PythonSledOptions {
  fn default() -> Self {
    Self::new()
  }
}

impl Pyi for PythonSledOptions {
  fn pyi() -> std::borrow::Cow<'static, str> {
r#"

class SledOptions:
  def __init__(self) -> None: ...

  @property
  def path(self) -> str:...
  
  @path.setter
  def path(self, value: str) -> None:...
  
  @property
  def cache_capacity(self) -> int:...
  
  @cache_capacity.setter
  def cache_capacity(self, value: int) -> None:...
  
  @property
  def mode(self) -> SledMode:...
  
  @mode.setter
  def mode(self, value: SledMode) -> None:...
  
  @property
  def use_compression(self) -> bool:...
  
  @use_compression.setter
  def use_compression(self, value: bool) -> None:...
  
  @property
  def compression_factor(self) -> int:...
  
  @compression_factor.setter
  def compression_factor(self, value: int) -> None:...
  
  @property
  def temporary(self) -> bool:...
  
  @temporary.setter
  def temporary(self, value: bool) -> None:...
  
  @property
  def create_new(self) -> bool:...
  
  @create_new.setter
  def create_new(self, value: bool) -> None:...
  
  @property
  def print_profile_on_drop(self) -> bool:...
  
  @print_profile_on_drop.setter
  def print_profile_on_drop(self, value: bool) -> None:...

  def __eq__(self, __value: SledOptions) -> bool: ...
  
  def __ne__(self, __value: SledOptions) -> bool: ...
  
  def __hash__(self) -> int: ...
  
  def __str__(self) -> str: ...
  
  def __repr__(self) -> str: ...

"#.into()
  }
}

#[pymethods]
impl PythonSledOptions {
  /// Returns the default configuration.
  #[new]
  pub fn new() -> Self {
    Self::from(SledOptions::default())
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
  pub fn set_mode(&mut self, mode: PythonSledMode) {
    self.mode = mode;
  }

  /// Gets the mode of the database (builder).
  #[getter]
  pub fn mode(&self) -> PythonSledMode {
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
