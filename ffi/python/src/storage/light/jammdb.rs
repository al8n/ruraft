use std::{
  hash::{DefaultHasher, Hash, Hasher},
  path::PathBuf,
  sync::Arc,
};

use nodecraft::{NodeAddress, NodeId};
use pyo3::{exceptions::PyTypeError, types::PyModule, *};
use ruraft_lightwal::jammdb::{Db as RustDb, DbOptions as RustDbOptions};

use crate::RaftData;

/// Options used to create Db.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[pyclass]
pub struct DbOptions {
  /// Sets the path to the database.
  path: Arc<PathBuf>,
  direct_writes: bool,
  strict_mode: bool,
  mmap_populate: bool,
  pagesize: u64,
  num_pages: usize,
}

impl From<&DbOptions> for RustDbOptions {
  fn from(db_options: &DbOptions) -> Self {
    let mut opts = RustDbOptions::new();
    opts = opts.pagesize(db_options.pagesize);

    opts = opts.direct_writes(db_options.direct_writes);
    opts = opts.num_pages(db_options.num_pages);
    opts = opts.strict_mode(db_options.strict_mode);
    opts.mmap_populate(db_options.mmap_populate)
  }
}

#[pymethods]
impl DbOptions {
  /// Returns the default configuration.
  #[new]
  pub fn new(path: PathBuf) -> Self {
    Self {
      path: Arc::new(path),
      direct_writes: false,
      strict_mode: false,
      mmap_populate: false,
      pagesize: page_size::get() as u64,
      num_pages: 32,
    }
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

  #[setter]
  pub fn set_direct_writes(&mut self, direct_writes: bool) {
    self.direct_writes = direct_writes;
  }

  #[getter]
  pub fn direct_writes(&self) -> bool {
    self.direct_writes
  }

  /// Sets the pagesize for the database
  ///
  /// By default, your OS's pagesize is used as the database's pagesize, but if the file is
  /// moved across systems with different page sizes, it is necessary to set the correct value.
  /// Trying to open an existing database with the incorrect page size will result in a panic.
  ///
  /// # Panics
  /// Will panic if you try to set the pagesize < 1024 bytes.
  #[setter]
  pub fn set_pagesize(&mut self, pagesize: u64) {
    self.pagesize = pagesize;
  }

  /// Returns the pagesize for the database
  #[getter]
  pub fn pagesize(&self) -> u64 {
    self.pagesize
  }

  /// Sets the number of pages to allocate for a new database file.
  ///
  /// The default `num_pages` is set to 32, so if your pagesize is 4096 bytes (4kb), then 131,072 bytes (128kb) will be allocated for the initial file.
  /// Setting `num_pages` when opening an existing database has no effect.
  ///
  /// # Panics
  /// Since a minimum of four pages are required for the database, this function will panic if you provide a value < 4.
  #[setter]
  pub fn set_num_pages(&mut self, num_pages: usize) {
    self.num_pages = num_pages;
  }

  /// Returns the number of pages to allocate for a new database file.
  #[getter]
  pub fn num_pages(&self) -> usize {
    self.num_pages
  }

  /// Enables or disables "Strict Mode", where each transaction will check the database for errors before finalizing a write.
  ///
  /// The default is `false`, but you may enable this if you want an extra degree of safety for your data at the cost of
  /// slower writes.
  #[setter]
  pub fn set_strict_mode(&mut self, strict_mode: bool) {
    self.strict_mode = strict_mode;
  }

  /// Returns whether or not "Strict Mode" is enabled.
  #[getter]
  pub fn strict_mode(&self) -> bool {
    self.strict_mode
  }

  /// Enables or disables the MAP_POPULATE flag for the `mmap` call, which will cause Linux to eagerly load pages into memory.
  ///
  /// The default is `false`, but you may enable this if your database file will stay smaller than your available memory.
  /// It is not recommended to enable this unless you know what you are doing.
  ///
  /// This setting only works on Linux, and is a no-op on other platforms.
  #[setter]
  pub fn set_mmap_populate(&mut self, mmap_populate: bool) {
    self.mmap_populate = mmap_populate;
  }

  /// Returns whether or not the MAP_POPULATE flag is enabled.
  #[getter]
  pub fn mmap_populate(&self) -> bool {
    self.mmap_populate
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

/// [`redb`](https://crates.io/crates/redb) database based on [`tokio`](https://tokio.rs) runtime.
#[cfg(feature = "tokio")]
#[pyclass]
pub struct TokioDb(RustDb<NodeId, NodeAddress, RaftData, agnostic::tokio::TokioRuntime>);

#[cfg(feature = "tokio")]
#[pymethods]
impl TokioDb {
  #[new]
  pub fn new(opts: DbOptions) -> PyResult<Self> {
    RustDb::new(opts.path(), (&opts).into())
      .map(Self)
      .map_err(|e| PyTypeError::new_err(e.to_string()))
  }
}

/// [`redb`](https://crates.io/crates/redb) database based on [`async-std`](https://crates.io/crates/async-std) runtime.
#[cfg(feature = "async-std")]
#[pyclass]
pub struct AsyncStdDb(RustDb<NodeId, NodeAddress, RaftData, agnostic::async_std::AsyncStdRuntime>);

#[cfg(feature = "async-std")]
#[pymethods]
impl AsyncStdDb {
  #[new]
  pub fn new(opts: DbOptions) -> PyResult<Self> {
    RustDb::new(opts.path(), (&opts).into())
      .map(Self)
      .map_err(|e| PyTypeError::new_err(e.to_string()))
  }
}

/// [`sled`](https://crates.io/crates/sled) database based on [`smol`](https://crates.io/crates/smol) runtime.
#[cfg(feature = "smol")]
#[pyclass]
pub struct SmolDb(RustDb<NodeId, NodeAddress, RaftData, agnostic::smol::SmolRuntime>);

#[cfg(feature = "smol")]
#[pymethods]
impl SmolDb {
  #[new]
  pub fn new(opts: DbOptions) -> PyResult<Self> {
    RustDb::new(opts.path(), (&opts).into())
      .map(Self)
      .map_err(|e| PyTypeError::new_err(e.to_string()))
  }
}

#[pymodule]
fn jammdb(_py: Python, m: &PyModule) -> PyResult<()> {
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
  let module = PyModule::new(py, "jammdb")?;
  jammdb(py, module)?;
  Ok(module)
}