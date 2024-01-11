use std::path::PathBuf;

use ruraft_lightwal::jammdb::DbOptions as RustDbOptions;

/// Options used to create Db.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
pub struct JammdbOptions {
  /// Sets the path to the database.
  path: PathBuf,
  direct_writes: bool,
  strict_mode: bool,
  mmap_populate: bool,
  pagesize: u64,
  num_pages: usize,
}

impl From<JammdbOptions> for RustDbOptions {
  fn from(db_options: JammdbOptions) -> Self {
    let mut opts = RustDbOptions::new(db_options.path);
    opts = opts.pagesize(db_options.pagesize);

    opts = opts.direct_writes(db_options.direct_writes);
    opts = opts.num_pages(db_options.num_pages);
    opts = opts.strict_mode(db_options.strict_mode);
    opts.mmap_populate(db_options.mmap_populate)
  }
}

impl JammdbOptions {
  /// Returns the default configuration.
  pub fn new(path: PathBuf) -> Self {
    Self {
      path,
      direct_writes: false,
      strict_mode: false,
      mmap_populate: false,
      pagesize: page_size::get() as u64,
      num_pages: 32,
    }
  }

  /// Sets the path of the database (builder).
  pub fn set_path(&mut self, path: PathBuf) {
    self.path = path;
  }

  /// Returns the path of the database (builder).
  pub fn path(&self) -> &PathBuf {
    &self.path
  }

  pub fn set_direct_writes(&mut self, direct_writes: bool) {
    self.direct_writes = direct_writes;
  }

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
  pub fn set_pagesize(&mut self, pagesize: u64) {
    self.pagesize = pagesize;
  }

  /// Returns the pagesize for the database
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
  pub fn set_num_pages(&mut self, num_pages: usize) {
    self.num_pages = num_pages;
  }

  /// Returns the number of pages to allocate for a new database file.
  pub fn num_pages(&self) -> usize {
    self.num_pages
  }

  /// Enables or disables "Strict Mode", where each transaction will check the database for errors before finalizing a write.
  ///
  /// The default is `false`, but you may enable this if you want an extra degree of safety for your data at the cost of
  /// slower writes.
  pub fn set_strict_mode(&mut self, strict_mode: bool) {
    self.strict_mode = strict_mode;
  }

  /// Returns whether or not "Strict Mode" is enabled.
  pub fn strict_mode(&self) -> bool {
    self.strict_mode
  }

  /// Enables or disables the MAP_POPULATE flag for the `mmap` call, which will cause Linux to eagerly load pages into memory.
  ///
  /// The default is `false`, but you may enable this if your database file will stay smaller than your available memory.
  /// It is not recommended to enable this unless you know what you are doing.
  ///
  /// This setting only works on Linux, and is a no-op on other platforms.
  pub fn set_mmap_populate(&mut self, mmap_populate: bool) {
    self.mmap_populate = mmap_populate;
  }

  /// Returns whether or not the MAP_POPULATE flag is enabled.
  pub fn mmap_populate(&self) -> bool {
    self.mmap_populate
  }
}
