use std::hash::{Hash, Hasher};

use pyo3::{exceptions::PyTypeError, types::PyModule, *};
use ruraft_bindings_common::storage::*;

#[cfg(feature = "redb")]
mod redb;
#[cfg(feature = "redb")]
pub use redb::*;
#[cfg(feature = "sled")]
mod sled;
#[cfg(feature = "sled")]
pub use sled::*;
#[cfg(feature = "jammdb")]
mod jammdb;
#[cfg(feature = "jammdb")]
pub use jammdb::*;

mod snapshot;
pub use snapshot::*;

use crate::Pyi;

#[pyclass]
#[derive(Clone, Debug, derive_more::From, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct LightWALOptions(ruraft_bindings_common::storage::LightBackendOptions);

impl From<LightWALOptions> for ruraft_bindings_common::storage::BackendOptions {
  fn from(opts: LightWALOptions) -> Self {
    ruraft_bindings_common::storage::BackendOptions::Light(opts.0)
  }
}

impl Pyi for LightWALOptions {
  fn pyi() -> std::borrow::Cow<'static, str> {
    let mut content = String::new();

    #[cfg(feature = "sled")]
    content.push_str(
      r#"

  @staticmethod
  def sled(opts: SledOptions) -> LightWALOptions:...

    "#,
    );

    #[cfg(feature = "redb")]
    content.push_str(
      r#"

  @staticmethod
  def redb(opts: RedbOptions) -> LightWALOptions:...

    "#,
    );

    #[cfg(feature = "jammdb")]
    content.push_str(
      r#"
  
  @staticmethod
  def jammdb(opts: JammdbOptions) -> LightWALOptions:...

    "#,
    );

    format!(
      r#"

class LighWALOptions:
  {content}
  
  def __eq__(self, __value: LighWALOptions) -> bool: ...
  
  def __ne__(self, __value: LighWALOptions) -> bool: ...
  
  def __hash__(self) -> int: ...
  
  def __str__(self) -> str: ...
  
  def __repr__(self) -> str: ...

"#
    )
    .into()
  }
}

#[pymethods]
impl LightWALOptions {
  #[cfg(feature = "sled")]
  #[staticmethod]
  pub fn sled(opts: PythonSledOptions) -> Self {
    Self(LightBackendOptions::Sled(opts.into()))
  }

  #[cfg(feature = "redb")]
  #[staticmethod]
  pub fn redb(opts: PythonRedbOptions) -> Self {
    Self(LightBackendOptions::Redb(opts.into()))
  }

  #[cfg(feature = "jammdb")]
  #[staticmethod]
  pub fn jammdb(opts: PythonJammdbOptions) -> Self {
    Self(LightBackendOptions::Jammdb(opts.into()))
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

#[derive(Debug, Clone, Eq, PartialEq, Hash, derive_more::From)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[pyclass]
pub struct StorageOptions(ruraft_bindings_common::storage::SupportedStorageOptions);

impl From<StorageOptions> for ruraft_bindings_common::storage::SupportedStorageOptions {
  fn from(opts: StorageOptions) -> Self {
    opts.0
  }
}

impl Pyi for StorageOptions {
  fn pyi() -> std::borrow::Cow<'static, str> {
    let mut content = String::new();

    #[cfg(any(feature = "redb", feature = "sled", feature = "jammdb"))]
    content.push_str(
      r#"

  @staticmethod
  def light(snapshot: SnapshotStorageOptions, wal: LighWALOptions) -> StorageOptions:...

    "#,
    );

    format!(
      r#"

class StorageOptions:
  {content}

  @staticmethod
  def memory() -> StorageOptions:...

  def __eq__(self, __value: StorageOptions) -> bool: ...
  
  def __ne__(self, __value: StorageOptions) -> bool: ...
  
  def __hash__(self) -> int: ...
  
  def __str__(self) -> str: ...
  
  def __repr__(self) -> str: ...

"#
    )
    .into()
  }
}

#[pymethods]
impl StorageOptions {
  /// Create a new storage options backed by light WAL with the given snapshot and light WAL options.
  #[staticmethod]
  pub fn light(snapshot: snapshot::SnapshotStorageOptions, opts: LightWALOptions) -> Self {
    Self(
      ruraft_bindings_common::storage::SupportedStorageOptions::new(snapshot.into(), opts.into()),
    )
  }

  /// Create a new memory storage, which is not persistent.
  /// This is useful for testing and do not use it in production.
  #[staticmethod]
  pub fn memory() -> Self {
    Self(
      ruraft_bindings_common::storage::SupportedStorageOptions::new(
        ruraft_bindings_common::storage::SnapshotStorageOptions::Memory,
        ruraft_bindings_common::storage::BackendOptions::Memory,
      ),
    )
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
}

pub fn register_storage_options(module: &PyModule) -> PyResult<()> {
  module.add_class::<LightWALOptions>()?;
  module.add_class::<StorageOptions>()?;

  #[cfg(feature = "sled")]
  {
    module.add_class::<PythonSledMode>()?;
    module.add_class::<PythonSledOptions>()?;
  }

  #[cfg(feature = "redb")]
  module.add_class::<PythonRedbOptions>()?;

  #[cfg(feature = "jammdb")]
  module.add_class::<PythonJammdbOptions>()?;

  register_snapshot_storage_options(module)?;
  Ok(())
}

pub fn storage_pyi() -> String {
  let mut pyi = String::new();

  #[cfg(feature = "sled")]
  {
    pyi.push_str(&PythonSledMode::pyi());
    pyi.push_str(&PythonSledOptions::pyi());
  }

  #[cfg(feature = "redb")]
  pyi.push_str(&PythonRedbOptions::pyi());

  #[cfg(feature = "jammdb")]
  pyi.push_str(&PythonJammdbOptions::pyi());

  pyi.push_str(&snapshot_storage_pyi());

  pyi.push_str(&LightWALOptions::pyi());
  pyi.push_str(&StorageOptions::pyi());

  pyi
}
