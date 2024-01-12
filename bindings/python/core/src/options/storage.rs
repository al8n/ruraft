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

#[pymethods]
impl StorageOptions {
  #[staticmethod]
  pub fn light(snapshot: snapshot::SnapshotStorageOptions, opts: LightWALOptions) -> Self {
    Self(
      ruraft_bindings_common::storage::SupportedStorageOptions::new(snapshot.into(), opts.into()),
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
  register_snapshot_storage_options(module)?;
  Ok(())
}
