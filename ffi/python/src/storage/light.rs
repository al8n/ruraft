use pyo3::prelude::*;

macro_rules! wal {
  ($rt: ident) => {
    paste::paste! {
      /// Raft WAL and snapshot implementation based on [`sled`](https://crates.io/crates/sled).
      #[derive(Clone)]
      #[pyclass]
      pub struct [< $rt Wal>] (
        std::sync::Arc<
          ruraft_lightwal::LightStorage<
            ruraft_snapshot::sync::FileSnapshotStorage<nodecraft::NodeId, nodecraft::NodeAddress, $rt>,
            RustDb<nodecraft::NodeId, nodecraft::NodeAddress, crate::RaftData, $rt>,
          >,
        >,
      );

      #[pymethods]
      impl [< $rt Wal >] {
        #[new]
        pub fn new(db_options: DbOptions, snapshot_options: crate::storage::snapshot::FileSnapshotStorageOptions) -> pyo3::PyResult<Self> {
          let snap = ruraft_snapshot::sync::FileSnapshotStorage::new(snapshot_options.into()).map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
          RustDb::new(db_options.into())
            .map(|db| Self(Arc::new(ruraft_lightwal::LightStorage::new(snap, db))))
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
        }
      }
    }
  };
}

#[cfg(feature = "sled")]
pub mod sled;

#[cfg(feature = "redb")]
pub mod redb;

#[cfg(feature = "jammdb")]
pub mod jammdb;

/// Expose [`ruraft-lightwal`](https://crates.io/crates/ruraft-lightwal) to a Python module.
#[pymodule]
pub fn light(py: Python, m: &PyModule) -> PyResult<()> {
  #[cfg(feature = "sled")]
  m.add_submodule(sled::submodule(py)?)?;
  #[cfg(feature = "redb")]
  m.add_submodule(redb::submodule(py)?)?;
  #[cfg(feature = "jammdb")]
  m.add_submodule(jammdb::submodule(py)?)?;
  Ok(())
}

// This function creates and returns the sled submodule.
pub fn submodule(py: Python) -> PyResult<&PyModule> {
  let module = PyModule::new(py, "light")?;
  light(py, module)?;
  Ok(module)
}
