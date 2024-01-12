use pyo3::{exceptions::PyIOError, PyErr};

use crate::{types::SnapshotMeta, IntoSupportedRuntime, RaftData};

pub type RaftStorage<R> = ruraft_bindings_common::storage::SupportedStorage<RaftData, R>;

#[derive(derive_more::From)]
pub struct SnapshotSource<R: IntoSupportedRuntime>(
  ruraft_core::storage::SnapshotSource<RaftStorage<R>>,
);

impl<R: IntoSupportedRuntime> core::fmt::Debug for SnapshotSource<R> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{:?}", self.0)
  }
}

impl<R: IntoSupportedRuntime> Clone for SnapshotSource<R> {
  fn clone(&self) -> Self {
    Self(self.0.clone())
  }
}

impl<R: IntoSupportedRuntime> SnapshotSource<R> {
  pub async fn open(&mut self) -> Result<(SnapshotMeta, R::Snapshot), PyErr> {
    self
      .0
      .open()
      .await
      .map_err(|e| PyIOError::new_err(e.to_string()))
      .map(|(meta, source)| (meta.into(), R::Snapshot::from(source)))
  }
}

macro_rules! snapshot_source {
  ($rt: literal) => {
    paste::paste! {
      #[pyo3::pyclass(name = "SnapshotSource")]
      #[derive(Clone, Debug)]
      pub struct [< $rt:camel SnapshotSource >] (SnapshotSource< agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >] >);

      impl crate::IntoPython for SnapshotSource<agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]> {
        type Target = [< $rt:camel SnapshotSource >];

        fn into_python(self) -> Self::Target {
          [< $rt:camel SnapshotSource >] (self)
        }
      }

      #[pyo3::pymethods]
      impl [< $rt:camel SnapshotSource >] {
        /// Open the snapshot source.
        pub fn open<'a>(&'a mut self, py: pyo3::Python<'a>) -> pyo3::PyResult<&'a pyo3::PyAny> {
          let mut this = self.0.clone();
          pyo3_asyncio::[< $rt:snake >]::future_into_py(py, async move {
            this.open().await.map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
          })
        }
      }
    }
  };
}

#[cfg(feature = "tokio")]
snapshot_source!("tokio");

#[cfg(feature = "async-std")]
snapshot_source!("async-std");
