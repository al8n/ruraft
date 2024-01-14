use pyo3::{exceptions::PyIOError, types::PyModule, PyErr};

use crate::{IntoSupportedRuntime, Pyi, RaftData};

pub type RaftStorage<R> = ruraft_bindings_common::storage::SupportedStorage<RaftData, R>;

mod snapshot;
pub use snapshot::*;

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

      impl $crate::Pyi for [< $rt:camel SnapshotSource >] {
        fn pyi() -> std::borrow::Cow<'static, str> {
          r#"
    
class SnapshotSource:
  async def open(self) -> Snapshot: ...

          "#.into()
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

#[cfg(feature = "tokio")]
pub fn register_tokio(py: pyo3::Python<'_>) -> pyo3::PyResult<&PyModule> {
  let subm = PyModule::new(py, "snapshot")?;
  subm.add_class::<SnapshotId>()?;
  subm.add_class::<SnapshotMeta>()?;
  subm.add_class::<TokioAsyncReader>()?;
  subm.add_class::<TokioSnapshot>()?;
  subm.add_class::<TokioSnapshotSource>()?;
  subm.add_class::<TokioSnapshotSink>()?;

  Ok(subm)
}

#[cfg(feature = "async-std")]
pub fn register_async_std(py: pyo3::Python<'_>) -> pyo3::PyResult<&PyModule> {
  let subm = PyModule::new(py, "snapshot")?;
  subm.add_class::<SnapshotId>()?;
  subm.add_class::<SnapshotMeta>()?;
  subm.add_class::<AsyncStdAsyncReader>()?;
  subm.add_class::<AsyncStdSnapshot>()?;
  subm.add_class::<AsyncStdSnapshotSource>()?;
  subm.add_class::<AsyncStdSnapshotSink>()?;
  Ok(subm)
}

const HEADER: &str = r#"

from typing import Protocol
from .membership import Membership

class AsyncRead(Protocol):
  async def read(self, chunk_size: int = 1024) -> memoryview:...

  async def read_exact(self, bytes: int) -> memoryview:...

  async def read_to_end(self, chunk_size: int = 1024) -> memoryview:...
  
  async def read_to_string(self, chunk_size: int = 1024) -> str:...

  def __aenter__(self) -> AsyncRead: ...

  def __aexit__(self, exc_type, exc_value, traceback) -> None: ...

class AsyncWrite(Protocol):
  async def write(self, data: bytes) -> None:...

  async def write_all(self, data: bytes) -> None:...

  async def flush(self) -> None:...

  async def close(self) -> None:...

  def __aenter__(self) -> AsyncWrite: ...

  def __aexit__(self, exc_type, exc_value, traceback) -> None: ...


"#;

#[cfg(feature = "tokio")]
pub fn pyi_tokio() -> String {
  let mut pyi = HEADER.to_string();

  pyi.push_str(&SnapshotId::pyi());
  pyi.push_str(&SnapshotMeta::pyi());
  pyi.push_str(&TokioAsyncReader::pyi());
  pyi.push_str(&TokioSnapshot::pyi());
  pyi.push_str(&TokioSnapshotSource::pyi());
  pyi.push_str(&TokioSnapshotSink::pyi());

  pyi
}

#[cfg(feature = "async-std")]
pub fn pyi_async_std() -> String {
  let mut pyi = HEADER.to_string();

  pyi.push_str(&SnapshotId::pyi());
  pyi.push_str(&SnapshotMeta::pyi());
  pyi.push_str(&AsyncStdAsyncReader::pyi());
  pyi.push_str(&AsyncStdSnapshot::pyi());
  pyi.push_str(&AsyncStdSnapshotSource::pyi());
  pyi.push_str(&AsyncStdSnapshotSink::pyi());

  pyi
}
