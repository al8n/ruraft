#![forbid(unsafe_code)]
#![allow(clippy::new_without_default)]

use std::pin::Pin;

use agnostic::Runtime;
use futures::{Future, FutureExt};
use pyo3::{types::PyModule, *};

pub mod fsm;
pub mod options;
pub mod raft;
pub mod storage;
pub use storage::RaftStorage;
pub mod transport;
pub mod types;

const INLINED_U8: usize = 64;

pub type RaftData = ::smallvec::SmallVec<[u8; INLINED_U8]>;

pub type RaftTransport<R> = ruraft_bindings_common::transport::SupportedTransport<RaftData, R>;

pub type Raft<R> = ruraft_bindings_common::Raft<fsm::FinateStateMachine<R>, RaftData, R>;
pub type RaftError<R> = ruraft_bindings_common::RaftError<fsm::FinateStateMachine<R>, RaftData, R>;

trait IntoPython: Sized {
  type Target: pyo3::PyClass
    + pyo3::IntoPy<pyo3::Py<pyo3::PyAny>>
    + for<'source> pyo3::FromPyObject<'source>;

  fn into_python(self) -> Self::Target;
}

trait FromPython: Sized {
  type Source;

  fn from_python(slf: Self::Source) -> Self;
}

#[derive(Copy, Clone)]
pub enum SupportedRuntime {
  #[cfg(feature = "tokio")]
  Tokio,
  #[cfg(feature = "async-std")]
  AsyncStd,
}

impl SupportedRuntime {
  fn future_into_py<F, T>(self, py: Python, fut: F) -> PyResult<&PyAny>
  where
    F: Future<Output = PyResult<T>> + Send + 'static,
    T: IntoPy<PyObject>,
  {
    match self {
      #[cfg(feature = "tokio")]
      Self::Tokio => pyo3_asyncio::tokio::future_into_py(py, fut),
      #[cfg(feature = "async-std")]
      Self::AsyncStd => pyo3_asyncio::async_std::future_into_py(py, fut),
    }
  }

  fn into_future(
    self,
    awaitable: &PyAny,
  ) -> PyResult<Pin<Box<dyn Future<Output = PyResult<PyObject>> + Send>>> {
    match self {
      #[cfg(feature = "tokio")]
      Self::Tokio => pyo3_asyncio::tokio::into_future(awaitable).map(|fut| fut.boxed()),
      #[cfg(feature = "async-std")]
      Self::AsyncStd => pyo3_asyncio::async_std::into_future(awaitable).map(|fut| fut.boxed()),
    }
  }
}

trait Pyi: PyClass {
  fn pyi() -> std::borrow::Cow<'static, str>;
}

pub trait IntoSupportedRuntime: Runtime {
  type Snapshot: pyo3::PyClass
    + pyo3::IntoPy<pyo3::Py<pyo3::PyAny>>
    + for<'source> pyo3::FromPyObject<'source>
    + From<ruraft_bindings_common::storage::SupportedSnapshot>
    + From<Box<dyn futures::AsyncRead + Send + Sync + Unpin + 'static>>;

  type SnapshotSink: pyo3::PyClass
    + pyo3::IntoPy<pyo3::Py<pyo3::PyAny>>
    + for<'source> pyo3::FromPyObject<'source>
    + From<ruraft_bindings_common::storage::SupportedSnapshotSink>;

  fn into_supported() -> SupportedRuntime;
}

#[cfg(feature = "tokio")]
impl IntoSupportedRuntime for agnostic::tokio::TokioRuntime {
  type Snapshot = crate::storage::TokioSnapshot;
  type SnapshotSink = crate::storage::TokioSnapshotSink;

  #[inline(always)]
  fn into_supported() -> SupportedRuntime {
    SupportedRuntime::Tokio
  }
}

#[cfg(feature = "async-std")]
impl IntoSupportedRuntime for agnostic::async_std::AsyncStdRuntime {
  type Snapshot = crate::storage::AsyncStdSnapshot;
  type SnapshotSink = crate::storage::AsyncStdSnapshotSink;

  #[inline(always)]
  fn into_supported() -> SupportedRuntime {
    SupportedRuntime::AsyncStd
  }
}

fn rewrite_on_modified(
  path: impl AsRef<std::path::Path>,
  new_content: &str,
) -> std::io::Result<()> {
  use std::fs::OpenOptions;
  use std::io::prelude::*;

  let path = path.as_ref();

  let mut file = OpenOptions::new()
    .create(true)
    .read(true)
    .write(true)
    .open(path)?;

  let mut contents = String::new();
  file.read_to_string(&mut contents)?;

  if contents != new_content {
    drop(file);

    let mut file = OpenOptions::new()
      .create(true)
      .write(true)
      .truncate(true)
      .open(path)?;

    file.write_all(new_content.as_bytes())?;
    file.flush()?;
  }

  Ok(())
}

pub fn register<'a>(
  py: Python<'a>,
  m: &'a pyo3::types::PyModule,
) -> pyo3::PyResult<()> {

  {
    let typem = types::register(py)?;
    m.add_submodule(typem)?;
  }

  {
    let membershipm = types::membership::register(py)?;
    m.add_submodule(membershipm)?;
  }

  {
    let optionsm = options::register(py)?;
    m.add_submodule(optionsm)?;
  }

  {
    #[cfg(feature = "tokio")]
    let snapshot = storage::register_tokio(py)?;
    #[cfg(feature = "async-std")]
    let snapshot = storage::register_async_std(py)?;
    m.add_submodule(snapshot)?;
  }

  {
    #[cfg(feature = "tokio")]
    raft::register_tokio(m)?;

    #[cfg(feature = "async-std")]
    raft::register_async_std(m)?;
  }

  Ok(())
}

pub fn generate<P: AsRef<std::path::Path>>(
  lib: &str,
  python_path: P,
) -> PyResult<()> {
  let python_path = python_path.as_ref();

  {
    let pyi = types::pyi();
    rewrite_on_modified(
      python_path.join("types.py"),
      format!("from {}.types import *", lib).as_str(),
    )?;
    rewrite_on_modified(python_path.join("types.pyi"), pyi.as_str())?;
  }

  {
    let pyi = types::membership::pyi();
    rewrite_on_modified(
      python_path.join("membership.py"),
      format!("from {}.membership import *", lib).as_str(),
    )?;
    rewrite_on_modified(python_path.join("membership.pyi"), pyi.as_str())?;
  }

  {
    let pyi = options::pyi();
    rewrite_on_modified(
      python_path.join("options.py"),
      format!("from {}.options import *", lib).as_str(),
    )?;
    rewrite_on_modified(python_path.join("options.pyi"), pyi.as_str())?;
  }

  {
    #[cfg(feature = "tokio")]
    let pyi = storage::pyi_tokio();
    #[cfg(feature = "async-std")]
    let pyi = storage::pyi_async_std();
    rewrite_on_modified(
      python_path.join("snapshot.py"),
      format!("from {}.snapshot import *", lib).as_str(),
    )?;
    rewrite_on_modified(python_path.join("snapshot.pyi"), pyi.as_str())?;
  }

  {
    let pyi = fsm::pyi();
    rewrite_on_modified(python_path.join("fsm.pyi"), pyi)?;
    let py = fsm::py();
    rewrite_on_modified(python_path.join("fsm.py"), py)?;
  }

  {
    #[cfg(feature = "tokio")]
    let pyi = raft::pyi_tokio();
    #[cfg(feature = "async-std")]
    let pyi = raft::pyi_async_std();

    rewrite_on_modified(
      python_path.join("__init__.py"),
      r#"
from ._internal import *
from .membership import *
from .options import *
from .types import *

__doc__ = _internal.__doc__
__all__ = _internal.__all__
 
    "#,
    )?;
    rewrite_on_modified(python_path.join("__init__.pyi"), pyi.as_str())?;
  }

  Ok(())
}
