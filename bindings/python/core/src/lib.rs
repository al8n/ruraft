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
enum SupportedRuntime {
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

trait IntoSupportedRuntime: Runtime {
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
  type Snapshot = crate::types::TokioSnapshot;
  type SnapshotSink = crate::types::TokioSnapshotSink;

  #[inline(always)]
  fn into_supported() -> SupportedRuntime {
    SupportedRuntime::Tokio
  }
}

#[cfg(feature = "async-std")]
impl IntoSupportedRuntime for agnostic::async_std::AsyncStdRuntime {
  type Snapshot = crate::types::AsyncStdSnapshot;
  type SnapshotSink = crate::types::AsyncStdSnapshotSink;

  #[inline(always)]
  fn into_supported() -> SupportedRuntime {
    SupportedRuntime::AsyncStd
  }
}
