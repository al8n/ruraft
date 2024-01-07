#![allow(clippy::new_without_default)]

use std::{cell::UnsafeCell, pin::Pin, sync::Arc};

use agnostic::Runtime;
use futures::{Future, FutureExt};
use nodecraft::{resolver::dns::DnsResolver, NodeAddress, NodeId};
use pyo3::{types::PyModule, *};
use ruraft_core::{sidecar::NoopSidecar, RaftCore};
use ruraft_ffi::storage::snapshot::SupportedSnapshotStorage;
use ruraft_lightwal::{sled::Db, LightStorage};
use ruraft_tcp::{net::wire::LpeWire, TcpTransport};

mod error;
mod fsm;
pub mod storage;
mod transport;
mod types;
mod utils;

const INLINED_U8: usize = 64;

type RaftData = ::smallvec::SmallVec<[u8; INLINED_U8]>;

type RaftTransport<R> =
  TcpTransport<NodeId, DnsResolver<R>, RaftData, LpeWire<NodeId, NodeAddress, RaftData>>;

type RaftStorage<R> =
  LightStorage<SupportedSnapshotStorage<R>, Db<NodeId, NodeAddress, RaftData, R>>;

type Raft<R> =
  RaftCore<crate::fsm::FinateStateMachine<R>, RaftStorage<R>, RaftTransport<R>, NoopSidecar<R>, R>;

// #[pyclass]
// #[cfg(feature = "tokio")]
// pub struct TokioRaft(Raft<TokioRuntime>);

// #[pymethods]
// impl TokioRaft {
//   #[staticmethod]
//   fn new() -> PyResult<Self> {
//     todo!()
//   }
// }

/// A fearless cell, which is highly unsafe
///
/// If your structure and its internals are safe to send to Python
/// (they implement Send and do not have internal mutability that could cause thread-safety issues),
/// and you don't plan to access it concurrently from Rust anymore,
/// you can avoid using a Mutex. Just ensure that any interaction with it in Python is done in a thread-safe manner, respecting Python's GIL.
struct FearlessCell<T: Sized>(Arc<UnsafeCell<T>>);

impl<T> Clone for FearlessCell<T> {
  fn clone(&self) -> Self {
    Self(self.0.clone())
  }
}

impl<T> FearlessCell<T> {
  fn new(val: T) -> Self {
    Self(Arc::new(UnsafeCell::new(val)))
  }

  /// # Safety
  ///
  /// - no data-race, struct will not be send back to Rust side anymore
  /// - must be called with GIL holds
  #[allow(clippy::mut_from_ref)]
  unsafe fn get_mut(&self) -> &mut T {
    &mut *self.0.get()
  }
}

unsafe impl<T> Send for FearlessCell<T> {}
unsafe impl<T> Sync for FearlessCell<T> {}

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
  fn into_supported() -> SupportedRuntime;
}

#[cfg(feature = "tokio")]
impl IntoSupportedRuntime for agnostic::tokio::TokioRuntime {
  #[inline(always)]
  fn into_supported() -> SupportedRuntime {
    SupportedRuntime::Tokio
  }
}

#[cfg(feature = "async-std")]
impl IntoSupportedRuntime for agnostic::async_std::AsyncStdRuntime {
  #[inline(always)]
  fn into_supported() -> SupportedRuntime {
    SupportedRuntime::AsyncStd
  }
}

/// Expose [`ruraft`](https://crates.io/crates/ruraft) Raft protocol implementation to a Python module.
#[pymodule]
pub fn prufty(py: Python, m: &PyModule) -> PyResult<()> {
  // m.add_submodule(storage::submodule(py)?)?;
  m.add_submodule(types::submodule(py)?)?;
  Ok(())
}
