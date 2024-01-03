#![allow(clippy::new_without_default)]

use std::{cell::UnsafeCell, sync::Arc};

use pyo3::{types::PyModule, *};
use ruraft_core::{sidecar::NoopSidecar, RaftCore};

mod error;
mod fsm;
mod io;
pub mod storage;
mod transport;
mod types;
mod utils;

const INLINED_U8: usize = 64;

type RaftData = ::smallvec::SmallVec<[u8; INLINED_U8]>;

// #[cfg(feature = "tokio")]
// pub struct TokioRaft(RaftCore<self::fsm::tokio::FinateStateMachine, _, _, NoopSidecar<agnostic::tokio::TokioRuntime>, agnostic::tokio::TokioRuntime>);

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

/// Expose [`ruraft`](https://crates.io/crates/ruraft) Raft protocol implementation to a Python module.
#[pymodule]
pub fn pyraft(py: Python, m: &PyModule) -> PyResult<()> {
  m.add_submodule(storage::submodule(py)?)?;
  m.add_submodule(types::submodule(py)?)?;
  m.add_submodule(fsm::submodule(py)?)?;
  m.add_submodule(io::submodule(py)?)?;
  Ok(())
}
