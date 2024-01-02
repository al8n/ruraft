#![allow(clippy::new_without_default)]

use std::cell::UnsafeCell;

use pyo3::{types::PyModule, *};
use ruraft_core::{sidecar::NoopSidecar, RaftCore};

mod error;
mod fsm;
mod io;
pub mod storage;
mod types;
mod utils;

const INLINED_U8: usize = 64;

type RaftData = ::smallvec::SmallVec<[u8; INLINED_U8]>;

// #[cfg(feature = "tokio")]
// pub struct TokioRaft(RaftCore<self::fsm::tokio::FinateStateMachine, _, _, NoopSidecar<agnostic::tokio::TokioRuntime>, agnostic::tokio::TokioRuntime>);

/// A fearless cell 
struct FearlessCell<T>(UnsafeCell<T>);


/// Expose [`ruraft`](https://crates.io/crates/ruraft) Raft protocol implementation to a Python module.
#[pymodule]
pub fn pyraft(py: Python, m: &PyModule) -> PyResult<()> {
  m.add_submodule(storage::submodule(py)?)?;
  m.add_submodule(types::submodule(py)?)?;
  m.add_submodule(fsm::submodule(py)?)?;
  m.add_submodule(io::submodule(py)?)?;
  Ok(())
}
