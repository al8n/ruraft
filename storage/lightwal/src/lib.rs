//! Lightwal is a lightweight write-ahead log for Ruraft.
#![forbid(unsafe_code)]
#![deny(warnings, missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

use std::sync::Arc;

use ruraft_core::storage::{LogStorage, RaftStorage, SnapshotStorage, StableStorage, Storage};

/// [`jammdb`](::jammdb) backed [`StableStorage`](ruraft_core::storage::StableStorage) and [`LogStorage`](ruraft_core::storage::LogStorage) implementor.
#[cfg(feature = "jammdb")]
#[cfg_attr(docsrs, doc(cfg(feature = "jammdb")))]
pub mod jammdb;

/// [`redb`](::redb) backed [`StableStorage`](ruraft_core::storage::StableStorage) and [`LogStorage`](ruraft_core::storage::LogStorage) implementor.
#[cfg(feature = "redb")]
#[cfg_attr(docsrs, doc(cfg(feature = "redb")))]
pub mod redb;

/// [`sled`](::sled) backed [`StableStorage`](ruraft_core::storage::StableStorage) and [`LogStorage`](ruraft_core::storage::LogStorage) implementor.
#[cfg(feature = "sled")]
#[cfg_attr(docsrs, doc(cfg(feature = "sled")))]
pub mod sled;

#[cfg(any(feature = "jammdb", feature = "redb", feature = "sled"))]
const LAST_CANIDATE_ID: &str = "__ruraft_last_candidate_id__";
#[cfg(any(feature = "jammdb", feature = "redb", feature = "sled"))]
const LAST_CANIDATE_ADDR: &str = "__ruraft_last_candidate_addr__";
#[cfg(any(feature = "jammdb", feature = "redb", feature = "sled"))]
const LAST_VOTE_TERM: &str = "__ruraft_last_vote_term__";
#[cfg(any(feature = "jammdb", feature = "redb", feature = "sled"))]
const CURRENT_TERM: &str = "__ruraft_current_term__";

#[cfg(all(
  feature = "metrics",
  any(feature = "jammdb", feature = "redb", feature = "sled")
))]
fn report_store_many(logs: usize, start: std::time::Instant) {
  let duration = start.elapsed();
  let nanos = duration.as_nanos(); // Get the elapsed time in nanoseconds
  let val = if nanos == 0 {
    0.0
  } else {
    (1_000_000_000.0 / nanos as f64) * logs as f64
  };

  metrics::histogram!("ruraft.lightwal.write_capacity").record(val);
  metrics::histogram!("ruraft.lightwal.store_logs").record(start.elapsed().as_secs_f64());
}

#[doc(hidden)]
pub trait Backend: sealed::Sealed {}

// Implementation of the private, sealed trait
mod sealed {
  use super::*;

  pub trait Sealed:
    LogStorage
    + StableStorage<
      Id = <Self as LogStorage>::Id,
      Address = <Self as LogStorage>::Address,
      Runtime = <Self as LogStorage>::Runtime,
    >
  {
  }

  impl<S> Sealed for S where
    S: LogStorage
      + StableStorage<
        Id = <Self as LogStorage>::Id,
        Address = <Self as LogStorage>::Address,
        Runtime = <Self as LogStorage>::Runtime,
      >
  {
  }

  impl<S: Sealed> Backend for S {}
}

/// [`SnapshotStorage`](ruraft_core::storage::SnapshotStorage) implementation which consists of a [`SnapshotStorage`] and a backend storage which implements [`LogStorage`] and [`StableStorage`].
pub struct LightStorage<S, B>(RaftStorage<Arc<B>, Arc<B>, S>);

impl<S, B> LightStorage<S, B> {
  /// Create a new [`LightStorage`] with the given [`SnapshotStorage`] and backend storage which implements [`LogStorage`] and [`StableStorage`].
  pub fn new(snapshot: S, backend: B) -> Self {
    let arc = Arc::new(backend);
    Self(RaftStorage::new(arc.clone(), arc, snapshot))
  }
}

impl<
    S: SnapshotStorage<
      Id = <B as LogStorage>::Id,
      Address = <B as LogStorage>::Address,
      Runtime = <B as LogStorage>::Runtime,
    >,
    B: Backend,
  > Storage for LightStorage<S, B>
{
  type Error = <RaftStorage<B, B, S> as Storage>::Error;

  type Id = <B as StableStorage>::Id;

  type Address = <B as StableStorage>::Address;

  type Stable = B;

  type Snapshot = S;

  type Log = B;

  type Runtime = <B as LogStorage>::Runtime;

  fn stable_store(&self) -> &Self::Stable {
    self.0.stable_store()
  }

  fn log_store(&self) -> &Self::Log {
    self.0.log_store()
  }

  fn snapshot_store(&self) -> &Self::Snapshot {
    self.0.snapshot_store()
  }
}

#[cfg(all(
  any(feature = "test", test),
  any(feature = "jammdb", feature = "redb", feature = "sled")
))]
mod test {
  pub use ruraft_core::tests::storage::*;
}
