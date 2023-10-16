mod snapshot;
pub use snapshot::*;
mod log;
pub use log::*;
mod stable;
pub use stable::*;

use crate::transport::{Address, Id};

/// Storage is a trait that must be implemented by the user to provide the persistent storage for the Raft.
pub trait Storage: Send + Sync + 'static {
  /// Errors returned by the storage.
  type Error: std::error::Error
    + From<<Self::Stable as StableStorage>::Error>
    + From<<Self::Snapshot as SnapshotStorage>::Error>
    + From<<Self::Log as LogStorage>::Error>;

  /// The id type used to identify nodes.
  type Id: Id;
  /// The address type of node.
  type Address: Address;

  /// Stable storage
  type Stable: StableStorage<Runtime = Self::Runtime>;

  /// Snapshot storage
  type Snapshot: SnapshotStorage<Id = Self::Id, Address = Self::Address, Runtime = Self::Runtime>;

  /// Log storage
  type Log: LogStorage<Id = Self::Id, Address = Self::Address, Runtime = Self::Runtime>;

  /// The async runtime used by the storage.
  type Runtime: agnostic::Runtime;

  /// Returns a reference to the stable storage.
  fn stable_store(&self) -> &Self::Stable;

  /// Returns a reference to the log storage.
  fn log_store(&self) -> &Self::Log;

  /// Returns a reference to the snapshot storage.
  fn snapshot_store(&self) -> &Self::Snapshot;
}

#[cfg(feature = "test")]
pub(super) mod tests {
  pub use super::log::tests::*;
}
