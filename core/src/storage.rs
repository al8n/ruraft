mod snapshot;
pub use snapshot::*;
mod log;
pub use log::*;
mod stable;
pub use stable::*;

use crate::transport::{Address, Id};

/// Represents a comprehensive set of errors arising from operations within the [`Storage`] trait.
///
/// This trait encapsulates a range of error types, providing a structured approach to categorizing
/// and handling storage-level errors. Implementers can leverage this to define both generic and
/// storage-specific error scenarios.
pub trait StorageError: Send + Sync + 'static {
  /// Stable storage errors
  type Stable: StableStorage;

  /// Snapshot storage errors
  type Snapshot: SnapshotStorage;

  /// Log storage errors
  type Log: LogStorage;

  /// Constructs an error associated with stable storage operations.
  fn stable(err: <Self::Stable as StableStorage>::Error) -> Self;

  /// Constructs an error associated with snapshot storage operations.
  fn snapshot(err: <Self::Snapshot as SnapshotStorage>::Error) -> Self;

  /// Constructs an error associated with log storage operations.
  fn log(err: <Self::Log as LogStorage>::Error) -> Self;

  /// Provides a flexible mechanism to define custom errors using a descriptive message.
  ///
  /// The resulting error message will be straightforward, avoiding capitalization or a trailing period.
  fn custom<T>(msg: T) -> Self
  where
    Self: Sized,
    T: core::fmt::Display;
}

/// Storage is a trait that must be implemented by the user to provide the persistent storage for the Raft.
pub trait Storage: Send + Sync + 'static {
  /// Errors returned by the storage.
  type Error: StorageError<Stable = Self::Stable, Snapshot = Self::Snapshot, Log = Self::Log>;

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
