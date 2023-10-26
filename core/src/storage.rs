mod snapshot;
pub use snapshot::*;
mod log;
pub use log::*;
mod stable;
pub use stable::*;

use std::{borrow::Cow, future::Future};

use crate::transport::{Address, Id};

/// Represents a comprehensive set of errors arising from operations within the [`Storage`] trait.
///
/// This trait encapsulates a range of error types, providing a structured approach to categorizing
/// and handling storage-level errors. Implementers can leverage this to define both generic and
/// storage-specific error scenarios.
pub trait StorageError: std::error::Error + Send + Sync + 'static {
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

  /// With extra message to explain the error.
  fn with_message(self, msg: Cow<'static, str>) -> Self;

  /// Provides a flexible mechanism to define custom errors using a descriptive message.
  ///
  /// The resulting error message will be straightforward, avoiding capitalization or a trailing period.
  fn custom<T>(msg: T) -> Self
  where
    Self: Sized,
    T: core::fmt::Display;
}

/// Provides a trait that can optionally be implemented by users of this crate
/// to store membership updates made in the replicated log. In general this is only
/// necessary for [`FinateStateMachine`](crate::FinateStateMachine) that mutate durable state directly instead of applying changes
/// in memory and snapshotting periodically. By storing membership changes, the
/// persistent [`FinateStateMachine`] state can behave as a complete snapshot, and be able to recover
/// without an external snapshot just for persisting the raft membership.
pub trait MembershipStorage: Send + Sync + 'static {
  /// Errors returned by the storage.
  type Error: std::error::Error + Send + Sync + 'static;

  /// The id type used to identify nodes.
  type Id: Id;
  /// The address type of node.
  type Address: Address;

  /// Invoked once a log entry containing a membership
  /// change is committed. It takes the term and index at which the membership was
  /// written and the membership value.
  fn store(
    &self,
    log: MembershipLog<Self::Id, Self::Address>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Invoked once many membership log entries are committed. It takes the term and index at which the membership was
  /// written and the membership value.
  fn store_many(
    &self,
    logs: impl IntoIterator<Item = MembershipLog<Self::Id, Self::Address>>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send;
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
  type Stable: StableStorage<Id = Self::Id, Address = Self::Address, Runtime = Self::Runtime>;

  /// Snapshot storage
  type Snapshot: SnapshotStorage<Id = Self::Id, Address = Self::Address, Runtime = Self::Runtime>;

  /// Log storage
  type Log: LogStorage<Id = Self::Id, Address = Self::Address, Runtime = Self::Runtime>;

  /// Membership storage
  type Membership: MembershipStorage<Id = Self::Id, Address = Self::Address>;

  /// The async runtime used by the storage.
  type Runtime: agnostic::Runtime;

  /// Returns a reference to the stable storage.
  fn stable_store(&self) -> &Self::Stable;

  /// Returns a reference to the log storage.
  fn log_store(&self) -> &Self::Log;

  /// Returns a reference to the snapshot storage.
  fn snapshot_store(&self) -> &Self::Snapshot;

  /// Returns a reference to the membership storage.
  fn membership_store(&self) -> Option<&Self::Membership>;
}

#[cfg(feature = "test")]
pub(super) mod tests {
  pub use super::log::tests::*;
}
