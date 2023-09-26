mod snapshot;
pub use snapshot::*;

pub trait StableStorage: Send + Sync + 'static {
  type Error: std::error::Error;
}

pub trait LogStorage: Send + Sync + 'static {
  type Error: std::error::Error;
}

/// Storage is a trait that must be implemented by the user to provide the persistent storage for the Raft.
pub trait Storage: Send + Sync + 'static {
  /// Errors returned by the storage.
  type Error: std::error::Error
    + From<<Self::Stable as StableStorage>::Error>
    + From<<Self::Snapshot as SnapshotStorage>::Error>
    + From<<Self::Log as LogStorage>::Error>;

  /// Stable storage
  type Stable: StableStorage;

  /// Snapshot storage
  type Snapshot: SnapshotStorage;

  /// Log storage
  type Log: LogStorage;

  /// Returns a reference to the stable storage.
  fn stable_store(&self) -> &Self::Stable;

  /// Returns a reference to the log storage.
  fn log_store(&self) -> &Self::Log;

  /// Returns a reference to the snapshot storage.
  fn snapshot_store(&self) -> &Self::Snapshot;
}

#[cfg(feature = "test")]
pub(super) mod tests {
  pub(crate) mod snapshot {
    pub use crate::storage::snapshot::tests::*;
  }
}
