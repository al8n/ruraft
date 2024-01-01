use ruraft_core::storage::RaftStorage;

use self::{log::MemoryLogStorage, snapshot::MemorySnapshotStorage, stable::MemoryStableStorage};

/// Memory based [`SnapshotStorage`](ruraft_core::storage::SnapshotStorage) implementation.
pub mod snapshot;

/// Memory based [`StableStorage`](ruraft_core::storage::StableStorage) implementation.
pub mod stable;

/// Memory based [`LogStorage`](ruraft_core::storage::LogStorage) implementation.
pub mod log;

/// Memory based [`Storage`](ruraft_core::storage::Storage) implementation.
pub type MemoryStorage<I, A, D, R> = RaftStorage<
  MemoryLogStorage<I, A, D, R>,
  MemoryStableStorage<I, A, R>,
  MemorySnapshotStorage<I, A, R>,
>;
