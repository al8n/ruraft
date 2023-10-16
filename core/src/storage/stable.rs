use std::future::Future;

use bytes::Bytes;

/// Used to provide stable storage
/// of key configurations to ensure safety.
/// e.g. votes are persisted to this storage.
pub trait StableStorage: Send + Sync + 'static {
  /// The error type returned by the stable storage.
  type Error: std::error::Error + Send + Sync + 'static;
  /// The async runtime used by the storage.
  type Runtime: agnostic::Runtime;

  /// Insert a key-value pair into the storage.
  fn insert(&self, key: Bytes, val: Bytes) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Returns the value for key, or a `None` if key was not found.
  fn get(&self, key: &[u8]) -> impl Future<Output = Result<Option<Bytes>, Self::Error>> + Send;

  /// Insert a key-`u64` pair into the storage.
  fn insert_u64(
    &self,
    key: Bytes,
    val: u64,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Returns the `u64` for key, or `None` if key was not found.
  fn get_u64(&self, key: &[u8]) -> impl Future<Output = Result<Option<u64>, Self::Error>> + Send;
}
