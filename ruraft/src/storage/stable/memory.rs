use std::{collections::HashMap, convert::Infallible, sync::Arc};

use agnostic::Runtime;
use async_lock::Mutex;
use bytes::Bytes;
use ruraft_core::storage::StableStorage;

#[derive(Default)]
struct Inner {
  kvs: HashMap<Bytes, Bytes>,
  kvu64s: HashMap<Bytes, u64>,
}

/// Implements the [`StableStorage`] trait.
///
/// **N.B.** It should NOT EVER be used for production. It is used only for
/// unit tests.
pub struct MemoryStableStorage<R: Runtime> {
  store: Arc<Mutex<Inner>>,
  _runtime: core::marker::PhantomData<R>,
}

impl<R: Runtime> Default for MemoryStableStorage<R> {
  fn default() -> Self {
    Self::new()
  }
}

impl<R: Runtime> Clone for MemoryStableStorage<R> {
  fn clone(&self) -> Self {
    Self {
      store: self.store.clone(),
      ..Default::default()
    }
  }
}

impl<R: Runtime> MemoryStableStorage<R> {
  /// Returns a new in-memory backend. Do not ever
  /// use for production. Only for testing.
  pub fn new() -> Self {
    Self {
      store: Default::default(),
      _runtime: std::marker::PhantomData,
    }
  }
}

#[async_trait::async_trait]
impl<R: Runtime> StableStorage for MemoryStableStorage<R> {
  /// The error type returned by the log storage.
  type Error = Infallible;
  /// The async runtime used by the storage.
  type Runtime = R;

  /// Insert a key-value pair into the storage.
  async fn insert(&self, key: Bytes, val: Bytes) -> Result<(), Self::Error> {
    self.store.lock().await.kvs.insert(key, val);
    Ok(())
  }

  /// Returns the value for key, or a `None` if key was not found.
  async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, Self::Error> {
    Ok(self.store.lock().await.kvs.get(key).cloned())
  }

  /// Insert a key-`u64` pair into the storage.
  async fn insert_u64(&self, key: Bytes, val: u64) -> Result<(), Self::Error> {
    self.store.lock().await.kvu64s.insert(key, val);
    Ok(())
  }

  /// Returns the `u64` for key, or `None` if key was not found.
  async fn get_u64(&self, key: &[u8]) -> Result<Option<u64>, Self::Error> {
    Ok(self.store.lock().await.kvu64s.get(key).copied())
  }
}
