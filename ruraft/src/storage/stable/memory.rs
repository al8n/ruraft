use std::{collections::HashMap, convert::Infallible, sync::Arc};

use agnostic::Runtime;
use async_lock::Mutex;
use bytes::Bytes;
use ruraft_core::{
  storage::StableStorage,
  transport::{Address, Id},
  Node,
};

struct Inner<I, A> {
  kvs: HashMap<Bytes, Bytes>,
  kvu64s: HashMap<Bytes, u64>,
  last_vote_cand: Option<Node<I, A>>,
  last_vote_term: Option<u64>,
  current_term: Option<u64>,
}

impl<I, A> Default for Inner<I, A> {
  fn default() -> Self {
    Self {
      kvs: Default::default(),
      kvu64s: Default::default(),
      last_vote_cand: Default::default(),
      last_vote_term: Default::default(),
      current_term: Default::default(),
    }
  }
}

/// Implements the [`StableStorage`] trait.
///
/// **N.B.** It should NOT EVER be used for production. It is used only for
/// unit tests.
pub struct MemoryStableStorage<I, A, R> {
  store: Arc<Mutex<Inner<I, A>>>,
  _runtime: core::marker::PhantomData<R>,
}

impl<I, A, R> Default for MemoryStableStorage<I, A, R> {
  fn default() -> Self {
    Self::new()
  }
}

impl<I, A, R> Clone for MemoryStableStorage<I, A, R> {
  fn clone(&self) -> Self {
    Self {
      store: self.store.clone(),
      ..Default::default()
    }
  }
}

impl<I, A, R> MemoryStableStorage<I, A, R> {
  /// Returns a new in-memory backend. Do not ever
  /// use for production. Only for testing.
  pub fn new() -> Self {
    Self {
      store: Default::default(),
      _runtime: std::marker::PhantomData,
    }
  }
}

impl<I: Id, A: Address, R: Runtime> StableStorage for MemoryStableStorage<I, A, R>
where
  I: Id + Send + Sync + 'static,
  I::Error: Send + Sync + 'static,
  A: Address + Send + Sync + 'static,
  A::Error: Send + Sync + 'static,
{
  /// The error type returned by the log storage.
  type Error = Infallible;
  /// The async runtime used by the storage.
  type Runtime = R;

  type Id = I;

  type Address = A;

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

  async fn current_term(&self) -> Result<Option<u64>, Self::Error> {
    Ok(self.store.lock().await.current_term)
  }

  async fn store_current_term(&self, term: u64) -> Result<(), Self::Error> {
    self.store.lock().await.current_term = Some(term);
    Ok(())
  }

  async fn last_vote_term(&self) -> Result<Option<u64>, Self::Error> {
    Ok(self.store.lock().await.last_vote_term)
  }

  async fn store_last_vote_term(&self, term: u64) -> Result<(), Self::Error> {
    self.store.lock().await.last_vote_term = Some(term);
    Ok(())
  }

  async fn last_vote_candidate(
    &self,
  ) -> Result<Option<ruraft_core::Node<Self::Id, Self::Address>>, Self::Error> {
    Ok(self.store.lock().await.last_vote_cand.clone())
  }

  async fn store_last_vote_candidate(
    &self,
    candidate: ruraft_core::Node<Self::Id, Self::Address>,
  ) -> Result<(), Self::Error> {
    self.store.lock().await.last_vote_cand = Some(candidate);
    Ok(())
  }
}
