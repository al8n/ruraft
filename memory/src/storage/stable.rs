use std::{convert::Infallible, sync::Arc};

use agnostic_lite::RuntimeLite;
use async_lock::Mutex;
use ruraft_core::{
  storage::StableStorage,
  transport::{Address, Id},
  Node,
};

#[derive(Debug)]
struct Inner<I, A> {
  last_vote_cand: Option<Node<I, A>>,
  last_vote_term: Option<u64>,
  current_term: Option<u64>,
}

impl<I, A> Default for Inner<I, A> {
  fn default() -> Self {
    Self {
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

impl<I: core::fmt::Debug, A: core::fmt::Debug, R> core::fmt::Debug
  for MemoryStableStorage<I, A, R>
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_tuple("MemoryStableStorage")
      .field(&self.store)
      .finish()
  }
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

impl<I: Id, A: Address, R: RuntimeLite> StableStorage for MemoryStableStorage<I, A, R>
where
  I: Id,
  A: Address,
{
  /// The error type returned by the log storage.
  type Error = Infallible;
  /// The async runtime used by the storage.
  type Runtime = R;

  type Id = I;

  type Address = A;

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
