use std::{collections::HashMap, ops::RangeBounds, sync::Arc};

use agnostic_lite::RuntimeLite;
use async_lock::Mutex;
use ruraft_core::{
  storage::{Log, LogStorage},
  transport::{Address, Id},
  CheapClone,
};

#[derive(Debug)]
struct Inner<I, A> {
  low_index: u64,
  high_index: u64,
  logs: HashMap<u64, Log<I, A>>,
}

impl<I, A> Default for Inner<I, A> {
  fn default() -> Self {
    Self {
      low_index: 0,
      high_index: 0,
      logs: HashMap::new(),
    }
  }
}

/// Error for [`MemoryLogStorage`]
#[derive(Debug, Clone, thiserror::Error)]
pub enum MemoryLogStorageError {
  /// Log not found in the log storage
  #[error("log with index {0} not found in storage")]
  LogNotFound(u64),
}

/// Implements the [`LogStorage`] trait.
///
/// **N.B.** It should NOT EVER be used for production. It is used only for
/// unit tests.
pub struct MemoryLogStorage<I, A, R> {
  store: Arc<Mutex<Inner<I, A>>>,
  _runtime: core::marker::PhantomData<R>,
}

impl<I: core::fmt::Debug, A: core::fmt::Debug, R> core::fmt::Debug for MemoryLogStorage<I, A, R> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_tuple("MemoryLogStorage")
      .field(&self.store)
      .finish()
  }
}

impl<I, A, R> Default for MemoryLogStorage<I, A, R> {
  fn default() -> Self {
    Self::new()
  }
}

impl<I, A, R> Clone for MemoryLogStorage<I, A, R> {
  fn clone(&self) -> Self {
    Self {
      store: self.store.clone(),
      ..Default::default()
    }
  }
}

impl<I, A, R> CheapClone for MemoryLogStorage<I, A, R> {}

impl<I, A, R> MemoryLogStorage<I, A, R> {
  /// Returns a new in-memory backend. Do not ever
  /// use for production. Only for testing.
  pub fn new() -> Self {
    Self {
      store: Default::default(),
      _runtime: std::marker::PhantomData,
    }
  }
}

impl<I, A, R> LogStorage for MemoryLogStorage<I, A, R>
where
  I: Id,
  A: Address,
  R: RuntimeLite,
{
  /// The error type returned by the log storage.
  type Error = MemoryLogStorageError;
  /// The async runtime used by the storage.
  type Runtime = R;
  type Id = I;
  type Address = A;

  async fn first_index(&self) -> Result<Option<u64>, Self::Error> {
    let index = self.store.lock().await.low_index;
    if index == 0 {
      return Ok(None);
    }
    Ok(Some(index))
  }

  async fn last_index(&self) -> Result<Option<u64>, Self::Error> {
    let index = self.store.lock().await.high_index;
    if index == 0 {
      return Ok(None);
    }
    Ok(Some(index))
  }

  async fn get_log(&self, index: u64) -> Result<Option<Log<Self::Id, Self::Address>>, Self::Error> {
    Ok(self.store.lock().await.logs.get(&index).cloned())
  }

  async fn store_log(&self, log: &Log<Self::Id, Self::Address>) -> Result<(), Self::Error> {
    let mut store = self.store.lock().await;
    store.logs.insert(log.index(), log.clone());
    if store.low_index == 0 {
      store.low_index = log.index();
    }

    if store.high_index <= log.index() {
      store.high_index = log.index();
    }
    Ok(())
  }

  async fn store_logs(&self, logs: &[Log<Self::Id, Self::Address>]) -> Result<(), Self::Error> {
    let mut store = self.store.lock().await;
    for l in logs {
      store.logs.insert(l.index(), l.clone());
      if store.low_index == 0 {
        store.low_index = l.index();
      }

      if store.high_index <= l.index() {
        store.high_index = l.index();
      }
    }
    Ok(())
  }

  async fn remove_range(&self, range: impl RangeBounds<u64> + Send) -> Result<(), Self::Error> {
    use core::ops::Bound;

    let mut store = self.store.lock().await;

    let begin = match range.start_bound() {
      Bound::Included(&n) => n,
      Bound::Excluded(&n) => n + 1,
      Bound::Unbounded => 0,
    };

    let end = match range.end_bound() {
      Bound::Included(&n) => n.checked_add(1).expect("out of range"),
      Bound::Excluded(&n) => n,
      Bound::Unbounded => panic!("unbounded end bound is not acceptable"),
    };

    for j in begin..end {
      store.logs.remove(&j);
    }

    if begin <= store.low_index {
      store.low_index = end + 1;
    }
    if end >= store.high_index {
      store.high_index = begin - 1;
    }
    if store.low_index > store.high_index {
      store.high_index = 0;
      store.low_index = 0;
    }

    Ok(())
  }
}
