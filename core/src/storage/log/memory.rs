use std::{collections::HashMap, convert::Infallible, sync::Arc};

use agnostic::Runtime;
use async_lock::Mutex;

use super::*;

#[derive(Default)]
struct Inner {
  low_index: u64,
  high_index: u64,
  logs: HashMap<u64, Log>,
}

/// Implements the [`LogStorage`] trait.
///
/// **N.B.** It should NOT EVER be used for production. It is used only for
/// unit tests.
pub struct MemoryLogStorage<R: Runtime> {
  store: Arc<Mutex<Inner>>,
  _runtime: core::marker::PhantomData<R>,
}

impl<R: Runtime> Default for MemoryLogStorage<R> {
  fn default() -> Self {
    Self::new()
  }
}

impl<R: Runtime> Clone for MemoryLogStorage<R> {
  fn clone(&self) -> Self {
    Self {
      store: self.store.clone(),
      ..Default::default()
    }
  }
}

impl<R: Runtime> MemoryLogStorage<R> {
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
impl<R: Runtime> LogStorage for MemoryLogStorage<R> {
  /// The error type returned by the log storage.
  type Error = Infallible;
  /// The async runtime used by the storage.
  type Runtime = R;

  async fn first_index(&self) -> Result<u64, Self::Error> {
    Ok(self.store.lock().await.low_index)
  }

  async fn last_index(&self) -> Result<u64, Self::Error> {
    Ok(self.store.lock().await.high_index)
  }

  async fn get_log(&self, index: u64) -> Result<Option<Log>, Self::Error> {
    Ok(self.store.lock().await.logs.get(&index).cloned())
  }

  async fn store_log(&self, log: &Log) -> Result<(), Self::Error> {
    let mut store = self.store.lock().await;
    store.logs.insert(log.index, log.clone());
    if store.low_index == 0 {
      store.low_index = log.index;
    }

    if store.high_index <= log.index {
      store.high_index = log.index;
    }
    Ok(())
  }

  async fn store_logs(&self, logs: &[Log]) -> Result<(), Self::Error> {
    let mut store = self.store.lock().await;
    for l in logs {
      store.logs.insert(l.index, l.clone());
      if store.low_index == 0 {
        store.low_index = l.index;
      }

      if store.high_index <= l.index {
        store.high_index = l.index;
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
