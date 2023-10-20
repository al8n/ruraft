use std::{collections::HashMap, convert::Infallible, ops::RangeBounds, sync::Arc};

use agnostic::Runtime;
use async_lock::Mutex;
use ruraft_core::{
  storage::{Log, LogStorage},
  transport::{Address, Id},
};

struct Inner<I: Id, A: Address> {
  low_index: u64,
  high_index: u64,
  logs: HashMap<u64, Log<I, A>>,
}

impl<I: Id, A: Address> Default for Inner<I, A> {
  fn default() -> Self {
    Self {
      low_index: 0,
      high_index: 0,
      logs: HashMap::new(),
    }
  }
}

/// Implements the [`LogStorage`] trait.
///
/// **N.B.** It should NOT EVER be used for production. It is used only for
/// unit tests.
pub struct MemoryLogStorage<I: Id, A: Address, R: Runtime> {
  store: Arc<Mutex<Inner<I, A>>>,
  _runtime: core::marker::PhantomData<R>,
}

impl<I: Id, A: Address, R: Runtime> Default for MemoryLogStorage<I, A, R> {
  fn default() -> Self {
    Self::new()
  }
}

impl<I: Id, A: Address, R: Runtime> Clone for MemoryLogStorage<I, A, R> {
  fn clone(&self) -> Self {
    Self {
      store: self.store.clone(),
      ..Default::default()
    }
  }
}

impl<I: Id, A: Address, R: Runtime> MemoryLogStorage<I, A, R> {
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
  I: Id + Send + Sync + 'static,
  A: Address + Send + Sync + 'static,
  R: Runtime,
{
  /// The error type returned by the log storage.
  type Error = Infallible;
  /// The async runtime used by the storage.
  type Runtime = R;
  type Id = I;
  type Address = A;

  fn first_index(&self) -> impl futures::Future<Output = Result<Option<u64>, Self::Error>> + Send {
    async move { Ok(Some(self.store.lock().await.low_index)) }
  }

  fn last_index(&self) -> impl futures::Future<Output = Result<Option<u64>, Self::Error>> + Send {
    async move { Ok(Some(self.store.lock().await.high_index)) }
  }

  fn get_log(
    &self,
    index: u64,
  ) -> impl futures::Future<Output = Result<Option<Log<Self::Id, Self::Address>>, Self::Error>> + Send
  {
    async move { Ok(self.store.lock().await.logs.get(&index).cloned()) }
  }

  fn store_log(
    &self,
    log: &Log<Self::Id, Self::Address>,
  ) -> impl futures::Future<Output = Result<(), Self::Error>> + Send {
    async move {
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
  }

  fn store_logs(
    &self,
    logs: &[Log<Self::Id, Self::Address>],
  ) -> impl futures::Future<Output = Result<(), Self::Error>> + Send {
    async move {
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
  }

  fn remove_range(
    &self,
    range: impl RangeBounds<u64> + Send,
  ) -> impl futures::Future<Output = Result<(), Self::Error>> + Send {
    async move {
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
}
