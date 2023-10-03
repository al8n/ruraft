use std::{collections::HashMap, convert::Infallible, ops::RangeBounds, sync::Arc};

use agnostic::Runtime;
use async_lock::Mutex;
use ruraft_core::{
  storage::{Log, LogStorage},
  transport::{NodeAddress, NodeId},
};

struct Inner<Id: NodeId, Address: NodeAddress> {
  low_index: u64,
  high_index: u64,
  logs: HashMap<u64, Log<Id, Address>>,
}

impl<Id: NodeId, Address: NodeAddress> Default for Inner<Id, Address> {
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
pub struct MemoryLogStorage<Id: NodeId, Address: NodeAddress, R: Runtime> {
  store: Arc<Mutex<Inner<Id, Address>>>,
  _runtime: core::marker::PhantomData<R>,
}

impl<Id: NodeId, Address: NodeAddress, R: Runtime> Default for MemoryLogStorage<Id, Address, R> {
  fn default() -> Self {
    Self::new()
  }
}

impl<Id: NodeId, Address: NodeAddress, R: Runtime> Clone for MemoryLogStorage<Id, Address, R> {
  fn clone(&self) -> Self {
    Self {
      store: self.store.clone(),
      ..Default::default()
    }
  }
}

impl<Id: NodeId, Address: NodeAddress, R: Runtime> MemoryLogStorage<Id, Address, R> {
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
impl<Id: NodeId, Address: NodeAddress, R: Runtime> LogStorage for MemoryLogStorage<Id, Address, R> {
  /// The error type returned by the log storage.
  type Error = Infallible;
  /// The async runtime used by the storage.
  type Runtime = R;
  type NodeId = Id;
  type NodeAddress = Address;

  async fn first_index(&self) -> Result<u64, Self::Error> {
    Ok(self.store.lock().await.low_index)
  }

  async fn last_index(&self) -> Result<u64, Self::Error> {
    Ok(self.store.lock().await.high_index)
  }

  async fn get_log(
    &self,
    index: u64,
  ) -> Result<Option<Log<Self::NodeId, Self::NodeAddress>>, Self::Error> {
    Ok(self.store.lock().await.logs.get(&index).cloned())
  }

  async fn store_log(&self, log: &Log<Self::NodeId, Self::NodeAddress>) -> Result<(), Self::Error> {
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

  async fn store_logs(
    &self,
    logs: &[Log<Self::NodeId, Self::NodeAddress>],
  ) -> Result<(), Self::Error> {
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
