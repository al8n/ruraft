use std::{
  collections::HashMap,
  sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
  },
  time::Instant,
};

use agnostic::Runtime;
use futures::{future::join_all, FutureExt, Stream};
use nodecraft::{resolver::AddressResolver, Address, Id};

use crate::{
  sidecar::Sidecar,
  storage::{SnapshotStorage, Storage},
  transport::{Transport, VoteRequest},
  FinateStateMachine, Node, RaftCore, Role,
};
pub use async_channel::{RecvError, TryRecvError};

use super::runner::RaftRunner;

/// Used to provide a unique ID for each observer to aid in
/// deregistration.
static NEXT_OBSERVER_ID: AtomicU64 = AtomicU64::new(0);

/// A unique ID for an observer.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct ObserverId(u64);

impl core::fmt::Display for ObserverId {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.0)
  }
}

impl core::fmt::Debug for ObserverId {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", self.0)
  }
}

/// Observation-specific data
#[derive(Clone)]
pub enum Observed<I: Id, A: Address> {
  /// Used for the data when leadership changes.
  Leader(Option<Node<I, A>>),
  /// Sent to observers when peers change.
  Peer { id: I, removed: bool },
  /// Sent when a node fails to heartbeat with the leader
  HeartbeatFailed { id: I, last_contact: Instant },
  /// Sent when a node resumes to heartbeat with the leader following failures
  HeartbeatResumed(I),
  /// Sent when a node has a role change
  Role(Role),
  /// Sent when we get a request vote RPC call
  RequestVote(VoteRequest<I, A>),
}

struct Inner<I: Id, A: Address> {
  // numObserved and numDropped are performance counters for this observer.
  num_observed: AtomicU64,
  num_dropped: AtomicU64,

  /// channel receives observations.
  tx: async_channel::Sender<Observed<I, A>>,

  rx: async_channel::Receiver<Observed<I, A>>,

  /// Will be called to determine if an observation should be sent to
  /// the channel.
  filter: Option<Box<dyn Fn(&Observed<I, A>) -> bool + Send + Sync + 'static>>,
}

/// Observer describes what to do with a given observation.
#[derive(Clone)]
pub struct Observer<I: Id, A: Address> {
  inner: Arc<Inner<I, A>>,

  /// blocking, if true, will cause Raft to block when sending an observation
  /// to this observer. This should generally be set to false.
  blocking: bool,

  /// The ID of this observer in the Raft map.
  id: ObserverId,
}

impl<I: Id, A: Address> Observer<I, A> {
  /// Returns the id of the [`Observer`].
  #[inline]
  pub const fn id(&self) -> ObserverId {
    self.id
  }

  /// Returns the number of observations.
  #[inline]
  pub fn observed(&self) -> u64 {
    self.inner.num_observed.load(Ordering::Acquire)
  }

  /// Returns the number of dropped observations due to blocking.
  #[inline]
  pub fn dropped(&self) -> u64 {
    self.inner.num_dropped.load(Ordering::Acquire)
  }

  /// Subscribes to the [`Observer`] and returns an [`Observable`].
  #[inline]
  pub fn subscribe(&self) -> Observable<I, A> {
    Observable {
      rx: self.inner.rx.clone(),
      id: self.id,
    }
  }
}

/// Observable is kind of a mpmc receiver, used to receive [`Observation`]s from an [`Observer`].
#[pin_project::pin_project]
#[derive(Clone)]
pub struct Observable<I: Id, A: Address> {
  #[pin]
  rx: async_channel::Receiver<Observed<I, A>>,
  id: ObserverId,
}

impl<I: Id, A: Address> Observable<I, A> {
  /// Returns the id of the parent [`Observer`].
  ///
  /// This id can be used to deregister the observer from the [`RaftCore`].
  #[inline]
  pub const fn id(&self) -> ObserverId {
    self.id
  }

  /// Receives an [`Observation`].
  pub async fn recv(&mut self) -> Result<Observed<I, A>, RecvError> {
    self.rx.recv().await
  }

  /// Attempts to receive an [`Observation`] without blocking.
  pub fn try_recv(&mut self) -> Result<Observed<I, A>, TryRecvError> {
    self.rx.try_recv()
  }
}

impl<I: Id, A: Address> Stream for Observable<I, A> {
  type Item = <async_channel::Receiver<Observed<I, A>> as Stream>::Item;

  fn poll_next(
    self: std::pin::Pin<&mut Self>,
    cx: &mut std::task::Context<'_>,
  ) -> std::task::Poll<Option<Self::Item>> {
    <async_channel::Receiver<Observed<I, A>> as Stream>::poll_next(self.project().rx, cx)
  }
}

impl<F, S, T, SC, R> RaftCore<F, S, T, SC, R>
where
  F: FinateStateMachine<
    Id = T::Id,
    Address = <T::Resolver as AddressResolver>::Address,
    Runtime = R,
  >,
  S: Storage<Id = T::Id, Address = <T::Resolver as AddressResolver>::Address, Runtime = R>,
  T: Transport<Runtime = R>,
  <T::Resolver as AddressResolver>::Address: Send + Sync + 'static,
  SC: Sidecar<Runtime = R>,
  R: Runtime,
{
  /// Registers a new observer.
  ///
  /// a function that can be registered in order to filter observations.
  /// The function reports whether the observation should be included - if
  /// it returns false, the observation will be filtered out.
  pub async fn register_observer<Filter>(
    &self,
    blocking: bool,
    filter: Option<Filter>,
  ) -> Observer<T::Id, <T::Resolver as AddressResolver>::Address>
  where
    Filter: for<'a> Fn(&'a Observed<T::Id, <T::Resolver as AddressResolver>::Address>) -> bool
      + Send
      + Sync
      + 'static,
  {
    let id = ObserverId(NEXT_OBSERVER_ID.fetch_add(1, Ordering::AcqRel));
    let (tx, rx) = async_channel::unbounded();
    let inner = Arc::new(Inner {
      num_observed: AtomicU64::new(0),
      num_dropped: AtomicU64::new(0),
      tx,
      rx,
      filter: filter.map(|f| {
        Box::new(f)
          as Box<
            dyn Fn(&Observed<T::Id, <T::Resolver as AddressResolver>::Address>) -> bool
              + Send
              + Sync
              + 'static,
          >
      }),
    });

    let observer = Observer {
      inner,
      blocking,
      id,
    };

    self
      .inner
      .observers
      .write()
      .await
      .insert(id, observer.clone());
    observer
  }

  /// Deregisters an observer.
  pub async fn deregister_observer(&self, id: &ObserverId) {
    if let Some(ob) = self.inner.observers.write().await.remove(id) {
      ob.inner.tx.close();
    }
  }
}

pub(crate) async fn observe<I: Id, A: Address>(
  observers: &async_lock::RwLock<HashMap<ObserverId, Observer<I, A>>>,
  o: Observed<I, A>,
) {
  // In general observers should not block. But in any case this isn't
  // disastrous as we only hold a read lock, which merely prevents
  // registration / deregistration of observers.
  let observers = observers.read().await;
  join_all(
    observers
      .iter()
      .filter_map(|(_, or)| match or.inner.filter.as_ref() {
        Some(f) if !f(&o) => None,
        _ => {
          let o = o.clone();
          Some(async move {
            if or.blocking {
              match or.inner.tx.send(o).await {
                Ok(_) => {
                  or.inner.num_observed.fetch_add(1, Ordering::AcqRel);
                }
                Err(_) => {
                  or.inner.num_dropped.fetch_add(1, Ordering::AcqRel);
                }
              }
            } else {
              futures::select! {
                _ = or.inner.tx.send(o).fuse() => {
                  or.inner.num_observed.fetch_add(1, Ordering::AcqRel);
                }
                default => {
                  or.inner.num_dropped.fetch_add(1, Ordering::AcqRel);
                }
              }
            }
          })
        }
      }),
  )
  .await;
}
