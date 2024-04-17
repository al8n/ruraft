use std::{
  collections::HashMap,
  marker::PhantomData,
  sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
  },
  time::Instant,
};

use agnostic_lite::RuntimeLite;
use futures::{future::join_all, FutureExt, Stream};
use nodecraft::{resolver::AddressResolver, CheapClone};

use crate::{
  sidecar::Sidecar,
  storage::Storage,
  transport::{Transport, VoteRequest},
  FinateStateMachine, Node, RaftCore, Role,
};
pub use async_channel::{RecvError, TryRecvError};

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
    write!(f, "ObserverId({})", self.0)
  }
}

impl core::fmt::Debug for ObserverId {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", self.0)
  }
}

/// ObservationFilter is a function that can be registered in order to filter observations.
pub trait ObservationFilter<I, A>: Send + Sync + 'static {
  /// Returns true if the observation should be observed.
  fn filter(&self, o: &Observation<I, A>) -> bool;
}

impl<I, A, F> ObservationFilter<I, A> for F
where
  F: Fn(&Observation<I, A>) -> bool + Send + Sync + 'static,
{
  #[inline]
  fn filter(&self, o: &Observation<I, A>) -> bool {
    (self)(o)
  }
}

/// ObserveAll is a filter that allows all observations.
pub struct ObserveAll<I, A>(PhantomData<(I, A)>);

impl<I, A> Default for ObserveAll<I, A> {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl<I, A> Clone for ObserveAll<I, A> {
  #[inline]
  fn clone(&self) -> Self {
    *self
  }
}

impl<I, A> Copy for ObserveAll<I, A> {}

impl<I, A> ObserveAll<I, A> {
  /// Returns a new ObserveAll filter.
  #[inline]
  pub const fn new() -> Self {
    Self(PhantomData)
  }
}

impl<I: Send + Sync + 'static, A: Send + Sync + 'static> ObservationFilter<I, A>
  for ObserveAll<I, A>
{
  #[inline]
  fn filter(&self, _o: &Observation<I, A>) -> bool {
    true
  }
}

/// Observation-specific data
#[derive(Clone)]
pub enum Observation<I, A> {
  /// Used for the data when leadership changes.
  Leader(Option<Node<I, A>>),
  /// Sent to observers when peers change.
  Peer {
    /// The ID of the peer.
    id: I,
    /// Whether the peer was removed.
    removed: bool,
  },
  /// Sent when a node fails to heartbeat with the leader
  HeartbeatFailed {
    /// The ID of the node that failed to heartbeat
    id: I,
    /// The last time we heard from the node
    last_contact: Instant,
  },
  /// Sent when a node resumes to heartbeat with the leader following failures
  HeartbeatResumed(I),
  /// Sent when a node has a role change
  Role(Role),
  /// Sent when we get a request vote RPC call
  RequestVote(VoteRequest<I, A>),
}

impl<I: CheapClone, A: CheapClone> CheapClone for Observation<I, A> {
  fn cheap_clone(&self) -> Self {
    match self {
      Self::Leader(l) => Self::Leader(l.cheap_clone()),
      Self::Peer { id, removed } => Self::Peer {
        id: id.cheap_clone(),
        removed: *removed,
      },
      Self::HeartbeatFailed { id, last_contact } => Self::HeartbeatFailed {
        id: id.cheap_clone(),
        last_contact: *last_contact,
      },
      Self::HeartbeatResumed(h) => Self::HeartbeatResumed(h.cheap_clone()),
      Self::Role(r) => Self::Role(*r),
      Self::RequestVote(req) => Self::RequestVote(req.cheap_clone()),
    }
  }
}

struct Inner<I, A> {
  // numObservation and numDropped are performance counters for this observer.
  num_observed: AtomicU64,
  num_dropped: AtomicU64,

  /// channel receives observations.
  tx: async_channel::Sender<Observation<I, A>>,

  // rx: async_channel::Receiver<Observation<I, A>>,
  /// Will be called to determine if an observation should be sent to
  /// the channel.
  filter: Option<Box<dyn ObservationFilter<I, A>>>,
}

/// Observer describes what to do with a given observation.
pub struct Observer<I, A> {
  inner: Arc<Inner<I, A>>,

  /// blocking, if true, will cause Raft to block when sending an observation
  /// to this observer. This should generally be set to false.
  blocking: bool,

  /// The ID of this observer in the Raft map.
  id: ObserverId,
}

impl<I, A> Observer<I, A> {
  /// Creates a new [`Observer`].
  pub fn new<Filter>(sender: ObserverSender<I, A>, blocking: bool, filter: Option<Filter>) -> Self
  where
    Filter: ObservationFilter<I, A>,
  {
    let id = ObserverId(NEXT_OBSERVER_ID.fetch_add(1, Ordering::AcqRel));
    let inner = Arc::new(Inner {
      num_observed: AtomicU64::new(0),
      num_dropped: AtomicU64::new(0),
      tx: sender.0,
      filter: filter.map(|f| Box::new(f) as Box<dyn ObservationFilter<I, A>>),
    });

    Observer {
      inner,
      blocking,
      id,
    }
  }

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
}

impl<I, A> Clone for Observer<I, A> {
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
      blocking: self.blocking,
      id: self.id,
    }
  }
}

impl<I, A> CheapClone for Observer<I, A> {}

/// Creates a bounded channel.
///
/// The created channel has space to hold at most `cap` [`Observation`]s at a time.
pub fn bounded<I, A>(cap: usize) -> (ObserverSender<I, A>, ObserverReceiver<I, A>) {
  let (tx, rx) = async_channel::bounded(cap);
  (ObserverSender(tx), ObserverReceiver(rx))
}

/// Creates an unbounded channel.
///
/// The created channel can hold an unlimited number of [`Observation`]s.
pub fn unbounded<I, A>() -> (ObserverSender<I, A>, ObserverReceiver<I, A>) {
  let (tx, rx) = async_channel::unbounded();
  (ObserverSender(tx), ObserverReceiver(rx))
}

/// ObserverSender is used to let users reuse the same sender for multiple observers.
pub struct ObserverSender<I, A>(async_channel::Sender<Observation<I, A>>);

impl<I, A> Clone for ObserverSender<I, A> {
  fn clone(&self) -> Self {
    Self(self.0.clone())
  }
}

impl<I, A> CheapClone for ObserverSender<I, A> {}

/// [`ObserverReceiver`] is kind of a mpmc receiver, used to receive [`Observation`]s from an [`Observer`]s.
#[pin_project::pin_project]
pub struct ObserverReceiver<I, A>(#[pin] async_channel::Receiver<Observation<I, A>>);

impl<I, A> Clone for ObserverReceiver<I, A> {
  fn clone(&self) -> Self {
    Self(self.0.clone())
  }
}

impl<I, A> CheapClone for ObserverReceiver<I, A> {}

impl<I, A> ObserverReceiver<I, A> {
  /// Receives a [`Observation`] from the channel.
  /// If the channel is empty, this method waits until there is a message.
  /// If the channel is closed, this method receives a message or returns an error if there are no more messages
  pub async fn recv(&self) -> Result<Observation<I, A>, RecvError> {
    self.0.recv().await
  }

  /// Attempts to receive an [`Observation`] without blocking.
  pub fn try_recv(&mut self) -> Result<Observation<I, A>, TryRecvError> {
    self.0.try_recv()
  }
}

impl<I, A> Stream for ObserverReceiver<I, A> {
  type Item = <async_channel::Receiver<Observation<I, A>> as Stream>::Item;

  fn poll_next(
    self: std::pin::Pin<&mut Self>,
    cx: &mut std::task::Context<'_>,
  ) -> std::task::Poll<Option<Self::Item>> {
    <async_channel::Receiver<Observation<I, A>> as Stream>::poll_next(self.project().0, cx)
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
  SC: Sidecar<Runtime = R>,
  R: RuntimeLite,
{
  /// Registers a new observer.
  ///
  /// a function that can be registered in order to filter observations.
  /// The function reports whether the observation should be included - if
  /// it returns false, the observation will be filtered out.
  pub async fn register_observer(
    &self,
    observer: &Observer<T::Id, <T::Resolver as AddressResolver>::Address>,
  ) {
    let id = observer.id();
    self
      .inner
      .observers
      .write()
      .await
      .insert(id, observer.clone());
  }

  /// Deregisters an observer.
  pub async fn deregister_observer(&self, id: &ObserverId) {
    if let Some(ob) = self.inner.observers.write().await.remove(id) {
      ob.inner.tx.close();
    }
  }
}

pub(crate) async fn observe<I: CheapClone + 'static, A: CheapClone + 'static>(
  observers: &async_lock::RwLock<HashMap<ObserverId, Observer<I, A>>>,
  o: Observation<I, A>,
) {
  // In general observers should not block. But in any case this isn't
  // disastrous as we only hold a read lock, which merely prevents
  // registration / deregistration of observers.
  let observers = observers.read().await;
  join_all(
    observers
      .iter()
      .filter_map(|(_, or)| match or.inner.filter.as_ref() {
        Some(f) if !f.filter(&o) => None,
        _ => {
          let o = o.cheap_clone();
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
                res = or.inner.tx.send(o).fuse() => {
                  match res {
                    Ok(_) => {
                      or.inner.num_observed.fetch_add(1, Ordering::AcqRel);
                    }
                    Err(_) => {
                      or.inner.num_dropped.fetch_add(1, Ordering::AcqRel);
                    }
                  }
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
