use std::{
  future::Future,
  pin::Pin,
  sync::Arc,
  task::{Context, Poll},
};

use crate::{
  membership::Membership,
  options::SnapshotVersion,
  transport::{Address, Id},
};

mod meta;
use futures::AsyncWrite;
// use futures::AsyncRead;
pub use meta::*;

use super::{Storage, StorageError};

#[auto_impl::auto_impl(Box, Arc)]
/// Used to allow for flexible implementations
/// of snapshot storage and retrieval. For example, a client could implement
/// a shared state store such as S3, allowing new nodes to restore snapshots
/// without streaming from the leader.
pub trait SnapshotStorage: Send + Sync + 'static {
  /// The error type returned by the snapshot storage.
  type Error: std::error::Error + From<std::io::Error> + Send + Sync + 'static;

  /// The async runtime used by the storage.
  type Runtime: agnostic::Runtime;

  /// The id type used to identify nodes.
  type Id: Id;
  /// The address type of node.
  type Address: Address;

  /// The sink type used to write snapshots.
  type Sink: SnapshotSink + Unpin;

  /// Used to begin a snapshot at a given index and term, and with
  /// the given committed configuration. The version parameter controls
  /// which snapshot version to create.
  fn create(
    &self,
    version: SnapshotVersion,
    term: u64,
    index: u64,
    membership: Membership<Self::Id, Self::Address>,
    membership_index: u64,
  ) -> impl Future<Output = Result<Self::Sink, Self::Error>> + Send;

  /// Used to list the available snapshots in the store.
  /// It should return then in descending order, with the highest index first.
  fn list(
    &self,
  ) -> impl Future<Output = Result<Vec<SnapshotMeta<Self::Id, Self::Address>>, Self::Error>> + Send;

  /// Open takes a snapshot ID and provides a reader.
  fn open(
    &self,
    id: SnapshotId,
  ) -> impl Future<
    Output = Result<
      (
        SnapshotMeta<Self::Id, Self::Address>,
        impl futures::AsyncRead + Send + Sync + Unpin + 'static,
      ),
      Self::Error,
    >,
  > + Send;
}

/// Returned by `start_snapshot`. The `FinateStateMachine` will write state
/// to the sink. On error, `cancel` will be invoked.
pub trait SnapshotSink: AsyncWrite + Send + Sync + Unpin {
  /// The id of the snapshot being written.
  fn id(&self) -> SnapshotId;

  /// Attempt to cancel the snapshot sink.
  ///
  /// On success, returns `Poll::Ready(Ok(()))`.
  ///
  /// If cancelling cannot immediately complete, this function returns
  /// `Poll::Pending` and arranges for the current task (via
  /// `cx.waker().wake_by_ref()`) to receive a notification when the object can make
  /// progress towards closing.
  fn poll_cancel(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>>;
}

impl<T: SnapshotSink> SnapshotSink for Box<T> {
  fn id(&self) -> SnapshotId {
    T::id(self)
  }

  fn poll_cancel(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
    Pin::new(&mut **self.get_mut()).poll_cancel(cx)
  }
}

impl<T: SnapshotSink> SnapshotSink for &mut T {
  fn id(&self) -> SnapshotId {
    T::id(self)
  }

  fn poll_cancel(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
    Pin::new(&mut **self.get_mut()).poll_cancel(cx)
  }
}

/// An extension trait which adds utility methods to `SnapshotSink` types.
pub trait SnapshotSinkExt: SnapshotSink {
  /// Creates a future which will entirely cancel this `SnapshotSink`.
  fn cancel(&mut self) -> Cancel<'_, Self>
  where
    Self: Unpin,
  {
    Cancel::new(self)
  }
}

impl<T: SnapshotSink + ?Sized> SnapshotSinkExt for T {}

/// Future for the [`cancel`](SnapshotSinkExt::cancel) method.
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Cancel<'a, W: ?Sized> {
  sink: &'a mut W,
}

impl<W: ?Sized + Unpin> Unpin for Cancel<'_, W> {}

impl<'a, W: SnapshotSink + ?Sized + Unpin> Cancel<'a, W> {
  pub(super) fn new(sink: &'a mut W) -> Self {
    Self { sink }
  }
}

impl<W: SnapshotSink + ?Sized + Unpin> Future for Cancel<'_, W> {
  type Output = std::io::Result<()>;

  fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    Pin::new(&mut *self.sink).poll_cancel(cx)
  }
}

/// Used to open the snapshot when user get the result from [`snapshot`] or [`snapshot_timeout`].
///
/// [`snapshot`]: struct.RaftCore#method.snapshot
/// [`snapshot_timeout`]: struct.RaftCore#method.snapshot_timeout
pub struct SnapshotSource<S>(Option<(SnapshotId, Arc<S>)>);

impl<S> core::fmt::Debug for SnapshotSource<S> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("SnapshotSource")
      .field("id", &self.0.as_ref().map(|(id, _)| id))
      .finish()
  }
}

impl<S> Clone for SnapshotSource<S> {
  fn clone(&self) -> Self {
    Self(self.0.clone())
  }
}

impl<S> Default for SnapshotSource<S> {
  fn default() -> Self {
    Self(None)
  }
}

impl<S> SnapshotSource<S> {
  pub(crate) fn new(id: SnapshotId, store: Arc<S>) -> Self {
    Self(Some((id, store)))
  }
}

impl<S: Storage> SnapshotSource<S> {
  /// Used to open the snapshot. This is filled in
  /// once the future returns with no error.
  pub async fn open(
    &mut self,
  ) -> Result<
    (
      SnapshotMeta<S::Id, S::Address>,
      Box<dyn futures::AsyncRead + Send + Sync + Unpin + 'static>,
    ),
    S::Error,
  > {
    match self.0.take() {
      Some((id, store)) => store
        .snapshot_store()
        .open(id)
        .await
        .map(|(id, reader)| (id, Box::new(reader) as Box<_>))
        .map_err(<S::Error as StorageError>::snapshot),
      // TODO: error cleanup
      None => Err(<S::Error as StorageError>::custom("no snapshot available")),
    }
  }
}
