use std::{future::Future, sync::Arc};

use crate::{
  membership::Membership,
  options::SnapshotVersion,
  transport::{Address, Id},
};

mod meta;
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

  // /// The source type used to read snapshots.
  // type Source: SnapshotSource<Id = Self::Id, Address = Self::Address>;

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
  ) -> impl Future<Output = Result<impl SnapshotSink, Self::Error>> + Send;

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
pub trait SnapshotSink: futures::io::AsyncWrite + Send + Sync + Unpin + 'static {
  /// The snapshot id for the parent snapshot.
  fn id(&self) -> SnapshotId;

  /// Cancel the sink.
  fn cancel(&mut self) -> impl Future<Output = std::io::Result<()>> + Send;
}

/// Used to open the snapshot when user get the result from [`snapshot`] or [`snapshot_timeout`].
/// 
/// [`snapshot`]: struct.RaftCore#method.snapshot
/// [`snapshot_timeout`]: struct.RaftCore#method.snapshot_timeout
pub struct SnapshotSource<S: Storage>(Option<(SnapshotId, Arc<S>)>);

impl<S: Storage> Default for SnapshotSource<S> {
  fn default() -> Self {
    Self(None)
  }
}

impl<S: Storage> SnapshotSource<S> {
  pub(crate) fn new(id: SnapshotId, store: Arc<S>) -> Self {
    Self(Some((id, store)))
  }

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
      Some((id, store)) => {
        store
          .snapshot_store()
          .open(id)
          .await
          .map(|(id, reader)| (id, Box::new(reader) as Box<_>))
          .map_err(<S::Error as StorageError>::snapshot)
      },
      // TODO: error cleanup
      None => Err(<S::Error as StorageError>::custom("no snapshot available")),
    }
  }
}

