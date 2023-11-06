use std::future::Future;

use crate::{
  membership::Membership,
  options::SnapshotVersion,
  transport::{Address, Id},
};

mod meta;
pub use meta::*;
use nodecraft::CheapClone;

/// Used to allow for flexible implementations
/// of snapshot storage and retrieval. For example, a client could implement
/// a shared state store such as S3, allowing new nodes to restore snapshots
/// without streaming from the leader.
pub trait SnapshotStorage: Send + Sync + 'static {
  /// The error type returned by the snapshot storage.
  type Error: std::error::Error + Send + Sync + 'static;
  /// The async runtime used by the storage.
  type Runtime: agnostic::Runtime;

  /// The id type used to identify nodes.
  type Id: Id + CheapClone + Send + Sync + 'static;
  /// The address type of node.
  type Address: Address + CheapClone + Send + Sync + 'static;

  type Sink: SnapshotSink<Runtime = Self::Runtime>;
  type Source: SnapshotSource<Id = Self::Id, Address = Self::Address, Runtime = Self::Runtime>;
  type Options;

  fn new(opts: Self::Options) -> impl Future<Output = Result<Self, Self::Error>> + Send
  where
    Self: Sized;

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
  fn open(&self, id: &SnapshotId)
    -> impl Future<Output = Result<Self::Source, Self::Error>> + Send;
}

/// Returned by `start_snapshot`. The `FinateStateMachine` will write state
/// to the sink. On error, `cancel` will be invoked.
pub trait SnapshotSink: futures::io::AsyncWrite + Send + Sync + Unpin + 'static {
  /// The async runtime used by the storage.
  type Runtime: agnostic::Runtime;

  /// The snapshot id for the parent snapshot.
  fn id(&self) -> SnapshotId;

  /// Cancel the sink.
  fn cancel(&mut self) -> impl Future<Output = std::io::Result<()>> + Send;

  /// Close the sink.
  fn close(self) -> impl Future<Output = std::io::Result<()>> + Send;
}

/// Returned by [`SnapshotStorage::open`]. The `FinateStateMachine` will read state
/// from the source. On error, `cancel` will be invoked.
pub trait SnapshotSource: futures::io::AsyncRead + Unpin + Send + Sync + 'static {
  /// The async runtime used by the storage.
  type Runtime: agnostic::Runtime;
  /// The id type used to identify nodes.
  type Id: Id + CheapClone + Send + Sync + 'static;
  /// The address type of node.
  type Address: Address + CheapClone + Send + Sync + 'static;

  fn meta(&self) -> &SnapshotMeta<Self::Id, Self::Address>;
}
