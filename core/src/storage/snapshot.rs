use crate::{
  membership::Membership,
  options::SnapshotVersion,
  transport::{NodeAddress, NodeId},
};

mod meta;
pub use meta::*;

/// Used to allow for flexible implementations
/// of snapshot storage and retrieval. For example, a client could implement
/// a shared state store such as S3, allowing new nodes to restore snapshots
/// without streaming from the leader.
#[async_trait::async_trait]
pub trait SnapshotStorage: Send + Sync + 'static {
  /// The error type returned by the snapshot storage.
  type Error: std::error::Error + Send + Sync + 'static;
  /// The async runtime used by the storage.
  type Runtime: agnostic::Runtime;

  /// The id type used to identify nodes.
  type NodeId: NodeId;
  /// The address type of node.
  type NodeAddress: NodeAddress;

  type Sink: SnapshotSink<Runtime = Self::Runtime>;
  type Source: SnapshotSource<
    NodeId = Self::NodeId,
    NodeAddress = Self::NodeAddress,
    Runtime = Self::Runtime,
  >;
  type Options;

  async fn new(opts: Self::Options) -> Result<Self, Self::Error>
  where
    Self: Sized;

  /// Used to begin a snapshot at a given index and term, and with
  /// the given committed configuration. The version parameter controls
  /// which snapshot version to create.
  async fn create(
    &self,
    version: SnapshotVersion,
    index: u64,
    term: u64,
    membership: Membership<Self::NodeId, Self::NodeAddress>,
    membership_index: u64,
  ) -> Result<Self::Sink, Self::Error>;

  /// Used to list the available snapshots in the store.
  /// It should return then in descending order, with the highest index first.
  async fn list(&self) -> Result<Vec<SnapshotMeta<Self::NodeId, Self::NodeAddress>>, Self::Error>;

  /// Open takes a snapshot ID and provides a reader.
  async fn open(&self, id: &SnapshotId) -> Result<Self::Source, Self::Error>;
}

/// Returned by `start_snapshot`. The `FinateStateMachine` will write state
/// to the sink. On error, `cancel` will be invoked.
#[async_trait::async_trait]
pub trait SnapshotSink: futures::io::AsyncWrite {
  /// The async runtime used by the storage.
  type Runtime: agnostic::Runtime;

  fn id(&self) -> &SnapshotId;

  async fn cancel(&mut self) -> std::io::Result<()>;
}

/// Returned by [`SnapshotStorage::open`]. The `FinateStateMachine` will read state
/// from the source. On error, `cancel` will be invoked.
pub trait SnapshotSource: futures::io::AsyncRead {
  /// The async runtime used by the storage.
  type Runtime: agnostic::Runtime;
  /// The id type used to identify nodes.
  type NodeId: NodeId;
  /// The address type of node.
  type NodeAddress: NodeAddress;

  fn meta(&self) -> &SnapshotMeta<Self::NodeId, Self::NodeAddress>;
}
