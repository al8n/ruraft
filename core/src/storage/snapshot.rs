use crate::{
  membership::Membership,
  options::SnapshotVersion,
  transport::{NodeAddress, NodeId},
};

#[viewit::viewit(
  vis_all = "pub(crate)",
  getters(vis_all = "pub"),
  setters(vis_all = "pub", style = "ref")
)]
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SnapshotId {
  index: u64,
  term: u64,
  timestamp: u64,
}

impl SnapshotId {
  #[inline]
  pub fn new(index: u64, term: u64) -> Self {
    let now = std::time::SystemTime::now()
      .duration_since(std::time::UNIX_EPOCH)
      .unwrap()
      .as_millis() as u64;

    Self {
      index,
      term,
      timestamp: now,
    }
  }
}

/// Metadata of a snapshot.
#[viewit::viewit(getters(vis_all = "pub"), setters(vis_all = "pub", style = "ref"))]
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SnapshotMeta<Id: NodeId, Address: NodeAddress> {
  /// The version number of the snapshot metadata. This does not cover
  /// the application's data in the snapshot, that should be versioned
  /// separately.
  version: SnapshotVersion,

  /// id is opaque to the store, and is used for opening.
  id: SnapshotId,

  /// The index when the snapshot was taken.
  index: u64,
  /// The term when the snapshot was taken.
  term: u64,

  /// Membership at the time of the snapshot.
  #[viewit(getter(style = "ref", const))]
  membership: Membership<Id, Address>,
  /// The index of the membership that was taken
  membership_index: u64,

  /// The size of the snapshot, in bytes.
  size: u64,
}

impl<Id: NodeId, Address: NodeAddress> Default for SnapshotMeta<Id, Address> {
  fn default() -> Self {
    Self::new()
  }
}

impl<Id: NodeId, Address: NodeAddress> SnapshotMeta<Id, Address> {
  /// Create a snapshot meta.
  #[inline]
  pub fn new() -> Self {
    Self {
      ..Default::default()
    }
  }
}

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

  /// Open takes a snapshot ID and provides a ReadCloser.
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
