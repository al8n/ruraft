use crate::{membership::Membership, options::SnapshotVersion};

use serde::{Deserialize, Serialize};

mod file;
pub use file::*;

#[cfg(any(feature = "test", test))]
mod memory;
#[cfg(any(feature = "test", test))]
#[cfg_attr(docsrs, doc(cfg(feature = "test")))]
pub use memory::*;

#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
#[viewit::viewit]
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SnapshotMeta {
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
  membership: Membership,
  /// The index of the membership that was taken
  membership_index: u64,

  /// The size of the snapshot, in bytes.
  size: u64,
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

  type Sink: SnapshotSink<Runtime = Self::Runtime>;
  type Source: SnapshotSource<Runtime = Self::Runtime>;
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
    membership: Membership,
    membership_index: u64,
  ) -> Result<Self::Sink, Self::Error>;

  /// Used to list the available snapshots in the store.
  /// It should return then in descending order, with the highest index first.
  async fn list(&self) -> Result<Vec<SnapshotMeta>, Self::Error>;

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

  fn meta(&self) -> &SnapshotMeta;
}

#[cfg(feature = "test")]
pub(crate) mod tests {
  pub use super::file::tests::*;
  pub use super::memory::tests::*;
}
