use agnostic::Runtime;
use nodecraft::{NodeAddress, NodeId};
use ruraft_core::storage::{SnapshotId, SnapshotStorage};

use std::{
  pin::Pin,
  task::{Context, Poll},
};

pub enum SupportedSnapshotSink<R> {
  File(ruraft_snapshot::sync::FileSnapshotSink<NodeId, NodeAddress, R>),
  Memory(ruraft_memory::storage::snapshot::MemorySnapshotSink<NodeId, NodeAddress, R>),
}

impl<R: Runtime> futures::AsyncWrite for SupportedSnapshotSink<R> {
  fn poll_write(
    self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &[u8],
  ) -> Poll<std::io::Result<usize>> {
    match self.get_mut() {
      Self::File(f) => Pin::new(f).poll_write(cx, buf),
      Self::Memory(f) => Pin::new(f).poll_write(cx, buf),
    }
  }

  fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
    match self.get_mut() {
      Self::File(f) => Pin::new(f).poll_flush(cx),
      Self::Memory(f) => Pin::new(f).poll_flush(cx),
    }
  }

  fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
    match self.get_mut() {
      Self::File(f) => Pin::new(f).poll_close(cx),
      Self::Memory(f) => Pin::new(f).poll_close(cx),
    }
  }
}

impl<R: Runtime> ruraft_core::storage::SnapshotSink for SupportedSnapshotSink<R> {
  type Runtime = R;

  fn id(&self) -> SnapshotId {
    match self {
      Self::File(f) => f.id(),
      Self::Memory(m) => m.id(),
    }
  }

  async fn cancel(&mut self) -> std::io::Result<()> {
    match self {
      Self::File(f) => f.cancel().await,
      Self::Memory(f) => f.cancel().await,
    }
  }
}

pub enum SupportedSnapshotSource<R> {
  File(ruraft_snapshot::sync::FileSnapshotSource<NodeId, NodeAddress, R>),
  Memory(ruraft_memory::storage::snapshot::MemorySnapshotSource<NodeId, NodeAddress, R>),
}

impl<R: Runtime> futures::AsyncRead for SupportedSnapshotSource<R> {
  fn poll_read(
    self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut [u8],
  ) -> Poll<std::io::Result<usize>> {
    match self.get_mut() {
      Self::File(f) => Pin::new(f).poll_read(cx, buf),
      Self::Memory(f) => Pin::new(f).poll_read(cx, buf),
    }
  }
}

impl<R: Runtime> ruraft_core::storage::SnapshotSource for SupportedSnapshotSource<R> {
  /// The async runtime used by the storage.
  type Runtime = R;
  /// The id type used to identify nodes.
  type Id = NodeId;
  /// The address type of node.
  type Address = NodeAddress;

  /// Returns the snapshot meta information.
  fn meta(&self) -> &ruraft_core::storage::SnapshotMeta<Self::Id, Self::Address> {
    match self {
      Self::File(f) => f.meta(),
      Self::Memory(f) => f.meta(),
    }
  }
}

#[derive(derive_more::From, derive_more::Display)]
pub enum SupportedSnapshotStorageError {
  Any(Box<dyn std::error::Error + Send + Sync + 'static>),
  IO(std::io::Error),
}

impl core::fmt::Debug for SupportedSnapshotStorageError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::Any(e) => write!(f, "{e:?}"),
      Self::IO(e) => write!(f, "{e:?}"),
    }
  }
}

impl std::error::Error for SupportedSnapshotStorageError {}

#[derive(derive_more::From)]
pub enum SupportedSnapshotStorage<R> {
  File(ruraft_snapshot::sync::FileSnapshotStorage<NodeId, NodeAddress, R>),
  Memory(ruraft_memory::storage::snapshot::MemorySnapshotStorage<NodeId, NodeAddress, R>),
}

impl<R: Runtime> SnapshotStorage for SupportedSnapshotStorage<R> {
  type Error = SupportedSnapshotStorageError;
  type Sink = SupportedSnapshotSink<R>;
  type Source = SupportedSnapshotSource<R>;
  type Runtime = R;
  type Id = NodeId;
  type Address = NodeAddress;

  async fn create(
    &self,
    version: ruraft_core::options::SnapshotVersion,
    index: u64,
    term: u64,
    membership: ruraft_core::membership::Membership<Self::Id, Self::Address>,
    membership_index: u64,
  ) -> Result<Self::Sink, Self::Error> {
    match self {
      Self::File(f) => f
        .create(version, term, index, membership, membership_index)
        .await
        .map(SupportedSnapshotSink::File)
        .map_err(|e| SupportedSnapshotStorageError::Any(Box::new(e))),
      Self::Memory(m) => m
        .create(version, term, index, membership, membership_index)
        .await
        .map(SupportedSnapshotSink::Memory)
        .map_err(|e| SupportedSnapshotStorageError::Any(Box::new(e))),
    }
  }

  async fn list(
    &self,
  ) -> Result<Vec<ruraft_core::storage::SnapshotMeta<Self::Id, Self::Address>>, Self::Error> {
    match self {
      Self::File(f) => f
        .list()
        .await
        .map_err(|e| SupportedSnapshotStorageError::Any(Box::new(e))),
      Self::Memory(m) => m
        .list()
        .await
        .map_err(|e| SupportedSnapshotStorageError::Any(Box::new(e))),
    }
  }

  async fn open(&self, id: &ruraft_core::storage::SnapshotId) -> Result<Self::Source, Self::Error> {
    match self {
      Self::File(f) => f
        .open(id)
        .await
        .map(SupportedSnapshotSource::File)
        .map_err(|e| SupportedSnapshotStorageError::Any(Box::new(e))),
      Self::Memory(m) => m
        .open(id)
        .await
        .map(SupportedSnapshotSource::Memory)
        .map_err(|e| SupportedSnapshotStorageError::Any(Box::new(e))),
    }
  }
}
