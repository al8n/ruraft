use agnostic::Runtime;
use futures::{AsyncRead, AsyncWrite};
use nodecraft::{NodeAddress, NodeId};
use ruraft_core::storage::{SnapshotId, SnapshotMeta, SnapshotSink, SnapshotStorage};
use ruraft_memory::storage::snapshot::MemorySnapshotSink;
use ruraft_snapshot::sync::FileSnapshotSink;

use std::{
  pin::Pin,
  task::{Context, Poll},
};

pub enum SupportedSnapshotSink<R> {
  File {
    id: SnapshotId,
    writer: FileSnapshotSink<NodeId, NodeAddress, R>,
  },
  Memory {
    id: SnapshotId,
    writer: MemorySnapshotSink<NodeId, NodeAddress>,
  },
}

impl<R: Runtime> futures::AsyncWrite for SupportedSnapshotSink<R> {
  fn poll_write(
    self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &[u8],
  ) -> Poll<std::io::Result<usize>> {
    match self.get_mut() {
      Self::File { id: _, writer } => Pin::new(writer).poll_write(cx, buf),
      Self::Memory { id: _, writer } => Pin::new(writer).poll_write(cx, buf),
    }
  }

  fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
    match self.get_mut() {
      Self::File { id: _, writer } => Pin::new(writer).poll_flush(cx),
      Self::Memory { id: _, writer } => Pin::new(writer).poll_flush(cx),
    }
  }

  fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
    match self.get_mut() {
      Self::File { id: _, writer } => Pin::new(writer).poll_close(cx),
      Self::Memory { id: _, writer } => Pin::new(writer).poll_close(cx),
    }
  }
}

impl<R: Runtime> ruraft_core::storage::SnapshotSink for SupportedSnapshotSink<R> {
  fn id(&self) -> SnapshotId {
    match self {
      Self::File { id, .. } => *id,
      Self::Memory { id, .. } => *id,
    }
  }

  async fn cancel(&mut self) -> std::io::Result<()> {
    match self {
      Self::File { id: _, writer } => writer.cancel().await,
      Self::Memory { id: _, writer } => writer.cancel().await,
    }
  }
}

#[repr(transparent)]
pub struct SupportedSnapshotSource {
  reader: Box<dyn AsyncRead + Send + Sync + Unpin + 'static>,
}

impl SupportedSnapshotSource {
  pub fn new(reader: impl AsyncRead + Send + Sync + Unpin + 'static) -> Self {
    Self {
      reader: Box::new(reader),
    }
  }
}

impl futures::AsyncRead for SupportedSnapshotSource {
  fn poll_read(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut [u8],
  ) -> Poll<std::io::Result<usize>> {
    let reader = self.reader.as_mut();
    Pin::new(reader).poll_read(cx, buf)
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
  type Runtime = R;
  type Id = NodeId;
  type Address = NodeAddress;
  type Sink = SupportedSnapshotSink<R>;

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
        .map(|sink| SupportedSnapshotSink::File {
          id: sink.id(),
          writer: sink,
        })
        .map_err(|e| SupportedSnapshotStorageError::Any(Box::new(e))),
      Self::Memory(m) => m
        .create(version, term, index, membership, membership_index)
        .await
        .map(|sink| SupportedSnapshotSink::Memory {
          id: sink.id(),
          writer: sink,
        })
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

  async fn open(
    &self,
    id: ruraft_core::storage::SnapshotId,
  ) -> Result<
    (
      SnapshotMeta<Self::Id, Self::Address>,
      impl futures::AsyncRead + Send + Sync + Unpin + 'static,
    ),
    Self::Error,
  > {
    match self {
      Self::File(f) => f
        .open(id)
        .await
        .map(|(meta, reader)| (meta, SupportedSnapshotSource::new(reader)))
        .map_err(|e| SupportedSnapshotStorageError::Any(Box::new(e))),
      Self::Memory(m) => m
        .open(id)
        .await
        .map(|(meta, reader)| (meta, SupportedSnapshotSource::new(reader)))
        .map_err(|e| SupportedSnapshotStorageError::Any(Box::new(e))),
    }
  }
}
