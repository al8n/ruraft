use std::{
  io,
  pin::Pin,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
  task::{Context, Poll},
};

use agnostic::Runtime;
use async_lock::RwLock;
use futures::{AsyncRead, AsyncWrite, FutureExt};

use crate::{membership::Membership, options::SnapshotVersion};

use super::{SnapshotId, SnapshotMeta, SnapshotSink, SnapshotSource, SnapshotStorage};

/// Implements the [`SnapshotStorage`] trait in memory and
/// retains only the most recent snapshot
///
/// **N.B.** This struct should only be used in test, and never be used in production.
#[derive(Debug, Default)]
pub struct MemorySnapshotStorage<R: Runtime> {
  latest: Arc<RwLock<MemorySnapshot>>,
  has_snapshot: AtomicBool,
  _runtime: std::marker::PhantomData<R>,
}

#[async_trait::async_trait]
impl<R: Runtime> SnapshotStorage for MemorySnapshotStorage<R> {
  type Error = io::Error;
  type Sink = MemorySnapshotSink<R>;
  type Source = MemorySnapshotSource<R>;
  type Options = ();
  type Runtime = R;

  async fn new(_opts: Self::Options) -> Result<Self, Self::Error>
  where
    Self: Sized,
  {
    Ok(Self {
      latest: Arc::new(RwLock::new(Default::default())),
      has_snapshot: AtomicBool::new(false),
      _runtime: std::marker::PhantomData,
    })
  }

  async fn create(
    &self,
    version: SnapshotVersion,
    index: u64,
    term: u64,
    membership: Membership,
    membership_index: u64,
  ) -> Result<Self::Sink, Self::Error> {
    if !version.valid() {
      return Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("invalid snapshot version: {}", version as u8),
      ));
    }

    let mut lock = self.latest.write().await;

    self.has_snapshot.store(true, Ordering::Release);
    *lock = MemorySnapshot {
      meta: SnapshotMeta {
        version,
        id: SnapshotId::new(index, term),
        membership,
        membership_index,
        size: 0,
        index,
        term,
      },
      contents: Default::default(),
    };

    Ok(MemorySnapshotSink {
      id: lock.meta.id,
      snap: self.latest.clone(),
      _runtime: std::marker::PhantomData,
    })
  }

  async fn list(&self) -> Result<Vec<SnapshotMeta>, Self::Error> {
    let lock = self.latest.read().await;
    if !self.has_snapshot.load(Ordering::Acquire) {
      return Ok(vec![]);
    }

    Ok(vec![lock.meta.clone()])
  }

  async fn open(&self, id: &SnapshotId) -> Result<Self::Source, Self::Error> {
    let lock = self.latest.read().await;
    if lock.meta.id.ne(id) {
      return Err(io::Error::new(
        io::ErrorKind::NotFound,
        format!(
          "failed to open snapshot id (term: {}, index: {})",
          lock.meta.id.term, lock.meta.id.index
        ),
      ));
    }

    // Make a copy of the contents, since a bytes.Buffer can only be read
    // once.
    Ok(MemorySnapshotSource {
      meta: lock.meta.clone(),
      contents: lock.contents.clone(),
      _runtime: std::marker::PhantomData,
    })
  }
}

#[derive(Debug, Default)]
struct MemorySnapshot {
  meta: SnapshotMeta,
  contents: Vec<u8>,
}

/// Implements [`SnapshotSink`] in memory
///
/// **N.B.** This struct should only be used in test, and never be used in production.
#[derive(Debug, Default, Clone)]
pub struct MemorySnapshotSink<R: Runtime> {
  snap: Arc<RwLock<MemorySnapshot>>,
  id: SnapshotId,
  _runtime: std::marker::PhantomData<R>,
}

impl<R: Runtime> AsyncWrite for MemorySnapshotSink<R> {
  fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
    let mut snap = futures::ready!(self.snap.write().poll_unpin(cx));
    snap.contents.extend_from_slice(buf);
    snap.meta.size += buf.len() as u64;
    Poll::Ready(Ok(buf.len()))
  }

  fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    Poll::Ready(Ok(()))
  }

  fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    Poll::Ready(Ok(()))
  }
}

#[async_trait::async_trait]
impl<R: Runtime> SnapshotSink for MemorySnapshotSink<R> {
  type Runtime = R;

  fn id(&self) -> &SnapshotId {
    &self.id
  }

  async fn cancel(&mut self) -> std::io::Result<()> {
    Ok(())
  }
}

/// Implements [`SnapshotSource`] in memory
///
/// **N.B.** This struct should only be used in test, and never be used in production.
#[derive(Debug, Clone)]
pub struct MemorySnapshotSource<R: Runtime> {
  meta: SnapshotMeta,
  contents: Vec<u8>,
  _runtime: std::marker::PhantomData<R>,
}

impl<R: Runtime> AsyncRead for MemorySnapshotSource<R> {
  fn poll_read(
    mut self: Pin<&mut Self>,
    _cx: &mut Context<'_>,
    buf: &mut [u8],
  ) -> Poll<io::Result<usize>> {
    let len = std::cmp::min(buf.len(), self.contents.len());
    buf[..len].copy_from_slice(&self.contents[..len]);
    self.contents.drain(..len);
    Poll::Ready(Ok(len))
  }
}

impl<R: Runtime> SnapshotSource for MemorySnapshotSource<R> {
  type Runtime = R;

  fn meta(&self) -> &SnapshotMeta {
    &self.meta
  }
}

#[cfg(feature = "test")]
pub(super) mod tests {
  use futures::{AsyncReadExt, AsyncWriteExt};

  use super::*;

  pub async fn test_memory_snapshot_storage_create<R: Runtime>() {
    let snap = MemorySnapshotStorage::<R>::new(()).await.unwrap();

    // check no snapshots
    let snaps = snap.list().await.unwrap();
    assert_eq!(snaps.len(), 0, "did not expect any snapshots");

    // create a new sink
    let mut sink = snap
      .create(SnapshotVersion::V1, 10, 3, Membership::default(), 2)
      .await
      .unwrap();

    // The sink is not done, should not be in a list!
    let snaps = snap.list().await.unwrap();
    assert_eq!(snaps.len(), 1, "should always be 1 snapshot");

    // Write to the sink
    sink.write_all(b"first\n").await.unwrap();
    sink.write_all(b"second\n").await.unwrap();

    // Done!
    sink.close().await.unwrap();

    // Should have a snapshot
    let snaps = snap.list().await.unwrap();
    assert_eq!(snaps.len(), 1, "expect a snapshots");

    // check the latest
    let latest = snaps.first().unwrap();
    assert_eq!(latest.id.index, 10, "expected index 10");
    assert_eq!(latest.id.term, 3, "expected term 3");
    assert_eq!(latest.membership_index, 2, "expected membership index 2");
    assert_eq!(latest.size, 13, "expected size 13");

    // Read the snapshot
    let mut source = snap.open(&latest.id).await.unwrap();
    let mut buf = vec![];
    source.read_to_end(&mut buf).await.unwrap();

    // Ensure a match
    assert_eq!(buf, b"first\nsecond\n", "expected contents to match");
  }

  pub async fn test_memory_snapshot_storage_open_snapshot_twice<R: Runtime>() {
    let snap = MemorySnapshotStorage::<R>::new(()).await.unwrap();

    // create a new sink
    let mut sink = snap
      .create(SnapshotVersion::V1, 10, 3, Membership::default(), 2)
      .await
      .unwrap();

    // Write to the sink
    sink.write_all(b"data\n").await.unwrap();
    sink.close().await.unwrap();

    // Read the snapshot a first time
    let mut source = snap.open(sink.id()).await.unwrap();

    // Read out everything
    let mut buf = vec![];
    source.read_to_end(&mut buf).await.unwrap();

    // Ensure a match
    assert_eq!(buf, b"data\n", "expected contents to match");

    // Read the snapshot a second time
    let mut source = snap.open(sink.id()).await.unwrap();
    // Read out everything
    let mut buf = vec![];
    source.read_to_end(&mut buf).await.unwrap();
    assert_eq!(buf, b"data\n", "expected contents to match");
  }
}
