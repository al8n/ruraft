use std::{
  io,
  pin::Pin,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
  task::{Context, Poll},
};

use agnostic_lite::RuntimeLite;
use async_lock::RwLock;
use futures::{AsyncRead, AsyncWrite, FutureExt};

use ruraft_core::{
  membership::Membership,
  options::SnapshotVersion,
  storage::{SnapshotId, SnapshotMeta, SnapshotSink, SnapshotStorage},
  transport::{Address, Id},
};

/// Implements the [`SnapshotStorage`] trait in memory and
/// retains only the most recent snapshot
///
/// **N.B.** This struct should only be used in test, and never be used in production.
pub struct MemorySnapshotStorage<I, A, R> {
  latest: Arc<RwLock<Option<MemorySnapshot<I, A>>>>,
  has_snapshot: AtomicBool,
  _runtime: std::marker::PhantomData<R>,
}

impl<I: core::fmt::Debug, A: core::fmt::Debug, R> core::fmt::Debug
  for MemorySnapshotStorage<I, A, R>
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("MemorySnapshotStorage")
      .field("latest", &self.latest)
      .field("has_snapshot", &self.has_snapshot)
      .finish()
  }
}

impl<I, A, R> Default for MemorySnapshotStorage<I, A, R> {
  fn default() -> Self {
    Self::new()
  }
}

impl<I, A, R> MemorySnapshotStorage<I, A, R> {
  /// Create a new memory snapshot storage
  pub fn new() -> Self {
    Self {
      latest: Arc::new(RwLock::new(Default::default())),
      has_snapshot: AtomicBool::new(false),
      _runtime: std::marker::PhantomData,
    }
  }
}

impl<I, A, R> SnapshotStorage for MemorySnapshotStorage<I, A, R>
where
  I: Id,
  A: Address,
  R: RuntimeLite,
{
  type Error = io::Error;
  type Runtime = R;
  type Id = I;
  type Address = A;
  type Sink = MemorySnapshotSink<I, A>;

  async fn create(
    &self,
    version: SnapshotVersion,
    index: u64,
    term: u64,
    membership: Membership<Self::Id, Self::Address>,
    membership_index: u64,
  ) -> Result<Self::Sink, Self::Error> {
    if !version.valid() {
      return Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("invalid snapshot version: {}", version as u8),
      ));
    }

    let mut lock = self.latest.write().await;
    let id = SnapshotId::new(index, term);

    self.has_snapshot.store(true, Ordering::Release);
    *lock = Some(MemorySnapshot {
      meta: SnapshotMeta {
        version,
        timestamp: id.timestamp(),
        membership,
        membership_index,
        size: 0,
        index,
        term,
      },
      contents: Default::default(),
    });

    Ok(MemorySnapshotSink {
      id: lock.as_ref().unwrap().meta.id(),
      snap: self.latest.clone(),
    })
  }

  async fn list(&self) -> Result<Vec<SnapshotMeta<Self::Id, Self::Address>>, Self::Error> {
    let lock = self.latest.read().await;
    if !self.has_snapshot.load(Ordering::Acquire) {
      return Ok(vec![]);
    }

    Ok(
      lock
        .as_ref()
        .map(|m| vec![m.meta.clone()])
        .unwrap_or_default(),
    )
  }

  async fn open(
    &self,
    id: SnapshotId,
  ) -> Result<
    (
      SnapshotMeta<Self::Id, Self::Address>,
      impl futures::AsyncRead + Send + Sync + Unpin + 'static,
    ),
    Self::Error,
  > {
    let lock = self.latest.read().await;
    if let Some(m) = lock.as_ref() {
      if m.meta.id().ne(&id) {
        return Err(io::Error::new(
          io::ErrorKind::NotFound,
          format!(
            "failed to open snapshot id (term: {}, index: {})",
            m.meta.term, m.meta.index
          ),
        ));
      }

      // Make a copy of the contents, since a bytes.Buffer can only be read
      // once.
      Ok((
        m.meta.clone(),
        MemorySnapshotSource {
          contents: m.contents.clone(),
        },
      ))
    } else {
      Err(io::Error::new(
        io::ErrorKind::NotFound,
        format!(
          "failed to open snapshot id (term: {}, index: {})",
          id.term(),
          id.index()
        ),
      ))
    }
  }
}

#[derive(Debug)]
struct MemorySnapshot<I, A> {
  meta: SnapshotMeta<I, A>,
  contents: Vec<u8>,
}

/// Implements [`SnapshotSink`] in memory
///
/// **N.B.** This struct should only be used in test, and never be used in production.
#[derive(Debug, Clone)]
pub struct MemorySnapshotSink<I, A> {
  snap: Arc<RwLock<Option<MemorySnapshot<I, A>>>>,
  id: SnapshotId,
}

impl<I: Id, A: Address> AsyncWrite for MemorySnapshotSink<I, A>
where
  I: Id,
  A: Address,
{
  fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
    let write_lock = self.snap.write();
    futures::pin_mut!(write_lock);
    let mut snap = futures::ready!(write_lock.poll_unpin(cx));
    match snap.as_mut() {
      Some(snap) => {
        snap.contents.extend_from_slice(buf);
        snap.meta.size += buf.len() as u64;
        Poll::Ready(Ok(buf.len()))
      }
      None => Poll::Ready(Err(io::Error::new(
        io::ErrorKind::NotFound,
        "snapshot not found",
      ))),
    }
  }

  fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    Poll::Ready(Ok(()))
  }

  fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    Poll::Ready(Ok(()))
  }
}

impl<I, A> SnapshotSink for MemorySnapshotSink<I, A>
where
  I: Id + Unpin,
  A: Address + Unpin,
{
  fn id(&self) -> SnapshotId {
    self.id
  }

  fn poll_cancel(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
    Poll::Ready(Ok(()))
  }
}

/// Implements [`SnapshotSource`] in memory
///
/// **N.B.** This struct should only be used in test, and never be used in production.
#[derive(Debug, Clone)]
pub struct MemorySnapshotSource {
  contents: Vec<u8>,
}

impl AsyncRead for MemorySnapshotSource {
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
