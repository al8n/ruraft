use std::{
  sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
  },
  time::Duration,
};

use agnostic_lite::RuntimeLite;
use futures::{AsyncRead, FutureExt, StreamExt};
use wg::future::AsyncWaitGroup;

const SNAPSHOT_RESTORE_MONITOR_INTERVAL: Duration = Duration::from_secs(10);

pub(crate) struct SnapshotRestoreMonitor<R: RuntimeLite> {
  wg: AsyncWaitGroup,
  shutdown_tx: async_channel::Sender<()>,
  _runtime: std::marker::PhantomData<R>,
}

impl<R: RuntimeLite> SnapshotRestoreMonitor<R> {
  pub(crate) fn new(ctr: Arc<AtomicU64>, size: u64, network_transfer: bool) -> Self {
    let (shutdown_tx, shutdown_rx) = async_channel::bounded(1);
    let wg = AsyncWaitGroup::new();
    let wg2 = wg.add(1);

    R::spawn_detach(async move { Self::run(ctr, wg2, size, network_transfer, shutdown_rx).await });

    Self {
      shutdown_tx,
      wg,
      _runtime: std::marker::PhantomData,
    }
  }

  pub(crate) async fn stop_and_wait(&self) {
    if self.shutdown_tx.close() {
      self.wg.wait().await;
    }
  }

  async fn run(
    ctr: Arc<AtomicU64>,
    wg: AsyncWaitGroup,
    size: u64,
    network_transfer: bool,
    shutdown_rx: async_channel::Receiver<()>,
  ) {
    let mut run_once = false;

    let mut ticker = R::interval(SNAPSHOT_RESTORE_MONITOR_INTERVAL);
    loop {
      futures::select! {
        _ = shutdown_rx.recv().fuse() => {
          if !run_once {
            Self::run_once(&ctr, size, network_transfer);
          }
          break;
        },
        _ = ticker.next().fuse() => {
          Self::run_once(&ctr, size, network_transfer);
          run_once = true;
        }
      }
    }
    wg.done();
  }

  fn run_once(ctr: &AtomicU64, size: u64, network_transfer: bool) {
    let read_bytes = ctr.load(Ordering::Acquire);
    let pct = (100 * read_bytes) as f64 / (size as f64);
    if network_transfer {
      tracing::info!(target = "ruraft.snapshot.restore.monitor", read_bytes = %read_bytes, percent_complete = %pct, "snapshot restore progress");
    } else {
      tracing::info!(target = "ruraft.snapshot.restore.monitor", read_bytes = %read_bytes, percent_complete = %pct, "snapshot network transfer progress");
    }
  }
}

#[pin_project::pin_project]
pub(crate) struct CountingSnapshotSourceReader<R> {
  #[pin]
  r: R,
  bytes: Arc<AtomicU64>,
}

impl<R> From<R> for CountingSnapshotSourceReader<R> {
  fn from(r: R) -> Self {
    Self {
      r,
      bytes: Arc::new(AtomicU64::new(0)),
    }
  }
}

impl<R> CountingSnapshotSourceReader<R> {
  pub fn ctr(&self) -> Arc<AtomicU64> {
    self.bytes.clone()
  }
}

impl<R: AsyncRead> AsyncRead for CountingSnapshotSourceReader<R> {
  fn poll_read(
    self: std::pin::Pin<&mut Self>,
    cx: &mut std::task::Context<'_>,
    buf: &mut [u8],
  ) -> std::task::Poll<std::io::Result<usize>> {
    let this = self.project();
    this.r.poll_read(cx, buf).map(|r| {
      r.map(|num| {
        this.bytes.fetch_add(num as u64, Ordering::AcqRel);
        num
      })
    })
  }
}
