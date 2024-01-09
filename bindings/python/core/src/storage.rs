use std::pin::Pin;

use futures::AsyncWrite;
use ruraft_core::storage::SnapshotSink;

pub mod light;

pub(crate) struct SnapshotSinkPtr {
  ptr: *mut (),
}

unsafe impl Send for SnapshotSinkPtr {}
unsafe impl Sync for SnapshotSinkPtr {}

impl SnapshotSinkPtr {
  pub(crate) fn new<T: SnapshotSink>(sink: T) -> Self {
    Self {
      ptr: Box::into_raw(Box::new(sink)) as *mut (),
    }
  }

  pub(crate) fn as_ref(&self) -> &impl SnapshotSink {
    unsafe { &*(self.ptr as *const dyn SnapshotSink) }
  }

  pub(crate) fn as_mut(&mut self) -> &mut impl SnapshotSink {
    unsafe { &mut *(self.ptr as *mut _) }
  }
}

impl AsyncWrite for SnapshotSinkPtr {
  fn poll_write(
    self: Pin<&mut Self>,
    cx: &mut std::task::Context<'_>,
    buf: &[u8],
  ) -> std::task::Poll<std::io::Result<usize>> {
    Pin::new(self.get_mut().as_mut()).poll_write(cx, buf)
  }

  fn poll_flush(
    self: Pin<&mut Self>,
    cx: &mut std::task::Context<'_>,
  ) -> std::task::Poll<std::io::Result<()>> {
    Pin::new(self.get_mut().as_mut()).poll_flush(cx)
  }

  fn poll_close(
    self: Pin<&mut Self>,
    cx: &mut std::task::Context<'_>,
  ) -> std::task::Poll<std::io::Result<()>> {
    Pin::new(self.get_mut().as_mut()).poll_close(cx)
  }
}

impl SnapshotSink for SnapshotSinkPtr {
  fn id(&self) -> &str {
    self.as_mut().id()
  }

  fn cancel(&mut self) -> impl futures::prelude::Future<Output = std::io::Result<()>> + Send {
    self.as_mut().cancel()
  }
}

impl Drop for SnapshotSinkPtr {
  fn drop(&mut self) {
    unsafe {
      Box::from_raw(self.ptr);
    }
  }
}
