use std::{
  io,
  pin::Pin,
  task::{Context, Poll},
};

use futures_io::{AsyncRead, AsyncWrite};

/// Discard is an `AsyncWrite` implementor on which all `write*` calls succeed
/// without doing anything.
#[derive(Debug, Default, Clone, Copy)]
pub struct Discard;

impl AsyncWrite for Discard {
  fn poll_write(
    self: Pin<&mut Self>,
    _cx: &mut Context<'_>,
    buf: &[u8],
  ) -> Poll<io::Result<usize>> {
    Poll::Ready(Ok(buf.len()))
  }

  fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    Poll::Ready(Ok(()))
  }

  fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    Poll::Ready(Ok(()))
  }
}

pin_project_lite::pin_project! {
  /// A LimitedReader reads from `R` but limits the amount of
/// data returned to just `N` bytes. Each call to `read*`
/// updates `N`` to reflect the new amount remaining.
/// `read*` returns EOF when `N == 0` or when the underlying R returns EOF.
pub struct LimitedReader<R> {
  limit: u64,
  #[pin]
  r: R,
}

}

impl<R> LimitedReader<R> {
  /// Creates a new [`LimitedReader`] with given limit and reader.
  pub fn new(limit: u64, reader: R) -> Self {
    Self { limit, r: reader }
  }

  /// Returns limit of the reader.
  pub const fn limit(&self) -> u64 {
    self.limit
  }

  /// Consumes the [`LimitedReader`] and returns the inner reader.
  pub fn into_inner(self) -> R {
    self.r
  }
}

impl<R: AsyncRead> AsyncRead for LimitedReader<R> {
  fn poll_read(
    self: std::pin::Pin<&mut Self>,
    cx: &mut Context<'_>,
    mut buf: &mut [u8],
  ) -> Poll<io::Result<usize>> {
    if self.limit == 0 {
      return Poll::Ready(Err(std::io::Error::new(
        std::io::ErrorKind::UnexpectedEof,
        "eof",
      )));
    }

    if buf.len() as u64 > self.limit {
      buf = &mut buf[..self.limit as usize];
    }

    self.project().r.poll_read(cx, buf)
  }
}
