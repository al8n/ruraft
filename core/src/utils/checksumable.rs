use futures::io::{AsyncRead, AsyncWrite};
use std::{
  hash::Hasher,
  io::{self, Read, Write},
  pin::Pin,
  task::{Context, Poll},
};

pub struct ChecksumableReader<R: Read, H: Hasher> {
  reader: R,
  hasher: H,
}

impl<R: Read, H: Hasher> ChecksumableReader<R, H> {
  pub fn new(reader: R, hasher: H) -> Self {
    Self { reader, hasher }
  }

  pub fn into_inner(self) -> R {
    self.reader
  }

  pub fn checksum(&self) -> u64 {
    self.hasher.finish()
  }
}

impl<R: Read, H: Hasher> Read for ChecksumableReader<R, H> {
  fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
    let n = self.reader.read(buf)?;
    self.hasher.write(&buf[..n]);
    Ok(n)
  }
}

pub struct ChecksumableWriter<W: Write, H: Hasher> {
  writer: W,
  hasher: H,
}

impl<W: Write, H: Hasher> ChecksumableWriter<W, H> {
  pub fn new(writer: W, hasher: H) -> Self {
    Self { writer, hasher }
  }

  pub fn into_inner(self) -> W {
    self.writer
  }

  pub fn inner(&self) -> &W {
    &self.writer
  }

  pub fn inner_mut(&mut self) -> &mut W {
    &mut self.writer
  }

  pub fn checksum(&self) -> u64 {
    self.hasher.finish()
  }
}

impl<W: Write, H: Hasher> Write for ChecksumableWriter<W, H> {
  fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
    self.hasher.write(buf);
    self.writer.write(buf)
  }

  fn flush(&mut self) -> io::Result<()> {
    self.writer.flush()
  }
}

#[pin_project::pin_project]
pub struct AsyncChecksumableReader<R: AsyncRead, H: Hasher> {
  #[pin]
  reader: R,
  hasher: H,
}

impl<R: AsyncRead, H: Hasher> AsyncChecksumableReader<R, H> {
  pub fn new(reader: R, hasher: H) -> Self {
    Self { reader, hasher }
  }

  pub fn into_inner(self) -> R {
    self.reader
  }

  pub fn checksum(&self) -> u64 {
    self.hasher.finish()
  }
}

impl<R: AsyncRead, H: Hasher> AsyncRead for AsyncChecksumableReader<R, H> {
  fn poll_read(
    self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut [u8],
  ) -> Poll<io::Result<usize>> {
    let this = self.project();
    match this.reader.poll_read(cx, buf) {
      Poll::Ready(Ok(n)) => {
        this.hasher.write(&buf[..n]);
        Poll::Ready(Ok(n))
      }
      Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
      Poll::Pending => Poll::Pending,
    }
  }
}

#[pin_project::pin_project]
pub struct AsyncChecksumableWriter<W: AsyncWrite, H: Hasher> {
  #[pin]
  writer: W,
  hasher: H,
}

impl<W: AsyncWrite, H: Hasher> AsyncChecksumableWriter<W, H> {
  pub fn new(writer: W, hasher: H) -> Self {
    Self { writer, hasher }
  }

  pub fn into_inner(self) -> W {
    self.writer
  }

  pub fn checksum(&self) -> u64 {
    self.hasher.finish()
  }
}

impl<W: AsyncWrite, H: Hasher> AsyncWrite for AsyncChecksumableWriter<W, H> {
  fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
    let this = self.project();
    this.hasher.write(buf);
    this.writer.poll_write(cx, buf)
  }

  fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    self.project().writer.poll_flush(cx)
  }

  fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    self.project().writer.poll_close(cx)
  }
}
