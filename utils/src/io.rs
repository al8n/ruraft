use std::{
  hash::Hasher,
  io::{self, Read, Write},
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

/// A LimitedReader reads from `R` but limits the amount of
/// data returned to just `N` bytes. Each call to `read*`
/// updates `N`` to reflect the new amount remaining.
/// `read*` returns EOF when `N == 0` or when the underlying R returns EOF.
#[pin_project::pin_project]
pub struct LimitedReader<R> {
  limit: u64,
  #[pin]
  r: R,
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

/// A reader that calculates a checksum while reading data.
///
/// This struct wraps another reader and a hasher, and it updates the hasher
/// with the data read from the inner reader. It allows for calculating a checksum
/// while reading data without needing a separate pass over the data.
///
/// # Type Parameters
///
/// * `R`: The type of the inner reader.
/// * `H`: The type of the hasher used for calculating the checksum.
pub struct ChecksumableReader<R: Read, H: Hasher> {
  reader: R,
  hasher: H,
}

impl<R: Read, H: Hasher> ChecksumableReader<R, H> {
  /// Creates a new `ChecksumableReader` with the given reader and hasher.
  ///
  /// # Arguments
  ///
  /// * `reader`: The reader from which to read data.
  /// * `hasher`: The hasher used to compute the checksum of the read data.
  ///
  /// # Returns
  ///
  /// A new `ChecksumableReader`.
  pub fn new(reader: R, hasher: H) -> Self {
    Self { reader, hasher }
  }

  /// Consumes the `ChecksumableReader` and returns the inner reader.
  ///
  /// This method can be used when you're done with the checksum calculation
  /// and want to access the underlying reader directly.
  ///
  /// # Returns
  ///
  /// The inner reader.
  pub fn into_inner(self) -> R {
    self.reader
  }

  /// Returns a reference to the inner reader.
  ///
  /// This method provides read-only access to the inner reader.
  ///
  /// # Returns
  ///
  /// A reference to the inner reader.
  pub fn inner(&self) -> &R {
    &self.reader
  }

  /// Returns a mutable reference to the inner reader.
  ///
  /// This method provides mutable access to the inner reader, allowing
  /// for operations that may change its state.
  ///
  /// > **However, it's important to note
  /// that if you read data directly to the inner reader using this mutable
  /// reference, those reads will not be accounted for in the checksum
  /// calculation. To ensure the checksum remains accurate, all reads
  /// should be performed through the `ChecksumableReader`.**
  ///
  /// # Returns
  ///
  /// A mutable reference to the inner reader.
  pub fn inner_mut(&mut self) -> &mut R {
    &mut self.reader
  }

  /// Returns the current checksum.
  ///
  /// This method returns the checksum calculated from the data that has been
  /// read so far. It does not consume the `ChecksumableReader`.
  ///
  /// # Returns
  ///
  /// The current checksum as a `u64`.
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

/// A writer that calculates a checksum while writing data.
///
/// This struct wraps another writer and a hasher, and it updates the hasher
/// with the data written to the inner writer. It allows for calculating a checksum
/// while writing data without needing a separate pass over the data.
///
/// # Type Parameters
///
/// * `W`: The type of the inner writer.
/// * `H`: The type of the hasher used for calculating the checksum.
pub struct ChecksumableWriter<W: Write, H: Hasher> {
  writer: W,
  hasher: H,
}

impl<W: Write, H: Hasher> ChecksumableWriter<W, H> {
  /// Creates a new `ChecksumableWriter` with the given writer and hasher.
  ///
  /// # Arguments
  ///
  /// * `writer`: The writer to which data will be written.
  /// * `hasher`: The hasher used to compute the checksum of the written data.
  ///
  /// # Returns
  ///
  /// A new `ChecksumableWriter`.
  pub fn new(writer: W, hasher: H) -> Self {
    Self { writer, hasher }
  }

  /// Consumes the `ChecksumableWriter` and returns the inner writer.
  ///
  /// This method can be used when you're done with the checksum calculation
  /// and want to access the underlying writer directly.
  ///
  /// # Returns
  ///
  /// The inner writer.
  pub fn into_inner(self) -> W {
    self.writer
  }

  /// Returns a reference to the inner writer.
  ///
  /// This method provides read-only access to the inner writer.
  ///
  /// # Returns
  ///
  /// A reference to the inner writer.
  pub fn inner(&self) -> &W {
    &self.writer
  }

  /// Returns a mutable reference to the inner writer.
  ///
  /// This method provides mutable access to the inner writer, allowing
  /// for operations that may change its state.
  ///
  /// > **However, it's important to note
  /// that if you write data directly to the inner writer using this mutable
  /// reference, those writes will not be accounted for in the checksum
  /// calculation. To ensure the checksum remains accurate, all writes
  /// should be performed through the `ChecksumableWriter`.**
  ///
  /// # Returns
  ///
  /// A mutable reference to the inner writer.
  pub fn inner_mut(&mut self) -> &mut W {
    &mut self.writer
  }

  /// Returns the current checksum.
  ///
  /// This method returns the checksum calculated from the data that has been
  /// read so far. It does not consume the `ChecksumableWriter`.
  ///
  /// # Returns
  ///
  /// The current checksum as a `u64`.
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

/// An async reader that calculates a checksum while reading data.
///
/// This struct wraps another async reader and a hasher, and it updates the hasher
/// with the data read from the inner reader. It allows for calculating a checksum
/// asynchronously while reading data without needing a separate pass over the data.
///
/// # Type Parameters
///
/// * `R`: The type of the inner async reader.
/// * `H`: The type of the hasher used for calculating the checksum.
#[pin_project::pin_project]
pub struct AsyncChecksumableReader<R: AsyncRead, H: Hasher> {
  #[pin]
  reader: R,
  hasher: H,
}

impl<R: AsyncRead, H: Hasher> AsyncChecksumableReader<R, H> {
  /// Creates a new `AsyncChecksumableReader` with the given async reader and hasher.
  ///
  /// # Arguments
  ///
  /// * `reader`: The async reader from which to read data.
  /// * `hasher`: The hasher used to compute the checksum of the read data.
  ///
  /// # Returns
  ///
  /// A new `AsyncChecksumableReader`.
  pub fn new(reader: R, hasher: H) -> Self {
    Self { reader, hasher }
  }

  /// Consumes the `AsyncChecksumableReader` and returns the inner async reader.
  ///
  /// This method can be used when you're done with the checksum calculation
  /// and want to access the underlying async reader directly.
  ///
  /// # Returns
  ///
  /// The inner async reader.
  pub fn into_inner(self) -> R {
    self.reader
  }

  /// Returns a reference to the inner reader.
  ///
  /// This method provides read-only access to the inner reader.
  ///
  /// # Returns
  ///
  /// A reference to the inner reader.
  pub fn inner(&self) -> &R {
    &self.reader
  }

  /// Returns a mutable reference to the inner reader.
  ///
  /// This method provides mutable access to the inner reader, allowing
  /// for operations that may change its state.
  ///
  /// > **However, it's important to note
  /// that if you read data directly to the inner reader using this mutable
  /// reference, those reads will not be accounted for in the checksum
  /// calculation. To ensure the checksum remains accurate, all reads
  /// should be performed through the `AsyncChecksumableReader`.**
  ///
  /// # Returns
  ///
  /// A mutable reference to the inner reader.
  pub fn inner_mut(&mut self) -> &mut R {
    &mut self.reader
  }

  /// Returns the current checksum.
  ///
  /// This method returns the checksum calculated from the data that has been
  /// read so far. It does not consume the `AsyncChecksumableReader`.
  ///
  /// # Returns
  ///
  /// The current checksum as a `u64`.
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

/// An async writer that calculates a checksum while writing data.
///
/// This struct wraps another async writer and a hasher, and it updates the hasher
/// with the data written to the inner writer. It allows for calculating a checksum
/// asynchronously while writing data without needing a separate pass over the data.
///
/// # Type Parameters
///
/// * `W`: The type of the inner async writer.
/// * `H`: The type of the hasher used for calculating the checksum.
#[pin_project::pin_project]
pub struct AsyncChecksumableWriter<W: AsyncWrite, H: Hasher> {
  #[pin]
  writer: W,
  hasher: H,
}

impl<W: AsyncWrite, H: Hasher> AsyncChecksumableWriter<W, H> {
  /// Creates a new `AsyncChecksumableWriter` with the given async writer and hasher.
  ///
  /// # Arguments
  ///
  /// * `writer`: The async writer to which data will be written.
  /// * `hasher`: The hasher used to compute the checksum of the written data.
  ///
  /// # Returns
  ///
  /// A new `AsyncChecksumableWriter`.
  pub fn new(writer: W, hasher: H) -> Self {
    Self { writer, hasher }
  }

  /// Consumes the `AsyncChecksumableWriter` and returns the inner async writer.
  ///
  /// This method can be used when you're done with the checksum calculation
  /// and want to access the underlying async writer directly.
  ///
  /// # Returns
  ///
  /// The inner async writer.
  pub fn into_inner(self) -> W {
    self.writer
  }

  /// Returns a reference to the inner writer.
  ///
  /// This method provides read-only access to the inner writer.
  ///
  /// # Returns
  ///
  /// A reference to the inner writer.
  pub fn inner(&self) -> &W {
    &self.writer
  }

  /// Returns a mutable reference to the inner writer.
  ///
  /// This method provides mutable access to the inner writer, allowing
  /// for operations that may change its state.
  ///
  /// > **However, it's important to note
  /// that if you write data directly to the inner writer using this mutable
  /// reference, those writes will not be accounted for in the checksum
  /// calculation. To ensure the checksum remains accurate, all writes
  /// should be performed through the `AsyncChecksumableWriter`.**
  ///
  /// # Returns
  ///
  /// A mutable reference to the inner writer.
  pub fn inner_mut(&mut self) -> &mut W {
    &mut self.writer
  }

  /// Returns the current checksum.
  ///
  /// This method returns the checksum calculated from the data that has been
  /// read so far. It does not consume the `AsyncChecksumableWriter`.
  ///
  /// # Returns
  ///
  /// The current checksum as a `u64`.
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
