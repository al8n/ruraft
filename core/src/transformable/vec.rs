use super::*;

#[async_trait::async_trait]
impl Transformable for Vec<u8> {
  type Error = BytesTransformableError;

  fn encode(&self, dst: &mut [u8]) -> Result<(), Self::Error> {
    encode_bytes(self.as_ref(), dst)
  }

  /// Encodes the value into the given writer.
  ///
  /// # Note
  /// The implementation of this method is not optimized, which means
  /// if your writer is expensive (e.g. [`TcpStream`](std::net::TcpStream), [`File`](std::fs::File)),
  /// it is better to use a [`BufWriter`](std::io::BufWriter)
  /// to wrap your orginal writer to cut down the number of I/O times.
  fn encode_to<W: std::io::Write>(&self, dst: &mut W) -> std::io::Result<()> {
    encode_bytes_to(self.as_ref(), dst)
  }

  /// Encodes the value into the given async writer.
  ///
  /// # Note
  /// The implementation of this method is not optimized, which means
  /// if your writer is expensive (e.g. `TcpStream`, `File`),
  /// it is better to use a [`BufWriter`](futures::io::BufWriter)
  /// to wrap your orginal writer to cut down the number of I/O times.
  async fn encode_to_async<W: futures::io::AsyncWrite + Send + Unpin>(
    &self,
    dst: &mut W,
  ) -> std::io::Result<()> {
    encode_bytes_to_async(self.as_ref(), dst).await
  }

  fn encoded_len(&self) -> usize {
    encoded_bytes_len(self.as_ref())
  }

  fn decode(src: &[u8]) -> Result<Self, Self::Error>
  where
    Self: Sized,
  {
    decode_bytes(src)
  }

  /// Decodes the value from the given reader.
  ///
  /// # Note
  /// The implementation of this method is not optimized, which means
  /// if your reader is expensive (e.g. [`TcpStream`](std::net::TcpStream), [`File`](std::fs::File)),
  /// it is better to use a [`BufReader`](std::io::BufReader)
  /// to wrap your orginal reader to cut down the number of I/O times.
  fn decode_from<R: std::io::Read>(src: &mut R) -> std::io::Result<Self>
  where
    Self: Sized,
  {
    decode_bytes_from(src)
  }

  /// Decodes the value from the given async reader.
  ///
  /// # Note
  /// The implementation of this method is not optimized, which means
  /// if your reader is expensive (e.g. `TcpStream`, `File`),
  /// it is better to use a [`BufReader`](futures::io::BufReader)
  /// to wrap your orginal reader to cut down the number of I/O times.
  async fn decode_from_async<R: futures::io::AsyncRead + Send + Unpin>(
    src: &mut R,
  ) -> std::io::Result<Self>
  where
    Self: Sized,
  {
    decode_bytes_from_async(src).await
  }
}
