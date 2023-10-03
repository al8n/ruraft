mod bytes;
mod string;
mod vec;

/// The type can transform its representation between structured and byte form.
#[async_trait::async_trait]
pub trait Transformable: Send + Sync + 'static {
  /// The error type returned when encoding or decoding fails.
  type Error: std::error::Error + Send + Sync + 'static;

  /// Encodes the value into the given buffer.
  fn encode(&self, dst: &mut [u8]) -> Result<(), Self::Error>;

  /// Encodes the value into the given writer.
  ///
  /// # Note
  /// For the implementation of some builtin structs
  /// (e.g. [`String`], [`SmolStr`](smol_str::SmolStr), [`Vec<u8>`], [`Bytes`](bytes::Bytes)).
  /// There is no optimization, so
  /// if your writer is expensive (e.g. [`TcpStream`](std::net::TcpStream)),
  /// it is better to use a [`BufWriter`](std::io::BufWriter)
  /// to wrap your orginal writer to cut down the number of I/O times.
  fn encode_to<W: std::io::Write>(&self, dst: &mut W) -> std::io::Result<()>;

  /// Encodes the value into the given async writer.
  async fn encode_to_async<W: futures::io::AsyncWrite + Send + Unpin>(
    &self,
    dst: &mut W,
  ) -> std::io::Result<()>;

  /// Returns the encoded length of the value.
  /// This is used to pre-allocate a buffer for encoding.
  fn encoded_len(&self) -> usize;

  /// Decodes the value from the given buffer.
  fn decode(src: &[u8]) -> Result<Self, Self::Error>
  where
    Self: Sized;

  /// Decodes the value from the given reader.
  fn decode_from<R: std::io::Read>(src: &mut R) -> std::io::Result<Self>
  where
    Self: Sized;

  /// Decodes the value from the given async reader.
  async fn decode_from_async<R: futures::io::AsyncRead + Send + Unpin>(
    src: &mut R,
  ) -> std::io::Result<Self>
  where
    Self: Sized;
}

#[async_trait::async_trait]
impl Transformable for () {
  type Error = std::convert::Infallible;

  fn encode(&self, _dst: &mut [u8]) -> Result<(), Self::Error> {
    Ok(())
  }

  fn encode_to<W: std::io::Write>(&self, _dst: &mut W) -> std::io::Result<()> {
    Ok(())
  }

  async fn encode_to_async<W: futures::io::AsyncWrite + Send + Unpin>(
    &self,
    _dst: &mut W,
  ) -> std::io::Result<()> {
    Ok(())
  }

  fn encoded_len(&self) -> usize {
    0
  }

  fn decode(_src: &[u8]) -> Result<Self, Self::Error>
  where
    Self: Sized,
  {
    Ok(())
  }

  fn decode_from<R: std::io::Read>(_src: &mut R) -> std::io::Result<Self>
  where
    Self: Sized,
  {
    Ok(())
  }

  async fn decode_from_async<R: futures::io::AsyncRead + Send + Unpin>(
    _src: &mut R,
  ) -> std::io::Result<Self>
  where
    Self: Sized,
  {
    Ok(())
  }
}

#[derive(Debug, thiserror::Error)]
pub enum BytesTransformableError {
  #[error(
    "buffer is too small, use `Transformable::encoded_len` to pre-allocate a buffer with enough space"
  )]
  EncodeBufferTooSmall,
  #[error("corrupted")]
  Corrupted,
  #[error("{0}")]
  Custom(Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl BytesTransformableError {
  /// Create a new `BytesTransformableError::Corrupted` error.
  #[inline]
  pub const fn corrupted() -> Self {
    Self::Corrupted
  }

  /// Create a new `BytesTransformableError::Custom` error.
  #[inline]
  pub fn custom<E>(err: E) -> Self
  where
    E: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
  {
    Self::Custom(err.into())
  }
}

#[derive(Debug, thiserror::Error)]
pub enum StringTransformableError {
  #[error(
    "buffer is too small, use `Transformable::encoded_len` to pre-allocate a buffer with enough space"
  )]
  EncodeBufferTooSmall,
  #[error("corrupted")]
  Corrupted,
  #[error("{0}")]
  Utf8Error(#[from] core::str::Utf8Error),
  #[error("{0}")]
  Custom(Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl StringTransformableError {
  /// Create a new `BytesTransformableError::Corrupted` error.
  #[inline]
  pub const fn corrupted() -> Self {
    Self::Corrupted
  }

  /// Create a new `BytesTransformableError::Custom` error.
  #[inline]
  pub fn custom<E>(err: E) -> Self
  where
    E: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
  {
    Self::Custom(err.into())
  }

  fn from_bytes_error(err: BytesTransformableError) -> Self {
    match err {
      BytesTransformableError::EncodeBufferTooSmall => Self::EncodeBufferTooSmall,
      BytesTransformableError::Corrupted => Self::Corrupted,
      BytesTransformableError::Custom(err) => Self::Custom(err),
    }
  }
}

const LEGNTH_SIZE: usize = core::mem::size_of::<u32>();

async fn decode_bytes_from_async<R: futures::io::AsyncRead + Unpin>(
  src: &mut R,
) -> std::io::Result<Vec<u8>> {
  use futures::AsyncReadExt;

  let mut len_buf = [0u8; LEGNTH_SIZE];
  src.read_exact(&mut len_buf).await?;
  let len = u32::from_be_bytes(len_buf) as usize;
  let mut buf = vec![0u8; len];
  src.read_exact(&mut buf).await.map(|_| buf)
}

fn decode_bytes_from<R: std::io::Read>(src: &mut R) -> std::io::Result<Vec<u8>> {
  let mut len_buf = [0u8; LEGNTH_SIZE];
  src.read_exact(&mut len_buf)?;
  let len = u32::from_be_bytes(len_buf) as usize;
  let mut buf = vec![0u8; len];
  src.read_exact(&mut buf).map(|_| buf)
}

fn decode_bytes(src: &[u8]) -> Result<Vec<u8>, BytesTransformableError> {
  let len = src.len();
  if len < core::mem::size_of::<u32>() {
    return Err(BytesTransformableError::Corrupted);
  }

  let len = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
  if len > len - core::mem::size_of::<u32>() {
    return Err(BytesTransformableError::Corrupted);
  }

  Ok(src[LEGNTH_SIZE..LEGNTH_SIZE + len].to_vec())
}

fn encode_bytes(src: &[u8], dst: &mut [u8]) -> Result<(), BytesTransformableError> {
  let encoded_len = src.len() + LEGNTH_SIZE;
  if dst.len() < encoded_len {
    return Err(BytesTransformableError::EncodeBufferTooSmall);
  }
  Ok(())
}

fn encode_bytes_to<W: std::io::Write>(src: &[u8], dst: &mut W) -> std::io::Result<()> {
  let len = src.len() as u32;
  dst
    .write_all(&len.to_be_bytes())
    .and_then(|_| dst.write_all(src))
}

async fn encode_bytes_to_async<W: futures::io::AsyncWrite + Unpin>(
  src: &[u8],
  dst: &mut W,
) -> std::io::Result<()> {
  use futures::io::AsyncWriteExt;

  let len = src.len() as u32;
  dst.write_all(&len.to_be_bytes()).await?;
  dst.write_all(src).await
}

fn encoded_bytes_len(src: &[u8]) -> usize {
  core::mem::size_of::<u32>() + src.len()
}
