use std::io;

use crate::utils::invalid_data;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::*;

/// The response returned from remote when an error occurs.
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ErrorResponse<I, A> {
  /// The header of the response
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the header of the response"),),
    setter(attrs(doc = "Set the header of the response"),)
  )]
  header: Header<I, A>,
  /// The error message
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the error message"),),
    setter(attrs(doc = "Set the error message"),)
  )]
  error: String,
}

impl<I, A> From<ErrorResponse<I, A>> for String {
  fn from(value: ErrorResponse<I, A>) -> Self {
    value.error
  }
}

impl<I, A> ErrorResponse<I, A> {
  /// Create a new [`ErrorResponse`] with the given header and error message.
  pub const fn new(header: Header<I, A>, error: String) -> Self {
    Self { header, error }
  }
}

// Encode
//
// --------------------------------------------------------
// | len (4 bytes) | header (variable) | error (variable) |
// --------------------------------------------------------
impl<I, A> Transformable for ErrorResponse<I, A>
where
  I: Transformable + Send + Sync + 'static,
  I::Error: Send + Sync + 'static,
  A: Transformable + Send + Sync + 'static,
  A::Error: Send + Sync + 'static,
{
  type Error = TransformError;

  fn encode(&self, dst: &mut [u8]) -> Result<(), Self::Error> {
    let encoded_len = self.encoded_len();

    if dst.len() < encoded_len {
      return Err(TransformError::EncodeBufferTooSmall);
    }
    let mut offset = 0;
    dst[offset..offset + MESSAGE_SIZE_LEN].copy_from_slice(&(encoded_len as u32).to_be_bytes());
    offset += MESSAGE_SIZE_LEN;

    let header_encoded_len = self.header.encoded_len();
    self
      .header
      .encode(&mut dst[offset..offset + header_encoded_len])?;
    offset += header_encoded_len;

    let error_encoded_len = self.error.len();
    dst[offset..offset + MESSAGE_SIZE_LEN]
      .copy_from_slice(&(error_encoded_len as u32).to_be_bytes());
    offset += MESSAGE_SIZE_LEN;
    dst[offset..offset + error_encoded_len].copy_from_slice(self.error.as_bytes());
    Ok(())
  }

  fn encode_to_writer<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
    let encoded_len = self.encoded_len();
    if encoded_len <= MAX_INLINED_BYTES {
      let mut buf = [0u8; MAX_INLINED_BYTES];
      self.encode(&mut buf).map_err(invalid_data)?;
      writer.write_all(&buf[..encoded_len])
    } else {
      let mut buf = vec![0u8; encoded_len];
      self.encode(&mut buf).map_err(invalid_data)?;
      writer.write_all(&buf)
    }
  }

  async fn encode_to_async_writer<W: AsyncWrite + Send + Unpin>(
    &self,
    writer: &mut W,
  ) -> io::Result<()>
  where
    Self::Error: Send + Sync + 'static,
  {
    let encoded_len = self.encoded_len();
    if encoded_len <= MAX_INLINED_BYTES {
      let mut buf = [0u8; MAX_INLINED_BYTES];
      self.encode(&mut buf).map_err(invalid_data)?;
      writer.write_all(&buf[..encoded_len]).await
    } else {
      let mut buf = vec![0u8; encoded_len];
      self.encode(&mut buf).map_err(invalid_data)?;
      writer.write_all(&buf).await
    }
  }

  fn encoded_len(&self) -> usize {
    MESSAGE_SIZE_LEN + self.header.encoded_len() + MESSAGE_SIZE_LEN + self.error.len()
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    let src_len = src.len();
    if src_len < MESSAGE_SIZE_LEN + 1 {
      return Err(TransformError::DecodeBufferTooSmall);
    }

    let mut offset = 0;
    let encoded_len =
      u32::from_be_bytes(src[offset..offset + MESSAGE_SIZE_LEN].try_into().unwrap()) as usize;
    if encoded_len > src_len - MESSAGE_SIZE_LEN {
      return Err(TransformError::DecodeBufferTooSmall);
    }

    offset += MESSAGE_SIZE_LEN;

    let (header_len, header) = Header::<I, A>::decode(&src[offset..])?;
    offset += header_len;

    let error_encoded_len =
      u32::from_be_bytes(src[offset..offset + MESSAGE_SIZE_LEN].try_into().unwrap()) as usize;
    offset += MESSAGE_SIZE_LEN;
    let error = core::str::from_utf8(&src[offset..offset + error_encoded_len])
      .map(|s| s.to_string())
      .map_err(Self::Error::decode)?;

    offset += error_encoded_len;
    Ok((offset, Self { header, error }))
  }

  fn decode_from_reader<R: std::io::Read>(reader: &mut R) -> std::io::Result<(usize, Self)>
  where
    Self: Sized,
  {
    let mut len = [0u8; MESSAGE_SIZE_LEN];
    reader.read_exact(&mut len)?;
    let msg_len = u32::from_be_bytes(len) as usize;

    if msg_len <= MAX_INLINED_BYTES {
      let mut buf = [0u8; MAX_INLINED_BYTES];
      buf[..MESSAGE_SIZE_LEN].copy_from_slice(&len);
      reader.read_exact(&mut buf[MESSAGE_SIZE_LEN..msg_len])?;
      Self::decode(&buf).map_err(invalid_data)
    } else {
      let mut buf = vec![0u8; msg_len];
      buf[..MESSAGE_SIZE_LEN].copy_from_slice(&len);
      reader.read_exact(&mut buf[MESSAGE_SIZE_LEN..])?;
      Self::decode(&buf).map_err(invalid_data)
    }
  }

  async fn decode_from_async_reader<R: AsyncRead + Send + Unpin>(
    reader: &mut R,
  ) -> io::Result<(usize, Self)>
  where
    Self: Sized,
    Self::Error: Send + Sync + 'static,
  {
    let mut len = [0u8; MESSAGE_SIZE_LEN];
    reader.read_exact(&mut len).await?;
    let msg_len = u32::from_be_bytes(len) as usize;

    if msg_len <= MAX_INLINED_BYTES {
      let mut buf = [0u8; MAX_INLINED_BYTES];
      buf[..MESSAGE_SIZE_LEN].copy_from_slice(&len);
      reader
        .read_exact(&mut buf[MESSAGE_SIZE_LEN..msg_len])
        .await?;
      Self::decode(&buf).map_err(invalid_data)
    } else {
      let mut buf = vec![0u8; msg_len];
      buf[..MESSAGE_SIZE_LEN].copy_from_slice(&len);
      reader.read_exact(&mut buf[MESSAGE_SIZE_LEN..]).await?;
      Self::decode(&buf).map_err(invalid_data)
    }
  }
}
