use std::io;

use futures::{AsyncWrite, AsyncRead, AsyncWriteExt, AsyncReadExt};
use nodecraft::Transformable;

use crate::utils::invalid_data;

use super::*;

/// The heartbeat command.
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct HeartbeatRequest<I, A> {
  /// The header of the request
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the header of the request"),),
    setter(attrs(doc = "Set the header of the request"),)
  )]
  header: Header<I, A>,

  /// Provide the current term and leader
  #[viewit(
    getter(const, attrs(doc = "Get the term of the heartbeat request"),),
    setter(attrs(doc = "Set the term of the heartbeat request"),)
  )]
  term: u64,
}

impl<I, A> HeartbeatRequest<I, A> {
  /// Create a new [`HeartbeatRequest`] with the given protocol version, id, address and default term.
  #[inline]
  pub const fn new(version: ProtocolVersion, id: I, addr: A) -> Self {
    Self {
      header: Header {
        protocol_version: version,
        from: Node::new(id, addr),
      },
      term: 0,
    }
  }

  /// Create a new [`HeartbeatRequest`] with the given protocol version, node and default term.
  #[inline]
  pub const fn from_node(version: ProtocolVersion, node: Node<I, A>) -> Self {
    Self {
      header: Header {
        protocol_version: version,
        from: node,
      },
      term: 0,
    }
  }

  /// Create a new [`HeartbeatRequest`] with the given header and default term.
  #[inline]
  pub const fn from_header(header: Header<I, A>) -> Self {
    Self { header, term: 0 }
  }
}

impl<I: CheapClone, A: CheapClone> CheapClone for HeartbeatRequest<I, A> {
  fn cheap_clone(&self) -> Self {
    Self {
      header: self.header.cheap_clone(),
      term: self.term,
    }
  }
}

// Encode
//
// ---------------------------------------------------------------------
// | tag (1 byte) | len (4 bytes) | term (8 bytes) | header (variable) |
// ---------------------------------------------------------------------
impl<I, A> Transformable for HeartbeatRequest<I, A>
where
  I: Transformable + Send + Sync + 'static,
  I::Error: Send + Sync + 'static,
  A: Transformable + Send + Sync + 'static,
  A::Error: Send + Sync + 'static,
{
  type Error = TransformError<I, A>;

  fn encode(&self, dst: &mut [u8]) -> Result<(), Self::Error> {
    let encoded_len = self.encoded_len();

    if dst.len() < encoded_len {
      return Err(TransformError::EncodeBufferTooSmall);
    }
    let mut offset = 0;
    dst[offset] = Self::TAG;
    offset += 1;
    dst[offset..offset + MESSAGE_SIZE_LEN].copy_from_slice(&(encoded_len as u32).to_be_bytes());
    offset += MESSAGE_SIZE_LEN;
    dst[offset..offset + core::mem::size_of::<u64>()].copy_from_slice(&self.term.to_be_bytes());
    offset += core::mem::size_of::<u64>();
    self.header.encode(&mut dst[offset..])
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
    1 + MESSAGE_SIZE_LEN + core::mem::size_of::<u64>() + self.header.encoded_len()
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
    let tag = src[offset];
    offset += 1;

    if tag != Self::TAG {
      return Err(TransformError::UnknownProtocolVersion(tag));
    }

    let encoded_len = u32::from_be_bytes(src[offset..offset + MESSAGE_SIZE_LEN].try_into().unwrap()) as usize;
    if encoded_len > src_len - MESSAGE_SIZE_LEN - 1 {
      return Err(TransformError::DecodeBufferTooSmall);
    }

    offset += MESSAGE_SIZE_LEN;
    let term = u64::from_be_bytes(src[offset..offset + core::mem::size_of::<u64>()].try_into().unwrap());
    offset += core::mem::size_of::<u64>();
    let (header_len, header) = Header::<I, A>::decode(&src[offset..])?;
    offset += header_len;
    Ok((
      offset,
      Self {
        header,
        term,
      },
    ))
  }

  fn decode_from_reader<R: std::io::Read>(reader: &mut R) -> std::io::Result<(usize, Self)>
  where
    Self: Sized,
  {
    let mut buf = [0u8; MESSAGE_SIZE_LEN + 1];
    reader.read_exact(&mut buf)?;
    let msg_len = u32::from_be_bytes(buf[1..].try_into().unwrap()) as usize;

    let remaining = msg_len - MESSAGE_SIZE_LEN - 1;
    if remaining <= MAX_INLINED_BYTES {
      let mut buf = [0u8; MAX_INLINED_BYTES];
      reader.read_exact(&mut buf[..remaining])?;
      Self::decode(&buf).map_err(invalid_data)
    } else {
      let mut buf = vec![0u8; remaining];
      reader.read_exact(&mut buf)?;
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
    let mut buf = [0u8; MESSAGE_SIZE_LEN + 1];
    reader.read_exact(&mut buf).await?;
    let msg_len = u32::from_be_bytes(buf[1..].try_into().unwrap()) as usize;

    let remaining = msg_len - MESSAGE_SIZE_LEN - 1;
    if remaining <= MAX_INLINED_BYTES {
      let mut buf = [0u8; MAX_INLINED_BYTES];
      reader.read_exact(&mut buf[..remaining]).await?;
      Self::decode(&buf).map_err(invalid_data)
    } else {
      let mut buf = vec![0u8; remaining];
      reader.read_exact(&mut buf).await?;
      Self::decode(&buf).map_err(invalid_data)
    }
  }
}
