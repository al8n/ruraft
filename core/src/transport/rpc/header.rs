use std::io;

use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::utils::invalid_data;

use super::*;

/// A common sub-structure used to pass along protocol version and
/// other information about the cluster.
#[viewit::viewit(
  vis_all = "pub(crate)",
  getters(vis_all = "pub"),
  setters(vis_all = "pub")
)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Header<I, A> {
  /// The protocol version of the request or response
  #[viewit(
    getter(
      const,
      attrs(doc = "Get the protocol version of the request or response"),
    ),
    setter(attrs(doc = "Set the protocol version of the request or response"),)
  )]
  protocol_version: ProtocolVersion,

  /// The id of the node sending the RPC Request or Response
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Get the node of the request or response"),
    ),
    setter(attrs(doc = "Set the node of the request or response"),)
  )]
  from: Node<I, A>,
}

impl<I: CheapClone, A: CheapClone> CheapClone for Header<I, A> {
  fn cheap_clone(&self) -> Self {
    Self {
      protocol_version: self.protocol_version,
      from: self.from.cheap_clone(),
    }
  }
}

impl<I, A> Header<I, A> {
  /// Create a new [`Header`] with the given `id` and `addr`.
  #[inline]
  pub const fn new(version: ProtocolVersion, id: I, addr: A) -> Self {
    Self {
      protocol_version: version,
      from: Node::new(id, addr),
    }
  }

  /// Create a new [`Header`] with the given [`ProtocolVersion`] and [`Node`].
  #[inline]
  pub const fn from_node(version: ProtocolVersion, node: Node<I, A>) -> Self {
    Self {
      protocol_version: version,
      from: node,
    }
  }

  /// Returns the address of the header.
  #[inline]
  pub const fn addr(&self) -> &A {
    self.from.addr()
  }

  /// Returns the id of the header.
  #[inline]
  pub const fn id(&self) -> &I {
    self.from.id()
  }
}

impl<I, A> From<(ProtocolVersion, Node<I, A>)> for Header<I, A> {
  #[inline]
  fn from((version, from): (ProtocolVersion, Node<I, A>)) -> Self {
    Self {
      protocol_version: version,
      from,
    }
  }
}

// Encode
//
// -----------------------------------------------------------------------
// | len (4 bytes) | version (1 bytes) | id (variable) | addr (variable) |
// -----------------------------------------------------------------------
impl<I, A> Transformable for Header<I, A>
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

    dst[..MESSAGE_SIZE_LEN].copy_from_slice(&(encoded_len as u32).to_be_bytes());
    let mut offset = MESSAGE_SIZE_LEN;
    dst[offset] = self.protocol_version as u8;
    offset += 1;
    let id_encoded_len = self.id().encoded_len();
    self
      .id()
      .encode(&mut dst[offset..offset + id_encoded_len])
      .map_err(TransformError::encode)?;
    offset += id_encoded_len;
    let addr_encoded_len = self.addr().encoded_len();
    self
      .addr()
      .encode(&mut dst[offset..offset + addr_encoded_len])
      .map_err(TransformError::encode)
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
    core::mem::size_of::<u32>() + 1 + self.id().encoded_len() + self.addr().encoded_len()
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    let src_len = src.len();
    if src_len < MESSAGE_SIZE_LEN {
      return Err(TransformError::DecodeBufferTooSmall);
    }

    let msg_len = u32::from_be_bytes(src[..MESSAGE_SIZE_LEN].try_into().unwrap()) as usize;
    if msg_len > src_len {
      return Err(TransformError::DecodeBufferTooSmall);
    }

    let mut offset = MESSAGE_SIZE_LEN;
    let version = ProtocolVersion::try_from(src[offset]).map_err(Self::Error::decode)?;
    offset += 1;
    let (id_len, id) = I::decode(&src[offset..]).map_err(Self::Error::decode)?;
    offset += id_len;
    let (addr_len, addr) = A::decode(&src[offset..]).map_err(Self::Error::decode)?;
    offset += addr_len;
    Ok((
      offset,
      Self {
        protocol_version: version,
        from: Node::new(id, addr),
      },
    ))
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

#[cfg(any(feature = "test", test))]
impl Header<smol_str::SmolStr, std::net::SocketAddr> {
  #[doc(hidden)]
  pub fn __large() -> Self {
    Self {
      protocol_version: ProtocolVersion::V1,
      from: Node::new("1".repeat(500).into(), "127.0.0.1:8080".parse().unwrap()),
    }
  }

  #[doc(hidden)]
  pub fn __small() -> Self {
    Self {
      protocol_version: ProtocolVersion::V1,
      from: Node::new("test".into(), "127.0.0.1:8080".parse().unwrap()),
    }
  }
}

#[cfg(test)]
unit_test_transformable_roundtrip!(Header <smol_str::SmolStr, std::net::SocketAddr> => header);
