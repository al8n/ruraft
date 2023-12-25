use ruraft_utils::{decode_varint, encode_varint, encoded_len_varint};

use crate::MESSAGE_SIZE_LEN;

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
// ------------------------------------------------------
// | len (4 bytes) | header (variable) | term (uvarint) |
// ------------------------------------------------------
impl<I, A> Transformable for HeartbeatRequest<I, A>
where
  I: Transformable,
  A: Transformable,
{
  type Error = TransformError;

  fn encode(&self, dst: &mut [u8]) -> Result<usize, Self::Error> {
    let encoded_len = self.encoded_len();

    if dst.len() < encoded_len {
      return Err(TransformError::EncodeBufferTooSmall);
    }
    let mut offset = 0;
    NetworkEndian::write_u32(&mut dst[..MESSAGE_SIZE_LEN], encoded_len as u32);
    offset += MESSAGE_SIZE_LEN;

    offset += self.header.encode(&mut dst[offset..])?;
    encode_varint(self.term, &mut dst[offset..])
      .map(|size| {
        offset += size;
        debug_assert_eq!(
          offset, encoded_len,
          "expected bytes wrote ({}) not match actual bytes wrote ({})",
          encoded_len, offset
        );
        offset
      })
      .map_err(Into::into)
  }

  fn encoded_len(&self) -> usize {
    MESSAGE_SIZE_LEN + self.header.encoded_len() + encoded_len_varint(self.term)
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    let src_len = src.len();
    if src_len < MESSAGE_SIZE_LEN {
      return Err(TransformError::DecodeBufferTooSmall);
    }

    let mut offset = 0;
    let encoded_len = NetworkEndian::read_u32(&src[offset..]) as usize;
    if encoded_len > src_len {
      return Err(TransformError::DecodeBufferTooSmall);
    }

    offset += MESSAGE_SIZE_LEN;
    let (header_len, header) = Header::<I, A>::decode(&src[offset..])?;
    offset += header_len;

    let (readed, term) = decode_varint(&src[offset..])?;
    offset += readed;

    Ok((offset, Self { header, term }))
  }
}

#[cfg(any(feature = "test", test))]
impl HeartbeatRequest<smol_str::SmolStr, std::net::SocketAddr> {
  #[doc(hidden)]
  pub fn __large() -> Self {
    Self {
      header: Header::__large(),
      term: 1,
    }
  }

  #[doc(hidden)]
  pub fn __small() -> Self {
    Self {
      header: Header::__small(),
      term: 1,
    }
  }
}

#[cfg(test)]
unit_test_transformable_roundtrip!(HeartbeatRequest <smol_str::SmolStr, std::net::SocketAddr> => heartbeat_request);
