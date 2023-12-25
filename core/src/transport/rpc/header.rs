use crate::MESSAGE_SIZE_LEN;

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
  I: Transformable,
  A: Transformable,
{
  type Error = TransformError;

  fn encode(&self, dst: &mut [u8]) -> Result<usize, Self::Error> {
    let encoded_len = self.encoded_len();

    if dst.len() < encoded_len {
      return Err(TransformError::EncodeBufferTooSmall);
    }

    NetworkEndian::write_u32(&mut dst[..MESSAGE_SIZE_LEN], encoded_len as u32);
    let mut offset = MESSAGE_SIZE_LEN;
    dst[offset] = self.protocol_version as u8;
    offset += 1;
    offset += self
      .id()
      .encode(&mut dst[offset..])
      .map_err(TransformError::encode)?;
    self
      .addr()
      .encode(&mut dst[offset..])
      .map(|addr_encoded_len| {
        offset += addr_encoded_len;
        debug_assert_eq!(
          offset, encoded_len,
          "expected bytes wrote ({}) not match actual bytes wrote ({})",
          encoded_len, offset
        );
        offset
      })
      .map_err(TransformError::encode)
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

    let msg_len = NetworkEndian::read_u32(&src[..MESSAGE_SIZE_LEN]) as usize;
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
    debug_assert_eq!(
      msg_len, offset,
      "expected bytes read ({}) not match actual bytes read ({})",
      msg_len, offset
    );
    Ok((
      offset,
      Self {
        protocol_version: version,
        from: Node::new(id, addr),
      },
    ))
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
