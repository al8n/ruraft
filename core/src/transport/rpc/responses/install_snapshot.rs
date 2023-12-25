use ruraft_utils::{decode_varint, encode_varint, encoded_len_varint};

use crate::MESSAGE_SIZE_LEN;

use super::*;

/// The response returned from an
/// [`InstallSnapshotRequest`].
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct InstallSnapshotResponse<I, A> {
  /// The header of the response
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the header of the response."),),
    setter(attrs(doc = "Set the header of the response."),)
  )]
  header: Header<I, A>,

  /// The term
  #[viewit(
    getter(const, attrs(doc = "Get the newer term if leader is out of date."),),
    setter(attrs(doc = "Set the newer term if leader is out of date."),)
  )]
  term: u64,

  /// Successfully install the snapshot or not.
  #[viewit(
    getter(const, attrs(doc = "Get if successfully install the snapshot or not."),),
    setter(attrs(doc = "Set if successfully install the snapshot or not."),)
  )]
  success: bool,
}

impl<I, A> InstallSnapshotResponse<I, A> {
  /// Create a new [`InstallSnapshotResponse`] with the given `id` and `addr` and `version`. Other fields
  /// are set to their default values.
  #[inline]
  pub const fn new(version: ProtocolVersion, id: I, addr: A) -> Self {
    Self {
      header: Header::new(version, id, addr),
      term: 0,
      success: false,
    }
  }

  /// Create a new [`InstallSnapshotResponse`] with the given protocol version, node and default values.
  #[inline]
  pub const fn from_node(version: ProtocolVersion, node: Node<I, A>) -> Self {
    Self {
      header: Header::from_node(version, node),
      term: 0,
      success: false,
    }
  }

  /// Create a new [`InstallSnapshotResponse`] with the given header and default values.
  #[inline]
  pub const fn from_header(header: Header<I, A>) -> Self {
    Self {
      header,
      term: 0,
      success: false,
    }
  }
}

impl<I: CheapClone, A: CheapClone> CheapClone for InstallSnapshotResponse<I, A> {
  fn cheap_clone(&self) -> Self {
    Self {
      header: self.header.cheap_clone(),
      term: self.term,
      success: self.success,
    }
  }
}

// Encode
//
// ----------------------------------------------------------------------------
// | len (4 bytes) | header (variable) | success (1 byte) | term (u64 varint) |
// ----------------------------------------------------------------------------
impl<I, A> Transformable for InstallSnapshotResponse<I, A>
where
  I: Transformable + Send + Sync + 'static,
  I::Error: Send + Sync + 'static,
  A: Transformable + Send + Sync + 'static,
  A::Error: Send + Sync + 'static,
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
    dst[offset] = self.success as u8;
    offset += 1;

    offset += encode_varint(self.term, &mut dst[offset..])?;
    debug_assert_eq!(
      offset, encoded_len,
      "expected bytes wrote ({}) not match actual bytes wrote ({})",
      encoded_len, offset
    );
    Ok(offset)
  }

  fn encoded_len(&self) -> usize {
    MESSAGE_SIZE_LEN + 1 + encoded_len_varint(self.term) + self.header.encoded_len()
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

    let success = src[offset] != 0;
    offset += 1;

    let (readed, term) = decode_varint(&src[offset..])?;
    offset += readed;

    debug_assert_eq!(
      offset, encoded_len,
      "expected bytes read ({}) not match actual bytes read ({})",
      encoded_len, offset
    );

    Ok((
      offset,
      Self {
        header,
        term,
        success,
      },
    ))
  }
}

#[cfg(any(feature = "test", test))]
impl InstallSnapshotResponse<smol_str::SmolStr, std::net::SocketAddr> {
  #[doc(hidden)]
  pub fn __large() -> Self {
    Self {
      header: Header::__large(),
      term: 1,
      success: false,
    }
  }

  #[doc(hidden)]
  pub fn __small() -> Self {
    Self {
      header: Header::__small(),
      term: 1,
      success: true,
    }
  }
}

#[cfg(test)]
unit_test_transformable_roundtrip!(TimeoutNowResponse <smol_str::SmolStr, std::net::SocketAddr> => install_snapshot_response);
