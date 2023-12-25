use ruraft_utils::{decode_varint, encode_varint, encoded_len_varint};

use crate::MESSAGE_SIZE_LEN;

use super::*;

/// The command used by a candidate to ask a Raft peer
/// for a vote in an election.
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct VoteRequest<I, A> {
  /// The header of the request
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the header of the request"),),
    setter(attrs(doc = "Set the header of the request"),)
  )]
  header: Header<I, A>,

  /// The term of the candidate
  #[viewit(
    getter(const, attrs(doc = "Get the term of the candidate"),),
    setter(attrs(doc = "Set the term of the candidate"),)
  )]
  term: u64,

  /// The index of the candidate's last log entry
  #[viewit(
    getter(const, attrs(doc = "Get the index of the candidate's last log entry"),),
    setter(attrs(doc = "Set the index of the candidate's last log entry"),)
  )]
  last_log_index: u64,

  /// The term of the candidate's last log entry
  #[viewit(
    getter(const, attrs(doc = "Get the term of the candidate's last log entry"),),
    setter(attrs(doc = "Set the term of the candidate's last log entry"),)
  )]
  last_log_term: u64,

  /// Used to indicate to peers if this vote was triggered by a leadership
  /// transfer. It is required for leadership transfer to work, because servers
  /// wouldn't vote otherwise if they are aware of an existing leader.
  #[viewit(
    getter(
      const,
      attrs(doc = "Get if this vote was triggered by a leadership transfer"),
    ),
    setter(attrs(doc = "Set if this vote was triggered by a leadership transfer"),)
  )]
  leadership_transfer: bool,
}

impl<I, A> VoteRequest<I, A> {
  /// Create a new [`VoteRequest`] with the given `version`, `id`, and `addr`. Other fields
  /// are set to their default values.
  #[inline]
  pub const fn new(version: ProtocolVersion, id: I, addr: A) -> Self {
    Self {
      header: Header {
        protocol_version: version,
        from: Node::new(id, addr),
      },
      term: 0,
      last_log_index: 0,
      last_log_term: 0,
      leadership_transfer: false,
    }
  }

  /// Create a new [`VoteRequest`] with the given protocol version, node and default values.
  #[inline]
  pub const fn from_node(version: ProtocolVersion, node: Node<I, A>) -> Self {
    Self {
      header: Header {
        protocol_version: version,
        from: node,
      },
      term: 0,
      last_log_index: 0,
      last_log_term: 0,
      leadership_transfer: false,
    }
  }

  /// Create a new [`VoteRequest`] with the given header and default values.
  #[inline]
  pub const fn from_header(header: Header<I, A>) -> Self {
    Self {
      header,
      term: 0,
      last_log_index: 0,
      last_log_term: 0,
      leadership_transfer: false,
    }
  }
}

impl<I: CheapClone, A: CheapClone> CheapClone for VoteRequest<I, A> {
  fn cheap_clone(&self) -> Self {
    Self {
      header: self.header.cheap_clone(),
      term: self.term,
      last_log_index: self.last_log_index,
      last_log_term: self.last_log_term,
      leadership_transfer: self.leadership_transfer,
    }
  }
}

// Encode
//
// -------------------------------------------------------------------------------------------------------------------------------------------
// | len (4 bytes) | header (variable) | leadership_transfer (1 byte) | term (uvarint) | last_log_index (uvarint) | last_log_index (uvarint) |
// -------------------------------------------------------------------------------------------------------------------------------------------
impl<I, A> Transformable for VoteRequest<I, A>
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
    NetworkEndian::write_u32(&mut dst[offset..], encoded_len as u32);
    offset += MESSAGE_SIZE_LEN;
    offset += self.header.encode(&mut dst[offset..])?;

    dst[offset] = self.leadership_transfer as u8;
    offset += 1;
    offset += encode_varint(self.term, &mut dst[offset..])?;
    offset += encode_varint(self.last_log_index, &mut dst[offset..])?;
    offset += encode_varint(self.last_log_term, &mut dst[offset..])?;

    debug_assert_eq!(
      offset, encoded_len,
      "expected bytes wrote ({}) not match actual bytes wrote ({})",
      encoded_len, offset
    );
    Ok(offset)
  }

  fn encoded_len(&self) -> usize {
    MESSAGE_SIZE_LEN
      + self.header.encoded_len()
      + 1
      + encoded_len_varint(self.term)
      + encoded_len_varint(self.last_log_index)
      + encoded_len_varint(self.last_log_term)
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
    let encoded_len = NetworkEndian::read_u32(&src[offset..]) as usize;
    if encoded_len > src_len {
      return Err(TransformError::DecodeBufferTooSmall);
    }
    offset += MESSAGE_SIZE_LEN;

    let (header_len, header) = Header::<I, A>::decode(&src[offset..])?;
    offset += header_len;

    let leadership_transfer = src[offset] != 0;
    offset += 1;

    let (readed, term) = decode_varint(&src[offset..])?;
    offset += readed;
    let (readed, last_log_index) = decode_varint(&src[offset..])?;
    offset += readed;
    let (readed, last_log_term) = decode_varint(&src[offset..])?;
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
        last_log_index,
        last_log_term,
        leadership_transfer,
      },
    ))
  }
}

#[cfg(any(feature = "test", test))]
impl VoteRequest<smol_str::SmolStr, std::net::SocketAddr> {
  #[doc(hidden)]
  pub fn __large() -> Self {
    Self {
      header: Header::__large(),
      term: 1,
      last_log_index: 1,
      last_log_term: 2,
      leadership_transfer: false,
    }
  }

  #[doc(hidden)]
  pub fn __small() -> Self {
    Self {
      header: Header::__small(),
      term: 1,
      last_log_index: 1,
      last_log_term: 2,
      leadership_transfer: true,
    }
  }
}

#[cfg(test)]
unit_test_transformable_roundtrip!(VoteRequest <smol_str::SmolStr, std::net::SocketAddr> => vote_request);
