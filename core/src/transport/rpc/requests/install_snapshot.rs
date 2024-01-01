use ruraft_utils::{decode_varint, encode_varint, encoded_len_varint};

use crate::MESSAGE_SIZE_LEN;

use super::*;

/// The command sent to a Raft peer to bootstrap its
/// log (and state machine) from a snapshot on another peer.
#[viewit::viewit]
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
  feature = "serde",
  serde(
    rename_all = "snake_case",
    bound(
      serialize = "I: Eq + core::hash::Hash + serde::Serialize, A: serde::Serialize",
      deserialize = "I: Eq + core::hash::Hash + core::fmt::Display + for<'a> serde::Deserialize<'a>, A: Eq + core::fmt::Display + for<'a> serde::Deserialize<'a>",
    )
  )
)]
pub struct InstallSnapshotRequest<I, A> {
  /// The header of the request
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the header of the request"),),
    setter(attrs(doc = "Set the header of the request"),)
  )]
  header: Header<I, A>,

  /// The snapshot version
  #[viewit(
    getter(const, attrs(doc = "Get the version of the install snapshot request"),),
    setter(attrs(doc = "Set the version of the install snapshot request"),)
  )]
  snapshot_version: SnapshotVersion,

  /// The term
  #[viewit(
    getter(const, attrs(doc = "Get the term of the install snapshot request"),),
    setter(attrs(doc = "Set the term of the install snapshot request"),)
  )]
  term: u64,

  /// The last index included in the snapshot
  #[viewit(
    getter(const, attrs(doc = "Get the last index included in the snapshot"),),
    setter(attrs(doc = "Set the last index included in the snapshot"),)
  )]
  last_log_index: u64,

  /// The last term included in the snapshot
  #[viewit(
    getter(const, attrs(doc = "Get the last term included in the snapshot"),),
    setter(attrs(doc = "Set the last term included in the snapshot"),)
  )]
  last_log_term: u64,

  /// Cluster membership.
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Get the [`Membership`] of the install snapshot request"),
    ),
    setter(attrs(doc = "Set the [`Membership`] of the install snapshot request"),)
  )]
  membership: Membership<I, A>,

  /// Log index where [`Membership`] entry was originally written.
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Get log index where [`Membership`] entry was originally written of the install snapshot request"
      ),
    ),
    setter(attrs(
      doc = "Set log index where [`Membership`] entry was originally written of the install snapshot request"
    ),)
  )]
  membership_index: u64,

  /// Size of the snapshot
  #[viewit(
    getter(const, attrs(doc = "Get the size of the snapshot"),),
    setter(attrs(doc = "Set the size of the snapshot"),)
  )]
  size: u64,
}

impl<I: core::hash::Hash + Eq, A: PartialEq> PartialEq for InstallSnapshotRequest<I, A> {
  fn eq(&self, other: &Self) -> bool {
    self.header == other.header
      && self.term == other.term
      && self.snapshot_version == other.snapshot_version
      && self.last_log_index == other.last_log_index
      && self.last_log_term == other.last_log_term
      && self.membership == other.membership
      && self.membership_index == other.membership_index
      && self.size == other.size
  }
}

impl<I: core::hash::Hash + Eq, A: Eq> Eq for InstallSnapshotRequest<I, A> {}

impl<I, A> InstallSnapshotRequest<I, A> {
  /// Create a new [`InstallSnapshotRequest`] with the given `version`, `id`, `addr` and `membership`. Other fields
  /// are set to their default values.
  #[inline]
  pub const fn new(version: ProtocolVersion, id: I, addr: A, membership: Membership<I, A>) -> Self {
    Self {
      header: Header {
        protocol_version: version,
        from: Node::new(id, addr),
      },
      term: 0,
      snapshot_version: SnapshotVersion::V1,
      last_log_index: 0,
      last_log_term: 0,
      membership,
      membership_index: 0,
      size: 0,
    }
  }

  /// Create a new [`InstallSnapshotRequest`] with the given protocol version, node, membership and default values.
  #[inline]
  pub const fn from_node(
    version: ProtocolVersion,
    node: Node<I, A>,
    membership: Membership<I, A>,
  ) -> Self {
    Self {
      header: Header {
        protocol_version: version,
        from: node,
      },
      term: 0,
      snapshot_version: SnapshotVersion::V1,
      last_log_index: 0,
      last_log_term: 0,
      membership,
      membership_index: 0,
      size: 0,
    }
  }

  /// Create a new [`InstallSnapshotRequest`] with the given header, membership and default values.
  #[inline]
  pub const fn from_header(header: Header<I, A>, membership: Membership<I, A>) -> Self {
    Self {
      header,
      term: 0,
      snapshot_version: SnapshotVersion::V1,
      last_log_index: 0,
      last_log_term: 0,
      membership,
      membership_index: 0,
      size: 0,
    }
  }
}

impl<I: CheapClone, A: CheapClone> CheapClone for InstallSnapshotRequest<I, A> {
  fn cheap_clone(&self) -> Self {
    Self {
      header: self.header.cheap_clone(),
      term: self.term,
      snapshot_version: self.snapshot_version,
      last_log_index: self.last_log_index,
      last_log_term: self.last_log_term,
      membership: self.membership.cheap_clone(),
      membership_index: self.membership_index,
      size: self.size,
    }
  }
}

// Encode
//
// -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
// | len (4 bytes) | header (variable) | version (1 byte) | size (uvarint) | term (uvarint) | last_log_index (uvarint) | last_log_term (uvarint) | membership_index (uvarint) | membership (variable) |
// -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
impl<I, A> Transformable for InstallSnapshotRequest<I, A>
where
  I: Id,
  A: Address,
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
    dst[offset] = self.snapshot_version as u8;
    offset += 1;
    offset += encode_varint(self.size, &mut dst[offset..])?;
    offset += encode_varint(self.term, &mut dst[offset..])?;
    offset += encode_varint(self.last_log_index, &mut dst[offset..])?;
    offset += encode_varint(self.last_log_term, &mut dst[offset..])?;
    offset += encode_varint(self.membership_index, &mut dst[offset..])?;
    self
      .membership
      .encode(&mut dst[offset..])
      .map(|size| {
        offset += size;
        debug_assert_eq!(
          offset, encoded_len,
          "expected bytes wrote ({}) not match actual bytes wrote ({})",
          encoded_len, offset
        );
        offset
      })
      .map_err(Self::Error::encode)
  }

  fn encoded_len(&self) -> usize {
    MESSAGE_SIZE_LEN
      + self.header.encoded_len()
      + 1
      + encoded_len_varint(self.size)
      + encoded_len_varint(self.term)
      + encoded_len_varint(self.last_log_index)
      + encoded_len_varint(self.last_log_term)
      + encoded_len_varint(self.membership_index)
      + self.membership.encoded_len()
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

    let (readed, header) = Header::<I, A>::decode(&src[offset..])?;
    offset += readed;
    let version = SnapshotVersion::try_from(src[offset]).map_err(Self::Error::from)?;
    offset += 1;
    let (readed, size) = decode_varint(&src[offset..])?;
    offset += readed;
    let (readed, term) = decode_varint(&src[offset..])?;
    offset += readed;
    let (readed, last_log_index) = decode_varint(&src[offset..])?;
    offset += readed;
    let (readed, last_log_term) = decode_varint(&src[offset..])?;
    offset += readed;
    let (readed, membership_index) = decode_varint(&src[offset..])?;
    offset += readed;
    let (membership_len, membership) =
      Membership::<I, A>::decode(&src[offset..]).map_err(Self::Error::decode)?;
    offset += membership_len;

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
        snapshot_version: version,
        last_log_index,
        last_log_term,
        membership,
        membership_index,
        size,
      },
    ))
  }
}

#[cfg(any(feature = "test", test))]
impl InstallSnapshotRequest<smol_str::SmolStr, std::net::SocketAddr> {
  #[doc(hidden)]
  pub fn __large() -> Self {
    Self {
      header: Header::__large(),
      term: 1,
      snapshot_version: SnapshotVersion::V1,
      last_log_index: 1,
      last_log_term: 2,
      membership: Membership::__large_membership(),
      membership_index: 1,
      size: 100,
    }
  }

  #[doc(hidden)]
  pub fn __small() -> Self {
    Self {
      header: Header::__small(),
      term: 1,
      snapshot_version: SnapshotVersion::V1,
      last_log_index: 1,
      last_log_term: 2,
      membership: Membership::__single_server(),
      membership_index: 1,
      size: 100,
    }
  }
}

#[cfg(test)]
unit_test_transformable_roundtrip!(InstallSnapshotRequest <smol_str::SmolStr, std::net::SocketAddr> => install_snapshot_request);
