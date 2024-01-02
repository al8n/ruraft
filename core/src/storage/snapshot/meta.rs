use byteorder::{ByteOrder, NetworkEndian};
use nodecraft::{CheapClone, Transformable};
use ruraft_utils::{decode_varint, encode_varint, encoded_len_varint};

use crate::{
  membership::MembershipTransformError, options::UnknownSnapshotVersion, MESSAGE_SIZE_LEN,
};

use super::*;

/// The id used to identify a snapshot.
#[viewit::viewit(setters(prefix = "with"))]
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SnapshotId {
  /// The index of the snapshot.
  #[viewit(
    getter(
      const,
      style = "move",
      attrs(doc = "Returns the index when the snapshot was taken.")
    ),
    setter(attrs(doc = "Sets the index when the snapshot was taken."))
  )]
  index: u64,

  /// The term of the snapshot.
  #[viewit(
    getter(
      const,
      style = "move",
      attrs(doc = "Returns the term when the snapshot was taken.")
    ),
    setter(attrs(doc = "Sets the term when the snapshot was taken."))
  )]
  term: u64,

  /// The timestamp of the snapshot.
  #[viewit(
    getter(
      const,
      style = "move",
      attrs(doc = "Returns the timestamp when the snapshot was taken.")
    ),
    setter(attrs(doc = "Sets the timestamp when the snapshot was taken."))
  )]
  timestamp: u64,
}

impl core::fmt::Display for SnapshotId {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}_{}_{}", self.term, self.index, self.timestamp)
  }
}

impl PartialOrd for SnapshotId {
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for SnapshotId {
  fn cmp(&self, other: &Self) -> std::cmp::Ordering {
    self
      .term
      .cmp(&other.term)
      .then(self.index.cmp(&other.index))
      .then(self.timestamp.cmp(&other.timestamp))
  }
}

impl SnapshotId {
  /// Create a snapshot id with the given index and term.
  #[inline]
  pub fn new(index: u64, term: u64) -> Self {
    let now = std::time::SystemTime::now()
      .duration_since(std::time::UNIX_EPOCH)
      .unwrap()
      .as_millis() as u64;

    Self {
      index,
      term,
      timestamp: now,
    }
  }
}

impl<I, A> PartialEq<SnapshotId> for SnapshotMeta<I, A> {
  fn eq(&self, other: &SnapshotId) -> bool {
    self.index == other.index && self.term == other.term && self.timestamp == other.timestamp
  }
}

/// Metadata of a snapshot.
#[viewit::viewit(
  getters(vis_all = "pub"),
  setters(vis_all = "pub", prefix = "with", style = "ref")
)]
#[derive(Debug)]
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
pub struct SnapshotMeta<I, A> {
  /// The version number of the snapshot metadata. This does not cover
  /// the application's data in the snapshot, that should be versioned
  /// separately.
  #[viewit(
    getter(const, attrs(doc = "Returns the version of the snapshot meta.")),
    setter(attrs(doc = "Sets the version of the snapshot meta."))
  )]
  version: SnapshotVersion,
  /// The term when the snapshot was taken.
  #[viewit(
    getter(const, attrs(doc = "Returns the term when the snapshot was taken.")),
    setter(attrs(doc = "Sets the term when the snapshot was taken."))
  )]
  term: u64,
  /// The index when the snapshot was taken.
  #[viewit(
    getter(const, attrs(doc = "Returns the index when the snapshot was taken.")),
    setter(attrs(doc = "Sets the index when the snapshot was taken."))
  )]
  index: u64,
  /// timestamp is opaque to the store, and is used for opening.
  #[viewit(
    getter(
      const,
      attrs(doc = "Returns the timestamp when the snapshot was taken.")
    ),
    setter(attrs(doc = "Sets the index when the snapshot was taken."))
  )]
  timestamp: u64,
  /// The size of the snapshot, in bytes.
  #[viewit(
    getter(const, attrs(doc = "Returns the size of the snapshot, in bytes.")),
    setter(attrs(doc = "Sets the size of the snapshot, in bytes."))
  )]
  size: u64,
  /// The index of the membership when the snapshot was taken
  #[viewit(
    getter(
      const,
      attrs(doc = "Returns the index of the membership when the snapshot was taken.")
    ),
    setter(attrs(doc = "Sets the index of the membership when the snapshot was taken."))
  )]
  membership_index: u64,
  /// Membership at the time of the snapshot.
  #[viewit(
    getter(
      style = "ref",
      const,
      attrs(doc = "Returns the membership at the time when the snapshot was taken.")
    ),
    setter(attrs(doc = "Sets the membership at the time when the snapshot was taken."))
  )]
  membership: Membership<I, A>,
}

impl<I, A> Clone for SnapshotMeta<I, A> {
  fn clone(&self) -> Self {
    Self {
      membership: self.membership.clone(),
      ..*self
    }
  }
}

impl<I, A> CheapClone for SnapshotMeta<I, A> {}

impl<I: core::hash::Hash + Eq, A: PartialEq> PartialEq for SnapshotMeta<I, A> {
  fn eq(&self, other: &Self) -> bool {
    self.version == other.version
      && self.term == other.term
      && self.index == other.index
      && self.timestamp == other.timestamp
      && self.size == other.size
      && self.membership_index == other.membership_index
      && self.membership == other.membership
  }
}

impl<I: core::hash::Hash + Eq, A: PartialEq> Eq for SnapshotMeta<I, A> {}

impl<I: core::hash::Hash + Eq, A: PartialEq> PartialOrd for SnapshotMeta<I, A> {
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl<I: core::hash::Hash + Eq, A: PartialEq> Ord for SnapshotMeta<I, A> {
  fn cmp(&self, other: &Self) -> std::cmp::Ordering {
    self
      .term
      .cmp(&other.term)
      .then_with(|| self.index.cmp(&other.index))
      .then_with(|| self.timestamp.cmp(&other.timestamp))
  }
}

impl<I, A> SnapshotMeta<I, A> {
  /// Create a snapshot meta with a [`Membership`](crate::membership::Membership), and keep
  /// other fields as default.
  #[inline]
  pub fn new(membership: Membership<I, A>) -> Self {
    Self {
      timestamp: std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64,
      version: SnapshotVersion::V1,
      term: 0,
      index: 0,
      size: 0,
      membership_index: 0,
      membership,
    }
  }

  /// Returns the id of the snapshot.
  #[inline]
  pub const fn id(&self) -> SnapshotId {
    SnapshotId {
      index: self.index,
      term: self.term,
      timestamp: self.timestamp,
    }
  }
}

/// Errors that can occur when transforming a [`SnapshotMeta`](crate::storage::SnapshotMeta) to its bytes representation.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotMetaTransformableError<I: Transformable, A: Transformable> {
  /// Encode buffer too small.
  #[error(
    "encode buffer too small, use `Transformable::encoded_len()` to pre-allocate the required size"
  )]
  EncodeBufferTooSmall,
  /// Membership transform error.
  #[error("{0}")]
  Membership(#[from] MembershipTransformError<I, A>),
  /// Encode varint error.
  #[error("{0}")]
  EncodeVarint(#[from] ruraft_utils::EncodeVarintError),
  /// Decode varint error.
  #[error("{0}")]
  DecodeVarint(#[from] ruraft_utils::DecodeVarintError),
  /// Corrupted log bytes data, which cannot be decoded back anymore because of losing information.
  #[error("corrupted")]
  Corrupted,
  /// Unknown snapshot version.
  #[error("{0}")]
  UnknownVersion(UnknownSnapshotVersion),
  /// Custom error.
  #[error("{0}")]
  Custom(Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl<I, A> Transformable for SnapshotMeta<I, A>
where
  I: Id,
  A: Address,
{
  type Error = SnapshotMetaTransformableError<I, A>;

  fn encode(&self, dst: &mut [u8]) -> Result<usize, Self::Error> {
    let encoded_len = self.encoded_len();
    if dst.len() < encoded_len {
      return Err(SnapshotMetaTransformableError::EncodeBufferTooSmall);
    }

    let mut offset = 0;
    NetworkEndian::write_u32(&mut dst[..MESSAGE_SIZE_LEN], encoded_len as u32);
    offset += MESSAGE_SIZE_LEN;
    dst[offset] = self.version as u8;
    offset += 1;
    offset += encode_varint(self.term, &mut dst[offset..])?;
    offset += encode_varint(self.index, &mut dst[offset..])?;
    offset += encode_varint(self.timestamp, &mut dst[offset..])?;
    offset += encode_varint(self.size, &mut dst[offset..])?;
    offset += encode_varint(self.membership_index, &mut dst[offset..])?;
    self
      .membership
      .encode(&mut dst[offset..])
      .map(|membership_encoded_len| {
        offset += membership_encoded_len;
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
    MESSAGE_SIZE_LEN
      + 1
      + encoded_len_varint(self.term)
      + encoded_len_varint(self.index)
      + encoded_len_varint(self.timestamp)
      + encoded_len_varint(self.size)
      + encoded_len_varint(self.membership_index)
      + self.membership.encoded_len()
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    let src_len = src.len();
    if src_len < MESSAGE_SIZE_LEN {
      return Err(SnapshotMetaTransformableError::Corrupted);
    }

    let mut offset = 0;
    let encoded_len = NetworkEndian::read_u32(&src[..MESSAGE_SIZE_LEN]);
    offset += MESSAGE_SIZE_LEN;
    let version = SnapshotVersion::try_from(src[offset])
      .map_err(SnapshotMetaTransformableError::UnknownVersion)?;
    offset += 1;
    let (readed, term) = decode_varint(&src[offset..])?;
    offset += readed;
    let (readed, index) = decode_varint(&src[offset..])?;
    offset += readed;
    let (readed, timestamp) = decode_varint(&src[offset..])?;
    offset += readed;
    let (readed, size) = decode_varint(&src[offset..])?;
    offset += readed;
    let (readed, membership_index) = decode_varint(&src[offset..])?;
    offset += readed;
    let (readed, membership) = Membership::decode(&src[offset..])?;
    offset += readed;
    debug_assert_eq!(
      encoded_len as usize, offset,
      "expected bytes read ({}) not match actual bytes read ({})",
      encoded_len, offset
    );
    Ok((
      offset,
      Self {
        version,
        term,
        index,
        timestamp,
        size,
        membership_index,
        membership,
      },
    ))
  }
}

#[cfg(test)]
mod tests {
  use std::net::SocketAddr;

  use super::*;

  #[tokio::test]
  async fn test_snapshot_meta_transformable_roundtrip() {
    test_transformable_roundtrip!(SnapshotMeta::<smol_str::SmolStr, SocketAddr> {
      SnapshotMeta {
        version: SnapshotVersion::V1,
        term: 1,
        index: 10,
        timestamp: 1000,
        size: 100,
        membership_index: 1,
        membership: Membership::__sample_membership(),
      }
    });

    test_transformable_roundtrip!(SnapshotMeta::<smol_str::SmolStr, SocketAddr> {
      SnapshotMeta {
        version: SnapshotVersion::V1,
        term: 1,
        index: 10,
        timestamp: 1000,
        size: 100,
        membership_index: 1,
        membership: Membership::__large_membership(),
      }
    });
  }
}
