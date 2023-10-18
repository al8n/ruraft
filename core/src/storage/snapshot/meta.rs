use std::mem;

use nodecraft::Transformable;

use crate::{
  membership::MembershipTransformableError, options::UnknownSnapshotVersion, utils::invalid_data,
};

use super::*;

#[viewit::viewit(
  vis_all = "pub(crate)",
  getters(vis_all = "pub"),
  setters(vis_all = "pub", style = "ref")
)]
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SnapshotId {
  index: u64,
  term: u64,
  timestamp: u64,
}

impl core::fmt::Display for SnapshotId {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}_{}_{}", self.term, self.index, self.timestamp)
  }
}

impl SnapshotId {
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

impl<I: Id, A: Address> PartialEq<SnapshotId> for SnapshotMeta<I, A> {
  fn eq(&self, other: &SnapshotId) -> bool {
    self.index == other.index && self.term == other.term && self.timestamp == other.timestamp
  }
}

/// Metadata of a snapshot.
#[viewit::viewit(
  getters(vis_all = "pub"),
  setters(vis_all = "pub", prefix = "with", style = "ref")
)]
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SnapshotMeta<I: Id, A: Address> {
  /// The version number of the snapshot metadata. This does not cover
  /// the application's data in the snapshot, that should be versioned
  /// separately.
  version: SnapshotVersion,
  /// The term when the snapshot was taken.
  term: u64,
  /// The index when the snapshot was taken.
  index: u64,
  /// timestamp is opaque to the store, and is used for opening.
  timestamp: u64,
  /// The size of the snapshot, in bytes.
  size: u64,
  /// The index of the membership that was taken
  membership_index: u64,
  /// Membership at the time of the snapshot.
  #[viewit(getter(style = "ref", const))]
  membership: Membership<I, A>,
}

impl<I: Id, A: Address> Default for SnapshotMeta<I, A> {
  fn default() -> Self {
    Self::new()
  }
}

const META_FIXED_FIELDS_SIZE: usize = mem::size_of::<SnapshotVersion>() + 5 * mem::size_of::<u64>();

impl<I: Id, A: Address> SnapshotMeta<I, A> {
  /// Create a snapshot meta.
  #[inline]
  pub fn new() -> Self {
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
      membership: Default::default(),
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

#[derive(thiserror::Error)]
pub enum SnapshotMetaTransformableError<I: Id, A: Address> {
  #[error(
    "encode buffer too small, use `Transformable::encoded_len()` to pre-allocate the required size"
  )]
  EncodeBufferTooSmall,
  #[error("{0}")]
  Membership(#[from] MembershipTransformableError<I, A>),
  #[error("corrupted")]
  Corrupted,
  #[error("{0}")]
  UnknownVersion(UnknownSnapshotVersion),
  #[error("{0}")]
  Custom(Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl<I: Id, A: Address> core::fmt::Debug for SnapshotMetaTransformableError<I, A> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    core::fmt::Display::fmt(&self, f)
  }
}

const U64_SIZE: usize = mem::size_of::<u64>();

impl<I, A> Transformable for SnapshotMeta<I, A>
where
  I: Id + Send + Sync + 'static,
  A: Address + Send + Sync + 'static,
{
  type Error = SnapshotMetaTransformableError<I, A>;

  fn encode(&self, dst: &mut [u8]) -> Result<(), Self::Error> {
    let encoded_len = self.encoded_len();
    if dst.len() < encoded_len {
      return Err(SnapshotMetaTransformableError::EncodeBufferTooSmall);
    }

    let mut offset = 0;
    dst[offset] = self.version as u8;
    offset += 1;
    dst[offset..offset + U64_SIZE].copy_from_slice(&self.term.to_be_bytes());
    offset += U64_SIZE;
    dst[offset..offset + U64_SIZE].copy_from_slice(&self.index.to_be_bytes());
    offset += U64_SIZE;
    dst[offset..offset + U64_SIZE].copy_from_slice(&self.timestamp.to_be_bytes());
    offset += U64_SIZE;
    dst[offset..offset + U64_SIZE].copy_from_slice(&self.size.to_be_bytes());
    offset += U64_SIZE;
    dst[offset..offset + U64_SIZE].copy_from_slice(&self.membership_index.to_be_bytes());
    offset += U64_SIZE;
    self
      .membership
      .encode(&mut dst[offset..])
      .map_err(Into::into)
  }

  fn encode_to_writer<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
    let mut buf = [0; META_FIXED_FIELDS_SIZE];
    let mut offset = 0;
    buf[offset] = self.version as u8;
    offset += 1;
    buf[offset..offset + U64_SIZE].copy_from_slice(&self.term.to_be_bytes());
    offset += U64_SIZE;
    buf[offset..offset + U64_SIZE].copy_from_slice(&self.index.to_be_bytes());
    offset += U64_SIZE;
    buf[offset..offset + U64_SIZE].copy_from_slice(&self.timestamp.to_be_bytes());
    offset += U64_SIZE;
    buf[offset..offset + U64_SIZE].copy_from_slice(&self.size.to_be_bytes());
    offset += U64_SIZE;
    buf[offset..offset + U64_SIZE].copy_from_slice(&self.membership_index.to_be_bytes());
    writer.write_all(&buf)?;
    self.membership.encode_to_writer(writer).map_err(Into::into)
  }

  async fn encode_to_async_writer<W: futures::io::AsyncWrite + Send + Unpin>(
    &self,
    writer: &mut W,
  ) -> std::io::Result<()> {
    use futures::AsyncWriteExt;

    let mut buf = [0; META_FIXED_FIELDS_SIZE];
    let mut offset = 0;
    buf[offset] = self.version as u8;
    offset += 1;
    buf[offset..offset + U64_SIZE].copy_from_slice(&self.term.to_be_bytes());
    offset += U64_SIZE;
    buf[offset..offset + U64_SIZE].copy_from_slice(&self.index.to_be_bytes());
    offset += U64_SIZE;
    buf[offset..offset + U64_SIZE].copy_from_slice(&self.timestamp.to_be_bytes());
    offset += U64_SIZE;
    buf[offset..offset + U64_SIZE].copy_from_slice(&self.size.to_be_bytes());
    offset += U64_SIZE;
    buf[offset..offset + U64_SIZE].copy_from_slice(&self.membership_index.to_be_bytes());
    writer.write_all(&buf).await?;
    self
      .membership
      .encode_to_async_writer(writer)
      .await
      .map_err(Into::into)
  }

  fn encoded_len(&self) -> usize {
    META_FIXED_FIELDS_SIZE + self.membership.encoded_len()
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    if src.len() < META_FIXED_FIELDS_SIZE {
      return Err(SnapshotMetaTransformableError::Corrupted);
    }

    let mut offset = 0;
    let version = SnapshotVersion::try_from(src[offset])
      .map_err(SnapshotMetaTransformableError::UnknownVersion)?;
    offset += 1;
    let term = u64::from_be_bytes(src[offset..offset + U64_SIZE].try_into().unwrap());
    offset += U64_SIZE;
    let index = u64::from_be_bytes(src[offset..offset + U64_SIZE].try_into().unwrap());
    offset += U64_SIZE;
    let timestamp = u64::from_be_bytes(src[offset..offset + U64_SIZE].try_into().unwrap());
    offset += U64_SIZE;
    let size = u64::from_be_bytes(src[offset..offset + U64_SIZE].try_into().unwrap());
    offset += U64_SIZE;
    let membership_index = u64::from_be_bytes(src[offset..offset + U64_SIZE].try_into().unwrap());
    offset += U64_SIZE;
    let (readed, membership) = Membership::decode(&src[offset..])?;
    offset += readed;
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

  fn decode_from_reader<R: std::io::Read>(reader: &mut R) -> std::io::Result<(usize, Self)>
  where
    Self: Sized,
  {
    let mut buf = [0; META_FIXED_FIELDS_SIZE];
    reader.read_exact(&mut buf)?;
    let mut offset = 0;
    let version = SnapshotVersion::try_from(buf[offset])
      .map_err(|e| invalid_data(Self::Error::UnknownVersion(e)))?;
    offset += 1;
    let term = u64::from_be_bytes(buf[offset..offset + U64_SIZE].try_into().unwrap());
    offset += U64_SIZE;
    let index = u64::from_be_bytes(buf[offset..offset + U64_SIZE].try_into().unwrap());
    offset += U64_SIZE;
    let timestamp = u64::from_be_bytes(buf[offset..offset + U64_SIZE].try_into().unwrap());
    offset += U64_SIZE;
    let size = u64::from_be_bytes(buf[offset..offset + U64_SIZE].try_into().unwrap());
    offset += U64_SIZE;
    let membership_index = u64::from_be_bytes(buf[offset..offset + U64_SIZE].try_into().unwrap());
    let (readed, membership) = Membership::decode_from_reader(reader)?;
    Ok((
      META_FIXED_FIELDS_SIZE + readed,
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

  async fn decode_from_async_reader<R: futures::io::AsyncRead + Send + Unpin>(
    reader: &mut R,
  ) -> std::io::Result<(usize, Self)>
  where
    Self: Sized,
  {
    use futures::AsyncReadExt;

    let mut buf = [0; META_FIXED_FIELDS_SIZE];
    reader.read_exact(&mut buf).await?;
    let mut offset = 0;
    let version = SnapshotVersion::try_from(buf[offset])
      .map_err(|e| invalid_data(Self::Error::UnknownVersion(e)))?;
    offset += 1;
    let term = u64::from_be_bytes(buf[offset..offset + U64_SIZE].try_into().unwrap());
    offset += U64_SIZE;
    let index = u64::from_be_bytes(buf[offset..offset + U64_SIZE].try_into().unwrap());
    offset += U64_SIZE;
    let timestamp = u64::from_be_bytes(buf[offset..offset + U64_SIZE].try_into().unwrap());
    offset += U64_SIZE;
    let size = u64::from_be_bytes(buf[offset..offset + U64_SIZE].try_into().unwrap());
    offset += U64_SIZE;
    let membership_index = u64::from_be_bytes(buf[offset..offset + U64_SIZE].try_into().unwrap());
    let (readed, membership) = Membership::decode_from_async_reader(reader).await?;
    Ok((
      META_FIXED_FIELDS_SIZE + readed,
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
