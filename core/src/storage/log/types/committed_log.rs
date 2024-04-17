use std::fmt::Debug;

use crate::{
  membership::{Membership, MembershipTransformError},
  MESSAGE_SIZE_LEN,
};
use byteorder::{ByteOrder, NetworkEndian};
use bytes::Bytes;
use nodecraft::{transformable::BytesTransformError, Address, Id, Transformable};
use ruraft_utils::{
  decode_varint, encode_varint, encoded_len_varint, DecodeVarintError, EncodeVarintError,
};
use smallvec::SmallVec;

/// Logs can be handled by the finate state machine.
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
  feature = "serde",
  serde(
    rename_all = "snake_case",
    bound(
      serialize = "I: Eq + core::hash::Hash + serde::Serialize, A: serde::Serialize",
      deserialize = "I: Eq + core::hash::Hash + core::fmt::Display + serde::Deserialize<'de>, A: Eq + core::fmt::Display + serde::Deserialize<'de>",
    )
  )
)]
pub enum CommittedLogKind<I, A> {
  /// A normal log entry.
  Log(Bytes),
  /// A membership change log entry.
  Membership(Membership<I, A>),
}

impl<I, A> Clone for CommittedLogKind<I, A> {
  fn clone(&self) -> Self {
    match self {
      Self::Log(data) => Self::Log(data.clone()),
      Self::Membership(membership) => Self::Membership(membership.clone()),
    }
  }
}

impl<I: Eq + core::hash::Hash, A: PartialEq> PartialEq for CommittedLogKind<I, A> {
  fn eq(&self, other: &Self) -> bool {
    match (self, other) {
      (Self::Log(data), Self::Log(other_data)) => data == other_data,
      (Self::Membership(membership), Self::Membership(other_membership)) => {
        membership == other_membership
      }
      _ => false,
    }
  }
}

impl<I: Eq + core::hash::Hash, A: Eq> Eq for CommittedLogKind<I, A> {}

/// A log entry that can be applied to the finate state machine.
#[viewit::viewit(setters(prefix = "with"))]
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
  feature = "serde",
  serde(
    rename_all = "snake_case",
    bound(
      serialize = "I: Eq + core::hash::Hash + serde::Serialize, A: serde::Serialize",
      deserialize = "I: Eq + core::hash::Hash + core::fmt::Display + serde::Deserialize<'de>, A: Eq + core::fmt::Display + serde::Deserialize<'de>",
    )
  )
)]
pub struct CommittedLog<I, A> {
  /// The index of the log entry.
  #[viewit(
    getter(
      const,
      style = "move",
      attrs(doc = "Returns the index of the log entry."),
    ),
    setter(attrs(doc = "Sets the index of the log entry."),)
  )]
  index: u64,
  /// The term of the log entry.
  #[viewit(
    getter(
      const,
      style = "move",
      attrs(doc = "Returns the term of the log entry."),
    ),
    setter(attrs(doc = "Sets the term of the log entry."),)
  )]
  term: u64,
  /// The kind of the log entry.
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Returns the kind of the log entry."),
    ),
    setter(attrs(doc = "Sets the kind of the log entry."),)
  )]
  kind: CommittedLogKind<I, A>,
}

impl<I, A> CommittedLog<I, A> {
  /// Creates a new log entry.
  pub fn new(term: u64, index: u64, kind: CommittedLogKind<I, A>) -> Self {
    Self { index, term, kind }
  }
}

impl<I, A> Clone for CommittedLog<I, A> {
  fn clone(&self) -> Self {
    Self {
      index: self.index,
      term: self.term,
      kind: self.kind.clone(),
    }
  }
}

impl<I: Eq + core::hash::Hash, A: PartialEq> PartialEq for CommittedLog<I, A> {
  fn eq(&self, other: &Self) -> bool {
    self.index == other.index && self.term == other.term && self.kind == other.kind
  }
}

impl<I: Eq + core::hash::Hash, A: Eq> Eq for CommittedLog<I, A> {}

impl<I: Eq + core::hash::Hash, A: Eq> PartialOrd for CommittedLog<I, A> {
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl<I: Eq + core::hash::Hash, A: Eq> Ord for CommittedLog<I, A> {
  fn cmp(&self, other: &Self) -> std::cmp::Ordering {
    self
      .term
      .cmp(&other.term)
      .then(self.index.cmp(&other.index))
  }
}

/// The transformation error of [`CommittedLog`].
#[derive(thiserror::Error)]
pub enum CommittedLogTransformError<I: Transformable, A: Transformable> {
  /// The membership transformation error.
  #[error(transparent)]
  Membership(#[from] MembershipTransformError<I, A>),
  /// The data transformation error.
  #[error(transparent)]
  Data(#[from] BytesTransformError),
  /// The encode buffer is too small.
  #[error("encode buffer too small")]
  EncodeBufferTooSmall,
  /// The log is corrupted.
  #[error("not enough bytes to decode")]
  NotEnoughBytes,
  /// The encode varint error.
  #[error(transparent)]
  EncodeVarint(#[from] EncodeVarintError),
  /// The decode varint error.
  #[error(transparent)]
  DecodeVarint(#[from] DecodeVarintError),
}

impl<I: Transformable + Debug, A: Transformable + Debug> std::fmt::Debug
  for CommittedLogTransformError<I, A>
{
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::Membership(err) => write!(f, "membership: {:?}", err),
      Self::Data(err) => write!(f, "data: {:?}", err),
      Self::EncodeBufferTooSmall => write!(f, "encode buffer too small"),
      Self::EncodeVarint(err) => write!(f, "encode varint: {:?}", err),
      Self::DecodeVarint(err) => write!(f, "decode varint: {:?}", err),
      Self::NotEnoughBytes => write!(f, "not enough bytes to decode"),
    }
  }
}

impl<I: Id, A: Address> Transformable for CommittedLog<I, A> {
  type Error = CommittedLogTransformError<I, A>;

  fn encode(&self, dst: &mut [u8]) -> Result<usize, Self::Error> {
    let encoded_len = self.encoded_len();

    if dst.len() < encoded_len {
      return Err(Self::Error::EncodeBufferTooSmall);
    }

    let mut offset = 0;
    NetworkEndian::write_u32(&mut dst[..MESSAGE_SIZE_LEN], encoded_len as u32);
    offset += MESSAGE_SIZE_LEN;

    offset += encode_varint(self.index, &mut dst[offset..]).map_err(Self::Error::EncodeVarint)?;
    offset += encode_varint(self.term, &mut dst[offset..]).map_err(Self::Error::EncodeVarint)?;
    match &self.kind {
      CommittedLogKind::Log(data) => {
        dst[offset] = 0;
        offset += 1;
        offset += data.encode(&mut dst[offset..]).map_err(Self::Error::Data)?;
      }
      CommittedLogKind::Membership(membership) => {
        dst[offset] = 1;
        offset += 1;
        offset += membership
          .encode(&mut dst[offset..])
          .map_err(Self::Error::Membership)?;
      }
    }
    debug_assert_eq!(
      offset, encoded_len,
      "expected bytes wrote ({}) not match actual bytes wrote ({})",
      encoded_len, offset
    );
    Ok(offset)
  }

  fn encoded_len(&self) -> usize {
    MESSAGE_SIZE_LEN
      + encoded_len_varint(self.index)
      + encoded_len_varint(self.term)
      + 1
      + match &self.kind {
        CommittedLogKind::Log(data) => data.encoded_len(),
        CommittedLogKind::Membership(membership) => membership.encoded_len(),
      }
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    let src_len = src.len();
    let mut cur = 0;
    if src_len < MESSAGE_SIZE_LEN {
      return Err(Self::Error::NotEnoughBytes);
    }
    let len = NetworkEndian::read_u32(&src[..MESSAGE_SIZE_LEN]) as usize;
    cur += MESSAGE_SIZE_LEN;
    if src_len < len {
      return Err(Self::Error::NotEnoughBytes);
    }

    let (readed, index) = decode_varint(&src[cur..]).map_err(Self::Error::DecodeVarint)?;
    cur += readed;
    let (readed, term) = decode_varint(&src[cur..]).map_err(Self::Error::DecodeVarint)?;
    cur += readed;

    let kind = match src[cur] {
      0 => {
        cur += 1;
        let (readed, data) = Bytes::decode(&src[cur..]).map_err(Self::Error::Data)?;
        cur += readed;
        CommittedLogKind::Log(data)
      }
      1 => {
        cur += 1;
        let (readed, membership) =
          Membership::decode(&src[cur..]).map_err(Self::Error::Membership)?;
        cur += readed;
        CommittedLogKind::Membership(membership)
      }
      _ => return Err(Self::Error::NotEnoughBytes),
    };

    debug_assert_eq!(
      cur, len,
      "expected bytes read ({}) not match actual bytes read ({})",
      len, cur
    );
    Ok((len, Self { index, term, kind }))
  }
}

/// A batch of committed logs.
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
  feature = "serde",
  serde(
    transparent,
    bound(
      serialize = "I: Eq + core::hash::Hash + serde::Serialize, A: serde::Serialize",
      deserialize = "I: Eq + core::hash::Hash + core::fmt::Display + serde::Deserialize<'de>, A: Eq + core::fmt::Display + serde::Deserialize<'de>",
    )
  )
)]
pub struct CommittedLogBatch<I, A>(SmallVec<[CommittedLog<I, A>; 4]>);

impl<I, A> core::ops::Deref for CommittedLogBatch<I, A> {
  type Target = SmallVec<[CommittedLog<I, A>; 4]>;

  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

impl<I, A> core::ops::DerefMut for CommittedLogBatch<I, A> {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.0
  }
}

impl<I, A> Clone for CommittedLogBatch<I, A> {
  fn clone(&self) -> Self {
    Self(self.0.clone())
  }
}

impl<I, A> PartialEq for CommittedLogBatch<I, A>
where
  I: Eq + core::hash::Hash,
  A: PartialEq,
{
  fn eq(&self, other: &Self) -> bool {
    self.0 == other.0
  }
}

impl<I, A> Eq for CommittedLogBatch<I, A>
where
  I: Eq + core::hash::Hash,
  A: Eq,
{
}

impl<I, A> Default for CommittedLogBatch<I, A> {
  fn default() -> Self {
    Self::new()
  }
}

impl<I, A> From<Vec<CommittedLog<I, A>>> for CommittedLogBatch<I, A> {
  fn from(logs: Vec<CommittedLog<I, A>>) -> Self {
    Self(logs.into())
  }
}

impl<I, A> From<SmallVec<[CommittedLog<I, A>; 4]>> for CommittedLogBatch<I, A> {
  fn from(logs: SmallVec<[CommittedLog<I, A>; 4]>) -> Self {
    Self(logs)
  }
}

impl<I, A> FromIterator<CommittedLog<I, A>> for CommittedLogBatch<I, A> {
  fn from_iter<T: IntoIterator<Item = CommittedLog<I, A>>>(iter: T) -> Self {
    Self(iter.into_iter().collect())
  }
}

impl<I, A> IntoIterator for CommittedLogBatch<I, A> {
  type Item = CommittedLog<I, A>;
  type IntoIter = smallvec::IntoIter<[CommittedLog<I, A>; 4]>;

  fn into_iter(self) -> Self::IntoIter {
    self.0.into_iter()
  }
}

impl<I, A> CommittedLogBatch<I, A> {
  /// Creates a new batch of committed logs.
  pub fn new() -> Self {
    Self(SmallVec::new())
  }

  /// Construct an empty vector with enough capacity pre-allocated to store at least `n`
  /// elements.
  ///
  /// Will create a heap allocation only if `n` is larger than the inline capacity.
  ///
  /// ```
  /// # use ruraft_core::CommittedLogBatch;
  ///
  /// let v: CommittedLogBatch<u64, std::net::SocketAddr, Vec<u8>> = CommittedLogBatch::with_capacity(100);
  ///
  /// assert!(v.is_empty());
  /// assert!(v.capacity() >= 100);
  /// ```
  pub fn with_capacity(capacity: usize) -> Self {
    Self(SmallVec::with_capacity(capacity))
  }
}

#[cfg(test)]
mod tests {
  use crate::membership;

  use super::{CommittedLog, CommittedLogKind};
  use smol_str::SmolStr;
  use std::net::SocketAddr;

  fn sample_membership() -> CommittedLog<SmolStr, SocketAddr> {
    CommittedLog::new(
      1,
      1,
      CommittedLogKind::Membership(membership::sample_membership()),
    )
  }

  fn large_membership() -> CommittedLog<SmolStr, SocketAddr> {
    CommittedLog::new(
      1,
      1,
      CommittedLogKind::Membership(membership::large_membership()),
    )
  }

  fn single_server() -> CommittedLog<SmolStr, SocketAddr> {
    CommittedLog::new(
      1,
      1,
      CommittedLogKind::Membership(membership::single_server()),
    )
  }

  fn data() -> CommittedLog<SmolStr, SocketAddr> {
    CommittedLog::new(1, 1, CommittedLogKind::Log(vec![1, 2, 3].into()))
  }

  fn large_data() -> CommittedLog<SmolStr, SocketAddr> {
    CommittedLog::new(1, 1, CommittedLogKind::Log(vec![255; 1000].into()))
  }

  #[tokio::test]
  async fn test_membership_transformable_roundtrip() {
    test_transformable_roundtrip!(CommittedLog < SmolStr, SocketAddr > { sample_membership() });
    test_transformable_roundtrip!(CommittedLog < SmolStr, SocketAddr > { single_server() });
    test_transformable_roundtrip!(CommittedLog < SmolStr, SocketAddr > { large_membership() });
    test_transformable_roundtrip!(CommittedLog < SmolStr, SocketAddr > { data() });
    test_transformable_roundtrip!(CommittedLog < SmolStr, SocketAddr > { large_data() });
  }
}
