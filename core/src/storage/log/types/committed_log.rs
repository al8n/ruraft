use std::{
  fmt::{Debug, Display},
  sync::Arc,
};

use crate::{
  membership::{Membership, MembershipTransformError},
  Data, MESSAGE_SIZE_LEN,
};
use byteorder::{ByteOrder, NetworkEndian};
use nodecraft::{Address, Id, Transformable};
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
      serialize = "I: Eq + core::hash::Hash + serde::Serialize, A: serde::Serialize, D: serde::Serialize",
      deserialize = "I: Eq + core::hash::Hash + core::fmt::Display + for<'a> serde::Deserialize<'a>, A: Eq + core::fmt::Display + for<'a> serde::Deserialize<'a>, D: for<'a> serde::Deserialize<'a>",
    )
  )
)]
pub enum CommittedLogKind<I, A, D> {
  /// A normal log entry.
  Log(Arc<D>),
  /// A membership change log entry.
  Membership(Membership<I, A>),
}

impl<I, A, D> Clone for CommittedLogKind<I, A, D> {
  fn clone(&self) -> Self {
    match self {
      Self::Log(data) => Self::Log(data.clone()),
      Self::Membership(membership) => Self::Membership(membership.clone()),
    }
  }
}

impl<I: Eq + core::hash::Hash, A: PartialEq, D: PartialEq> PartialEq for CommittedLogKind<I, A, D> {
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

impl<I: Eq + core::hash::Hash, A: Eq, D: Eq> Eq for CommittedLogKind<I, A, D> {}

/// A log entry that can be applied to the finate state machine.
#[viewit::viewit(setters(prefix = "with"))]
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
  feature = "serde",
  serde(
    rename_all = "snake_case",
    bound(
      serialize = "I: Eq + core::hash::Hash + serde::Serialize, A: serde::Serialize, D: serde::Serialize",
      deserialize = "I: Eq + core::hash::Hash + core::fmt::Display + for<'a> serde::Deserialize<'a>, A: Eq + core::fmt::Display + for<'a> serde::Deserialize<'a>, D: for<'a> serde::Deserialize<'a>",
    )
  )
)]
pub struct CommittedLog<I, A, D> {
  /// The index of the log entry.
  #[viewit(
    getter(const, style="move", attrs(doc = "Returns the index of the log entry."),),
    setter(attrs(doc = "Sets the index of the log entry."),)
  )]
  index: u64,
  /// The term of the log entry.
  #[viewit(
    getter(const, style="move", attrs(doc = "Returns the term of the log entry."),),
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
  kind: CommittedLogKind<I, A, D>,
}

impl<I, A, D> CommittedLog<I, A, D> {
  /// Creates a new log entry.
  pub fn new(term: u64, index: u64, kind: CommittedLogKind<I, A, D>) -> Self {
    Self { index, term, kind }
  }
}

impl<I, A, D> Clone for CommittedLog<I, A, D> {
  fn clone(&self) -> Self {
    Self {
      index: self.index,
      term: self.term,
      kind: self.kind.clone(),
    }
  }
}

impl<I: Eq + core::hash::Hash, A: PartialEq, D: PartialEq> PartialEq for CommittedLog<I, A, D> {
  fn eq(&self, other: &Self) -> bool {
    self.index == other.index && self.term == other.term && self.kind == other.kind
  }
}

impl<I: Eq + core::hash::Hash, A: Eq, D: Eq> Eq for CommittedLog<I, A, D> {}

impl<I: Eq + core::hash::Hash, A: Eq, D: Eq> PartialOrd for CommittedLog<I, A, D> {
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl<I: Eq + core::hash::Hash, A: Eq, D: Eq> Ord for CommittedLog<I, A, D> {
  fn cmp(&self, other: &Self) -> std::cmp::Ordering {
    self
      .term
      .cmp(&other.term)
      .then(self.index.cmp(&other.index))
  }
}

/// The transformation error of [`CommittedLog`].
pub enum CommittedLogTransformError<I: Transformable, A: Transformable, D: Transformable> {
  /// The membership transformation error.
  Membership(MembershipTransformError<I, A>),
  /// The data transformation error.
  Data(D::Error),
  /// The encode buffer is too small.
  EncodeBufferTooSmall,
  /// The log is corrupted.
  Corrupted,
  /// The encode varint error.
  EncodeVarint(EncodeVarintError),
  /// The decode varint error.
  DecodeVarint(DecodeVarintError),
}

impl<I: Transformable + Debug, A: Transformable + Debug, D: Transformable> std::fmt::Debug
  for CommittedLogTransformError<I, A, D>
{
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::Membership(err) => write!(f, "membership: {:?}", err),
      Self::Data(err) => write!(f, "data: {:?}", err),
      Self::EncodeBufferTooSmall => write!(f, "encode buffer too small"),
      Self::EncodeVarint(err) => write!(f, "encode varint: {:?}", err),
      Self::DecodeVarint(err) => write!(f, "decode varint: {:?}", err),
      Self::Corrupted => write!(f, "corrupted"),
    }
  }
}

impl<I: Transformable + Display, A: Transformable + Display, D: Transformable> std::fmt::Display
  for CommittedLogTransformError<I, A, D>
{
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::Membership(err) => write!(f, "membership: {}", err),
      Self::Data(err) => write!(f, "data: {}", err),
      Self::EncodeBufferTooSmall => write!(f, "encode buffer too small"),
      Self::EncodeVarint(err) => write!(f, "encode varint: {}", err),
      Self::DecodeVarint(err) => write!(f, "decode varint: {}", err),
      Self::Corrupted => write!(f, "corrupted"),
    }
  }
}

impl<I: Transformable + Debug + Display, A: Transformable + Debug + Display, D: Transformable>
  std::error::Error for CommittedLogTransformError<I, A, D>
{
}

impl<I: Id, A: Address, D: Data> Transformable for CommittedLog<I, A, D> {
  type Error = CommittedLogTransformError<I, A, D>;

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
      return Err(Self::Error::Corrupted);
    }
    let len = NetworkEndian::read_u32(&src[..MESSAGE_SIZE_LEN]) as usize;
    cur += MESSAGE_SIZE_LEN;
    if src_len < len {
      return Err(Self::Error::Corrupted);
    }

    let (readed, index) = decode_varint(&src[cur..]).map_err(Self::Error::DecodeVarint)?;
    cur += readed;
    let (readed, term) = decode_varint(&src[cur..]).map_err(Self::Error::DecodeVarint)?;
    cur += readed;

    let kind = match src[cur] {
      0 => {
        cur += 1;
        let (readed, data) = D::decode(&src[cur..]).map_err(Self::Error::Data)?;
        cur += readed;
        CommittedLogKind::Log(Arc::new(data))
      }
      1 => {
        cur += 1;
        let (readed, membership) =
          Membership::decode(&src[cur..]).map_err(Self::Error::Membership)?;
        cur += readed;
        CommittedLogKind::Membership(membership)
      }
      _ => return Err(Self::Error::Corrupted),
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
      serialize = "I: Eq + core::hash::Hash + serde::Serialize, A: serde::Serialize, D: serde::Serialize",
      deserialize = "I: Eq + core::hash::Hash + core::fmt::Display + for<'a> serde::Deserialize<'a>, A: Eq + core::fmt::Display + for<'a> serde::Deserialize<'a>, D: for<'a> serde::Deserialize<'a>",
    )
  )
)]
pub struct CommittedLogBatch<I, A, D>(SmallVec<[CommittedLog<I, A, D>; 4]>);

impl<I, A, D> core::ops::Deref for CommittedLogBatch<I, A, D> {
  type Target = SmallVec<[CommittedLog<I, A, D>; 4]>;

  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

impl<I, A, D> core::ops::DerefMut for CommittedLogBatch<I, A, D> {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.0
  }
}

impl<I, A, D> Clone for CommittedLogBatch<I, A, D> {
  fn clone(&self) -> Self {
    Self(self.0.clone())
  }
}

impl<I, A, D> PartialEq for CommittedLogBatch<I, A, D>
where
  I: Eq + core::hash::Hash,
  A: PartialEq,
  D: PartialEq,
{
  fn eq(&self, other: &Self) -> bool {
    self.0 == other.0
  }
}

impl<I, A, D> Eq for CommittedLogBatch<I, A, D>
where
  I: Eq + core::hash::Hash,
  A: Eq,
  D: Eq,
{
}

impl<I, A, D> Default for CommittedLogBatch<I, A, D> {
  fn default() -> Self {
    Self::new()
  }
}

impl<I, A, D> From<Vec<CommittedLog<I, A, D>>> for CommittedLogBatch<I, A, D> {
  fn from(logs: Vec<CommittedLog<I, A, D>>) -> Self {
    Self(logs.into())
  }
}

impl<I, A, D> From<SmallVec<[CommittedLog<I, A, D>; 4]>> for CommittedLogBatch<I, A, D> {
  fn from(logs: SmallVec<[CommittedLog<I, A, D>; 4]>) -> Self {
    Self(logs)
  }
}

impl<I, A, D> FromIterator<CommittedLog<I, A, D>> for CommittedLogBatch<I, A, D> {
  fn from_iter<T: IntoIterator<Item = CommittedLog<I, A, D>>>(iter: T) -> Self {
    Self(iter.into_iter().collect())
  }
}

impl<I, A, D> IntoIterator for CommittedLogBatch<I, A, D> {
  type Item = CommittedLog<I, A, D>;
  type IntoIter = smallvec::IntoIter<[CommittedLog<I, A, D>; 4]>;

  fn into_iter(self) -> Self::IntoIter {
    self.0.into_iter()
  }
}

impl<I, A, D> CommittedLogBatch<I, A, D> {
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
  use std::{net::SocketAddr, sync::Arc};

  fn sample_membership() -> CommittedLog<SmolStr, SocketAddr, Vec<u8>> {
    CommittedLog::new(
      1,
      1,
      CommittedLogKind::Membership(membership::sample_membership()),
    )
  }

  fn large_membership() -> CommittedLog<SmolStr, SocketAddr, Vec<u8>> {
    CommittedLog::new(
      1,
      1,
      CommittedLogKind::Membership(membership::large_membership()),
    )
  }

  fn single_server() -> CommittedLog<SmolStr, SocketAddr, Vec<u8>> {
    CommittedLog::new(
      1,
      1,
      CommittedLogKind::Membership(membership::single_server()),
    )
  }

  fn data() -> CommittedLog<SmolStr, SocketAddr, Vec<u8>> {
    CommittedLog::new(1, 1, CommittedLogKind::Log(Arc::new(vec![1, 2, 3])))
  }

  fn large_data() -> CommittedLog<SmolStr, SocketAddr, Vec<u8>> {
    CommittedLog::new(1, 1, CommittedLogKind::Log(Arc::new(vec![255; 1000])))
  }

  #[tokio::test]
  async fn test_membership_transformable_roundtrip() {
    test_transformable_roundtrip!(CommittedLog < SmolStr, SocketAddr, Vec<u8> > { sample_membership() });
    test_transformable_roundtrip!(CommittedLog < SmolStr, SocketAddr, Vec<u8> > { single_server() });
    test_transformable_roundtrip!(CommittedLog < SmolStr, SocketAddr, Vec<u8> > { large_membership() });
    test_transformable_roundtrip!(CommittedLog < SmolStr, SocketAddr, Vec<u8> > { data() });
    test_transformable_roundtrip!(CommittedLog < SmolStr, SocketAddr, Vec<u8> > { large_data() });
  }
}
