use std::fmt::{Debug, Display};

use byteorder::{ByteOrder, NetworkEndian};
use nodecraft::Transformable;
use ruraft_utils::{
  decode_varint, encode_varint, encoded_len_varint, DecodeVarintError, EncodeVarintError,
};

use crate::{membership::MembershipTransformError, MESSAGE_SIZE_LEN};

use super::*;

/// Logs can be handled by the finate state machine.
#[derive(Debug)]
pub enum FinateStateMachineLogKind<I, A, D> {
  /// A normal log entry.
  Log(Arc<D>),
  /// A membership change log entry.
  Membership(Membership<I, A>),
}

impl<I, A, D> Clone for FinateStateMachineLogKind<I, A, D> {
  fn clone(&self) -> Self {
    match self {
      Self::Log(data) => Self::Log(data.clone()),
      Self::Membership(membership) => Self::Membership(membership.clone()),
    }
  }
}

impl<I: Eq + core::hash::Hash, A: PartialEq, D: PartialEq> PartialEq
  for FinateStateMachineLogKind<I, A, D>
{
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

impl<I: Eq + core::hash::Hash, A: Eq, D: Eq> Eq for FinateStateMachineLogKind<I, A, D> {}

/// A log entry that can be applied to the finate state machine.
#[viewit::viewit(setters(prefix = "with"))]
#[derive(Debug)]
pub struct FinateStateMachineLog<I, A, D> {
  /// The index of the log entry.
  #[viewit(
    getter(const, attrs(doc = "Returns the index of the log entry."),),
    setter(attrs(doc = "Sets the index of the log entry."),)
  )]
  index: u64,
  /// The term of the log entry.
  #[viewit(
    getter(const, attrs(doc = "Returns the term of the log entry."),),
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
  kind: FinateStateMachineLogKind<I, A, D>,
}

impl<I, A, D> FinateStateMachineLog<I, A, D> {
  /// Creates a new log entry.
  pub fn new(term: u64, index: u64, kind: FinateStateMachineLogKind<I, A, D>) -> Self {
    Self { index, term, kind }
  }
}

impl<I, A, D> Clone for FinateStateMachineLog<I, A, D> {
  fn clone(&self) -> Self {
    Self {
      index: self.index,
      term: self.term,
      kind: self.kind.clone(),
    }
  }
}

impl<I: Eq + core::hash::Hash, A: PartialEq, D: PartialEq> PartialEq
  for FinateStateMachineLog<I, A, D>
{
  fn eq(&self, other: &Self) -> bool {
    self.index == other.index && self.term == other.term && self.kind == other.kind
  }
}

impl<I: Eq + core::hash::Hash, A: Eq, D: Eq> Eq for FinateStateMachineLog<I, A, D> {}

/// The transformation error of [`FinateStateMachineLog`].
pub enum FinateStateMachineLogTransformError<I: Transformable, A: Transformable, D: Transformable> {
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
  for FinateStateMachineLogTransformError<I, A, D>
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
  for FinateStateMachineLogTransformError<I, A, D>
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
  std::error::Error for FinateStateMachineLogTransformError<I, A, D>
{
}

impl<I: Id, A: Address, D: Data> Transformable for FinateStateMachineLog<I, A, D> {
  type Error = FinateStateMachineLogTransformError<I, A, D>;

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
      FinateStateMachineLogKind::Log(data) => {
        dst[offset] = 0;
        offset += 1;
        offset += data.encode(&mut dst[offset..]).map_err(Self::Error::Data)?;
      }
      FinateStateMachineLogKind::Membership(membership) => {
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
        FinateStateMachineLogKind::Log(data) => data.encoded_len(),
        FinateStateMachineLogKind::Membership(membership) => membership.encoded_len(),
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
        FinateStateMachineLogKind::Log(Arc::new(data))
      }
      1 => {
        cur += 1;
        let (readed, membership) =
          Membership::decode(&src[cur..]).map_err(Self::Error::Membership)?;
        cur += readed;
        FinateStateMachineLogKind::Membership(membership)
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

#[cfg(test)]
mod tests {
  use crate::membership;

  use super::{FinateStateMachineLog, FinateStateMachineLogKind};
  use smol_str::SmolStr;
  use std::{net::SocketAddr, sync::Arc};

  fn sample_membership() -> FinateStateMachineLog<SmolStr, SocketAddr, Vec<u8>> {
    FinateStateMachineLog::new(
      1,
      1,
      FinateStateMachineLogKind::Membership(membership::sample_membership()),
    )
  }

  fn large_membership() -> FinateStateMachineLog<SmolStr, SocketAddr, Vec<u8>> {
    FinateStateMachineLog::new(
      1,
      1,
      FinateStateMachineLogKind::Membership(membership::large_membership()),
    )
  }

  fn single_server() -> FinateStateMachineLog<SmolStr, SocketAddr, Vec<u8>> {
    FinateStateMachineLog::new(
      1,
      1,
      FinateStateMachineLogKind::Membership(membership::single_server()),
    )
  }

  fn data() -> FinateStateMachineLog<SmolStr, SocketAddr, Vec<u8>> {
    FinateStateMachineLog::new(
      1,
      1,
      FinateStateMachineLogKind::Log(Arc::new(vec![1, 2, 3])),
    )
  }

  fn large_data() -> FinateStateMachineLog<SmolStr, SocketAddr, Vec<u8>> {
    FinateStateMachineLog::new(
      1,
      1,
      FinateStateMachineLogKind::Log(Arc::new(vec![255; 1000])),
    )
  }

  #[tokio::test]
  async fn test_membership_transformable_roundtrip() {
    test_transformable_roundtrip!(FinateStateMachineLog < SmolStr, SocketAddr, Vec<u8> > { sample_membership() });
    test_transformable_roundtrip!(FinateStateMachineLog < SmolStr, SocketAddr, Vec<u8> > { single_server() });
    test_transformable_roundtrip!(FinateStateMachineLog < SmolStr, SocketAddr, Vec<u8> > { large_membership() });
    test_transformable_roundtrip!(FinateStateMachineLog < SmolStr, SocketAddr, Vec<u8> > { data() });
    test_transformable_roundtrip!(FinateStateMachineLog < SmolStr, SocketAddr, Vec<u8> > { large_data() });
  }
}
