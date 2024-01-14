use std::{sync::Arc, time::SystemTime};

use byteorder::{ByteOrder, NetworkEndian};
use nodecraft::{Address, Id, Transformable};
use ruraft_utils::{decode_varint, encode_varint, encoded_len_varint};
use smallvec::SmallVec;

use crate::{membership::Membership, Data, MESSAGE_SIZE_LEN};

/// Describes various types of log entries.
#[derive(Debug)]
#[non_exhaustive]
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
pub enum LogKind<I, A, D> {
  /// Holds the log entry's type-specific data, which will be applied to a user [`FinateStateMachine`](crate::FinateStateMachine).
  Data(Arc<D>),

  /// Used to assert leadership.
  Noop,

  /// Used to ensure all preceding operations have been
  /// applied to the [`FinateStateMachine`]. It is similar to LogNoop, but instead of returning
  /// once committed, it only returns once the [`FinateStateMachine`] manager acks it. Otherwise,
  /// it is possible there are operations committed but not yet applied to
  /// the [`FinateStateMachine`].
  Barrier,

  /// Establishes a membership change. It is
  /// created when a server is added, removed, promoted, etc.
  Membership(Membership<I, A>),
}

impl<I: core::hash::Hash + Eq, A: PartialEq, D: PartialEq> PartialEq for LogKind<I, A, D> {
  fn eq(&self, other: &Self) -> bool {
    match (self, other) {
      (Self::Data(data), Self::Data(other_data)) => data == other_data,
      (Self::Noop, Self::Noop) => true,
      (Self::Barrier, Self::Barrier) => true,
      (Self::Membership(membership), Self::Membership(other_membership)) => {
        membership == other_membership
      }
      _ => false,
    }
  }
}

impl<I: core::hash::Hash + Eq, A: Eq, D: Eq> Eq for LogKind<I, A, D> {}

impl<I, A, D> Clone for LogKind<I, A, D> {
  fn clone(&self) -> Self {
    match self {
      Self::Data(data) => Self::Data(data.clone()),
      Self::Noop => Self::Noop,
      Self::Barrier => Self::Barrier,
      Self::Membership(membership) => Self::Membership(membership.clone()),
    }
  }
}

impl<I, A, D> LogKind<I, A, D> {
  fn tag(&self) -> u8 {
    match self {
      Self::Data(_) => 0,
      Self::Noop => 1,
      Self::Barrier => 2,
      Self::Membership(_) => 3,
    }
  }
}

/// Log entries are replicated to all members of the Raft cluster
/// and form the heart of the replicated state machine.
///
/// The `clone` on `Log` is cheap and not require deep copy and allocation.
#[viewit::viewit(vis_all = "pub(crate)", getters(vis_all = "pub"), setters(skip))]
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
pub struct Log<I, A, D> {
  /// Holds the kind of the log entry.
  #[viewit(
    getter(
      vis = "pub",
      const,
      style = "ref",
      attrs(doc = "Returns the log entry's kind.")
    ),
    setter(vis = "pub(crate)", attrs(doc = "Sets the log entry's kind."))
  )]
  #[cfg_attr(feature = "serde", serde(flatten))]
  kind: LogKind<I, A, D>,

  /// Holds the index of the log entry.
  #[viewit(
    getter(vis = "pub", const, attrs(doc = "Returns the log entry's index.")),
    setter(vis = "pub(crate)", attrs(doc = "Sets the log entry's index."))
  )]
  index: u64,

  /// Holds the term of the log entry.
  #[viewit(
    getter(vis = "pub", const, attrs(doc = "Returns the log entry's term.")),
    setter(vis = "pub(crate)", attrs(doc = "Sets the log entry's term."))
  )]
  term: u64,

  /// Stores the time (timestamp in milliseconds) the leader first appended this log to it's
  /// [`LogStorage`]. Followers will observe the leader's time. It is not used for
  /// coordination or as part of the replication protocol at all. It exists only
  /// to provide operational information for example how many seconds worth of
  /// logs are present on the leader which might impact follower's ability to
  /// catch up after restoring a large snapshot. We should never rely on this
  /// being in the past when appending on a follower or reading a log back since
  /// the clock skew can mean a follower could see a log with a future timestamp.
  /// In general too the leader is not required to persist the log before
  /// delivering to followers although the current implementation happens to do
  /// this.
  #[viewit(
    getter(
      vis = "pub",
      const,
      attrs(
        doc = "Returns the time (timestamp in milliseconds) the leader first appended this log to it's
    [`LogStorage`]."
      )
    ),
    setter(
      vis = "pub(crate)",
      attrs(
        doc = "Sets  the time (timestamp in milliseconds) the leader first appended this log to it's
    [`LogStorage`]."
      )
    )
  )]
  #[cfg_attr(
    feature = "serde",
    serde(with = "crate::utils::serde_system_time::option")
  )]
  appended_at: Option<SystemTime>,
}

impl<I: core::hash::Hash + Eq, A: PartialEq, D: PartialEq> PartialEq for Log<I, A, D> {
  fn eq(&self, other: &Self) -> bool {
    self.index == other.index
      && self.term == other.term
      && self.kind == other.kind
      && self.appended_at == other.appended_at
  }
}

impl<I: core::hash::Hash + Eq, A: Eq, D: Eq> Eq for Log<I, A, D> {}

impl<I, A, D> Clone for Log<I, A, D> {
  fn clone(&self) -> Self {
    Self {
      index: self.index,
      term: self.term,
      kind: self.kind.clone(),
      appended_at: self.appended_at,
    }
  }
}

impl<I, A, D> Log<I, A, D> {
  /// Create a [`Log`]
  #[inline]
  pub fn new(data: D) -> Self {
    Self {
      index: 0,
      term: 0,
      kind: LogKind::Data(Arc::new(data)),
      appended_at: None,
    }
  }

  /// Only used for testing.
  #[cfg(feature = "test")]
  pub fn set_index(mut self, index: u64) -> Self {
    self.index = index;
    self
  }

  /// Only used for testing.
  #[cfg(feature = "test")]
  pub fn set_term(mut self, term: u64) -> Self {
    self.term = term;
    self
  }

  #[inline]
  pub(crate) const fn is_membership(&self) -> bool {
    matches!(self.kind, LogKind::Membership(_))
  }

  #[inline]
  pub(crate) fn is_noop(&self) -> bool {
    matches!(self.kind, LogKind::Noop)
  }

  #[inline]
  pub(crate) const fn crate_new(term: u64, index: u64, kind: LogKind<I, A, D>) -> Self {
    Self {
      index,
      term,
      kind,
      appended_at: None,
    }
  }

  /// Only used for testing.
  #[cfg(any(feature = "test", test))]
  #[inline]
  #[doc(hidden)]
  pub const fn __crate_new(term: u64, index: u64, kind: LogKind<I, A, D>) -> Self {
    Self::crate_new(term, index, kind)
  }
}

/// Errors that can occur when transforming a [`Log`] to its bytes representation.
#[derive(thiserror::Error)]
pub enum LogTransformError<I: Transformable, A: Transformable, D: Transformable> {
  /// Id transform error.
  #[error("{0}")]
  Id(I::Error),
  /// Address transform error.
  #[error("{0}")]
  Address(A::Error),
  /// Data transform error.
  #[error("{0}")]
  Data(D::Error),
  /// Membership transform error.
  #[error("{0}")]
  Membership(crate::membership::MembershipTransformError<I, A>),
  /// Encode varint error.
  #[error("{0}")]
  EncodeVarint(#[from] ruraft_utils::EncodeVarintError),
  /// Decode varint error.
  #[error("{0}")]
  DecodeVarint(#[from] ruraft_utils::DecodeVarintError),
  /// Encode buffer too small.
  #[error("dst buffer is too small")]
  EncodeBufferTooSmall,
  /// Unknown log kind.
  #[error("unknown log kind {0}")]
  UnknownLogKind(u8),
  /// Corrupted log bytes data, which cannot be decoded back anymore because of losing information.
  #[error("{0}")]
  Corrupted(&'static str),
}

impl<I, A, D> core::fmt::Debug for LogTransformError<I, A, D>
where
  I: Id,
  A: Address,
  D: Transformable,
{
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::Id(arg0) => f.debug_tuple("Id").field(arg0).finish(),
      Self::Address(arg0) => f.debug_tuple("Address").field(arg0).finish(),
      Self::Data(arg0) => f.debug_tuple("Data").field(arg0).finish(),
      Self::Membership(arg0) => f.debug_tuple("Membership").field(arg0).finish(),
      Self::EncodeBufferTooSmall => f.debug_tuple("EncodeBufferTooSmall").finish(),
      Self::Corrupted(arg0) => f.debug_tuple("Corrupted").field(arg0).finish(),
      Self::UnknownLogKind(arg0) => f.debug_tuple("UnknownLogKind").field(arg0).finish(),
      Self::EncodeVarint(arg0) => f.debug_tuple("EncodeVarint").field(arg0).finish(),
      Self::DecodeVarint(arg0) => f.debug_tuple("DecodeVarint").field(arg0).finish(),
    }
  }
}

// Log Binary Format
//
// --------------------------------------------------------------------------------------------------------
// | total_len (4 bytes) | kind (1 byte) | index (8 bytes) | term (8 bytes) | append_at (8 bytes) | data |
// --------------------------------------------------------------------------------------------------------
impl<I, A, D> Transformable for Log<I, A, D>
where
  I: Id,
  A: Address,
  D: Data,
{
  type Error = LogTransformError<I, A, D>;

  fn encode(&self, dst: &mut [u8]) -> Result<usize, Self::Error> {
    let encoded_len = self.encoded_len();

    if dst.len() < encoded_len {
      return Err(LogTransformError::EncodeBufferTooSmall);
    }

    let mut cur = 0;
    NetworkEndian::write_u32(&mut dst[..MESSAGE_SIZE_LEN], encoded_len as u32);
    cur += MESSAGE_SIZE_LEN;
    dst[cur] = self.kind.tag();
    cur += 1;

    cur += encode_varint(self.index, &mut dst[cur..])?;
    cur += encode_varint(self.term, &mut dst[cur..])?;

    const ENCODED_SYSTEMTIME_LEN: usize = 12;

    match self.appended_at {
      None => {
        dst[cur..cur + ENCODED_SYSTEMTIME_LEN].copy_from_slice(&[0; ENCODED_SYSTEMTIME_LEN]);
        cur += ENCODED_SYSTEMTIME_LEN;
      }
      Some(instant) => {
        instant
          .encode(&mut dst[cur..cur + ENCODED_SYSTEMTIME_LEN])
          .unwrap();
        cur += ENCODED_SYSTEMTIME_LEN;
      }
    }

    match &self.kind {
      LogKind::Data(d) => {
        cur += d.encode(&mut dst[cur..]).map_err(LogTransformError::Data)?;
      }
      LogKind::Membership(m) => {
        cur += m
          .encode(&mut dst[cur..])
          .map_err(LogTransformError::Membership)?;
      }
      LogKind::Noop | LogKind::Barrier => {}
    }
    debug_assert_eq!(
      cur, encoded_len,
      "expected bytes wrote ({}) not match actual bytes wrote ({})",
      encoded_len, cur
    );
    Ok(cur)
  }

  fn encoded_len(&self) -> usize {
    MESSAGE_SIZE_LEN
      + 1
      + encoded_len_varint(self.index)
      + encoded_len_varint(self.term)
      + 12
      + match &self.kind {
        LogKind::Data(d) => d.encoded_len(),
        LogKind::Noop => 0,
        LogKind::Barrier => 0,
        LogKind::Membership(m) => m.encoded_len(),
      }
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    let src_len = src.len();
    if src_len < MESSAGE_SIZE_LEN {
      return Err(LogTransformError::Corrupted("corrupted log"));
    }

    let mut cur = 0;
    let encoded_len = NetworkEndian::read_u32(&src[..MESSAGE_SIZE_LEN]) as usize;
    cur += MESSAGE_SIZE_LEN;
    if src_len < encoded_len {
      return Err(LogTransformError::Corrupted("corrupted log"));
    }

    let tag = src[cur];
    cur += 1;

    let (readed, index) = decode_varint(&src[cur..])?;
    cur += readed;
    let (readed, term) = decode_varint(&src[cur..])?;
    cur += readed;

    let appended_at = &src[cur..cur + 12];
    let (readed, appended_at) = if appended_at == [0; 12] {
      (12, None)
    } else {
      let (readed, appended_at) = SystemTime::decode(appended_at).unwrap();
      (readed, Some(appended_at))
    };
    cur += readed;

    let log = match tag {
      0 => {
        let (readed, data) = D::decode(&src[cur..]).map_err(LogTransformError::Data)?;
        cur += readed;
        Log {
          kind: LogKind::Data(Arc::new(data)),
          index,
          term,
          appended_at,
        }
      }
      1 => Log {
        kind: LogKind::Noop,
        index,
        term,
        appended_at,
      },
      2 => Log {
        kind: LogKind::Barrier,
        index,
        term,
        appended_at,
      },
      3 => {
        let (readed, membership) =
          Membership::decode(&src[cur..]).map_err(LogTransformError::Membership)?;
        cur += readed;
        Log {
          kind: LogKind::Membership(membership),
          index,
          term,
          appended_at,
        }
      }
      _ => return Err(LogTransformError::UnknownLogKind(tag)),
    };
    debug_assert_eq!(
      cur, encoded_len,
      "expected bytes read ({}) not match actual bytes read ({})",
      encoded_len, cur
    );
    Ok((cur, log))
  }
}

/// A batch of logs.
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
pub struct LogBatch<I, A, D>(SmallVec<[Log<I, A, D>; 2]>);

impl<I, A, D> core::ops::Deref for LogBatch<I, A, D> {
  type Target = SmallVec<[Log<I, A, D>; 2]>;

  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

impl<I, A, D> core::ops::DerefMut for LogBatch<I, A, D> {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.0
  }
}

impl<I, A, D> Clone for LogBatch<I, A, D> {
  fn clone(&self) -> Self {
    Self(self.0.clone())
  }
}

impl<I, A, D> PartialEq for LogBatch<I, A, D>
where
  I: Eq + core::hash::Hash,
  A: PartialEq,
  D: PartialEq,
{
  fn eq(&self, other: &Self) -> bool {
    self.0 == other.0
  }
}

impl<I, A, D> Eq for LogBatch<I, A, D>
where
  I: Eq + core::hash::Hash,
  A: Eq,
  D: Eq,
{
}

impl<I, A, D> Default for LogBatch<I, A, D> {
  fn default() -> Self {
    Self::new()
  }
}

impl<I, A, D> From<Vec<Log<I, A, D>>> for LogBatch<I, A, D> {
  fn from(logs: Vec<Log<I, A, D>>) -> Self {
    Self(logs.into())
  }
}

impl<I, A, D> From<SmallVec<[Log<I, A, D>; 2]>> for LogBatch<I, A, D> {
  fn from(logs: SmallVec<[Log<I, A, D>; 2]>) -> Self {
    Self(logs)
  }
}

impl<I, A, D> FromIterator<Log<I, A, D>> for LogBatch<I, A, D> {
  fn from_iter<T: IntoIterator<Item = Log<I, A, D>>>(iter: T) -> Self {
    Self(iter.into_iter().collect())
  }
}

impl<I, A, D> IntoIterator for LogBatch<I, A, D> {
  type Item = Log<I, A, D>;
  type IntoIter = smallvec::IntoIter<[Log<I, A, D>; 2]>;

  fn into_iter(self) -> Self::IntoIter {
    self.0.into_iter()
  }
}

impl<I, A, D> LogBatch<I, A, D> {
  /// Create a new [`LogBatch`].
  pub fn new() -> Self {
    Self(SmallVec::new())
  }

  /// Construct an empty vector with enough capacity pre-allocated to store at least `n`
  /// elements.
  ///
  /// Will create a heap allocation only if `n` is larger than the inline capacity.
  ///
  /// ```
  /// # use ruraft_core::storage::LogBatch;
  ///
  /// let v: LogBatch<u64, std::net::SocketAddr, Vec<u8>> = LogBatch::with_capacity(100);
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
  use std::net::SocketAddr;

  use crate::membership::sample_membership;

  use super::*;
  use nodecraft::{NodeAddress, NodeId};
  use smol_str::SmolStr;

  async fn test_log_transformable_in<I, A, D>(log: Log<I, A, D>)
  where
    I: Id,
    A: Address,
    D: Data + PartialEq + core::fmt::Debug,
  {
    let mut buf = vec![0; log.encoded_len()];
    log.encode(&mut buf).unwrap();

    let (_, decoded) = Log::<I, A, D>::decode(&buf).unwrap();
    assert_eq!(log, decoded);

    let mut buf = Vec::with_capacity(log.encoded_len());
    log.encode_to_writer(&mut buf).unwrap();

    let (_, decoded) = Log::<I, A, D>::decode_from_reader(&mut std::io::Cursor::new(&buf)).unwrap();
    assert_eq!(log, decoded);

    let mut buf = Vec::with_capacity(log.encoded_len());
    log.encode_to_async_writer(&mut buf).await.unwrap();

    let (_, decoded) = Log::<I, A, D>::decode_from_async_reader(&mut buf.as_slice())
      .await
      .unwrap();
    assert_eq!(log, decoded);
  }

  #[tokio::test]
  async fn test_log_transformable() {
    let log: Log<NodeId, NodeAddress, Vec<u8>> = Log {
      kind: LogKind::Data(Arc::new(vec![1, 2, 3])),
      index: 1,
      term: 1,
      appended_at: None,
    };

    test_log_transformable_in(log).await;

    let log: Log<NodeId, NodeAddress, Vec<u8>> = Log {
      kind: LogKind::Data(Arc::new((0..=u8::MAX).collect())),
      index: 1,
      term: 1,
      appended_at: Some(SystemTime::now()),
    };

    test_log_transformable_in(log).await;

    let log: Log<NodeId, NodeAddress, Vec<u8>> = Log {
      kind: LogKind::Noop,
      index: 1,
      term: 1,
      appended_at: None,
    };

    test_log_transformable_in(log).await;

    let log: Log<NodeId, NodeAddress, Vec<u8>> = Log {
      kind: LogKind::Noop,
      index: 1,
      term: 1,
      appended_at: Some(SystemTime::now()),
    };

    test_log_transformable_in(log).await;

    let log: Log<NodeId, NodeAddress, Vec<u8>> = Log {
      kind: LogKind::Barrier,
      index: 1,
      term: 1,
      appended_at: None,
    };

    test_log_transformable_in(log).await;

    let log: Log<NodeId, NodeAddress, Vec<u8>> = Log {
      kind: LogKind::Barrier,
      index: 1,
      term: 1,
      appended_at: Some(SystemTime::now()),
    };

    test_log_transformable_in(log).await;

    let log: Log<SmolStr, SocketAddr, Membership<SmolStr, SocketAddr>> = Log {
      kind: LogKind::Membership(sample_membership()),
      index: 1,
      term: 1,
      appended_at: None,
    };
    test_log_transformable_in(log).await;

    let log: Log<SmolStr, SocketAddr, Membership<SmolStr, SocketAddr>> = Log {
      kind: LogKind::Membership(sample_membership()),
      index: 1,
      term: 1,
      appended_at: Some(SystemTime::now()),
    };

    test_log_transformable_in(log).await;
  }
}
