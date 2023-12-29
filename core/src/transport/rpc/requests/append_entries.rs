use ruraft_utils::{decode_varint, encode_varint, encoded_len_varint};

use crate::{Data, MESSAGE_SIZE_LEN};

use super::*;

/// The command used to append entries to the
/// replicated log.
#[viewit::viewit]
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AppendEntriesRequest<I, A, D> {
  /// The header of the request
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the header of the request"),),
    setter(attrs(doc = "Set the header of the request"),)
  )]
  #[cfg_attr(
    feature = "serde",
    serde(
      bound = "I: Eq + ::core::hash::Hash + ::core::fmt::Display + ::serde::Serialize + for<'a> ::serde::Deserialize<'a>, A: Eq + ::core::fmt::Display + ::serde::Serialize + for<'a> ::serde::Deserialize<'a>, D: ::serde::Serialize + for<'a> ::serde::Deserialize<'a>"
    )
  )]
  header: Header<I, A>,

  /// Provide the current term and leader
  #[viewit(
    getter(const, attrs(doc = "Get the term the append request"),),
    setter(attrs(doc = "Set the term the append request"),)
  )]
  term: u64,

  /// Provide the previous entries for integrity checking
  #[viewit(
    getter(
      const,
      attrs(doc = "Get the previous log entry index of the append request"),
    ),
    setter(attrs(doc = "Set the previous log entry index of the append request"),)
  )]
  prev_log_entry: u64,
  /// Provide the previous term for integrity checking
  #[viewit(
    getter(
      const,
      attrs(doc = "Get the previous log entry term of the append request"),
    ),
    setter(attrs(doc = "Set the previous log entry term of the append request"),)
  )]
  prev_log_term: u64,

  /// New entries to commit
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Get the log entries of the append request"),
    ),
    setter(attrs(doc = "Set the log entries of the append request"),)
  )]
  #[cfg_attr(
    feature = "serde",
    serde(
      bound = "I: Eq + ::core::hash::Hash + ::core::fmt::Display + ::serde::Serialize + for<'a> ::serde::Deserialize<'a>, A: Eq + ::core::fmt::Display + ::serde::Serialize + for<'a> ::serde::Deserialize<'a>, D: ::serde::Serialize + for<'a> ::serde::Deserialize<'a>"
    )
  )]
  entries: Vec<Log<I, A, D>>,

  /// Commit index on the leader
  #[viewit(
    getter(
      const,
      attrs(doc = "Get the commit index of the leader of the append request"),
    ),
    setter(attrs(doc = "Set the commit index of the leader of the append request"),)
  )]
  leader_commit: u64,
}

impl<I: Clone, A: Clone, D> Clone for AppendEntriesRequest<I, A, D> {
  fn clone(&self) -> Self {
    Self {
      header: self.header.clone(),
      term: self.term,
      prev_log_entry: self.prev_log_entry,
      prev_log_term: self.prev_log_term,
      entries: self.entries.clone(),
      leader_commit: self.leader_commit,
    }
  }
}

impl<I: core::hash::Hash + Eq, A: PartialEq, D: PartialEq> PartialEq
  for AppendEntriesRequest<I, A, D>
{
  fn eq(&self, other: &Self) -> bool {
    self.header == other.header
      && self.term == other.term
      && self.prev_log_entry == other.prev_log_entry
      && self.prev_log_term == other.prev_log_term
      && self.entries == other.entries
      && self.leader_commit == other.leader_commit
  }
}

impl<I: core::hash::Hash + Eq, A: Eq, D: Eq> Eq for AppendEntriesRequest<I, A, D> {}

impl<I, A, D> AppendEntriesRequest<I, A, D> {
  /// Create a new [`AppendEntriesRequest`] with the given `id` and `addr` and `version`. Other fields
  /// are set to their default values.
  #[inline]
  pub const fn new(version: ProtocolVersion, id: I, addr: A) -> Self {
    Self {
      header: Header {
        protocol_version: version,
        from: Node::new(id, addr),
      },
      term: 0,
      prev_log_entry: 0,
      prev_log_term: 0,
      entries: Vec::new(),
      leader_commit: 0,
    }
  }

  /// Create a new [`AppendEntriesRequest`] with the given protocol version, node and default values.
  #[inline]
  pub const fn from_node(version: ProtocolVersion, node: Node<I, A>) -> Self {
    Self {
      header: Header {
        protocol_version: version,
        from: node,
      },
      term: 0,
      prev_log_entry: 0,
      prev_log_term: 0,
      entries: Vec::new(),
      leader_commit: 0,
    }
  }

  /// Create a new [`AppendEntriesRequest`] with the given header and default values.
  #[inline]
  pub const fn from_header(header: Header<I, A>) -> Self {
    Self {
      header,
      term: 0,
      prev_log_entry: 0,
      prev_log_term: 0,
      entries: Vec::new(),
      leader_commit: 0,
    }
  }

  /// Returns a mutable reference of the log entries.
  #[inline]
  pub fn entries_mut(&mut self) -> &mut Vec<Log<I, A, D>> {
    &mut self.entries
  }
}

// Encode
//
// --------------------------------------------------------------------------------------------------------------------------------------------------------------------
// | len (4 bytes) | header (variable) | term (uvarint) | prev_log_entry (uvarint) | prev_log_term (uvarint) | leader_commit (uvarint) | num_logs | log1 | log2 | ... |
// --------------------------------------------------------------------------------------------------------------------------------------------------------------------
impl<I, A, D> Transformable for AppendEntriesRequest<I, A, D>
where
  I: Id,
  A: Address,
  D: Data,
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

    offset += encode_varint(self.term, &mut dst[offset..])?;
    offset += encode_varint(self.prev_log_entry, &mut dst[offset..])?;
    offset += encode_varint(self.prev_log_term, &mut dst[offset..])?;
    offset += encode_varint(self.leader_commit, &mut dst[offset..])?;
    let num_entries = self.entries.len() as u32;
    dst[offset..offset + core::mem::size_of::<u32>()].copy_from_slice(&num_entries.to_be_bytes());
    offset += core::mem::size_of::<u32>();

    for entry in &self.entries {
      offset += entry
        .encode(&mut dst[offset..])
        .map_err(Self::Error::encode)?;
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
      + self.header.encoded_len()
      + encoded_len_varint(self.term)
      + encoded_len_varint(self.prev_log_entry)
      + encoded_len_varint(self.prev_log_term)
      + encoded_len_varint(self.leader_commit)
      + core::mem::size_of::<u32>()
      + { self.entries.iter().map(|e| e.encoded_len()).sum::<usize>() }
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

    let (header_size, header) = <Header<I, A> as Transformable>::decode(&src[offset..])?;
    offset += header_size;

    let (readed, term) = decode_varint(&src[offset..])?;
    offset += readed;

    let (readed, prev_log_entry) = decode_varint(&src[offset..])?;
    offset += readed;

    let (readed, prev_log_term) = decode_varint(&src[offset..])?;
    offset += readed;

    let (readed, leader_commit) = decode_varint(&src[offset..])?;
    offset += readed;

    let num_entrires = {
      let mut buf = [0u8; core::mem::size_of::<u32>()];
      buf.copy_from_slice(&src[offset..offset + core::mem::size_of::<u32>()]);
      offset += core::mem::size_of::<u32>();
      u32::from_be_bytes(buf) as usize
    };

    let mut entries = Vec::with_capacity(num_entrires);

    for _ in 0..num_entrires {
      let (readed, entry) =
        <Log<I, A, D> as Transformable>::decode(&src[offset..]).map_err(Self::Error::decode)?;
      offset += readed;
      entries.push(entry);
    }

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
        prev_log_entry,
        prev_log_term,
        entries,
        leader_commit,
      },
    ))
  }
}

#[cfg(any(feature = "test", test))]
impl AppendEntriesRequest<smol_str::SmolStr, std::net::SocketAddr, Vec<u8>> {
  #[doc(hidden)]
  pub fn __large() -> Self {
    use crate::storage::LogKind;
    use std::sync::Arc;

    Self {
      header: Header::__large(),
      term: 1,
      prev_log_entry: 2,
      prev_log_term: 2,
      entries: vec![
        Log::__crate_new(3, 2, LogKind::Noop),
        Log::__crate_new(4, 2, LogKind::Barrier),
        Log::__crate_new(5, 2, LogKind::Data(Arc::new(vec![1, 2, 3]))),
        Log::__crate_new(6, 2, LogKind::Noop),
        Log::__crate_new(7, 2, LogKind::Barrier),
        Log::__crate_new(8, 2, LogKind::Data(Arc::new(vec![1, 2, 3]))),
        Log::__crate_new(9, 2, LogKind::Noop),
        Log::__crate_new(10, 2, LogKind::Barrier),
        {
          let mut l = Log::__crate_new(11, 2, LogKind::Data(Arc::new(vec![1, 2, 3])));
          l.appended_at = Some(std::time::SystemTime::now());
          l
        },
        Log::__crate_new(3, 2, LogKind::Noop),
        Log::__crate_new(4, 2, LogKind::Barrier),
        Log::__crate_new(5, 2, LogKind::Data(Arc::new(vec![1, 2, 3]))),
        Log::__crate_new(6, 2, LogKind::Noop),
        Log::__crate_new(7, 2, LogKind::Barrier),
        Log::__crate_new(8, 2, LogKind::Data(Arc::new(vec![1, 2, 3]))),
        Log::__crate_new(9, 2, LogKind::Noop),
        Log::__crate_new(10, 2, LogKind::Barrier),
        {
          let mut l = Log::__crate_new(11, 2, LogKind::Data(Arc::new(vec![1, 2, 3])));
          l.appended_at = Some(std::time::SystemTime::now());
          l
        },
      ],
      leader_commit: 3,
    }
  }

  #[doc(hidden)]
  pub fn __small() -> Self {
    use crate::storage::LogKind;
    use std::sync::Arc;

    Self {
      header: Header::__small(),
      term: 1,
      prev_log_entry: 2,
      prev_log_term: 2,
      entries: vec![
        Log::__crate_new(3, 2, LogKind::Noop),
        Log::__crate_new(4, 2, LogKind::Barrier),
        Log::__crate_new(5, 2, LogKind::Data(Arc::new(vec![1, 2, 3]))),
      ],
      leader_commit: 3,
    }
  }
}

#[cfg(test)]
unit_test_transformable_roundtrip!(AppendEntriesRequest <smol_str::SmolStr, std::net::SocketAddr, Vec<u8>> => append_entries_request);
