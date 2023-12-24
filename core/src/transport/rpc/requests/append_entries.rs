use std::io;

use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use nodecraft::{Address, Id};
use ruraft_utils::{decode_varint, encode_varint, encoded_len_varint};

use crate::{utils::invalid_data, Data};

use super::*;

/// The command used to append entries to the
/// replicated log.
#[viewit::viewit]
#[derive(Debug, Clone)]
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
  I: Id + Send + Sync + 'static,
  I::Error: Send + Sync + 'static,
  A: Address + Send + Sync + 'static,
  A::Error: Send + Sync + 'static,
  D: Data,
  D::Error: Send + Sync + 'static,
{
  type Error = TransformError;

  fn encode(&self, dst: &mut [u8]) -> Result<(), Self::Error> {
    let encoded_len = self.encoded_len();
    if dst.len() < encoded_len {
      return Err(TransformError::EncodeBufferTooSmall);
    }

    let mut offset = 0;
    dst[..MESSAGE_SIZE_LEN].copy_from_slice(&(encoded_len as u32).to_be_bytes());
    offset += MESSAGE_SIZE_LEN;

    let header_encoded_len = self.header.encoded_len();
    self.header.encode(&mut dst[offset..])?;
    offset += header_encoded_len;

    offset += encode_varint(self.term, &mut dst[offset..])?;
    offset += encode_varint(self.prev_log_entry, &mut dst[offset..])?;
    offset += encode_varint(self.prev_log_term, &mut dst[offset..])?;
    offset += encode_varint(self.leader_commit, &mut dst[offset..])?;
    let num_entries = self.entries.len() as u32;
    dst[offset..offset + core::mem::size_of::<u32>()].copy_from_slice(&num_entries.to_be_bytes());
    offset += core::mem::size_of::<u32>();

    for entry in &self.entries {
      let encoded_log_len = entry.encoded_len();
      entry
        .encode(&mut dst[offset..])
        .map_err(Self::Error::encode)?;
      offset += encoded_log_len;
    }

    Ok(())
  }

  fn encode_to_writer<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
    let encoded_len = self.encoded_len();
    if encoded_len <= MAX_INLINED_BYTES {
      let mut buf = [0u8; MAX_INLINED_BYTES];
      self.encode(&mut buf).map_err(invalid_data)?;
      writer.write_all(&buf[..encoded_len])
    } else {
      let mut buf = vec![0u8; encoded_len];
      self.encode(&mut buf).map_err(invalid_data)?;
      writer.write_all(&buf)
    }
  }

  async fn encode_to_async_writer<W: AsyncWrite + Send + Unpin>(
    &self,
    writer: &mut W,
  ) -> io::Result<()>
  where
    Self::Error: Send + Sync + 'static,
  {
    let encoded_len = self.encoded_len();
    if encoded_len <= MAX_INLINED_BYTES {
      let mut buf = [0u8; MAX_INLINED_BYTES];
      self.encode(&mut buf).map_err(invalid_data)?;
      writer.write_all(&buf[..encoded_len]).await
    } else {
      let mut buf = vec![0u8; encoded_len];
      self.encode(&mut buf).map_err(invalid_data)?;
      writer.write_all(&buf).await
    }
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
    let encoded_len =
      u32::from_be_bytes(src[offset..offset + MESSAGE_SIZE_LEN].try_into().unwrap()) as usize;
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

  fn decode_from_reader<R: std::io::Read>(reader: &mut R) -> std::io::Result<(usize, Self)>
  where
    Self: Sized,
  {
    let mut len = [0u8; MESSAGE_SIZE_LEN];
    reader.read_exact(&mut len)?;
    let msg_len = u32::from_be_bytes(len) as usize;

    if msg_len <= MAX_INLINED_BYTES {
      let mut buf = [0u8; MAX_INLINED_BYTES];
      buf[..MESSAGE_SIZE_LEN].copy_from_slice(&len);
      reader.read_exact(&mut buf[MESSAGE_SIZE_LEN..msg_len])?;
      Self::decode(&buf).map_err(invalid_data)
    } else {
      let mut buf = vec![0u8; msg_len];
      buf[..MESSAGE_SIZE_LEN].copy_from_slice(&len);
      reader.read_exact(&mut buf[MESSAGE_SIZE_LEN..])?;
      Self::decode(&buf).map_err(invalid_data)
    }
  }

  async fn decode_from_async_reader<R: AsyncRead + Send + Unpin>(
    reader: &mut R,
  ) -> io::Result<(usize, Self)>
  where
    Self: Sized,
    Self::Error: Send + Sync + 'static,
  {
    let mut len = [0u8; MESSAGE_SIZE_LEN];
    reader.read_exact(&mut len).await?;
    let msg_len = u32::from_be_bytes(len) as usize;

    if msg_len <= MAX_INLINED_BYTES {
      let mut buf = [0u8; MAX_INLINED_BYTES];
      buf[..MESSAGE_SIZE_LEN].copy_from_slice(&len);
      reader
        .read_exact(&mut buf[MESSAGE_SIZE_LEN..msg_len])
        .await?;
      Self::decode(&buf).map_err(invalid_data)
    } else {
      let mut buf = vec![0u8; msg_len];
      buf[..MESSAGE_SIZE_LEN].copy_from_slice(&len);
      reader.read_exact(&mut buf[MESSAGE_SIZE_LEN..]).await?;
      Self::decode(&buf).map_err(invalid_data)
    }
  }
}

#[cfg(test)]
mod tests {
  use crate::storage::LogKind;

  use super::*;
  use smol_str::SmolStr;
  use std::{net::SocketAddr, sync::Arc};

  type TestRequest = AppendEntriesRequest<SmolStr, SocketAddr, Vec<u8>>;

  #[tokio::test]
  async fn test_encode_decode_small_request_round_trip() {
    test_transformable_roundtrip!(TestRequest {
      TestRequest {
        header: Header {
          protocol_version: ProtocolVersion::V1,
          from: Node::new("node1".into(), "127.0.0.1:8080".parse().unwrap()),
        },
        term: 1,
        prev_log_entry: 2,
        prev_log_term: 2,
        entries: vec![
          Log::crate_new(3, 2, LogKind::Noop),
          Log::crate_new(4, 2, LogKind::Barrier),
          Log::crate_new(5, 2, LogKind::Data(Arc::new(vec![1, 2, 3]))),
        ],
        leader_commit: 3,
      }
    });
  }

  #[tokio::test]
  async fn test_encode_decode_round_trip() {
    test_transformable_roundtrip!(TestRequest {
      TestRequest {
        header: Header {
          protocol_version: ProtocolVersion::V1,
          from: Node::new("node1".into(), "127.0.0.1:8080".parse().unwrap()),
        },
        term: 1,
        prev_log_entry: 2,
        prev_log_term: 2,
        entries: vec![
          Log::crate_new(3, 2, LogKind::Noop),
          Log::crate_new(4, 2, LogKind::Barrier),
          Log::crate_new(5, 2, LogKind::Data(Arc::new(vec![1, 2, 3]))),
          Log::crate_new(6, 2, LogKind::Noop),
          Log::crate_new(7, 2, LogKind::Barrier),
          Log::crate_new(8, 2, LogKind::Data(Arc::new(vec![1, 2, 3]))),
          Log::crate_new(9, 2, LogKind::Noop),
          Log::crate_new(10, 2, LogKind::Barrier),
          {
            let mut l = Log::crate_new(11, 2, LogKind::Data(Arc::new(vec![1, 2, 3])));
            l.appended_at = Some(std::time::SystemTime::now());
            l
          },
          Log::crate_new(3, 2, LogKind::Noop),
          Log::crate_new(4, 2, LogKind::Barrier),
          Log::crate_new(5, 2, LogKind::Data(Arc::new(vec![1, 2, 3]))),
          Log::crate_new(6, 2, LogKind::Noop),
          Log::crate_new(7, 2, LogKind::Barrier),
          Log::crate_new(8, 2, LogKind::Data(Arc::new(vec![1, 2, 3]))),
          Log::crate_new(9, 2, LogKind::Noop),
          Log::crate_new(10, 2, LogKind::Barrier),
          {
            let mut l = Log::crate_new(11, 2, LogKind::Data(Arc::new(vec![1, 2, 3])));
            l.appended_at = Some(std::time::SystemTime::now());
            l
          },
        ],
        leader_commit: 3,
      }
    });
  }
}
