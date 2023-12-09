use std::{future::Future, io, mem, ops::RangeBounds, sync::Arc, time::Instant};

use futures::AsyncWrite;
#[cfg(feature = "metrics")]
use futures::FutureExt;
use nodecraft::Transformable;

use crate::{
  membership::Membership,
  transport::{Address, Id},
  utils::invalid_data,
  Data,
};

/// A log entry that contains a new membership.
#[derive(Clone)]
pub struct MembershipLog<I, A> {
  membership: Membership<I, A>,
  index: u64,
  term: u64,
}

impl<I, A> MembershipLog<I, A> {
  /// Returns the index of the log entry.
  #[inline]
  pub const fn index(&self) -> u64 {
    self.index
  }

  /// Returns the term of the log entry.
  #[inline]
  pub const fn term(&self) -> u64 {
    self.term
  }

  /// Returns the membership of the log entry.
  #[inline]
  pub const fn membership(&self) -> &Membership<I, A> {
    &self.membership
  }
}

/// Describes various types of log entries.
#[derive(Debug)]
#[non_exhaustive]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(untagged, rename_all = "camelCase"))]
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
  Membership(
    #[cfg_attr(
      feature = "serde",
      serde(
        bound = "I: Eq + ::core::hash::Hash + ::core::fmt::Display + ::serde::Serialize + for<'a> ::serde::Deserialize<'a>, A: Eq + ::core::fmt::Display + ::serde::Serialize + for<'a> ::serde::Deserialize<'a>"
      )
    )]
    Membership<I, A>,
  ),
}

impl<I: Clone, A: Clone, D> Clone for LogKind<I, A, D> {
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
  #[cfg_attr(
    feature = "serde",
    serde(
      bound = "I: Eq + ::core::hash::Hash + ::core::fmt::Display + ::serde::Serialize + for<'a> ::serde::Deserialize<'a>, A: Eq + ::core::fmt::Display + ::serde::Serialize + for<'a> ::serde::Deserialize<'a>, D: ::serde::Serialize + for<'a> ::serde::Deserialize<'a>"
    )
  )]
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
  #[cfg_attr(feature = "serde", serde(with = "crate::utils::serde_instant::option"))]
  appended_at: Option<Instant>,
}

impl<I: Clone, A: Clone, D> Clone for Log<I, A, D> {
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

  #[inline]
  pub(crate) const fn is_membership(&self) -> bool {
    matches!(self.kind, LogKind::Membership(_))
  }

  #[inline]
  pub(crate) const fn is_data(&self) -> bool {
    matches!(self.kind, LogKind::Data(_))
  }

  #[inline]
  pub(crate) const fn crate_new(index: u64, term: u64, kind: LogKind<I, A, D>) -> Self {
    Self {
      index,
      term,
      kind,
      appended_at: None,
    }
  }
}

#[derive(thiserror::Error)]
pub enum LogTransformError<I: Transformable, A: Transformable, D: Transformable> {
  #[error("{0}")]
  Id(I::Error),
  #[error("{0}")]
  Address(A::Error),
  #[error("{0}")]
  Data(D::Error),
  #[error("{0}")]
  Membership(crate::membership::MembershipTransformError<I, A>),
  #[error("dst buffer is too small")]
  EncodeBufferTooSmall,
  #[error("unknown log kind {0}")]
  UnknownLogKind(u8),
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
    }
  }
}

const LEN_SIZE: usize = mem::size_of::<u32>();
const LOG_HEADER_SIZE: usize = LEN_SIZE + 1 + 8 + 8 + 12;
// inlined max 64 bytes on stack when encoding/decoding
const INLINED: usize = 256;

struct Header {
  encoded_len: usize,
  tag: u8,
  index: u64,
  term: u64,
  appended_at: Option<Instant>,
}

impl Header {
  fn from_bytes(b: [u8; LOG_HEADER_SIZE]) -> Self {
    let encoded_len = u32::from_be_bytes([b[0], b[1], b[2], b[3]]) as usize;
    let tag = b[4];
    let index = u64::from_be_bytes([b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12]]);
    let term = u64::from_be_bytes([b[13], b[14], b[15], b[16], b[17], b[18], b[19], b[20]]);
    let appended_at = &b[21..33];
    let appended_at = if appended_at == [0; 12] {
      None
    } else {
      Some(Instant::decode(appended_at).unwrap().1)
    };

    Self {
      encoded_len,
      tag,
      index,
      term,
      appended_at,
    }
  }
}

fn encode_header(header: Header, dst: &mut [u8]) {
  let Header {
    encoded_len,
    tag,
    index,
    term,
    appended_at,
  } = header;

  let mut cur = 0;
  dst[cur..cur + 4].copy_from_slice((encoded_len as u32).to_be_bytes().as_slice());
  cur += 4;
  dst[cur] = tag;
  cur += 1;
  dst[cur..cur + 8].copy_from_slice(index.to_be_bytes().as_slice());
  cur += 8;
  dst[cur..cur + 8].copy_from_slice(term.to_be_bytes().as_slice());
  cur += 8;

  const ENCODED_INSTANT_LEN: usize = 12;

  match appended_at {
    None => {
      dst[cur..cur + ENCODED_INSTANT_LEN].copy_from_slice(&[0; ENCODED_INSTANT_LEN]);
    }
    Some(instant) => {
      instant
        .encode(&mut dst[cur..cur + ENCODED_INSTANT_LEN])
        .unwrap();
    }
  }
}

fn decode_header<I, A, D>(src: &[u8]) -> Result<Header, LogTransformError<I, A, D>>
where
  I: Transformable,
  A: Transformable,
  D: Transformable,
{
  let mut cur = 0;
  let total_len = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
  cur += 4;
  if src.len() < total_len {
    return Err(LogTransformError::Corrupted("corrupted log"));
  }

  let tag = src[cur];
  cur += 1;
  match src[cur] {
    0..=3 => {}
    v => return Err(LogTransformError::UnknownLogKind(v)),
  };

  let index = u64::from_be_bytes([
    src[cur],
    src[cur + 1],
    src[cur + 2],
    src[cur + 3],
    src[cur + 4],
    src[cur + 5],
    src[cur + 6],
    src[cur + 7],
  ]);
  cur += 8;

  let term = u64::from_be_bytes([
    src[cur],
    src[cur + 1],
    src[cur + 2],
    src[cur + 3],
    src[cur + 4],
    src[cur + 5],
    src[cur + 6],
    src[cur + 7],
  ]);
  cur += 8;

  let appended_at = &src[cur..cur + 12];
  let appended_at = if appended_at == [0; 12] {
    None
  } else {
    Some(Instant::decode(appended_at).unwrap().1)
  };

  Ok(Header {
    encoded_len: total_len,
    tag,
    index,
    term,
    appended_at,
  })
}

fn decode_log_data<I, A, D>(
  total_len: usize,
  index: u64,
  term: u64,
  appended_at: Option<Instant>,
  src: &[u8],
) -> Result<Log<I, A, D>, LogTransformError<I, A, D>>
where
  I: Transformable,
  A: Transformable,
  D: Transformable,
{
  let (_, data) =
  D::decode(&src).map_err(|e| LogTransformError::Data(e))?;
  Ok(
    Log {
      kind: LogKind::Data(Arc::new(data)),
      index,
      term,
      appended_at,
    },
  )
}

// Log Binary Format
//
// --------------------------------------------------------------------------------------------------------
// | total_len (4 bytes) | kind (1 byte) | index (8 bytes) | term (8 bytes) | append_at (12 bytes) | data |
// --------------------------------------------------------------------------------------------------------
impl<I, A, D> Transformable for Log<I, A, D>
where
  I: Id + Send + Sync + 'static,
  <I as Transformable>::Error: Send + Sync + 'static,
  A: Address + Send + Sync + 'static,
  <A as Transformable>::Error: Send + Sync + 'static,
  D: Data,
  <D as Transformable>::Error: Send + Sync + 'static,
{
  type Error = LogTransformError<I, A, D>;

  fn encode(&self, dst: &mut [u8]) -> Result<(), Self::Error> {
    let encoded_len = self.encoded_len();

    if dst.len() < encoded_len {
      return Err(LogTransformError::EncodeBufferTooSmall);
    }

    let tag = self.kind.tag();
    let header = Header {
      encoded_len,
      tag,
      index: self.index,
      term: self.term,
      appended_at: self.appended_at,
    };
    match &self.kind {
      LogKind::Data(d) => {
        encode_header(header, dst);
        let data_encoded_len = d.encoded_len();
        d.encode(&mut dst[LOG_HEADER_SIZE..LOG_HEADER_SIZE + data_encoded_len])
          .map_err(LogTransformError::Data)
      }
      LogKind::Noop => {
        encode_header(header, dst);
        Ok(())
      }
      LogKind::Barrier => {
        encode_header(header, dst);
        Ok(())
      }
      LogKind::Membership(m) => {
        encode_header(header, dst);
        let membership_encoded_len = m.encoded_len();
        m.encode(&mut dst[LOG_HEADER_SIZE..LOG_HEADER_SIZE + membership_encoded_len])
          .map_err(LogTransformError::Membership)
      }
    }
  }

  fn encode_to_writer<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
    let encoded_len = self.encoded_len();
    if encoded_len <= INLINED {
      let mut buf = [0; INLINED];
      self.encode(&mut buf).map_err(invalid_data)?;
      writer.write_all(&buf[..encoded_len])
    } else {
      let mut buf = vec![0; encoded_len];
      self.encode(&mut buf).map_err(invalid_data)?;
      writer.write_all(&buf[..])
    }
  }

  async fn encode_to_async_writer<W: AsyncWrite + Send + Unpin>(
    &self,
    writer: &mut W,
  ) -> io::Result<()>
  where
    Self::Error: Send + Sync + 'static,
  {
    use futures::AsyncWriteExt;

    let encoded_len = self.encoded_len();
    if encoded_len <= INLINED {
      let mut buf = [0; INLINED];
      self.encode(&mut buf).map_err(invalid_data)?;
      writer.write_all(&buf[..encoded_len]).await
    } else {
      let mut buf = vec![0; encoded_len];
      self.encode(&mut buf).map_err(invalid_data)?;
      writer.write_all(&buf[..]).await
    }
  }

  fn encoded_len(&self) -> usize {
    match &self.kind {
      LogKind::Data(d) => LOG_HEADER_SIZE + d.encoded_len(),
      LogKind::Noop => LOG_HEADER_SIZE,
      LogKind::Barrier => LOG_HEADER_SIZE,
      LogKind::Membership(m) => LOG_HEADER_SIZE + m.encoded_len(),
    }
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    let Header {
      encoded_len,
      tag,
      index,
      term,
      appended_at,
    } = decode_header(src)?;
    match tag {
      0 => {
        let (_, data) =
          D::decode(&src[LOG_HEADER_SIZE..encoded_len]).map_err(LogTransformError::Data)?;
        Ok((
          encoded_len,
          Log {
            kind: LogKind::Data(Arc::new(data)),
            index,
            term,
            appended_at,
          },
        ))
      }
      1 => Ok((
        encoded_len,
        Log {
          kind: LogKind::Noop,
          index,
          term,
          appended_at,
        },
      )),
      2 => Ok((
        encoded_len,
        Log {
          kind: LogKind::Barrier,
          index,
          term,
          appended_at,
        },
      )),
      3 => {
        let (_, membership) = Membership::decode(&src[LOG_HEADER_SIZE..encoded_len])
          .map_err(LogTransformError::Membership)?;
        Ok((
          encoded_len,
          Log {
            kind: LogKind::Membership(membership),
            index,
            term,
            appended_at,
          },
        ))
      }
      _ => Err(LogTransformError::UnknownLogKind(tag)),
    }
  }

  fn decode_from_reader<R: io::Read>(reader: &mut R) -> io::Result<(usize, Self)>
  where
    Self: Sized,
  {
    let mut buf = [0; LOG_HEADER_SIZE];
    reader.read_exact(&mut buf)?;
    let total_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if total_len <= INLINED {
      let mut buf = [0; INLINED];
      reader.read_exact(&mut buf[..total_len])?;
      Self::decode(&buf[..total_len]).map_err(invalid_data)
    } else {
      let mut buf = vec![0; total_len];
      reader.read_exact(&mut buf)?;
      Self::decode(&buf).map_err(invalid_data)
    }
  }

  async fn decode_from_async_reader<R: futures::io::AsyncRead + Send + Unpin>(
    reader: &mut R,
  ) -> io::Result<(usize, Self)>
  where
    Self: Sized,
    Self::Error: Send + Sync + 'static,
  {
    use futures::AsyncReadExt;

    let mut buf = [0; LOG_HEADER_SIZE];
    reader.read_exact(&mut buf).await?;
    let Header {
      encoded_len: total_len,
      tag,
      index,
      term,
      appended_at,
    } = Header::from_bytes(buf);

    let remaining = total_len - LOG_HEADER_SIZE;
    match tag {
      0 => {
        if remaining <= INLINED {
          let mut dst = [0; INLINED];
          reader.read_exact(&mut dst[..remaining]).await?;
          decode_log_data(total_len, index, term, appended_at, &dst[..remaining]).map(|l| (total_len, l)).map_err(invalid_data)
        } else {
          let mut dst = vec![0; remaining];
          reader.read_exact(&mut dst).await?;
          decode_log_data(total_len, index, term, appended_at, &dst).map(|l| (total_len, l)).map_err(invalid_data)
        }
      }
      1 => Ok((
        total_len,
        Log {
          kind: LogKind::Noop,
          index,
          term,
          appended_at,
        },
      )),
      2 => Ok((
        total_len,
        Log {
          kind: LogKind::Barrier,
          index,
          term,
          appended_at,
        },
      )),
      3 => {
        if remaining <= INLINED {
          let mut dst = [0; INLINED];
          reader.read_exact(&mut dst[..remaining]).await?;
          let (_, data) = Membership::decode(&dst[..remaining])
            .map_err(|e| invalid_data(LogTransformError::Membership(e)))?;
          Ok((
            total_len,
            Log {
              kind: LogKind::Membership(data),
              index,
              term,
              appended_at,
            },
          ))
        } else {
          let mut dst = vec![0; remaining];
          reader.read_exact(&mut dst).await?;
          let (_, data) = Membership::decode(&dst[..remaining])
            .map_err(|e| invalid_data(LogTransformError::Membership(e)))?;
          Ok((
            total_len,
            Log {
              kind: LogKind::Membership(data),
              index,
              term,
              appended_at,
            },
          ))
        }
      }
      _ => Err(invalid_data(LogTransformError::UnknownLogKind(tag))),
    }
  }
}

/// Used to provide an trait for storing
/// and retrieving logs in a durable fashion.
///
/// **N.B.** The implementation of [`LogStorage`] must be thread-safe.
pub trait LogStorage: Send + Sync + 'static {
  /// The error type returned by the log storage.
  type Error: std::error::Error + Clone + Send + Sync + 'static;
  /// The async runtime used by the storage.
  type Runtime: agnostic::Runtime;

  /// The id type used to identify nodes.
  type Id: Id;
  /// The address type of node.
  type Address: Address;
  /// The log entry's type-specific data, which will be applied to a user [`FinateStateMachine`](crate::FinateStateMachine).
  type Data: Data;

  /// Returns the first index written. `None` or `Some(0)` means no entries.
  fn first_index(&self) -> impl Future<Output = Result<Option<u64>, Self::Error>> + Send;

  /// Returns the last index written. `None` or `Some(0)` means no entries.
  fn last_index(&self) -> impl Future<Output = Result<Option<u64>, Self::Error>> + Send;

  /// Gets a log entry at a given index.
  fn get_log(
    &self,
    index: u64,
  ) -> impl Future<Output = Result<Option<Log<Self::Id, Self::Address, Self::Data>>, Self::Error>> + Send;

  /// Stores a log entry
  fn store_log(
    &self,
    log: &Log<Self::Id, Self::Address, Self::Data>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Stores multiple log entries. By default the logs stored may not be contiguous with previous logs (i.e. may have a gap in Index since the last log written). If an implementation can't tolerate this it may optionally implement `MonotonicLogStore` to indicate that this is not allowed. This changes Raft's behaviour after restoring a user snapshot to remove all previous logs instead of relying on a "gap" to signal the discontinuity between logs before the snapshot and logs after.
  fn store_logs(
    &self,
    logs: &[Log<Self::Id, Self::Address, Self::Data>],
  ) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Removes a range of log entries.
  fn remove_range(
    &self,
    range: impl RangeBounds<u64> + Send,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// An optional method for [`LogStorage`] implementations that
  /// cannot tolerate gaps in between the Index values of consecutive log entries. For example,
  /// this may allow more efficient indexing because the Index values are densely populated. If true is
  /// returned, Raft will avoid relying on gaps to trigger re-synching logs on followers after a
  /// snapshot is restored. The [`LogStorage`] must have an efficient implementation of
  /// [`LogStorage::remove_range`] for the case where all logs are removed, as this must be called after snapshot restore when gaps are not allowed.
  /// We avoid deleting all records for [`LogStorage`] that when this function return false
  /// because although it's always correct to do so, it has a major negative performance impact on the BoltDB store that is currently
  /// the most widely used.
  fn is_monotonic() -> bool {
    false
  }
}

#[cfg(feature = "metrics")]
pub(crate) enum LogStorageExtError<E: std::error::Error> {
  LogStorageError(E),
  NotFound,
  GiveMeADescriptiveName,
}

#[cfg(feature = "metrics")]
pub(crate) trait LogStorageExt: LogStorage {
  fn oldest_log(
    &self,
  ) -> impl Future<
    Output = Result<
      Option<Log<Self::Id, Self::Address, Self::Data>>,
      LogStorageExtError<Self::Error>,
    >,
  > + Send {
    async move {
      // We might get unlucky and have a truncate right between getting first log
      // index and fetching it so keep trying until we succeed or hard fail.
      let mut last_fail_idx = 0;
      let mut last_err: Option<Self::Error> = None;
      loop {
        let Some(first_idx) = self
          .first_index()
          .await
          .map_err(LogStorageExtError::LogStorageError)?
        else {
          return Err(LogStorageExtError::NotFound);
        };

        if first_idx == 0 {
          return Err(LogStorageExtError::NotFound);
        }

        if first_idx == last_fail_idx {
          // Got same index as last time around which errored, don't bother trying
          // to fetch it again just return the error.
          if let Some(last_err) = last_err {
            return Err(LogStorageExtError::LogStorageError(last_err));
          } else {
            return Err(LogStorageExtError::GiveMeADescriptiveName);
          }
        }

        match self.get_log(first_idx).await {
          Ok(log) => return Ok(log),
          Err(err) => {
            // We failed, keep trying to see if there is a new firstIndex
            last_fail_idx = first_idx;
            last_err = Some(err);
          }
        }
      }
    }
  }

  fn emit_metrics(
    &self,
    interval: std::time::Duration,
    stop_rx: async_channel::Receiver<()>,
  ) -> impl Future<Output = ()> + Send
  where
    <<Self::Runtime as agnostic::Runtime>::Sleep as std::future::Future>::Output: Send,
  {
    async move {
      loop {
        futures::select! {
          _ = <Self::Runtime as agnostic::Runtime>::sleep(interval).fuse() => {
            // In error case emit 0 as the age
            let mut age_ms = 0;
            if let Ok(Some(log)) = self.oldest_log().await {
              match log.appended_at {
                Some(append_at) => {
                  age_ms = append_at.elapsed().as_millis() as u64;
                }
                None => {
                  age_ms = 0;
                }
              }
            }
            metrics::gauge!("ruraft.log.oldest.ms", age_ms as f64);
          },
          _ = stop_rx.recv().fuse() => return,
        }
      }
    }
  }
}

#[cfg(feature = "metrics")]
impl<T: LogStorage> LogStorageExt for T {}

#[cfg(all(feature = "test", feature = "metrics"))]
pub(super) mod tests {
  use smol_str::SmolStr;

  use super::*;
  use std::net::SocketAddr;

  struct TestCase {
    name: &'static str,
    logs: Vec<Log<SmolStr, SocketAddr, Vec<u8>>>,
    want_idx: u64,
    want_err: bool,
  }

  pub async fn test_oldest_log<
    S: LogStorage<Id = SmolStr, Address = SocketAddr, Data = Vec<u8>>,
  >(
    store: S,
  ) {
    let cases = vec![
      TestCase {
        name: "empty logs",
        logs: Vec::new(),
        want_idx: 0,
        want_err: true,
      },
      TestCase {
        name: "simple case",
        logs: vec![
          Log::crate_new(1, 1234, LogKind::Noop),
          Log::crate_new(1, 1235, LogKind::Noop),
          Log::crate_new(2, 1236, LogKind::Noop),
        ],
        want_idx: 1234,
        want_err: false,
      },
    ];

    for case in cases {
      store
        .store_logs(&case.logs)
        .await
        .expect("expected store logs not to fail");

      let got = store.oldest_log().await;
      if case.want_err && got.is_ok() {
        panic!("{}: wanted error got ok", case.name);
      }

      if !case.want_err && got.is_err() {
        panic!("{}: wanted no error but got err", case.name);
      }

      if let Ok(Some(got)) = got {
        assert_eq!(
          got.index, case.want_idx,
          "{}: got index {}, want {}",
          case.name, got.index, case.want_idx
        );
      }
    }
  }
}
