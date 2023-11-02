use std::{
  future::Future,
  ops::RangeBounds,
  sync::Arc,
  time::{Duration, Instant},
};

use async_channel::Receiver;
use bytes::Bytes;
use futures::FutureExt;

use crate::{
  membership::Membership,
  transport::{Address, Id},
};

pub struct MembershipLog<I: Id, A: Address> {
  pub membership: Arc<Membership<I, A>>,
  pub index: u64,
  pub term: u64,
}

impl<I: Id, A: Address> MembershipLog<I, A> {
  pub(crate) fn new(term: u64, index: u64, membership: Arc<Membership<I, A>>) -> Self {
    Self {
      membership,
      index,
      term,
    }
  }
}

/// Describes various types of log entries.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(untagged, rename_all = "camelCase"))]
pub enum LogKind<I: Id, A: Address> {
  /// Holds end-user data and extension.
  User {
    /// Holds the log entry's type-specific data, which will be applied to a user [`FinateStateMachine`].
    data: Bytes,
    /// Extensions holds an opaque byte slice of information for middleware. It
    /// is up to the client of the library to properly modify this as it adds
    /// layers and remove those layers when appropriate. This value is a part of
    /// the log, so very large values could cause timing issues.
    ///
    /// N.B. It is _up to the client_ to handle upgrade paths. For instance if
    /// using this with go-raftchunking, the client should ensure that all Raft
    /// peers are using a version that can handle that extension before ever
    /// actually triggering chunking behavior. It is sometimes sufficient to
    /// ensure that non-leaders are upgraded first, then the current leader is
    /// upgraded, but a leader changeover during this process could lead to
    /// trouble, so gating extension behavior via some flag in the client
    /// program is also a good idea.
    extension: Bytes,
  },
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
  Membership(Arc<Membership<I, A>>),
}

/// Log entries are replicated to all members of the Raft cluster
/// and form the heart of the replicated state machine.
///
/// The `clone` on `Log` is cheap and not require deep copy and allocation.
#[viewit::viewit(
  vis_all = "pub(crate)",
  getters(vis_all = "pub"),
  setters(vis_all = "pub")
)]
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Log<I: Id, A: Address> {
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
  kind: LogKind<I, A>,

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
  #[cfg_attr(feature = "serde", serde(with = "serde_instant::option"))]
  appended_at: Option<Instant>,
}

#[cfg(feature = "serde")]
mod serde_instant {
  use serde::{Deserialize, Serialize, Serializer};
  use std::time::{Duration, Instant};

  #[derive(Serialize, Deserialize)]
  struct SerializableInstant {
    secs: u64,
    nanos: u32,
  }

  impl From<Instant> for SerializableInstant {
    fn from(instant: Instant) -> Self {
      let duration_since_epoch = instant.elapsed();
      SerializableInstant {
        secs: duration_since_epoch.as_secs(),
        nanos: duration_since_epoch.subsec_nanos(),
      }
    }
  }

  impl Into<Instant> for SerializableInstant {
    fn into(self) -> Instant {
      Instant::now() - Duration::new(self.secs, self.nanos)
    }
  }

  pub(super) mod option {
    use super::*;

    pub fn serialize<S: Serializer>(
      instant: &Option<Instant>,
      serializer: S,
    ) -> Result<S::Ok, S::Error> {
      let serializable_instant: Option<SerializableInstant> = (*instant).map(Into::into);
      serializable_instant.serialize(serializer)
    }

    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
      deserializer: D,
    ) -> Result<Option<Instant>, D::Error> {
      let serializable_instant = Option::<SerializableInstant>::deserialize(deserializer)?;
      Ok(serializable_instant.map(Into::into))
    }
  }

  pub fn serialize<S: Serializer>(instant: &Instant, serializer: S) -> Result<S::Ok, S::Error> {
    let serializable_instant: SerializableInstant = instant.clone().into();
    serializable_instant.serialize(serializer)
  }

  pub fn deserialize<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
  ) -> Result<Instant, D::Error> {
    let serializable_instant = SerializableInstant::deserialize(deserializer)?;
    Ok(serializable_instant.into())
  }
}

impl<I: Id, A: Address> Log<I, A> {
  /// Create a [`Log`]
  #[inline]
  pub const fn new(data: Bytes) -> Self {
    Self {
      index: 0,
      term: 0,
      kind: LogKind::User {
        data,
        extension: Bytes::new(),
      },
      appended_at: None,
    }
  }

  /// Create a [`Log`] with extension
  #[inline]
  pub const fn with_extension(data: Bytes, extension: Bytes) -> Self {
    Self {
      index: 0,
      term: 0,
      kind: LogKind::User { data, extension },
      appended_at: None,
    }
  }

  #[inline]
  pub(crate) const fn is_membership(&self) -> bool {
    matches!(self.kind, LogKind::Membership(_))
  }

  #[inline]
  pub(crate) const fn is_user(&self) -> bool {
    matches!(self.kind, LogKind::User { .. })
  }

  #[inline]
  pub(crate) const fn crate_new(index: u64, term: u64, kind: LogKind<I, A>) -> Self {
    Self {
      index,
      term,
      kind,
      appended_at: None,
    }
  }
}

/// Used to provide an trait for storing
/// and retrieving logs in a durable fashion.
///
/// **N.B.** The implementation of [`LogStorage`] must be thread-safe.
pub trait LogStorage: Clone + Send + Sync + 'static {
  /// The error type returned by the log storage.
  type Error: std::error::Error + Clone + Send + Sync + 'static;
  /// The async runtime used by the storage.
  type Runtime: agnostic::Runtime;

  /// The id type used to identify nodes.
  type Id: Id;
  /// The address type of node.
  type Address: Address;

  /// Returns the first index written. `None` or `Some(0)` means no entries.
  fn first_index(&self) -> impl Future<Output = Result<Option<u64>, Self::Error>> + Send;

  /// Returns the last index written. `None` or `Some(0)` means no entries.
  fn last_index(&self) -> impl Future<Output = Result<Option<u64>, Self::Error>> + Send;

  /// Gets a log entry at a given index.
  fn get_log(
    &self,
    index: u64,
  ) -> impl Future<Output = Result<Option<Log<Self::Id, Self::Address>>, Self::Error>> + Send;

  /// Stores a log entry
  fn store_log(
    &self,
    log: &Log<Self::Id, Self::Address>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Stores multiple log entries. By default the logs stored may not be contiguous with previous logs (i.e. may have a gap in Index since the last log written). If an implementation can't tolerate this it may optionally implement `MonotonicLogStore` to indicate that this is not allowed. This changes Raft's behaviour after restoring a user snapshot to remove all previous logs instead of relying on a "gap" to signal the discontinuity between logs before the snapshot and logs after.
  fn store_logs(
    &self,
    logs: &[Log<Self::Id, Self::Address>],
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

// #[cfg(feature = "metrics")]
pub(crate) enum LogStorageExtError<E: std::error::Error> {
  LogStorageError(E),
  NotFound,
  GiveMeADescriptiveName,
}

pub(crate) trait LogStorageExt: LogStorage {
  fn oldest_log(
    &self,
  ) -> impl Future<
    Output = Result<Option<Log<Self::Id, Self::Address>>, LogStorageExtError<Self::Error>>,
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

  #[cfg(feature = "metrics")]
  fn emit_metrics(
    &self,
    interval: Duration,
    stop_rx: Receiver<()>,
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

impl<T: LogStorage> LogStorageExt for T {}

#[cfg(all(feature = "test", feature = "metrics"))]
pub(super) mod tests {
  use super::*;
  use std::net::SocketAddr;

  struct TestCase {
    name: &'static str,
    logs: Vec<Log<String, SocketAddr>>,
    want_idx: u64,
    want_err: bool,
  }

  pub async fn test_oldest_log<S: LogStorage<Id = String, Address = SocketAddr>>(store: S) {
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
