use std::{ops::RangeBounds, time::Duration};

use async_channel::Receiver;
use bytes::Bytes;
use futures::FutureExt;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

use crate::utils::now_timestamp;

#[cfg(any(feature = "test", test))]
mod memory;
#[cfg(any(feature = "test", test))]
#[cfg_attr(docsrs, doc(cfg(feature = "test")))]
pub use memory::*;

/// Describes various types of log entries.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize_repr, Deserialize_repr)]
#[repr(u8)]
pub enum LogKind {
  /// Applied to a user [`FinateStateMachine`].
  User,
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
  Memberhsip,
}

impl LogKind {
  /// Returns a string representation of the log type.
  #[inline]
  pub const fn as_string(&self) -> &'static str {
    match self {
      Self::User => "user",
      Self::Noop => "noop",
      Self::Barrier => "barrier",
      Self::Memberhsip => "membership",
    }
  }
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
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Log {
  /// Holds the index of the log entry.
  #[viewit(
    getter(vis = "pub(crate)", const, attrs(doc = "Returns the log entry's index.")),
    setter(vis = "pub(crate)", attrs(doc = "Sets the log entry's index."))
  )]
  index: u64,
  /// Holds the term of the log entry.
  #[viewit(
    getter(vis = "pub(crate)", const, attrs(doc = "Returns the log entry's term.")),
    setter(vis = "pub(crate)", attrs(doc = "Sets the log entry's term."))
  )]
  term: u64,
  /// Holds the kind of the log entry.
  #[viewit(
    getter(vis = "pub(crate)", const, attrs(doc = "Returns the log entry's kind.")),
    setter(vis = "pub(crate)", attrs(doc = "Sets the log entry's kind."))
  )]
  kind: LogKind,

  /// Holds the log entry's type-specific data.
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Returns the log entry's type-specific data.")
    ),
    setter(attrs(doc = "Sets the log entry's type-specific data."))
  )]
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
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Returns the log entry's extensions.")
    ),
    setter(attrs(doc = "Sets the log entry's extensions."))
  )]
  extension: Bytes,

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
      vis = "pub(crate)",
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
  appended_at: u64,
}

impl Log {
  /// Create a [`Log`]
  #[inline]
  pub const fn new(data: Bytes) -> Self {
    Self {
      index: 0,
      term: 0,
      kind: LogKind::User,
      data,
      extension: Bytes::new(),
      appended_at: 0,
    }
  }

  /// Create a [`Log`] with extension
  #[inline]
  pub const fn with_extension(data: Bytes, extension: Bytes) -> Self {
    Self {
      index: 0,
      term: 0,
      kind: LogKind::User,
      data,
      extension,
      appended_at: 0,
    }
  }

  #[inline]
  pub(crate) const fn crate_new(index: u64, term: u64, kind: LogKind) -> Self {
    Self {
      index,
      term,
      kind,
      data: Bytes::new(),
      extension: Bytes::new(),
      appended_at: 0,
    }
  }
}

/// Used to provide an trait for storing
/// and retrieving logs in a durable fashion.
///
/// **N.B.** The implementation of [`LogStorage`] must be thread-safe.
#[async_trait::async_trait]
pub trait LogStorage: Clone + Send + Sync + 'static {
  /// The error type returned by the log storage.
  type Error: std::error::Error + Send + Sync + 'static;
  /// The async runtime used by the storage.
  type Runtime: agnostic::Runtime;

  /// Returns the first index written. 0 for no entries.
  async fn first_index(&self) -> Result<u64, Self::Error>;

  /// Returns the last index written. 0 for no entries.
  async fn last_index(&self) -> Result<u64, Self::Error>;

  /// Gets a log entry at a given index.
  async fn get_log(&self, index: u64) -> Result<Option<Log>, Self::Error>;

  /// Stores a log entry
  async fn store_log(&self, log: &Log) -> Result<(), Self::Error>;

  /// Stores multiple log entries. By default the logs stored may not be contiguous with previous logs (i.e. may have a gap in Index since the last log written). If an implementation can't tolerate this it may optionally implement `MonotonicLogStore` to indicate that this is not allowed. This changes Raft's behaviour after restoring a user snapshot to remove all previous logs instead of relying on a "gap" to signal the discontinuity between logs before the snapshot and logs after.
  async fn store_logs(&self, logs: &[Log]) -> Result<(), Self::Error>;

  /// Removes a range of log entries.
  async fn remove_range(&self, range: impl RangeBounds<u64> + Send) -> Result<(), Self::Error>;

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

#[async_trait::async_trait]
pub(crate) trait LogStorageExt: LogStorage {
  async fn oldest_log(&self) -> Result<Log, LogStorageExtError<Self::Error>> {
    // We might get unlucky and have a truncate right between getting first log
    // index and fetching it so keep trying until we succeed or hard fail.
    let mut last_fail_idx = 0;
    let mut last_err: Option<Self::Error> = None;
    loop {
      let first_idx = self
        .first_index()
        .await
        .map_err(LogStorageExtError::LogStorageError)?;
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
        Ok(Some(log)) => return Ok(log),
        Ok(None) => return Err(LogStorageExtError::NotFound),
        Err(err) => {
          // We failed, keep trying to see if there is a new firstIndex
          last_fail_idx = first_idx;
          last_err = Some(err);
        }
      }
    }
  }

  #[cfg(feature = "metrics")]
  async fn emit_metrics(&self, interval: Duration, stop_rx: Receiver<()>)
  where
    <<Self::Runtime as agnostic::Runtime>::Sleep as std::future::Future>::Output: Send,
  {
    loop {
      futures::select! {
        _ = <Self::Runtime as agnostic::Runtime>::sleep(interval).fuse() => {
          // In error case emit 0 as the age
          let mut age_ms = 0;
          if let Ok(log) = self.oldest_log().await {
            if log.appended_at != 0 {
              age_ms = now_timestamp() - log.appended_at;
            }
          }
          metrics::gauge!("ruraft.log.oldest.ms", age_ms as f64);
        },
        _ = stop_rx.recv().fuse() => return,
      }
    }
  }
}

impl<T: LogStorage> LogStorageExt for T {}

#[cfg(all(feature = "test", feature = "metrics"))]
pub(super) mod tests {
  use agnostic::Runtime;

  use super::*;

  struct TestCase {
    name: &'static str,
    logs: Vec<Log>,
    want_idx: u64,
    want_err: bool,
  }

  pub async fn test_oldest_log<R: Runtime>() {
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
          Log::crate_new(1, 1234, LogKind::User),
          Log::crate_new(1, 1235, LogKind::User),
          Log::crate_new(2, 1236, LogKind::User),
        ],
        want_idx: 1234,
        want_err: false,
      },
    ];

    for case in cases {
      let store = MemoryLogStorage::<R>::new();
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

      if let Ok(got) = got {
        assert_eq!(
          got.index, case.want_idx,
          "{}: got index {}, want {}",
          case.name, got.index, case.want_idx
        );
      }
    }
  }
}
