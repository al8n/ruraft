use std::{future::Future, ops::RangeBounds, sync::Arc, time::Instant};

#[cfg(feature = "metrics")]
use futures::FutureExt;

use crate::{
  membership::Membership,
  transport::{Address, Id},
  Data,
};

/// A log entry that contains a new membership.
#[derive(Clone)]
pub struct MembershipLog<I: Id, A: Address> {
  membership: Arc<Membership<I, A>>,
  index: u64,
  term: u64,
}

impl<I: Id, A: Address> MembershipLog<I, A> {
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
  pub const fn membership(&self) -> &Arc<Membership<I, A>> {
    &self.membership
  }
}

/// Describes various types of log entries.
#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(untagged, rename_all = "camelCase"))]
pub enum LogKind<I: Id, A: Address, D: Data> {
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
  Membership(Arc<Membership<I, A>>),
}

impl<I: Id, A: Address, D: Data> Clone for LogKind<I, A, D> {
  fn clone(&self) -> Self {
    match self {
      Self::Data(data) => Self::Data(data.clone()),
      Self::Noop => Self::Noop,
      Self::Barrier => Self::Barrier,
      Self::Membership(membership) => Self::Membership(membership.clone()),
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
  setters(skip)
)]
#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Log<I: Id, A: Address, D: Data> {
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

impl<I: Id, A: Address, D: Data> Clone for Log<I, A, D> {
  fn clone(&self) -> Self {
    Self {
      index: self.index,
      term: self.term,
      kind: self.kind.clone(),
      appended_at: self.appended_at,
    }
  }
}

impl<I: Id, A: Address, D: Data> Log<I, A, D> {
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
  use super::*;
  use std::net::SocketAddr;

  struct TestCase {
    name: &'static str,
    logs: Vec<Log<String, SocketAddr, Vec<u8>>>,
    want_idx: u64,
    want_err: bool,
  }

  pub async fn test_oldest_log<S: LogStorage<Id = String, Address = SocketAddr, Data = Vec<u8>>>(store: S) {
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
