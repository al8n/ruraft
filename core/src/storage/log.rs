use std::{future::Future, ops::RangeBounds};

#[cfg(feature = "metrics")]
use futures::FutureExt;

use crate::{
  membership::Membership,
  transport::{Address, Id},
  Data,
};

mod types;
pub use types::*;

/// A log entry that contains a new membership.
pub struct MembershipLog<I, A> {
  membership: Membership<I, A>,
  index: u64,
  term: u64,
}

impl<I, A> Clone for MembershipLog<I, A> {
  fn clone(&self) -> Self {
    Self {
      membership: self.membership.clone(),
      index: self.index,
      term: self.term,
    }
  }
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

#[auto_impl::auto_impl(Box, Arc)]
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
                  age_ms = append_at.duration_since(std::time::UNIX_EPOCH).unwrap_or(std::time::Duration::ZERO).as_millis() as u64;
                }
                None => {
                  age_ms = 0;
                }
              }
            }

            let gauge = metrics::gauge!("ruraft.log.oldest.ms");
            gauge.set(age_ms as f64);
          },
          _ = stop_rx.recv().fuse() => return,
        }
      }
    }
  }
}

#[cfg(feature = "metrics")]
impl<T: LogStorage> LogStorageExt for T {}
