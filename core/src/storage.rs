mod snapshot;
use nodecraft::CheapClone;
pub use snapshot::*;
mod log;
pub use log::*;
mod stable;
pub use stable::*;

use std::{borrow::Cow, future::Future};

use crate::{
  transport::{Address, Id},
  Data, State,
};

/// Represents a comprehensive set of errors arising from operations within the [`Storage`] trait.
///
/// This trait encapsulates a range of error types, providing a structured approach to categorizing
/// and handling storage-level errors. Implementers can leverage this to define both generic and
/// storage-specific error scenarios.
pub trait StorageError: std::error::Error + Send + Sync + 'static {
  /// Stable storage errors
  type Stable: StableStorage;

  /// Snapshot storage errors
  type Snapshot: SnapshotStorage;

  /// Log storage errors
  type Log: LogStorage;

  /// Constructs an error associated with stable storage operations.
  fn stable(err: <Self::Stable as StableStorage>::Error) -> Self;

  /// Constructs an error associated with snapshot storage operations.
  fn snapshot(err: <Self::Snapshot as SnapshotStorage>::Error) -> Self;

  /// Constructs an error associated with log storage operations.
  fn log(err: <Self::Log as LogStorage>::Error) -> Self;

  /// With extra message to explain the error.
  fn with_message(self, msg: Cow<'static, str>) -> Self;

  /// Constructs an error associated with IO operations within the storage.
  fn io(err: std::io::Error) -> Self;

  /// Provides a flexible mechanism to define custom errors using a descriptive message.
  ///
  /// The resulting error message will be straightforward, avoiding capitalization or a trailing period.
  fn custom<T>(msg: T) -> Self
  where
    Self: Sized,
    T: core::fmt::Display;
}

/// Provides a trait that can optionally be implemented by users of this crate
/// to store membership updates made in the replicated log. In general this is only
/// necessary for [`FinateStateMachine`](crate::FinateStateMachine) that mutate durable state directly instead of applying changes
/// in memory and snapshotting periodically. By storing membership changes, the
/// persistent [`FinateStateMachine`] state can behave as a complete snapshot, and be able to recover
/// without an external snapshot just for persisting the raft membership.
pub trait MembershipStorage: Send + Sync + 'static {
  /// Errors returned by the storage.
  type Error: std::error::Error + Send + Sync + 'static;

  /// The id type used to identify nodes.
  type Id: Id;
  /// The address type of node.
  type Address: Address;

  /// Invoked once a log entry containing a membership
  /// change is committed. It takes the term and index at which the membership was
  /// written and the membership value.
  fn store(
    &self,
    log: MembershipLog<Self::Id, Self::Address>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Invoked once many membership log entries are committed. It takes the term and index at which the membership was
  /// written and the membership value.
  fn store_many(
    &self,
    logs: impl IntoIterator<Item = MembershipLog<Self::Id, Self::Address>>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Storage is a trait that must be implemented by the user to provide the persistent storage for the Raft.
pub trait Storage: Send + Sync + 'static {
  /// Errors returned by the storage.
  type Error: StorageError<Stable = Self::Stable, Snapshot = Self::Snapshot, Log = Self::Log>;

  /// The id type used to identify nodes.
  type Id: Id + CheapClone + Send + Sync + 'static;
  /// The address type of node.
  type Address: Address + CheapClone + Send + Sync + 'static;
  /// The log entry's type-specific data, which will be applied to a user [`FinateStateMachine`](crate::FinateStateMachine).
  type Data: Data;

  /// Stable storage
  type Stable: StableStorage<Id = Self::Id, Address = Self::Address, Runtime = Self::Runtime>;

  /// Snapshot storage
  type Snapshot: SnapshotStorage<Id = Self::Id, Address = Self::Address, Runtime = Self::Runtime>;

  /// Log storage
  type Log: LogStorage<
    Id = Self::Id,
    Address = Self::Address,
    Data = Self::Data,
    Runtime = Self::Runtime,
  >;

  /// Membership storage
  type Membership: MembershipStorage<Id = Self::Id, Address = Self::Address>;

  /// The async runtime used by the storage.
  type Runtime: agnostic::Runtime;

  /// Returns a reference to the stable storage.
  fn stable_store(&self) -> &Self::Stable;

  /// Returns a reference to the log storage.
  fn log_store(&self) -> &Self::Log;

  /// Returns a reference to the snapshot storage.
  fn snapshot_store(&self) -> &Self::Snapshot;

  /// Returns a reference to the membership storage.
  fn membership_store(&self) -> Option<&Self::Membership>;
}

pub(crate) async fn compact_logs<S: Storage>(
  ls: &S::Log,
  state: &State,
  snap_idx: u64,
  trailing_logs: u64,
) -> Result<(), S::Error> {
  #[cfg(feature = "metrics")]
  let start = std::time::Instant::now();

  #[cfg(feature = "metrics")]
  scopeguard::defer!(metrics::histogram!(
    "ruraft.snapshot.compact_logs",
    start.elapsed().as_millis() as f64
  ));

  let last_log = state.last_log();
  compact_logs_with_trailing::<S>(ls, snap_idx, last_log.index, trailing_logs).await
}

/// Takes the last inclusive index of a snapshot,
/// the lastLogIdx, and and the trailingLogs and trims the logs that
/// are no longer needed.
pub(crate) async fn compact_logs_with_trailing<S: Storage>(
  ls: &S::Log,
  snap_idx: u64,
  last_log_idx: u64,
  traling_logs: u64,
) -> Result<(), S::Error> {
  // Determine log ranges to compact
  let Some(min_log) = ls.first_index().await.map_err(|e| {
    tracing::error!(target = "ruraft.snapshot.runner", err = %e, "failed to get first log index");
    <S::Error as StorageError>::log(e)
  })?
  else {
    return Ok(());
  };

  // Check if we have enough logs to truncate
  // Use a consistent value for traling_logs for the duration of this method
  // call to avoid surprising behaviour.
  if last_log_idx <= traling_logs {
    return Ok(());
  }

  // Truncate up to the end of the snapshot, or `traling_logs`
  // back from the head, which ever is further back. This ensures
  // at least `traling_logs` entries, but does not allow logs
  // after the snapshot to be removed.
  let max_log = snap_idx.min(last_log_idx.saturating_sub(traling_logs));
  if min_log > max_log {
    tracing::info!(target = "ruraft.snapshot.runner", "no logs to truncate");
    return Ok(());
  }

  tracing::info!(target="ruraft.snapshot.runner", from = %min_log, to = %max_log, "compacting logs");

  // Compact the logs
  ls.remove_range(min_log..=min_log).await.map_err(|e| {
    tracing::error!(target = "ruraft.snapshot.runner", err = %e, "log compaction failed");
    <S::Error as StorageError>::log(e).with_message(Cow::Borrowed("log compaction failed"))
  })
}

/// Removes all old logs from the store. This is used for
/// MonotonicLogStores after restore. Callers should verify that the store
/// implementation is monotonic prior to calling.
pub(crate) async fn remove_old_logs<S: Storage>(ls: &S::Log) -> Result<(), S::Error> {
  #[cfg(feature = "metrics")]
  let start = std::time::Instant::now();

  #[cfg(feature = "metrics")]
  scopeguard::defer!(metrics::histogram!(
    "ruraft.snapshot.remove_old_logs",
    start.elapsed().as_millis() as f64
  ));

  match ls.last_index().await {
    Ok(None) | Ok(Some(0)) => Ok(()),
    Ok(Some(last_log_index)) => {
      tracing::info!(
        target = "ruraft.snapshot.runner",
        "removing all old logs from log store"
      );
      // call compactLogsWithTrailing with lastLogIdx for snapIdx since
      // it will take the lesser of lastLogIdx and snapIdx to figure out
      // the end for which to apply trailingLogs.
      compact_logs_with_trailing::<S>(ls, last_log_index, last_log_index, 0).await
    }
    Err(e) => Err(
      <S::Error as StorageError>::log(e)
        .with_message(Cow::Borrowed("failed to get last log index")),
    ),
  }
}

#[cfg(any(feature = "test", test))]
pub(super) mod tests {
  use crate::Node;

  use super::{Log, LogStorage, StableStorage};
  use smol_str::SmolStr;
  use std::net::SocketAddr;

  pub async fn first_index<S: LogStorage<Id = SmolStr, Address = SocketAddr, Data = Vec<u8>>>(
    s: &S,
  ) {
    // Should get 0 index on empty log
    assert!(s.first_index().await.unwrap().is_none());

    // Set a mock raft log
    let logs = vec![
      Log::new(b"log1".to_vec()).set_index(1),
      Log::new(b"log2".to_vec()).set_index(2),
      Log::new(b"log3".to_vec()).set_index(3),
    ];
    s.store_logs(&logs).await.unwrap();

    // Fetch the first Raft index
    assert_eq!(s.first_index().await.unwrap().unwrap(), 1);
  }

  pub async fn last_index<S: LogStorage<Id = SmolStr, Address = SocketAddr, Data = Vec<u8>>>(
    s: &S,
  ) {
    // Should get 0 index on empty log
    assert!(s.last_index().await.unwrap().is_none());

    // Set a mock raft log
    let logs = vec![
      Log::new(b"log1".to_vec()).set_index(1),
      Log::new(b"log2".to_vec()).set_index(2),
      Log::new(b"log3".to_vec()).set_index(3),
    ];
    s.store_logs(&logs).await.unwrap();

    // Fetch the first Raft index
    assert_eq!(s.last_index().await.unwrap().unwrap(), 3);
  }

  pub async fn get_log<S: LogStorage<Id = SmolStr, Address = SocketAddr, Data = Vec<u8>>>(s: &S) {
    // Should get 0 index on empty log
    assert!(s.get_log(1).await.unwrap().is_none());

    // Set a mock raft log
    let logs = vec![
      Log::new(b"log1".to_vec()).set_index(1),
      Log::new(b"log2".to_vec()).set_index(2),
      Log::new(b"log3".to_vec()).set_index(3),
    ];
    let log2 = logs[1].clone();
    s.store_logs(&logs).await.unwrap();

    assert_eq!(s.get_log(2).await.unwrap().unwrap(), log2);
  }

  pub async fn store_log<S: LogStorage<Id = SmolStr, Address = SocketAddr, Data = Vec<u8>>>(s: &S) {
    assert!(s.get_log(1).await.unwrap().is_none());

    // Set a mock raft log
    let log = Log::new(b"log1".to_vec()).set_index(1);
    s.store_log(&log).await.unwrap();

    // Fetch the first Raft index
    assert_eq!(s.get_log(1).await.unwrap().unwrap(), log);
  }

  pub async fn store_logs<S: LogStorage<Id = SmolStr, Address = SocketAddr, Data = Vec<u8>>>(
    s: &S,
  ) {
    // Set a mock raft log
    let logs = vec![
      Log::new(b"log1".to_vec()).set_index(1),
      Log::new(b"log2".to_vec()).set_index(2),
    ];
    s.store_logs(&logs).await.unwrap();

    assert_eq!(s.get_log(1).await.unwrap().unwrap(), logs[0]);
    assert_eq!(s.get_log(2).await.unwrap().unwrap(), logs[1]);
    assert!(s.get_log(3).await.unwrap().is_none());
  }

  pub async fn remove_range<S: LogStorage<Id = SmolStr, Address = SocketAddr, Data = Vec<u8>>>(
    s: &S,
  ) {
    // Set a mock raft log
    let logs = vec![
      Log::new(b"log1".to_vec()).set_index(1),
      Log::new(b"log2".to_vec()).set_index(2),
      Log::new(b"log3".to_vec()).set_index(3),
    ];
    s.store_logs(&logs).await.unwrap();

    s.remove_range(1..=2).await.unwrap();

    assert!(s.get_log(1).await.unwrap().is_none());
    assert!(s.get_log(2).await.unwrap().is_none());
    assert_eq!(s.get_log(3).await.unwrap().unwrap(), logs[2]);
  }

  pub async fn current_term<S: StableStorage<Id = SmolStr, Address = SocketAddr>>(s: &S) {
    assert!(s.current_term().await.unwrap().is_none());
    s.store_current_term(1).await.unwrap();
    assert_eq!(s.current_term().await.unwrap().unwrap(), 1);
  }

  pub async fn last_vote_term<S: StableStorage<Id = SmolStr, Address = SocketAddr>>(s: &S) {
    assert!(s.last_vote_term().await.unwrap().is_none());
    s.store_last_vote_term(1).await.unwrap();
    assert_eq!(s.last_vote_term().await.unwrap().unwrap(), 1);
  }

  pub async fn last_vote_candidate<S: StableStorage<Id = SmolStr, Address = SocketAddr>>(s: &S) {
    assert!(s.last_vote_candidate().await.unwrap().is_none());
    s.store_last_vote_candidate(Node::new(
      SmolStr::new("node1"),
      SocketAddr::from(([127, 0, 0, 1], 8080)),
    ))
    .await
    .unwrap();
    assert_eq!(
      s.last_vote_candidate().await.unwrap().unwrap(),
      Node::new(
        SmolStr::new("node1"),
        SocketAddr::from(([127, 0, 0, 1], 8080))
      )
    );
  }

  #[cfg(all(feature = "test", feature = "metrics"))]
  pub async fn oldest_log<S: LogStorage<Id = SmolStr, Address = SocketAddr, Data = Vec<u8>>>(
    store: &S,
  ) {
    use crate::storage::{LogKind, LogStorageExt};

    struct TestCase {
      name: &'static str,
      logs: Vec<Log<SmolStr, SocketAddr, Vec<u8>>>,
      want_idx: u64,
      want_err: bool,
    }

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
