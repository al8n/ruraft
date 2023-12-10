//! Lightwal is a lightweight write-ahead log for Ruraft.
#![forbid(unsafe_code)]
#![deny(warnings, missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

#[cfg(not(any(feature = "redb", feature = "sled", feature = "jammdb")))]
compile_error!("At least one of the following features must be enabled: redb, sled, jammdb");

/// [`jammdb`](::jammdb) backed [`StableStorage`](ruraft_core::storage::StableStorage) and [`LogStorage`](ruraft_core::storage::LogStorage) implementor.
#[cfg(feature = "jammdb")]
#[cfg_attr(docsrs, doc(cfg(feature = "jammdb")))]
pub mod jammdb;

/// [`redb`](::redb) backed [`StableStorage`](ruraft_core::storage::StableStorage) and [`LogStorage`](ruraft_core::storage::LogStorage) implementor.
#[cfg(feature = "redb")]
#[cfg_attr(docsrs, doc(cfg(feature = "redb")))]
pub mod redb;

/// [`sled`](::sled) backed [`StableStorage`](ruraft_core::storage::StableStorage) and [`LogStorage`](ruraft_core::storage::LogStorage) implementor.
#[cfg(feature = "sled")]
#[cfg_attr(docsrs, doc(cfg(feature = "sled")))]
pub mod sled;

const LAST_CANIDATE_ID: &str = "__ruraft_last_candidate_id__";
const LAST_CANIDATE_ADDR: &str = "__ruraft_last_candidate_addr__";
const LAST_VOTE_TERM: &str = "__ruraft_last_vote_term__";
const CURRENT_TERM: &str = "__ruraft_current_term__";

#[cfg(feature = "metrics")]
fn report_store_many(logs: usize, start: std::time::Instant) {
  let duration = start.elapsed();
  let nanos = duration.as_nanos(); // Get the elapsed time in nanoseconds
  let val = if nanos == 0 {
    0.0
  } else {
    (1_000_000_000.0 / nanos as f64) * logs as f64
  };

  metrics::histogram!("ruraft.lightwal.write_capacity", val);
  metrics::histogram!("ruraft.lightwal.store_logs", start.elapsed().as_secs_f64());
}

#[cfg(test)]
mod test {
  use std::net::SocketAddr;

  use ruraft_core::storage::{Log, LogStorage, StableStorage};
  use smol_str::SmolStr;

  pub async fn test_first_index<
    S: LogStorage<Id = SmolStr, Address = SocketAddr, Data = Vec<u8>>,
  >(
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

  pub async fn test_last_index<
    S: LogStorage<Id = SmolStr, Address = SocketAddr, Data = Vec<u8>>,
  >(
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

  pub async fn test_get_log<S: LogStorage<Id = SmolStr, Address = SocketAddr, Data = Vec<u8>>>(
    s: &S,
  ) {
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

  pub async fn test_store_log<S: LogStorage<Id = SmolStr, Address = SocketAddr, Data = Vec<u8>>>(
    s: &S,
  ) {
    assert!(s.get_log(1).await.unwrap().is_none());

    // Set a mock raft log
    let log = Log::new(b"log1".to_vec()).set_index(1);
    s.store_log(&log).await.unwrap();

    // Fetch the first Raft index
    assert_eq!(s.get_log(1).await.unwrap().unwrap(), log);
  }

  pub async fn test_store_logs<
    S: LogStorage<Id = SmolStr, Address = SocketAddr, Data = Vec<u8>>,
  >(
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

  pub async fn test_remove_range<
    S: LogStorage<Id = SmolStr, Address = SocketAddr, Data = Vec<u8>>,
  >(
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

  pub async fn test_current_term<S: StableStorage<Id = SmolStr, Address = SocketAddr>>(s: &S) {
    assert!(s.current_term().await.unwrap().is_none());
    s.store_current_term(1).await.unwrap();
    assert_eq!(s.current_term().await.unwrap().unwrap(), 1);
  }

  pub async fn test_last_vote_term<S: StableStorage<Id = SmolStr, Address = SocketAddr>>(s: &S) {
    assert!(s.last_vote_term().await.unwrap().is_none());
    s.store_last_vote_term(1).await.unwrap();
    assert_eq!(s.last_vote_term().await.unwrap().unwrap(), 1);
  }

  pub async fn test_last_vote_candidate<S: StableStorage<Id = SmolStr, Address = SocketAddr>>(
    s: &S,
  ) {
    assert!(s.last_vote_candidate().await.unwrap().is_none());
    s.store_last_vote_candidate(ruraft_core::Node::new(
      SmolStr::new("node1"),
      SocketAddr::from(([127, 0, 0, 1], 8080)),
    ))
    .await
    .unwrap();
    assert_eq!(
      s.last_vote_candidate().await.unwrap().unwrap(),
      ruraft_core::Node::new(
        SmolStr::new("node1"),
        SocketAddr::from(([127, 0, 0, 1], 8080))
      )
    );
  }
}
