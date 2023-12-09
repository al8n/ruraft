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
