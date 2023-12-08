//! Lightwal is a lightweight write-ahead log for Ruraft.
#![forbid(unsafe_code)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

#[cfg(not(any(feature = "redb", feature = "sled", feature = "jammdb")))]
compile_error!("At least one of the following features must be enabled: redb, sled, jammdb");

#[cfg(feature = "jammdb")]
mod jammdb_;
#[cfg(feature = "jammdb")]
pub use jammdb_::*;

#[cfg(feature = "redb")]
mod redb_;
#[cfg(feature = "redb")]
pub use redb_::*;

#[cfg(feature = "sled")]
mod sled_;
#[cfg(feature = "sled")]
pub use sled_::*;

const LAST_CANIDATE_ID: &str = "__ruraft_last_candidate_id__";
const LAST_CANIDATE_ADDR: &str = "__ruraft_last_candidate_addr__";
const LAST_VOTED_FOR: &str = "__ruraft_last_vote_term__";
const CURRENT_TERM: &str = "__ruraft_current_term__";
