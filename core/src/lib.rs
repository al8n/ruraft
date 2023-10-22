//!
#![allow(clippy::type_complexity)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
// #![deny(missing_docs)]
#![forbid(unsafe_code)]

/// Errors implementation for the Raft.
pub mod error;

/// Configurations for the Raft.
pub mod options;

mod fsm;
pub use fsm::*;

/// Membership for the Raft cluster.
pub mod membership;
mod raft;
pub use raft::*;

///
pub mod sidecar;
pub mod storage;
/// Transport related trait and structs for the Raft.
pub mod transport;

/// utils functions or structs
pub mod utils;

mod commitment;

/// All unit test fns are exported in the `tests` module.
/// This module is used for users want to use other async runtime,
/// and want to use the test if ruraft also works with their runtime.
///
#[cfg(feature = "test")]
pub mod tests {
  pub use super::storage::tests::*;
  pub use super::transport::tests::*;

  /// Sequential access lock for tests.
  static ACCESS_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

  pub fn run<B, F>(block_on: B, fut: F)
  where
    B: FnOnce(F) -> F::Output,
    F: std::future::Future<Output = ()>,
  {
    let _mu = ACCESS_LOCK.lock();
    initialize_tests_tracing();
    block_on(fut);
  }

  pub fn initialize_tests_tracing() {
    use std::sync::Once;
    static TRACE: Once = Once::new();
    TRACE.call_once(|| {
      let filter = std::env::var("SHOWBIZ_TESTING_LOG").unwrap_or_else(|_| "debug".to_owned());
      tracing::subscriber::set_global_default(
        tracing_subscriber::fmt::fmt()
          .without_time()
          .with_line_number(true)
          .with_env_filter(filter)
          .with_file(false)
          .with_target(true)
          .with_ansi(true)
          .finish(),
      )
      .unwrap();
    });
  }
}
