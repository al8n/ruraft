//!
#![forbid(unsafe_code)]
// #![deny(missing_docs)]

mod command;

/// Errors implementation for the Raft.
pub mod error;

/// Configurations for the Raft.
pub mod options;

mod fsm;
/// Membership for the Raft cluster.
pub mod membership;
mod raft;
pub mod storage;
mod transport;
/// utils functions or structs
pub mod utils;

/// Node id
pub struct NodeId {}

/// All unit test fns are exported in the `tests` module.
/// This module is used for users want to use other async runtime,
/// and want to use the test if ruraft also works with their runtime.
///
#[cfg(feature = "test")]
pub mod tests {
  pub use super::storage::tests::snapshot::*;

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
