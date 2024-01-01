pub use paste;

/// Storage layer unit tests
pub mod storage {
  pub use super::super::storage::tests::*;
}

/// Transport layer unit tests
pub mod transport {
  pub use super::super::transport::tests::*;
}

/// Sequential access lock for tests.
static ACCESS_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

/// Run the unit test with a given async runtime sequentially.
pub fn run<B, F>(block_on: B, fut: F)
where
  B: FnOnce(F) -> F::Output,
  F: std::future::Future<Output = ()>,
{
  let _mu = ACCESS_LOCK.lock();
  initialize_tests_tracing();
  block_on(fut);
}

/// Initialize the tracing subscriber for the unit tests.
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
