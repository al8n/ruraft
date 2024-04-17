use agnostic::{async_std::AsyncStdRuntime, RuntimeLite};
use ruraft_core::{tests::run as run_unit_test, unit_tests};

fn run(fut: impl std::future::Future<Output = ()>) {
  run_unit_test(|fut| AsyncStdRuntime::block_on(fut), fut)
}

#[path = "async_std/sync.rs"]
mod sync;
