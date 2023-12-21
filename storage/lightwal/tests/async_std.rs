use agnostic::{async_std::AsyncStdRuntime, Runtime};
use ruraft_core::{tests::run as run_unit_test, unit_tests};

fn run(fut: impl std::future::Future<Output = ()>) {
  run_unit_test(|fut| AsyncStdRuntime::block_on(fut), fut)
}

#[path = "async_std/jammdb.rs"]
mod jammdb;
#[path = "async_std/redb.rs"]
mod redb;
#[path = "async_std/sled.rs"]
mod sled;
