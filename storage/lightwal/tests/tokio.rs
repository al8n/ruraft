use agnostic::tokio::TokioRuntime;
use ruraft_core::{tests::run as run_unit_test, unit_tests};

fn run(fut: impl std::future::Future<Output = ()>) {
  let runtime = ::tokio::runtime::Runtime::new().unwrap();
  run_unit_test(|fut| runtime.block_on(fut), fut)
}

#[path = "tokio/jammdb.rs"]
mod jammdb;
#[path = "tokio/redb.rs"]
mod redb;
#[path = "tokio/sled.rs"]
mod sled;
