use agnostic::{smol::SmolRuntime, RuntimeLite};
use ruraft_core::{tests::run as run_unit_test, unit_tests};

fn run(fut: impl std::future::Future<Output = ()>) {
  run_unit_test(|fut| SmolRuntime::block_on(fut), fut)
}

#[path = "smol/jammdb.rs"]
mod jammdb;
#[path = "smol/redb.rs"]
mod redb;
#[path = "smol/sled.rs"]
mod sled;
