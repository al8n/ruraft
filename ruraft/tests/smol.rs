use agnostic::{smol::SmolRuntime, RuntimeLite};
use ruraft_core::tests::run as run_unit_test;

fn run(fut: impl std::future::Future<Output = ()>) {
  run_unit_test(|fut| SmolRuntime::block_on(fut), fut)
}
