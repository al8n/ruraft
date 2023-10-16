use agnostic::{async_std::AsyncStdRuntime, Runtime};
use ruraft_core::tests::run as run_unit_test;

fn run(fut: impl std::future::Future<Output = ()>) {
  run_unit_test(|fut| AsyncStdRuntime::block_on(fut), fut)
}
