use agnostic::tokio::TokioRuntime;
use ruraft_core::tests::run as run_unit_test;

fn run(fut: impl std::future::Future<Output = ()>) {
  let runtime = ::tokio::runtime::Runtime::new().unwrap();
  run_unit_test(|fut| runtime.block_on(fut), fut)
}
