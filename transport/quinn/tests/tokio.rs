use agnostic::tokio::TokioRuntime;
use ruraft_core::{tests::run as run_unit_test, unit_tests};
use ruraft_quinn::tests::*;

fn run(fut: impl std::future::Future<Output = ()>) {
  let runtime = ::tokio::runtime::Runtime::new().unwrap();
  run_unit_test(|fut| runtime.block_on(fut), fut)
}

unit_tests!(
  TokioRuntime => run(
    start_and_shutdown, // yes
    heartbeat_fastpath,
    close_streams,
    append_entries,
    append_entries_pipeline,
    append_entries_pipeline_close_streams,
    append_entries_pipeline_max_rpc_inflight_0,
    append_entries_pipeline_max_rpc_inflight_one,
    append_entries_pipeline_max_rpc_inflight_default,
    append_entries_pipeline_max_rpc_inflight_some,
    install_snapshot,
    pooled_conn,
    vote,
    timeout_now,
  )
);

#[test]
fn test() {
  run(append_entries::<TokioRuntime>())
}
