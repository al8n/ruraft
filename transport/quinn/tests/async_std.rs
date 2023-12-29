use agnostic::{async_std::AsyncStdRuntime, Runtime};
use ruraft_core::{tests::run as run_unit_test, unit_tests};
use ruraft_quinn::tests::*;

fn run(fut: impl std::future::Future<Output = ()>) {
  run_unit_test(|fut| AsyncStdRuntime::block_on(fut), fut)
}

unit_tests!(
  AsyncStdRuntime => run(
    start_and_shutdown,
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
