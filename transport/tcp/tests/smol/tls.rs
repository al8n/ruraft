use super::*;

use ruraft_tcp::tests::tls::*;

unit_tests!(
  SmolRuntime => run(
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
  )
);
