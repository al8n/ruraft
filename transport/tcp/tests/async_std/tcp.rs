use super::*;

use ruraft_tcp::tests::tcp::*;

unit_tests!(
  AsyncStdRuntime => run(
    start_and_shutdown,
    // heartbeat_fastpath,
    // close_streams,
    append_entries,
    append_entries_pipeline,
    // append_entries_pipeline_close_streams,
    // append_entries_pipeline_max_rpc_inflight,
    install_snapshot,
    vote,
  )
);
