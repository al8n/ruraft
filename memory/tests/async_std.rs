use agnostic::{async_std::AsyncStdRuntime, Runtime};
use ruraft_core::{tests::run as run_unit_test, unit_tests};
use ruraft_memory::tests::*;

fn run(fut: impl std::future::Future<Output = ()>) {
  run_unit_test(|fut| AsyncStdRuntime::block_on(fut), fut)
}

unit_tests!(
  AsyncStdRuntime => run(
    memory_log_storage_first_index,
    memory_log_storage_last_index,
    memory_log_storage_get_log,
    memory_log_storage_store_log,
    memory_log_storage_store_logs,
    memory_log_storage_remove_range,
    memory_log_storage_oldest_log,
    memory_stable_storage_current_term,
    memory_stable_storage_last_vote_term,
    memory_stable_storage_last_vote_candidate,
    memory_snapshot_storage_create,
    memory_snapshot_storage_open_snapshot_twice,
  )
);
