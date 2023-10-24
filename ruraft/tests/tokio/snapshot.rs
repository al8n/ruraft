#[cfg(unix)]
use ruraft::tests::test_file_snapshot_storage_bad_perm;
use ruraft::tests::{
  test_file_snapshot_storage_cancel_snapshot, test_file_snapshot_storage_create_snapshot,
  test_file_snapshot_storage_create_snapshot_missing_parent_dir,
  test_file_snapshot_storage_missing_parent_dir, test_file_snapshot_storage_ordering,
  test_file_snapshot_storage_retention, test_memory_snapshot_storage_create,
  test_memory_snapshot_storage_open_snapshot_twice,
};

use super::*;

#[test]
fn file_snapshot_storage_create_snapshot_missing_parent_dir() {
  run(test_file_snapshot_storage_create_snapshot_missing_parent_dir::<TokioRuntime>())
}

#[test]
fn file_snapshot_storage_create_snapshot() {
  run(test_file_snapshot_storage_create_snapshot::<TokioRuntime>())
}

#[test]
fn file_snapshot_storage_cancel_snapshot() {
  run(test_file_snapshot_storage_cancel_snapshot::<TokioRuntime>())
}

#[test]
fn file_snapshot_storage_ordering() {
  run(test_file_snapshot_storage_ordering::<TokioRuntime>())
}

#[test]
fn file_snapshot_storage_missing_parent_dir() {
  run(test_file_snapshot_storage_missing_parent_dir::<TokioRuntime>())
}

#[test]
fn file_snapshot_storage_retention() {
  run(test_file_snapshot_storage_retention::<TokioRuntime>())
}

#[cfg(unix)]
#[test]
fn file_snapshot_storage_bad_perm() {
  run(test_file_snapshot_storage_bad_perm::<TokioRuntime>())
}

#[test]
fn memory_snapshot_storage_open_snapshot_twice() {
  run(test_memory_snapshot_storage_open_snapshot_twice::<
    TokioRuntime,
  >())
}

#[test]
fn memory_snapshot_storage_create() {
  run(test_memory_snapshot_storage_create::<TokioRuntime>())
}