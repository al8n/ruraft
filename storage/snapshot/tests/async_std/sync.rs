#[cfg(unix)]
use ruraft_snapshot::sync::tests::file_snapshot_storage_bad_perm;
use ruraft_snapshot::sync::tests::*;

use super::*;

unit_tests!(
  AsyncStdRuntime => run(
    file_snapshot_storage_create_snapshot_missing_parent_dir,
    file_snapshot_storage_create_snapshot,
    file_snapshot_storage_cancel_snapshot,
    file_snapshot_storage_ordering,
    file_snapshot_storage_missing_parent_dir,
    file_snapshot_storage_retention,
  )
);

#[cfg(unix)]
#[test]
fn test_file_snapshot_storage_bad_perm() {
  run(file_snapshot_storage_bad_perm::<AsyncStdRuntime>())
}
