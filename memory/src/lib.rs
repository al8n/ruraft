//! Inmemory [`SnapshotStorage`](ruraft_core::storage::SnapshotStorage), [`StableStorage`](ruraft_core::storage::StableStorage),
//! [`LogStorage`](ruraft_core::storage::LogStorage) and [`Transport`](ruraft_core::transport::Transport) implementors.
//!
//! > You should only use these implementators when testing and never use them in a production environment.
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
#![deny(missing_docs, warnings)]
#![forbid(unsafe_code)]

/// Memory based [`Storage`](ruraft_core::storage::Storage) implementor.
pub mod storage;

/// Memory based [`Transport`](ruraft_core::transport::Transport) implementor.
pub mod transport;

/// Exports unit tests to let users test [`MemorySnapshotStorage`] implementation if they want to
/// use their own [`agnostic::Runtime`] implementation.
#[cfg(any(feature = "test", test))]
#[cfg_attr(docsrs, doc(cfg(any(test, feature = "test"))))]
pub mod tests {
  use std::net::SocketAddr;

  use agnostic::Runtime;
  use futures::Future;
  use ruraft_core::{membership::Membership, options::SnapshotVersion, tests::storage::*};
  use smol_str::SmolStr;

  use crate::{
    storage::{
      log::MemoryLogStorage, snapshot::MemorySnapshotStorage, stable::MemoryStableStorage,
    },
    transport::{MemoryAddress, MemoryAddressResolver},
  };

  pub use super::transport::tests::*;

  /// [`MemoryAddressResolver`] test
  ///
  /// - Description:
  ///   - Test that the [`MemoryTransport`] [`MemoryAddress`] and [`MemoryAddressResolver`] can send and receive messages, and the timeout also works.
  pub async fn memory_transport_resolver_address_write_timeout<R: Runtime>()
  where
    <R::Sleep as Future>::Output: Send + 'static,
  {
    let resolver = MemoryAddressResolver::<_, R>::new();
    memory_transport_write_timeout(
      SmolStr::from("id1"),
      MemoryAddress::new(),
      resolver,
      SmolStr::from("id2"),
      MemoryAddress::new(),
      resolver,
    )
    .await
  }

  /// [`MemoryLogStorage`] test
  ///
  /// Description:
  ///
  /// Test get first index
  pub async fn memory_log_storage_first_index<R: Runtime>() {
    let s = MemoryLogStorage::<_, _, _, R>::new();
    first_index(&s).await
  }

  /// [`MemoryLogStorage`] test
  ///
  /// Description:
  ///
  /// Test get last index
  pub async fn memory_log_storage_last_index<R: Runtime>() {
    let s = MemoryLogStorage::<_, _, _, R>::new();
    last_index(&s).await
  }

  /// [`MemoryLogStorage`] test
  ///
  /// Description:
  ///
  /// Test get log
  pub async fn memory_log_storage_get_log<R: Runtime>() {
    let s = MemoryLogStorage::<_, _, _, R>::new();
    get_log(&s).await
  }

  /// [`MemoryLogStorage`] test
  ///
  /// Description:
  ///
  /// Test store log
  pub async fn memory_log_storage_store_log<R: Runtime>() {
    let s = MemoryLogStorage::<_, _, _, R>::new();
    store_log(&s).await
  }

  /// [`MemoryLogStorage`] test
  ///
  /// Description:
  ///
  /// Test store logs
  pub async fn memory_log_storage_store_logs<R: Runtime>() {
    let s = MemoryLogStorage::<_, _, _, R>::new();
    store_logs(&s).await
  }

  /// [`MemoryLogStorage`] test
  ///
  /// Description:
  ///
  /// Test remove logs by range
  pub async fn memory_log_storage_remove_range<R: Runtime>() {
    let s = MemoryLogStorage::<_, _, _, R>::new();
    remove_range(&s).await
  }

  /// [`MemoryLogStorage`] test
  ///
  /// Description:
  ///
  /// Test oldest log
  #[cfg(all(feature = "test", feature = "metrics"))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "test", feature = "metrics"))))]
  pub async fn memory_log_storage_oldest_log<R: Runtime>() {
    let s = MemoryLogStorage::<_, _, _, R>::new();
    oldest_log(&s).await
  }

  /// [`MemoryStableStorage`] test
  ///
  /// Description:
  ///
  /// Test current term
  pub async fn memory_stable_storage_current_term<R: Runtime>() {
    let s = MemoryStableStorage::<_, _, R>::new();
    current_term(&s).await
  }

  /// [`MemoryStableStorage`] test
  ///
  /// Description:
  ///
  /// Test last vote term
  pub async fn memory_stable_storage_last_vote_term<R: Runtime>() {
    let s = MemoryStableStorage::<_, _, R>::new();
    last_vote_term(&s).await
  }

  /// [`MemoryStableStorage`] test
  ///
  /// Description:
  ///
  /// Test last vote candidate
  pub async fn memory_stable_storage_last_vote_candidate<R: Runtime>() {
    let s = MemoryStableStorage::<_, _, R>::new();
    last_vote_candidate(&s).await
  }

  /// Test [`MemorySnapshotStorage`].
  ///
  /// Description:
  /// - create snapshot
  pub async fn memory_snapshot_storage_create<R: Runtime>() {
    use futures::io::{AsyncReadExt, AsyncWriteExt};
    use ruraft_core::storage::{SnapshotSink, SnapshotStorage};

    let snap = MemorySnapshotStorage::<SmolStr, SocketAddr, R>::new();

    // check no snapshots
    let snaps = snap.list().await.unwrap();
    assert_eq!(snaps.len(), 0, "did not expect any snapshots");

    // create a new sink
    let mut sink = snap
      .create(SnapshotVersion::V1, 10, 3, Membership::__empty(), 2)
      .await
      .unwrap();

    // The sink is not done, should not be in a list!
    let snaps = snap.list().await.unwrap();
    assert_eq!(snaps.len(), 1, "should always be 1 snapshot");

    // Write to the sink
    sink.write_all(b"first\n").await.unwrap();
    sink.write_all(b"second\n").await.unwrap();

    // Done!
    sink.close().await.unwrap();

    // Should have a snapshot
    let snaps = snap.list().await.unwrap();
    assert_eq!(snaps.len(), 1, "expect a snapshots");

    // check the latest
    let latest = snaps.first().unwrap();
    assert_eq!(latest.index, 10, "expected index 10");
    assert_eq!(latest.term, 3, "expected term 3");
    assert_eq!(latest.membership_index, 2, "expected membership index 2");
    assert_eq!(latest.size, 13, "expected size 13");

    // Read the snapshot
    let mut source = snap.open(&latest.id()).await.unwrap();
    let mut buf = vec![];
    source.read_to_end(&mut buf).await.unwrap();

    // Ensure a match
    assert_eq!(buf, b"first\nsecond\n", "expected contents to match");
  }

  /// Test [`MemorySnapshotStorage`].
  ///
  /// Description:
  /// - open snapshot twice
  pub async fn memory_snapshot_storage_open_snapshot_twice<R: Runtime>() {
    use futures::io::{AsyncReadExt, AsyncWriteExt};
    use ruraft_core::storage::{SnapshotSink, SnapshotStorage};

    let snap = MemorySnapshotStorage::<SmolStr, SocketAddr, R>::new();

    // create a new sink
    let mut sink = snap
      .create(SnapshotVersion::V1, 10, 3, Membership::__empty(), 2)
      .await
      .unwrap();

    // Write to the sink
    sink.write_all(b"data\n").await.unwrap();
    sink.cancel().await.unwrap();

    // Read the snapshot a first time
    let mut source = snap.open(&sink.id()).await.unwrap();

    // Read out everything
    let mut buf = vec![];
    source.read_to_end(&mut buf).await.unwrap();

    // Ensure a match
    assert_eq!(buf, b"data\n", "expected contents to match");

    // Read the snapshot a second time
    let mut source = snap.open(&sink.id()).await.unwrap();
    // Read out everything
    let mut buf = vec![];
    source.read_to_end(&mut buf).await.unwrap();
    assert_eq!(buf, b"data\n", "expected contents to match");
  }
}
