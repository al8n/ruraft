//! Durable [`SnapshotStorage`](ruraft_core::storage::SnapshotStorage) implementors
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
#![deny(missing_docs, warnings)]
#![forbid(unsafe_code)]

/// [`SnapshotStorage`](ruraft_core::storage::SnapshotStorage) implementor based on [`std::fs::File`].
pub mod sync;
