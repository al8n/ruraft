//! [`Wire`](ruraft_core::transport::Wire) implementors for [`ruraft`](https://github.com/al8n/ruraft) crate.
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
#![deny(missing_docs, warnings)]
#![forbid(unsafe_code)]

/// MsgPack based [`Wire`](ruraft_core::transport::Wire) implementation.
#[cfg(feature = "rmp")]
#[cfg_attr(docsrs, doc(cfg(feature = "rmp")))]
pub mod rmp;

mod lpe;
pub use lpe::*;
