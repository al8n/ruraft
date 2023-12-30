//!
#![forbid(unsafe_code)]
// #![deny(warnings)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

pub mod transport;

mod api;
pub use api::*;

/// All unit test fns are exported in the `tests` module.
/// This module is used for users want to use other async runtime,
/// and want to use the test if ruraft also works with their runtime.
///
#[cfg(any(feature = "test", test))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "test", test))))]
pub mod tests;

/// Re-exports [`ruraft-memory`](ruraft_memory) crate.
pub mod memory {
  pub use ruraft_memory::*;
}
