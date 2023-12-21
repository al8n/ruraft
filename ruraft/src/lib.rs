//!
#![forbid(unsafe_code)]
// #![deny(warnings)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

pub mod transport;

mod api;
pub use api::*;

#[cfg(any(feature = "test", test))]
pub mod tests {
  pub use ruraft_core::tests::*;
  pub use ruraft_memory::tests::*;
}

/// Re-exports [`ruraft-memory`](ruraft_memory) crate.
pub mod memory {
  pub use ruraft_memory::*;
}