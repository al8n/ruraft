//!
#![forbid(unsafe_code)]
// #![deny(missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

pub mod transport;

pub mod storage;

#[cfg(any(feature = "test", test))]
pub mod tests {
  pub use super::storage::tests::*;
  pub use ruraft_core::tests::*;
}

#[test]
fn test_() {}
