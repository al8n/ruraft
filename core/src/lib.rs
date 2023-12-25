//!
#![allow(clippy::type_complexity)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
// #![deny(missing_docs)]
// #![deny(warnings)]
#![forbid(unsafe_code)]

pub use nodecraft::CheapClone;

const MESSAGE_SIZE_LEN: usize = core::mem::size_of::<u32>();

/// A trait for the data type that can be used as the user data of the Raft.
pub trait Data: Transformable + Send + Sync + 'static {}

impl<T: Transformable + Send + Sync + 'static> Data for T {}

/// Add `test` prefix to the predefined unit test fn with a given [`Runtime`](agonstic::Runtime)
#[cfg(any(feature = "test", test))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "test", test))))]
#[macro_export]
macro_rules! unit_tests {
  ($runtime:ty => $run:ident($($fn:ident), +$(,)?)) => {
    $(
      ::ruraft_core::tests::paste::paste! {
        #[test]
        fn [< test_ $fn >] () {
          $run($fn::<$runtime>());
        }
      }
    )*
  };
}

#[cfg(test)]
macro_rules! test_transformable_roundtrip {
  ($ty: ty { $init: expr }) => {{
    use crate::TestTransformable;

    <$ty>::assert_transformable(|| $init).await;
  }};
}

/// Errors implementation for the Raft.
pub mod error;

/// Configurations for the Raft.
pub mod options;

mod fsm;
pub use fsm::*;

/// Membership for the Raft cluster.
pub mod membership;
mod raft;
use nodecraft::Transformable;
pub use raft::*;

///
pub mod sidecar;
/// Storage layer traits and structs for the Raft.
pub mod storage;
/// Transport related trait and structs for the Raft.
pub mod transport;

/// utils functions or structs
pub mod utils;

#[cfg(feature = "metrics")]
mod metrics;

/// A helper trait for testing [`Transformable`] implementation.
#[cfg(any(feature = "test", test))]
#[doc(hidden)]
pub trait TestTransformable: Transformable + Eq + core::fmt::Debug + Sized {
  #[doc(hidden)]
  fn assert_transformable(init: impl FnOnce() -> Self) -> impl core::future::Future<Output = ()>
  where
    <Self as Transformable>::Error: core::fmt::Debug + Send + Sync + 'static,
  {
    async move {
      let val = init();
      let mut buf = std::vec![0; val.encoded_len()];
      val.encode(&mut buf).unwrap();
      let (_, decoded) = Self::decode(&buf).unwrap();
      assert_eq!(val, decoded);

      let mut buf = std::vec::Vec::new();
      val.encode_to_writer(&mut buf).unwrap();
      let (_, decoded) = Self::decode_from_reader(&mut buf.as_slice()).unwrap();
      assert_eq!(decoded, val);

      let mut buf = std::vec::Vec::new();
      val.encode_to_async_writer(&mut buf).await.unwrap();
      let (_, decoded) = Self::decode_from_async_reader(&mut buf.as_slice())
        .await
        .unwrap();
      assert_eq!(decoded, val);
    }
  }
}

#[cfg(test)]
impl<T: Transformable + Eq + core::fmt::Debug + Sized> TestTransformable for T {}

/// All unit test fns are exported in the `tests` module.
/// This module is used for users want to use other async runtime,
/// and want to use the test if ruraft also works with their runtime.
///
#[cfg(any(feature = "test", test))]
pub mod tests {
  pub use paste;

  /// Storage layer unit tests
  pub mod storage {
    pub use super::super::storage::tests::*;
  }

  // pub use super::transport::tests::*;

  /// Sequential access lock for tests.
  static ACCESS_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

  pub fn run<B, F>(block_on: B, fut: F)
  where
    B: FnOnce(F) -> F::Output,
    F: std::future::Future<Output = ()>,
  {
    let _mu = ACCESS_LOCK.lock();
    initialize_tests_tracing();
    block_on(fut);
  }

  pub fn initialize_tests_tracing() {
    use std::sync::Once;
    static TRACE: Once = Once::new();
    TRACE.call_once(|| {
      let filter = std::env::var("SHOWBIZ_TESTING_LOG").unwrap_or_else(|_| "debug".to_owned());
      tracing::subscriber::set_global_default(
        tracing_subscriber::fmt::fmt()
          .without_time()
          .with_line_number(true)
          .with_env_filter(filter)
          .with_file(false)
          .with_target(true)
          .with_ansi(true)
          .finish(),
      )
      .unwrap();
    });
  }
}
