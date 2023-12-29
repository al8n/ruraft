use agnostic::{async_std::AsyncStdRuntime, Runtime};
use ruraft_core::{tests::run as run_unit_test, unit_tests};

fn run(fut: impl std::future::Future<Output = ()>) {
  run_unit_test(|fut| AsyncStdRuntime::block_on(fut), fut)
}

#[path = "async_std/tcp.rs"]
mod tcp;

#[path = "async_std/tls.rs"]
#[cfg(feature = "tls")]
mod tls;

#[path = "async_std/native_tls.rs"]
#[cfg(feature = "native-tls")]
mod native_tls;
