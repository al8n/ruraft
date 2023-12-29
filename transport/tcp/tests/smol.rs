use agnostic::{smol::SmolRuntime, Runtime};
use ruraft_core::{tests::run as run_unit_test, unit_tests};

fn run(fut: impl std::future::Future<Output = ()>) {
  run_unit_test(|fut| SmolRuntime::block_on(fut), fut)
}

#[path = "smol/tcp.rs"]
mod tcp;

#[path = "smol/tls.rs"]
#[cfg(feature = "tls")]
mod tls;

#[path = "smol/native_tls.rs"]
#[cfg(feature = "native-tls")]
mod native_tls;
