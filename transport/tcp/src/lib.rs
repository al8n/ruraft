//! TCP transport implementation for [ruraft](https://github.com/al8n/ruraft).
#![deny(warnings, missing_docs)]
#![forbid(unsafe_code)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

/// TLS([`rustls`](https://github.com/smol-rs/async-rustls)) transport
#[cfg(feature = "tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "tls")))]
pub mod tls;

/// TLS([`native-tls`](https://github.com/async-email/async-native-tls)) transport
#[cfg(feature = "native-tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "native-tls")))]
pub mod native_tls;

mod tcp;
pub use tcp::*;

/// Re-export the [`ruraft_net`] crate.
pub mod net {
  pub use ruraft_net::*;
}

/// Exports unit tests to let users test transport implementation based on this crate.
#[cfg(any(feature = "test", test))]
pub mod tests {
  use agnostic::Runtime;
  use futures::Future;
  use ruraft_net::{resolver::SocketAddrResolver, tests, wire::LpeWire, Header, ProtocolVersion};
  use smol_str::SmolStr;
  use std::{
    net::SocketAddr,
    sync::atomic::{AtomicU16, Ordering},
  };

  static PORT: AtomicU16 = AtomicU16::new(19090);

  fn header1() -> Header<SmolStr, SocketAddr> {
    let addr = format!("127.0.0.1:{}", PORT.fetch_add(1, Ordering::SeqCst));
    Header::new(
      ProtocolVersion::V1,
      SmolStr::new("header1"),
      addr.parse().unwrap(),
    )
  }

  fn header2() -> Header<SmolStr, SocketAddr> {
    let addr = format!("127.0.0.1:{}", PORT.fetch_add(1, Ordering::SeqCst));
    Header::new(
      ProtocolVersion::V1,
      SmolStr::new("header2"),
      addr.parse().unwrap(),
    )
  }

  fn fake_header() -> Header<SmolStr, SocketAddr> {
    let addr = format!("127.0.0.1:{}", PORT.fetch_add(1, Ordering::SeqCst));
    Header::new(
      ProtocolVersion::V1,
      SmolStr::new("fake_header"),
      addr.parse().unwrap(),
    )
  }

  fn tcp_stream_layer<R: Runtime>() -> crate::tcp::Tcp<R> {
    crate::tcp::Tcp::new()
  }

  #[cfg(feature = "native-tls")]
  fn native_tls_stream_layer<R: Runtime>() -> crate::native_tls::NativeTls<R> {
    todo!()
  }

  #[cfg(feature = "tls")]
  fn tls_stream_layer<R: Runtime>() -> crate::tls::Tls<R> {
    todo!()
  }

  macro_rules! tests_mod {
    ($mod:ident::$ty:ident::$stream_layer:ident) => {
      #[doc = concat!("Unit tests for `", stringify!($mod), "`")]
      pub mod $mod {
        use super::*;

        #[doc = concat!("Test start and shutdown for [`", stringify!($ty), "`](", stringify!(crate::$mod::$ty), ").")]
        pub async fn start_and_shutdown<R: Runtime>()
        where
          <R::Sleep as Future>::Output: Send + 'static,
        {
          tests::start_and_shutdown::<_, R>($stream_layer::<R>()).await;
        }

        #[doc = concat!("Test fastpath heartbeat for [`", stringify!($ty), "`](", stringify!(crate::$mod::$ty), ").")]
        pub async fn heartbeat_fastpath<R: Runtime>()
        where
          <R::Sleep as Future>::Output: Send + 'static,
        {
          tests::heartbeat_fastpath::<_, _, Vec<u8>, _, LpeWire<_, _, _>>(header1(), $stream_layer::<R>(), SocketAddrResolver::<R>::new(), header2(), $stream_layer::<R>(), SocketAddrResolver::<R>::new()).await;
        }

        #[doc = concat!("Test close streams for [`", stringify!($ty), "`](", stringify!(crate::$mod::$ty), ").")]
        pub async fn close_streams<R: Runtime>()
        where
          <R::Sleep as Future>::Output: Send + 'static,
        {
          tests::close_streams::<_, _, Vec<u8>, _, LpeWire<_, _, _>>(header1(), $stream_layer::<R>(), SocketAddrResolver::<R>::new(), header2(), $stream_layer::<R>(), SocketAddrResolver::<R>::new()).await;
        }

        #[doc = concat!("Test append entries for [`", stringify!($ty), "`](", stringify!(crate::$mod::$ty), ").")]
        pub async fn append_entries<R: Runtime>()
        where
          <R::Sleep as Future>::Output: Send + 'static,
        {
          tests::append_entries::<_, _, Vec<u8>, _, LpeWire<_, _, _>>(header1(), $stream_layer::<R>(), SocketAddrResolver::<R>::new(), header2(), $stream_layer::<R>(), SocketAddrResolver::<R>::new()).await;
        }

        #[doc = concat!("Test append entries pipeline for [`", stringify!($ty), "`](", stringify!(crate::$mod::$ty), ").")]
        pub async fn append_entries_pipeline<R: Runtime>()
        where
          <R::Sleep as Future>::Output: Send + 'static,
        {
          tests::append_entries_pipeline::<_, _, Vec<u8>, _, LpeWire<_, _, _>>(header1(), $stream_layer::<R>(), SocketAddrResolver::<R>::new(), header2(), $stream_layer::<R>(), SocketAddrResolver::<R>::new()).await;
        }

        #[doc = concat!("Test append entries pipeline and close streams for [`", stringify!($ty), "`](", stringify!(crate::$mod::$ty), ").")]
        pub async fn append_entries_pipeline_close_streams<R: Runtime>()
        where
          <R::Sleep as Future>::Output: Send + 'static,
        {
          tests::append_entries_pipeline_close_streams::<_, _, Vec<u8>, _, LpeWire<_, _, _>>(header1(), $stream_layer::<R>(), SocketAddrResolver::<R>::new(), header2(), $stream_layer::<R>(), SocketAddrResolver::<R>::new()).await;
        }

        #[doc = concat!("Test append entries pipeline max rpc inflight for [`", stringify!($ty), "`](", stringify!(crate::$mod::$ty), ").")]
        pub async fn append_entries_pipeline_max_rpc_inflight_default<R: Runtime>()
        where
          <R::Sleep as Future>::Output: Send + 'static,
        {
          tests::append_entries_pipeline_max_rpc_inflight_default::<_, _, Vec<u8>, _, LpeWire<_, _, _>>(header1(), $stream_layer::<R>(), SocketAddrResolver::<R>::new(), header2(), $stream_layer::<R>(), SocketAddrResolver::<R>::new()).await;
        }

        #[doc = concat!("Test append entries pipeline max rpc inflight for [`", stringify!($ty), "`](", stringify!(crate::$mod::$ty), ").")]
        pub async fn append_entries_pipeline_max_rpc_inflight_0<R: Runtime>()
        where
          <R::Sleep as Future>::Output: Send + 'static,
        {
          tests::append_entries_pipeline_max_rpc_inflight_0::<_, _, Vec<u8>, _, LpeWire<_, _, _>>(header1(), $stream_layer::<R>(), SocketAddrResolver::<R>::new(), header2(), $stream_layer::<R>(), SocketAddrResolver::<R>::new()).await;
        }

        #[doc = concat!("Test append entries pipeline max rpc inflight for [`", stringify!($ty), "`](", stringify!(crate::$mod::$ty), ").")]
        pub async fn append_entries_pipeline_max_rpc_inflight_some<R: Runtime>()
        where
          <R::Sleep as Future>::Output: Send + 'static,
        {
          tests::append_entries_pipeline_max_rpc_inflight_some::<_, _, Vec<u8>, _, LpeWire<_, _, _>>(header1(), $stream_layer::<R>(), SocketAddrResolver::<R>::new(), header2(), $stream_layer::<R>(), SocketAddrResolver::<R>::new()).await;
        }

        #[doc = concat!("Test append entries pipeline max rpc inflight for [`", stringify!($ty), "`](", stringify!(crate::$mod::$ty), ").")]
        pub async fn append_entries_pipeline_max_rpc_inflight_one<R: Runtime>()
        where
          <R::Sleep as Future>::Output: Send + 'static,
        {
          tests::append_entries_pipeline_max_rpc_inflight_one::<_, _, Vec<u8>, _, LpeWire<_, _, _>>(header1(), $stream_layer::<R>(), SocketAddrResolver::<R>::new(), header2(), $stream_layer::<R>(), SocketAddrResolver::<R>::new()).await;
        }

        #[doc = concat!("Test install snapshot for [`", stringify!($ty), "`](", stringify!(crate::$mod::$ty), ").")]
        pub async fn install_snapshot<R: Runtime>()
        where
          <R::Sleep as Future>::Output: Send + 'static,
        {
          tests::install_snapshot::<_, _, Vec<u8>, _, LpeWire<_, _, _>>(header1(), $stream_layer::<R>(), SocketAddrResolver::<R>::new(), header2(), $stream_layer::<R>(), SocketAddrResolver::<R>::new(), fake_header()).await;
        }

        #[doc = concat!("Test vote for [`", stringify!($ty), "`](", stringify!(crate::$mod::$ty), ").")]
        pub async fn vote<R: Runtime>()
        where
          <R::Sleep as Future>::Output: Send + 'static,
        {
          tests::vote::<_, _, Vec<u8>, _, LpeWire<_, _, _>>(header1(), $stream_layer::<R>(), SocketAddrResolver::<R>::new(), header2(), $stream_layer::<R>(), SocketAddrResolver::<R>::new(), fake_header()).await;
        }

        #[doc = concat!("Test pooled connection for [`", stringify!($ty), "`](", stringify!(crate::$mod::$ty), ").")]
        pub async fn pooled_conn<R: Runtime>()
        where
          <R::Sleep as Future>::Output: Send + 'static,
        {
          tests::pooled_conn::<_, _, Vec<u8>, _, LpeWire<_, _, _>>(header1(), $stream_layer::<R>(), SocketAddrResolver::<R>::new(), header2(), $stream_layer::<R>(), SocketAddrResolver::<R>::new()).await;
        }
      }
    };
  }

  tests_mod!(tcp::TcpStreamLayer::tcp_stream_layer);

  #[cfg(feature = "native-tls")]
  tests_mod!(native_tls::NativeTls::native_tls_stream_layer);

  #[cfg(feature = "tls")]
  tests_mod!(tls::Tls::tls_stream_layer);
}
