//! [`Wire`](ruraft_core::transport::Wire) implementors for [`ruraft`](https://github.com/al8n/ruraft) crate.
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
#![deny(missing_docs)]
#![forbid(unsafe_code)]

#[cfg(test)]
macro_rules! unittest_encode_decode_request {
  ($wire:ident { $($variant:ident), + $(,)? }) => {
    $(
      paste::paste! {
        {
          let req = ::ruraft_core::transport::Request::$variant( ::ruraft_core::transport:: [< $variant Request >] ::__small());
          let bytes = <$wire <::smol_str::SmolStr, ::std::net::SocketAddr, ::std::vec::Vec<u8>> as ::ruraft_core::transport::Wire>::encode_request(&req).unwrap();
          let decoded_req = <$wire <::smol_str::SmolStr, ::std::net::SocketAddr, ::std::vec::Vec<u8>> as ::ruraft_core::transport::Wire>::decode_request(&bytes)
            .unwrap();
          assert_eq!(req, decoded_req);
        }

        {
          let req = ::ruraft_core::transport::Request::$variant( ::ruraft_core::transport:: [< $variant Request >] ::__large());
          let bytes = <$wire <::smol_str::SmolStr, ::std::net::SocketAddr, ::std::vec::Vec<u8>> as ::ruraft_core::transport::Wire>::encode_request(&req).unwrap();
          let decoded_req = <$wire <::smol_str::SmolStr, ::std::net::SocketAddr, ::std::vec::Vec<u8>> as ::ruraft_core::transport::Wire>::decode_request(&bytes)
            .unwrap();
          assert_eq!(req, decoded_req);
        }

        {
          let req = ::ruraft_core::transport::Request::$variant( ::ruraft_core::transport:: [< $variant Request >] ::__small());
          let mut bytes = Vec::new();
          <$wire <::smol_str::SmolStr, ::std::net::SocketAddr, ::std::vec::Vec<u8>> as ::ruraft_core::transport::Wire>::encode_request_to_writer(&req, &mut bytes).await.unwrap();
          let decoded_req = <$wire <::smol_str::SmolStr, ::std::net::SocketAddr, ::std::vec::Vec<u8>> as ::ruraft_core::transport::Wire>::decode_request_from_reader(&mut futures_util::io::Cursor::new(&bytes))
            .await
            .unwrap();
          assert_eq!(req, decoded_req);
        }

        {
          let req = ::ruraft_core::transport::Request::$variant( ::ruraft_core::transport:: [< $variant Request >] ::__large());
          let mut bytes = Vec::new();
          <$wire <::smol_str::SmolStr, ::std::net::SocketAddr, ::std::vec::Vec<u8>> as ::ruraft_core::transport::Wire>::encode_request_to_writer(&req, &mut bytes).await.unwrap();
          let decoded_req = <$wire <::smol_str::SmolStr, ::std::net::SocketAddr, ::std::vec::Vec<u8>> as ::ruraft_core::transport::Wire>::decode_request_from_reader(&mut futures_util::io::Cursor::new(&bytes))
            .await
            .unwrap();
          assert_eq!(req, decoded_req);
        }

        {
          let req = ::ruraft_core::transport::Request::$variant( ::ruraft_core::transport:: [< $variant Request >] ::__large());

          let bytes = <$wire <::smol_str::SmolStr, ::std::net::SocketAddr, ::std::vec::Vec<u8>> as ::ruraft_core::transport::Wire>::encode_request(&req).unwrap();
          let decoded_req = <$wire <::smol_str::SmolStr, ::std::net::SocketAddr, ::std::vec::Vec<u8>> as ::ruraft_core::transport::Wire>::decode_request_from_reader(&mut futures_util::io::Cursor::new(&bytes))
            .await
            .unwrap();
          assert_eq!(req, decoded_req);
        }

        {
          let req = ::ruraft_core::transport::Request::$variant( ::ruraft_core::transport:: [< $variant Request >] ::__large());
          let mut bytes = Vec::new();
          <$wire <::smol_str::SmolStr, ::std::net::SocketAddr, ::std::vec::Vec<u8>> as ::ruraft_core::transport::Wire>::encode_request_to_writer(&req, &mut bytes).await.unwrap();
          let decoded_req = <$wire <::smol_str::SmolStr, ::std::net::SocketAddr, ::std::vec::Vec<u8>> as ::ruraft_core::transport::Wire>::decode_request(&bytes)
            .unwrap();
          assert_eq!(req, decoded_req);
        }
      }
    )*
  };
}

#[cfg(test)]
macro_rules! unittest_encode_decode_response {
  ($wire:ident { $($variant:ident), + $(,)? }) => {
    $(
      paste::paste! {
        {
          let resp = ::ruraft_core::transport::Response::$variant(::ruraft_core::transport:: [< $variant Response >] ::__small());
          let bytes = <$wire <::smol_str::SmolStr, ::std::net::SocketAddr, ::std::vec::Vec<u8>> as ::ruraft_core::transport::Wire>::encode_response(&resp).unwrap();
          let decoded_resp = <$wire <::smol_str::SmolStr, ::std::net::SocketAddr, ::std::vec::Vec<u8>> as ::ruraft_core::transport::Wire>::decode_response(&bytes)
            .unwrap();
          assert_eq!(resp, decoded_resp);
        }

        {
          let resp = ::ruraft_core::transport::Response::$variant(::ruraft_core::transport:: [< $variant Response >] ::__large());
          let bytes = <$wire <::smol_str::SmolStr, ::std::net::SocketAddr, ::std::vec::Vec<u8>> as ::ruraft_core::transport::Wire>::encode_response(&resp).unwrap();
          let decoded_resp = <$wire <::smol_str::SmolStr, ::std::net::SocketAddr, ::std::vec::Vec<u8>> as ::ruraft_core::transport::Wire>::decode_response(&bytes)
            .unwrap();
          assert_eq!(resp, decoded_resp);
        }

        {
          let resp = ::ruraft_core::transport::Response::$variant(::ruraft_core::transport:: [< $variant Response >] ::__small());
          let mut bytes = Vec::new();
          <$wire <::smol_str::SmolStr, ::std::net::SocketAddr, ::std::vec::Vec<u8>> as ::ruraft_core::transport::Wire>::encode_response_to_writer(&resp, &mut bytes).await.unwrap();
          let decoded_resp = <$wire <::smol_str::SmolStr, ::std::net::SocketAddr, ::std::vec::Vec<u8>> as ::ruraft_core::transport::Wire>::decode_response_from_reader(&mut futures_util::io::Cursor::new(&bytes))
            .await
            .unwrap();
          assert_eq!(resp, decoded_resp);
        }

        {
          let resp = ::ruraft_core::transport::Response::$variant(::ruraft_core::transport:: [< $variant Response >] ::__large());
          let mut bytes = Vec::new();
          <$wire <::smol_str::SmolStr, ::std::net::SocketAddr, ::std::vec::Vec<u8>> as ::ruraft_core::transport::Wire>::encode_response_to_writer(&resp, &mut bytes).await.unwrap();
          let decoded_resp = <$wire <::smol_str::SmolStr, ::std::net::SocketAddr, ::std::vec::Vec<u8>> as ::ruraft_core::transport::Wire>::decode_response_from_reader(&mut futures_util::io::Cursor::new(&bytes))
            .await
            .unwrap();
          assert_eq!(resp, decoded_resp);
        }

        {
          let req = ::ruraft_core::transport::Response::$variant( ::ruraft_core::transport:: [< $variant Response >] ::__large());

          let bytes = <$wire <::smol_str::SmolStr, ::std::net::SocketAddr, ::std::vec::Vec<u8>> as ::ruraft_core::transport::Wire>::encode_response(&req).unwrap();
          let decoded_req = <$wire <::smol_str::SmolStr, ::std::net::SocketAddr, ::std::vec::Vec<u8>> as ::ruraft_core::transport::Wire>::decode_response_from_reader(&mut futures_util::io::Cursor::new(&bytes))
            .await
            .unwrap();
          assert_eq!(req, decoded_req);
        }

        {
          let req = ::ruraft_core::transport::Response::$variant( ::ruraft_core::transport:: [< $variant Response >] ::__large());
          let mut bytes = Vec::new();
          <$wire <::smol_str::SmolStr, ::std::net::SocketAddr, ::std::vec::Vec<u8>> as ::ruraft_core::transport::Wire>::encode_response_to_writer(&req, &mut bytes).await.unwrap();
          let decoded_req = <$wire <::smol_str::SmolStr, ::std::net::SocketAddr, ::std::vec::Vec<u8>> as ::ruraft_core::transport::Wire>::decode_response(&bytes)
            .unwrap();
          assert_eq!(req, decoded_req);
        }
      }
    )*
  };
}

/// MsgPack based [`Wire`](ruraft_core::transport::Wire) implementation.
#[cfg(feature = "rmp")]
#[cfg_attr(docsrs, doc(cfg(feature = "rmp")))]
pub mod rmp;

/// [`bincode`](https://crates.io/crates/bincode) based [`Wire`](ruraft_core::transport::Wire) implementation.
#[cfg(feature = "rmp")]
#[cfg_attr(docsrs, doc(cfg(feature = "rmp")))]
pub mod bincode;

mod lpe;
pub use lpe::*;
