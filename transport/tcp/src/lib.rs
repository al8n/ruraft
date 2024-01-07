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
  use ruraft_net::{
    resolver::SocketAddrResolver, tests, tests_mod, wire::LpeWire, Header, ProtocolVersion,
  };
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
    Header::new(
      ProtocolVersion::V1,
      SmolStr::new("header2"),
      "127.0.0.1:0".parse().unwrap(),
    )
  }

  async fn tcp_stream_layer<R: Runtime>() -> crate::tcp::Tcp<R> {
    crate::tcp::Tcp::new()
  }

  #[cfg(feature = "native-tls")]
  async fn native_tls_stream_layer<R: Runtime>() -> crate::native_tls::NativeTls<R> {
    use async_native_tls::{Identity, TlsAcceptor, TlsConnector};

    use crate::native_tls::NativeTls;

    let keys = test_cert_gen::gen_keys();

    let identity = Identity::from_pkcs12(
      &keys.server.cert_and_key_pkcs12.pkcs12.0,
      &keys.server.cert_and_key_pkcs12.password,
    )
    .unwrap();

    let acceptor = TlsAcceptor::from(::native_tls::TlsAcceptor::new(identity).unwrap());
    let connector = TlsConnector::new().danger_accept_invalid_certs(true);

    NativeTls::new("localhost".to_string(), acceptor, connector)
  }

  #[cfg(feature = "tls")]
  async fn tls_stream_layer<R: Runtime>() -> crate::tls::Tls<R> {
    use std::sync::Arc;

    use crate::tls::Tls;
    use rustls::{
      client::danger::ServerCertVerifier,
      pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
      SignatureScheme,
    };

    #[derive(Debug)]
    struct SkipServerVerification;

    impl SkipServerVerification {
      fn new() -> Arc<Self> {
        Arc::new(Self)
      }
    }

    impl ServerCertVerifier for SkipServerVerification {
      fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
      ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
      }

      fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
      ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
      }

      fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
      ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
      }

      fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
          SignatureScheme::RSA_PKCS1_SHA1,
          SignatureScheme::ECDSA_SHA1_Legacy,
          SignatureScheme::RSA_PKCS1_SHA256,
          SignatureScheme::ECDSA_NISTP256_SHA256,
          SignatureScheme::RSA_PKCS1_SHA384,
          SignatureScheme::ECDSA_NISTP384_SHA384,
          SignatureScheme::RSA_PKCS1_SHA512,
          SignatureScheme::ECDSA_NISTP521_SHA512,
          SignatureScheme::RSA_PSS_SHA256,
          SignatureScheme::RSA_PSS_SHA384,
          SignatureScheme::RSA_PSS_SHA512,
          SignatureScheme::ED25519,
          SignatureScheme::ED448,
        ]
      }
    }

    let certs = test_cert_gen::gen_keys();

    let cfg = rustls::ServerConfig::builder()
      .with_no_client_auth()
      .with_single_cert(
        vec![CertificateDer::from(
          certs.server.cert_and_key.cert.get_der().to_vec(),
        )],
        PrivateKeyDer::from(PrivatePkcs8KeyDer::from(
          certs.server.cert_and_key.key.get_der().to_vec(),
        )),
      )
      .expect("bad certificate/key");
    let acceptor = futures_rustls::TlsAcceptor::from(Arc::new(cfg));

    let cfg = rustls::ClientConfig::builder()
      .dangerous()
      .with_custom_certificate_verifier(SkipServerVerification::new())
      .with_no_client_auth();
    let connector = futures_rustls::TlsConnector::from(Arc::new(cfg));
    Tls::new(
      rustls::pki_types::ServerName::IpAddress(
        "127.0.0.1".parse::<std::net::IpAddr>().unwrap().into(),
      ),
      acceptor,
      connector,
    )
  }

  tests_mod!(tcp::TcpStreamLayer::tcp_stream_layer);

  #[cfg(feature = "native-tls")]
  tests_mod!(native_tls::NativeTls::native_tls_stream_layer);

  #[cfg(feature = "tls")]
  tests_mod!(tls::Tls::tls_stream_layer);
}
