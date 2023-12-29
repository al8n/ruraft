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

  fn fake_header() -> Header<SmolStr, SocketAddr> {
    let addr = format!("127.0.0.1:{}", PORT.fetch_add(1, Ordering::SeqCst));
    Header::new(
      ProtocolVersion::V1,
      SmolStr::new("fake_header"),
      addr.parse().unwrap(),
    )
  }

  async fn tcp_stream_layer<R: Runtime>() -> crate::tcp::Tcp<R> {
    crate::tcp::Tcp::new()
  }

  #[cfg(feature = "tls")]
  fn key_path() -> std::path::PathBuf {
    std::env::current_dir().unwrap().join("tests/server.key")
  }

  #[cfg(feature = "tls")]
  fn cert_path() -> std::path::PathBuf {
    std::env::current_dir().unwrap().join("tests/server.cert")
  }

  #[cfg(feature = "native-tls")]
  fn identity_path() -> std::path::PathBuf {
    std::env::current_dir().unwrap().join("tests/identity.pfx")
  }

  #[cfg(feature = "native-tls")]
  async fn native_tls_stream_layer<R: Runtime>() -> crate::native_tls::NativeTls<R> {
    use std::{fs::File, io::Read};

    use async_native_tls::{TlsAcceptor, TlsConnector};

    use crate::native_tls::NativeTls;

    let mut file = File::open(identity_path()).unwrap();
    let mut identity = vec![];
    file.read_to_end(&mut identity).unwrap();

    let acceptor = TlsAcceptor::new(futures::io::Cursor::new(identity), "ruraft")
      .await
      .unwrap();
    let connector = TlsConnector::new().danger_accept_invalid_certs(true);

    NativeTls::new("localhost".to_string(), acceptor, connector)
  }

  #[cfg(feature = "tls")]
  async fn tls_stream_layer<R: Runtime>() -> crate::tls::Tls<R> {
    use std::{
      fs::File,
      io::{self, BufReader},
      path::PathBuf,
      sync::Arc,
    };

    use async_rustls::rustls::{self, Certificate, PrivateKey};

    use crate::tls::Tls;

    fn load_certificates_from_pem(path: PathBuf) -> std::io::Result<Vec<Certificate>> {
      let file = File::open(path)?;
      let mut reader = BufReader::new(file);
      rustls_pemfile::certs(&mut reader)
        .map(|v| {
          v.map(|cert| Certificate(cert.to_vec()))
            .map_err(|_| io::ErrorKind::InvalidData.into())
        })
        .collect()
    }

    fn load_keys(path: PathBuf) -> io::Result<Vec<PrivateKey>> {
      rustls_pemfile::pkcs8_private_keys(&mut BufReader::new(File::open(path)?))
        .map(|key| key.map(|keys| PrivateKey(keys.secret_pkcs8_der().to_vec())))
        .collect()
    }

    struct SkipServerVerification;

    impl SkipServerVerification {
      fn new() -> Arc<Self> {
        Arc::new(Self)
      }
    }

    impl rustls::client::ServerCertVerifier for SkipServerVerification {
      fn verify_server_cert(
        &self,
        _end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
      ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::ServerCertVerified::assertion())
      }
    }

    let certs = load_certificates_from_pem(cert_path()).unwrap();
    let mut keys = load_keys(key_path()).unwrap();

    let mut root_store = rustls::RootCertStore::empty();
    root_store.add(&certs[0]).unwrap();

    let cfg = rustls::ServerConfig::builder()
      .with_safe_defaults()
      .with_no_client_auth()
      .with_single_cert(certs, keys.remove(0))
      .expect("bad certificate/key");
    let acceptor = async_rustls::TlsAcceptor::from(Arc::new(cfg));

    let cfg = rustls::ClientConfig::builder()
      .with_safe_defaults()
      .with_custom_certificate_verifier(SkipServerVerification::new())
      .with_no_client_auth();
    let connector = async_rustls::TlsConnector::from(Arc::new(cfg));
    Tls::new(
      rustls::ServerName::IpAddress("127.0.0.1".parse().unwrap()),
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
