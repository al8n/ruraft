//! QUIC transport implementation (based on [`quinn`](https://crates.io/crates/quinn)) for [`ruraft`](https://github.com/al8n/ruraft).
#![deny(warnings, missing_docs)]
#![forbid(unsafe_code)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

use std::{
  io,
  net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
  pin::Pin,
  sync::{atomic::Ordering, Arc},
  task::{Context, Poll},
  time::Duration,
};

use agnostic::{net::Net, Runtime};
use atomic_time::AtomicDuration;
use futures::{AsyncRead, AsyncWrite, Future};
use nodecraft::resolver::AddressResolver;
use quinn::{ClientConfig, Endpoint, EndpointConfig, RecvStream, SendStream, ServerConfig};
use ruraft_net::{stream::*, NetTransport};

/// Quinn transport
pub type QuinnTransport<I, A, D, W> =
  NetTransport<I, A, D, Quinn<<A as AddressResolver>::Runtime>, W>;

/// Quinn stream layer
pub struct Quinn<R> {
  client_config: ClientConfig,
  server_config: ServerConfig,
  server_name: String,
  _marker: std::marker::PhantomData<R>,
}

impl<R: Runtime> Quinn<R> {
  /// Create a new Quinn transport
  pub fn new(
    server_name: String,
    client_config: ClientConfig,
    server_config: ServerConfig,
  ) -> Self {
    Self {
      client_config,
      server_config,
      server_name,
      _marker: std::marker::PhantomData,
    }
  }
}

impl<R: Runtime> StreamLayer for Quinn<R> {
  type Listener = QuinnListener<R>;

  type Stream = QuinnStream<R>;

  async fn connect(&self, addr: SocketAddr) -> io::Result<Self::Stream> {
    let local_addr = if addr.is_ipv4() {
      SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
    } else {
      SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 0)
    };
    let mut enp = Endpoint::client(local_addr)?;
    enp.set_default_client_config(self.client_config.clone());

    match enp
      .connect_with(self.client_config.clone(), addr, &self.server_name)
      .map_err(|e| io::Error::new(io::ErrorKind::ConnectionRefused, e))?
      .await
    {
      Ok(conn) => match conn.open_bi().await {
        Ok((s, r)) => Ok(QuinnStream::new(r, s)),
        Err(e) => Err(io::Error::new(io::ErrorKind::ConnectionRefused, e)),
      },
      Err(e) => Err(io::Error::new(io::ErrorKind::ConnectionRefused, e)),
    }
  }

  async fn bind(&mut self, bind_addr: SocketAddr) -> io::Result<Self::Listener> {
    let socket = std::net::UdpSocket::bind(bind_addr)?;
    Endpoint::new(
      EndpointConfig::default(),
      Some(self.server_config.clone()),
      socket,
      Arc::new(<<R::Net as Net>::Quinn as Default>::default()),
    )
    .map(|endpoint| Self::Listener {
      endpoint,
      _marker: std::marker::PhantomData,
    })
  }
}

/// Listener of the Quinn stream layer
pub struct QuinnListener<R> {
  endpoint: Endpoint,
  _marker: std::marker::PhantomData<R>,
}

impl<R: Runtime> Listener for QuinnListener<R> {
  type Stream = QuinnStream<R>;

  async fn accept(&mut self) -> io::Result<(Self::Stream, SocketAddr)> {
    match self.endpoint.accept().await {
      Some(connecting) => {
        let remote = connecting.remote_address();
        match connecting.await {
          Ok(conn) => match conn.accept_bi().await {
            Ok((s, r)) => Ok((QuinnStream::new(r, s), remote)),
            Err(e) => Err(io::Error::new(io::ErrorKind::ConnectionRefused, e)),
          },
          Err(e) => Err(io::Error::new(io::ErrorKind::ConnectionRefused, e)),
        }
      }
      None => Err(io::Error::new(
        io::ErrorKind::NotConnected,
        "endpoint closed",
      )),
    }
  }

  fn local_addr(&self) -> io::Result<SocketAddr> {
    self.endpoint.local_addr()
  }
}

/// Quinn stream owned read half
pub struct QuinnStreamOwnedReadHalf<R> {
  stream: RecvStream,
  timeout: AtomicDuration,
  _marker: std::marker::PhantomData<R>,
}

impl<R: Runtime> AsyncRead for QuinnStreamOwnedReadHalf<R> {
  fn poll_read(
    self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut [u8],
  ) -> Poll<io::Result<usize>> {
    let self_ref = Pin::into_inner(self);
    let recv_stream = &mut self_ref.stream;
    let read_timeout = self_ref.timeout.load(Ordering::Acquire);
    if read_timeout == Duration::ZERO {
      let fut = recv_stream.read(buf);
      futures::pin_mut!(fut);
      match fut.as_mut().poll(cx) {
        Poll::Ready(Ok(n)) => Poll::Ready(Ok(n.unwrap_or(0))),
        Poll::Ready(Err(e)) => Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, e))),
        Poll::Pending => Poll::Pending,
      }
    } else {
      let fut = R::timeout(read_timeout, recv_stream.read(buf));
      futures::pin_mut!(fut);
      match fut.poll(cx) {
        Poll::Ready(Ok(rst)) => Poll::Ready(
          rst
            .map(|n| n.unwrap_or(0))
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e)),
        ),
        Poll::Ready(Err(e)) => Poll::Ready(Err(io::Error::new(io::ErrorKind::TimedOut, e))),
        Poll::Pending => Poll::Pending,
      }
    }
  }
}

/// Quinn stream owned write half
pub struct QuinnStreamOwnedWriteHalf<R> {
  stream: SendStream,
  timeout: AtomicDuration,
  _marker: std::marker::PhantomData<R>,
}

impl<R: Runtime> AsyncWrite for QuinnStreamOwnedWriteHalf<R> {
  fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
    let self_ref = Pin::into_inner(self);
    let stream = &mut self_ref.stream;
    let timeout = self_ref.timeout.load(Ordering::Acquire);
    if timeout == Duration::ZERO {
      let fut = stream.write(buf);
      futures::pin_mut!(fut);
      match fut.as_mut().poll(cx) {
        Poll::Ready(rst) => Poll::Ready(rst.map_err(|e| io::Error::new(io::ErrorKind::Other, e))),
        Poll::Pending => Poll::Pending,
      }
    } else {
      let fut = R::timeout(timeout, stream.write(buf));
      futures::pin_mut!(fut);
      match fut.poll(cx) {
        Poll::Ready(Ok(rst)) => {
          Poll::Ready(rst.map_err(|e| io::Error::new(io::ErrorKind::Other, e)))
        }
        Poll::Ready(Err(e)) => Poll::Ready(Err(io::Error::new(io::ErrorKind::TimedOut, e))),
        Poll::Pending => Poll::Pending,
      }
    }
  }

  fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    Poll::Ready(Ok(()))
  }

  fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    let self_ref = Pin::into_inner(self);
    let send_stream = &mut self_ref.stream;
    let timeout = self_ref.timeout.load(Ordering::Acquire);
    if timeout == Duration::ZERO {
      let fut = send_stream.finish();
      futures::pin_mut!(fut);
      match fut.as_mut().poll(cx) {
        Poll::Ready(Ok(_)) => Poll::Ready(Ok(())),
        Poll::Ready(Err(e)) => Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, e))),
        Poll::Pending => Poll::Pending,
      }
    } else {
      let fut = R::timeout(timeout, send_stream.finish());
      futures::pin_mut!(fut);
      match fut.poll(cx) {
        Poll::Ready(Ok(rst)) => {
          Poll::Ready(rst.map_err(|e| io::Error::new(io::ErrorKind::Other, e)))
        }
        Poll::Ready(Err(e)) => Poll::Ready(Err(io::Error::new(io::ErrorKind::TimedOut, e))),
        Poll::Pending => Poll::Pending,
      }
    }
  }
}

/// Quinn stream
#[pin_project::pin_project]
pub struct QuinnStream<R> {
  #[pin]
  recv_stream: QuinnStreamOwnedReadHalf<R>,
  #[pin]
  write_stream: QuinnStreamOwnedWriteHalf<R>,
  _marker: std::marker::PhantomData<R>,
}

impl<R> QuinnStream<R> {
  fn new(recv_stream: RecvStream, write_stream: SendStream) -> Self {
    Self {
      recv_stream: QuinnStreamOwnedReadHalf {
        stream: recv_stream,
        timeout: AtomicDuration::new(Duration::ZERO),
        _marker: std::marker::PhantomData,
      },
      write_stream: QuinnStreamOwnedWriteHalf {
        stream: write_stream,
        timeout: AtomicDuration::new(Duration::ZERO),
        _marker: std::marker::PhantomData,
      },
      _marker: std::marker::PhantomData,
    }
  }
}

impl<R: Runtime> AsyncRead for QuinnStream<R> {
  fn poll_read(
    self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut [u8],
  ) -> Poll<io::Result<usize>> {
    self.project().recv_stream.poll_read(cx, buf)
  }
}

impl<R: Runtime> AsyncWrite for QuinnStream<R> {
  fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
    self.project().write_stream.poll_write(cx, buf)
  }

  fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    self.project().write_stream.poll_flush(cx)
  }

  fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    self.project().write_stream.poll_close(cx)
  }
}

impl<R: Runtime> Connection for QuinnStream<R> {
  type OwnedReadHalf = QuinnStreamOwnedReadHalf<R>;

  type OwnedWriteHalf = QuinnStreamOwnedWriteHalf<R>;

  fn set_write_timeout(&self, timeout: Option<Duration>) {
    self
      .write_stream
      .timeout
      .store(timeout.unwrap_or(Duration::ZERO), Ordering::Release)
  }

  fn write_timeout(&self) -> Option<Duration> {
    let t = self.write_stream.timeout.load(Ordering::Acquire);
    if t == Duration::ZERO {
      None
    } else {
      Some(t)
    }
  }

  fn set_read_timeout(&self, timeout: Option<Duration>) {
    self
      .recv_stream
      .timeout
      .store(timeout.unwrap_or(Duration::ZERO), Ordering::Release)
  }

  fn read_timeout(&self) -> Option<Duration> {
    let t = self.recv_stream.timeout.load(Ordering::Acquire);
    if t == Duration::ZERO {
      None
    } else {
      Some(t)
    }
  }

  fn into_split(self) -> (Self::OwnedReadHalf, Self::OwnedWriteHalf) {
    (self.recv_stream, self.write_stream)
  }
}

/// Exports unit tests to let users test transport implementation based on this crate.
#[cfg(any(feature = "test", test))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "test", test))))]
pub mod tests {
  pub use super::testcases::quinn::*;
}

#[cfg(any(feature = "test", test))]
mod testcases {
  use super::*;
  use ::quinn::{ClientConfig, ServerConfig};
  use futures::Future;
  use ruraft_net::{
    resolver::SocketAddrResolver, tests, tests_mod, wire::LpeWire, Header, ProtocolVersion,
  };
  use smol_str::SmolStr;
  use std::{
    error::Error,
    net::SocketAddr,
    sync::{
      atomic::{AtomicU16, Ordering},
      Arc,
    },
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

  fn configures() -> Result<(ServerConfig, ClientConfig), Box<dyn Error>> {
    let (server_config, _server_cert) = configure_server()?;
    let client_config = configure_client();
    Ok((server_config, client_config))
  }

  fn configure_client() -> ClientConfig {
    let crypto = rustls::ClientConfig::builder()
      .with_safe_defaults()
      .with_custom_certificate_verifier(SkipServerVerification::new())
      .with_no_client_auth();

    ClientConfig::new(Arc::new(crypto))
  }

  fn configure_server() -> Result<(ServerConfig, Vec<u8>), Box<dyn Error>> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_der = cert.serialize_der().unwrap();
    let priv_key = cert.serialize_private_key_der();
    let priv_key = rustls::PrivateKey(priv_key);
    let cert_chain = vec![rustls::Certificate(cert_der.clone())];

    let mut server_config = ServerConfig::with_single_cert(cert_chain, priv_key)?;
    let transport_config = Arc::get_mut(&mut server_config.transport).unwrap();
    transport_config.max_concurrent_uni_streams(0_u8.into());

    Ok((server_config, cert_der))
  }

  #[allow(unused)]
  const ALPN_QUIC_HTTP: &[&[u8]] = &[b"hq-29"];

  async fn stream_layer<R: Runtime>() -> Quinn<R> {
    let server_name = "localhost".to_string();
    let (server_config, client_config) = configures().unwrap();
    Quinn::new(server_name, client_config, server_config)
  }

  tests_mod!(quinn::Quinn::stream_layer);
}
