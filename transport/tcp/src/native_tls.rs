#![allow(missing_docs)]

use std::{
  io,
  pin::Pin,
  task::{Context, Poll},
  time::Duration,
};

use agnostic::{
  net::{Net, TcpListener, TcpStream},
  Runtime,
};
use async_native_tls::{
  AcceptError, Certificate, Identity, Protocol, TlsAcceptor, TlsConnector,
  TlsStream as AsyncNativeTlsStream,
};
use futures::{AsyncRead, AsyncWrite};
use nodecraft::resolver::AddressResolver;
use ruraft_net::{stream::*, NetTransport};

/// Tls transport based on native tls, comparing to the [`TlsTransport`](crate::tls::TlsTransport),
/// this transport is using the native-tls but 
/// will add more overhead (deep clone on [`NativeTlsStreamOptions`] required) when building every connection.
pub type NativeTlsTransport<I, A, D, F, W> =
  NetTransport<I, A, D, NativeTls<F, <A as AddressResolver>::Runtime>, W>;

/// Tls stream layer
#[repr(transparent)]
pub struct NativeTls<F, R> {
  _marker: std::marker::PhantomData<(F, R)>,
}

impl<F, R> Default for NativeTls<F, R> {
  fn default() -> Self {
    Self::new()
  }
}

impl<F, R> NativeTls<F, R> {
  /// Create a new tcp stream layer
  #[inline]
  pub const fn new() -> Self {
    Self {
      _marker: std::marker::PhantomData,
    }
  }
}

impl<F, R: Runtime> StreamLayer for NativeTls<F, R>
where
  F: AsyncRead + Send + Sync + Unpin + 'static,
{
  type Listener = NativeTlsListener<F, R>;
  type Stream = NativeTlsStream<R>;
}

/// Options for configuring the TLS listener binding.
pub struct NativeNativeTlsListenerOptions<R> {
  /// The password for the identity file.
  pub password: String,
  /// The identity file.
  pub file: R,
}

/// Listener of the TLS stream layer
pub struct NativeTlsListener<F, R: Runtime> {
  ln: <R::Net as Net>::TcpListener,
  acceptor: TlsAcceptor,
  _marker: std::marker::PhantomData<F>,
}

impl<F, R: Runtime> Listener for NativeTlsListener<F, R>
where
  F: AsyncRead + Send + Sync + Unpin + 'static,
{
  type Stream = NativeTlsStream<R>;

  type Options = NativeNativeTlsListenerOptions<F>;

  async fn bind(addr: std::net::SocketAddr, bind_opts: Self::Options) -> io::Result<Self>
  where
    Self: Sized,
  {
    let acceptor = TlsAcceptor::new(bind_opts.file, bind_opts.password)
      .await
      .map_err(|e| match e {
        AcceptError::Io(e) => e,
        AcceptError::NativeTls(e) => io::Error::new(io::ErrorKind::Other, e),
      })?;
    <<R::Net as Net>::TcpListener as TcpListener>::bind(addr)
      .await
      .map(|ln| Self {
        ln,
        acceptor,
        _marker: std::marker::PhantomData,
      })
  }

  async fn accept(&self) -> io::Result<(Self::Stream, std::net::SocketAddr)> {
    let (conn, addr) = self.ln.accept().await?;
    let stream = self
      .acceptor
      .accept(conn)
      .await
      .map_err(|e| io::Error::new(io::ErrorKind::ConnectionRefused, e))?;
    Ok((NativeTlsStream { stream }, addr))
  }

  fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
    self.ln.local_addr()
  }
}

/// The options used to connect remote node.
#[viewit::viewit(
  vis_all = "pub(crate)",
  getters(vis_all = "pub"),
  setters(vis_all = "pub", prefix = "with")
)]
#[derive(Clone)]
pub struct NativeTlsStreamOptions {
  /// The domain name of the server.
  #[viewit(getter(skip), setter(skip))]
  domain: String,
  /// The identity to be used for client certificate authentication.
  #[viewit(getter(style = "ref"))]
  identity: Option<Identity>,
  /// The minimum supported protocol version.
  min_protocol: Option<Protocol>,
  /// The maximum supported protocol version.
  max_protocol: Option<Protocol>,
  /// Certificates to the set of roots that the connector will trust.
  #[viewit(setter(skip), getter(style = "ref"))]
  root_certificates: Vec<Certificate>,
  /// Controls the use of certificate validation.
  accept_invalid_certs: bool,
  /// Controls the use of hostname verification.
  accept_invalid_hostnames: bool,
  /// Controls the use of Server Name Indication (SNI).
  use_sni: bool,
}

impl NativeTlsStreamOptions {
  /// Creates a new `NativeTlsStreamOptions` with the given domain name.
  pub fn new(domain: impl Into<String>) -> Self {
    Self {
      domain: domain.into(),
      identity: None,
      min_protocol: None,
      max_protocol: None,
      root_certificates: vec![],
      accept_invalid_certs: false,
      accept_invalid_hostnames: false,
      use_sni: true,
    }
  }

  /// Adds a certificate to the set of roots that the connector will trust.
  ///
  /// The connector will use the system's trust root by default. This method can be used to add
  /// to that set when communicating with servers not trusted by the system.
  ///
  /// Defaults to an empty set.
  pub fn add_root_certificate(&mut self, cert: Certificate) {
    self.root_certificates.push(cert);
  }

  /// Adds multiple certificates to the set of roots that the connector will trust.
  pub fn add_root_certificates(mut self, certs: Vec<Certificate>) -> Self {
    self.root_certificates.extend(certs);
    self
  }
}

/// The owned read half of the connection
#[pin_project::pin_project]
pub struct NativeTlsStreamOwnedReadHalf<R: Runtime> {
  #[pin]
  inner: futures::io::ReadHalf<AsyncNativeTlsStream<<R::Net as Net>::TcpStream>>,
}

impl<R: Runtime> AsyncRead for NativeTlsStreamOwnedReadHalf<R> {
  fn poll_read(
    self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut [u8],
  ) -> Poll<io::Result<usize>> {
    self.project().inner.poll_read(cx, buf)
  }
}

/// The owned write half of the connection
#[pin_project::pin_project]
pub struct NativeTlsStreamOwnedWriteHalf<R: Runtime> {
  #[pin]
  inner: futures::io::WriteHalf<AsyncNativeTlsStream<<R::Net as Net>::TcpStream>>,
}

impl<R: Runtime> AsyncWrite for NativeTlsStreamOwnedWriteHalf<R> {
  fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
    self.project().inner.poll_write(cx, buf)
  }

  fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    self.project().inner.poll_flush(cx)
  }

  fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    self.project().inner.poll_close(cx)
  }
}

/// TLS connection of the TCP stream layer.
#[pin_project::pin_project]
pub struct NativeTlsStream<R: Runtime> {
  #[pin]
  stream: AsyncNativeTlsStream<<R::Net as Net>::TcpStream>,
}

impl<R: Runtime> AsyncRead for NativeTlsStream<R> {
  fn poll_read(
    self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut [u8],
  ) -> Poll<io::Result<usize>> {
    self.project().stream.poll_read(cx, buf)
  }
}

impl<R: Runtime> AsyncWrite for NativeTlsStream<R> {
  fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
    self.project().stream.poll_write(cx, buf)
  }

  fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    self.project().stream.poll_flush(cx)
  }

  fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    self.project().stream.poll_close(cx)
  }
}

impl<R: Runtime> Connection for NativeTlsStream<R> {
  type OwnedReadHalf = NativeTlsStreamOwnedReadHalf<R>;

  type OwnedWriteHalf = NativeTlsStreamOwnedWriteHalf<R>;

  type Options = NativeTlsStreamOptions;

  async fn connect(addr: std::net::SocketAddr, opts: Self::Options) -> io::Result<Self>
  where
    Self: Sized,
  {
    let mut connector = TlsConnector::new()
      .use_sni(opts.use_sni())
      .danger_accept_invalid_certs(opts.accept_invalid_certs())
      .danger_accept_invalid_hostnames(opts.accept_invalid_hostnames())
      .max_protocol_version(opts.max_protocol())
      .min_protocol_version(opts.min_protocol());

    if let Some(identity) = opts.identity() {
      connector = connector.identity(identity.clone());
    }

    for cert in opts.root_certificates {
      connector = connector.add_root_certificate(cert);
    }

    let conn = <<R::Net as Net>::TcpStream as TcpStream>::connect(addr).await?;
    let stream = connector
      .connect(opts.domain, conn)
      .await
      .map_err(|e| io::Error::new(io::ErrorKind::ConnectionRefused, e))?;
    Ok(Self { stream })
  }

  fn set_write_timeout(&self, timeout: Option<Duration>) {
    self.stream.get_ref().set_write_timeout(timeout)
  }

  fn write_timeout(&self) -> Option<Duration> {
    self.stream.get_ref().write_timeout()
  }

  fn set_read_timeout(&self, timeout: Option<Duration>) {
    self.stream.get_ref().set_read_timeout(timeout)
  }

  fn read_timeout(&self) -> Option<Duration> {
    self.stream.get_ref().read_timeout()
  }

  fn into_split(self) -> (Self::OwnedReadHalf, Self::OwnedWriteHalf) {
    use futures::AsyncReadExt;

    let (r, w) = self.stream.split();
    (NativeTlsStreamOwnedReadHalf { inner: r }, NativeTlsStreamOwnedWriteHalf { inner: w })
  }
}
