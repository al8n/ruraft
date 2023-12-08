#![allow(missing_docs)]

use std::{
  io,
  net::SocketAddr,
  pin::Pin,
  task::{Context, Poll},
  time::Duration,
};

use agnostic::{
  net::{Net, TcpListener, TcpStream},
  Runtime,
};
use async_native_tls::TlsStream as AsyncNativeTlsStream;
pub use async_native_tls::{TlsAcceptor, TlsConnector};
use futures::{AsyncRead, AsyncWrite};
use nodecraft::resolver::AddressResolver;
use ruraft_net::{stream::*, NetTransport};

/// Tls transport based on native tls
pub type NativeTlsTransport<I, A, D, W> =
  NetTransport<I, A, D, NativeTls<<A as AddressResolver>::Runtime>, W>;

/// Tls stream layer
pub struct NativeTls<R> {
  acceptor: Option<TlsAcceptor>,
  connector: TlsConnector,
  domain: String,
  _marker: std::marker::PhantomData<R>,
}

impl<R> NativeTls<R> {
  /// Create a new tcp stream layer
  #[inline]
  pub fn new(
    server_name: impl Into<String>,
    acceptor: TlsAcceptor,
    connector: TlsConnector,
  ) -> Self {
    Self {
      acceptor: Some(acceptor),
      connector,
      domain: server_name.into(),
      _marker: std::marker::PhantomData,
    }
  }
}

impl<R: Runtime> StreamLayer for NativeTls<R> {
  type Listener = NativeTlsListener<R>;
  type Stream = NativeTlsStream<R>;

  async fn connect(&self, addr: SocketAddr) -> io::Result<Self::Stream> {
    let conn = <<R::Net as Net>::TcpStream as TcpStream>::connect(addr).await?;
    let stream = self
      .connector
      .connect(self.domain.clone(), conn)
      .await
      .map_err(|e| io::Error::new(io::ErrorKind::ConnectionRefused, e))?;
    Ok(NativeTlsStream { stream })
  }

  async fn bind(&mut self, addr: SocketAddr) -> io::Result<Self::Listener> {
    let acceptor = self
      .acceptor
      .take()
      .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "already bind to local machine"))?;
    <<R::Net as Net>::TcpListener as TcpListener>::bind(addr)
      .await
      .map(|ln| NativeTlsListener { ln, acceptor })
  }
}

/// Listener of the TLS stream layer
pub struct NativeTlsListener<R: Runtime> {
  ln: <R::Net as Net>::TcpListener,
  acceptor: TlsAcceptor,
}

impl<R: Runtime> Listener for NativeTlsListener<R> {
  type Stream = NativeTlsStream<R>;

  async fn accept(&mut self) -> io::Result<(Self::Stream, std::net::SocketAddr)> {
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
    (
      NativeTlsStreamOwnedReadHalf { inner: r },
      NativeTlsStreamOwnedWriteHalf { inner: w },
    )
  }
}
