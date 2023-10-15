//! TCP transport implementation for [ruraft](https://github.com/al8n/ruraft).
#![deny(unsafe_code, missing_docs)]

use std::{
  io,
  net::SocketAddr,
  pin::Pin,
  task::{Context, Poll},
  time::Duration,
};

use agnostic::{
  net::{Net, TcpListener as _, TcpStream as _},
  Runtime,
};
use futures::{AsyncRead, AsyncWrite};
use nodecraft::resolver::AddressResolver;
use ruraft_net::stream::{Connection, Listener, StreamLayer};
pub use ruraft_net::{Error, NetTransport, NetTransportOptions};

/// Tcp transport
pub type TcpTransport<I, R, W> = NetTransport<I, R, Tcp<<R as AddressResolver>::Runtime>, W>;

/// Tcp stream layer
#[repr(transparent)]
pub struct Tcp<R: Runtime> {
  _marker: std::marker::PhantomData<R>,
}

impl<R: Runtime> Default for Tcp<R> {
  fn default() -> Self {
    Self::new()
  }
}

impl<R: Runtime> Tcp<R> {
  /// Create a new tcp stream layer
  #[inline]
  pub const fn new() -> Self {
    Self {
      _marker: std::marker::PhantomData,
    }
  }
}

impl<R: Runtime> StreamLayer for Tcp<R> {
  type Listener = TcpListener<R>;
  type Stream = TcpStream<R>;
}

/// Listener of the TCP stream layer
#[repr(transparent)]
pub struct TcpListener<R: Runtime>(<R::Net as Net>::TcpListener);

#[async_trait::async_trait]
impl<R: Runtime> Listener for TcpListener<R> {
  type Stream = TcpStream<R>;

  async fn bind(addr: SocketAddr) -> io::Result<Self>
  where
    Self: Sized,
  {
    <<R::Net as Net>::TcpListener as agnostic::net::TcpListener>::bind(addr)
      .await
      .map(Self)
  }

  async fn accept(&self) -> io::Result<(Self::Stream, SocketAddr)> {
    self
      .0
      .accept()
      .await
      .map(|(conn, addr)| (TcpStream(conn), addr))
  }

  fn local_addr(&self) -> io::Result<SocketAddr> {
    self.0.local_addr()
  }
}

/// Connection of the TCP stream layer
#[pin_project::pin_project]
pub struct TcpStream<R: Runtime>(#[pin] <R::Net as Net>::TcpStream);

impl<R: Runtime> AsyncRead for TcpStream<R> {
  fn poll_read(
    self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut [u8],
  ) -> Poll<io::Result<usize>> {
    self.project().0.poll_read(cx, buf)
  }
}

impl<R: Runtime> AsyncWrite for TcpStream<R> {
  fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
    self.project().0.poll_write(cx, buf)
  }

  fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    self.project().0.poll_flush(cx)
  }

  fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    self.project().0.poll_close(cx)
  }
}

#[async_trait::async_trait]
impl<R: Runtime> Connection for TcpStream<R> {
  async fn connect(addr: SocketAddr) -> io::Result<Self>
  where
    Self: Sized,
  {
    <<R::Net as Net>::TcpStream as agnostic::net::TcpStream>::connect(addr)
      .await
      .map(Self)
  }

  fn set_write_timeout(&self, timeout: Option<Duration>) {
    self.0.set_write_timeout(timeout)
  }

  fn write_timeout(&self) -> Option<Duration> {
    self.0.write_timeout()
  }

  fn set_read_timeout(&self, timeout: Option<Duration>) {
    self.0.set_read_timeout(timeout)
  }

  fn read_timeout(&self) -> Option<Duration> {
    self.0.read_timeout()
  }
}
