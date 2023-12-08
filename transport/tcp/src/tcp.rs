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
use ruraft_net::{
  stream::{Connection, Listener, StreamLayer},
  NetTransport,
};

/// Tcp transport
pub type TcpTransport<I, A, D, W> = NetTransport<I, A, D, Tcp<<A as AddressResolver>::Runtime>, W>;

/// Tcp stream layer
#[repr(transparent)]
pub struct Tcp<R> {
  _marker: std::marker::PhantomData<R>,
}

impl<R> Default for Tcp<R> {
  fn default() -> Self {
    Self::new()
  }
}

impl<R> Tcp<R> {
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

  async fn connect(&self, addr: SocketAddr) -> io::Result<Self::Stream> {
    <<R::Net as Net>::TcpStream as agnostic::net::TcpStream>::connect(addr)
      .await
      .map(TcpStream)
  }

  async fn bind(&mut self, addr: SocketAddr) -> io::Result<Self::Listener> {
    <<R::Net as Net>::TcpListener as agnostic::net::TcpListener>::bind(addr)
      .await
      .map(TcpListener)
  }
}

/// Listener of the TCP stream layer
#[repr(transparent)]
pub struct TcpListener<R: Runtime>(<R::Net as Net>::TcpListener);

impl<R: Runtime> Listener for TcpListener<R> {
  type Stream = TcpStream<R>;

  async fn accept(&mut self) -> io::Result<(Self::Stream, SocketAddr)> {
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

impl<R: Runtime> Connection for TcpStream<R> {
  type OwnedReadHalf = <<R::Net as Net>::TcpStream as agnostic::net::TcpStream>::OwnedReadHalf;

  type OwnedWriteHalf = <<R::Net as Net>::TcpStream as agnostic::net::TcpStream>::OwnedWriteHalf;

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

  fn into_split(self) -> (Self::OwnedReadHalf, Self::OwnedWriteHalf) {
    self.0.into_split()
  }
}
