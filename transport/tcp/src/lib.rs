use std::{time::Duration, net::SocketAddr, io, task::{Context, Poll}, pin::Pin};

use futures::{AsyncWrite, AsyncRead};
use nodecraft::resolver::AddressResolver;
pub use ruraft_net::{NetTransport, NetTransportOptions, Error};
use ruraft_net::stream::{Connection, Listener, StreamLayer};
use agnostic::{Runtime, net::{TcpStream as _, TcpListener as _, Net}};

/// Tcp transport
pub type TcpTransport<I, R, W> = NetTransport<I, R, Tcp<<R as AddressResolver>::Runtime>, W>;

/// Tcp stream layer
#[repr(transparent)]
pub struct Tcp<R: Runtime> {
  _marker: std::marker::PhantomData<R>,
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
    Self: Sized {
    <<R::Net as Net>::TcpListener as agnostic::net::TcpListener>::bind(addr).await.map(Self)
  }

  async fn accept(&self) -> io::Result<(Self::Stream, SocketAddr)> {
    self.0.accept().await.map(|(conn, addr)| (TcpStream(conn), addr))
  }

  fn local_addr(&self) -> io::Result<SocketAddr> {
    self.0.local_addr()
  }
}

/// Connection of the TCP stream layer
#[pin_project::pin_project]
pub struct TcpStream<R: Runtime>(#[pin] <R::Net as Net>::TcpStream);

impl<R: Runtime> AsyncRead for TcpStream<R>
{
  fn poll_read(
    self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut [u8],
  ) -> Poll<io::Result<usize>> {
    self.project().0.poll_read(cx, buf)
  }
}

impl<R: Runtime> AsyncWrite for TcpStream<R>
{
  fn poll_write(
    self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &[u8],
  ) -> Poll<io::Result<usize>> {
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
    Self: Sized
  {
    <<R::Net as Net>::TcpStream as agnostic::net::TcpStream>::connect(addr).await.map(Self)
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
