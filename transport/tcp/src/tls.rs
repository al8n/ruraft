use std::{
  io,
  net::SocketAddr,
  pin::Pin,
  sync::Arc,
  task::{Context, Poll},
  time::Duration,
};

use agnostic::{
  net::{Net, TcpListener, TcpStream},
  Runtime,
};
use async_rustls::{client, server, TlsAcceptor, TlsConnector};
use futures::{AsyncRead, AsyncWrite};
use nodecraft::resolver::AddressResolver;
use ruraft_net::{stream::*, NetTransport};
use rustls::{ClientConfig, ServerConfig, ServerName};

/// Tls transport based on `rustls`
pub type TlsTransport<I, A, D, W> = NetTransport<I, A, D, Tls<<A as AddressResolver>::Runtime>, W>;

/// Tls stream layer
pub struct Tls<R> {
  domain: ServerName,
  server_config: Arc<ServerConfig>,
  client_config: Arc<ClientConfig>,
  _marker: std::marker::PhantomData<R>,
}

impl<R> Tls<R> {
  /// Create a new tcp stream layer
  #[inline]
  pub fn new(domain: ServerName, server_config: ServerConfig, client_config: ClientConfig) -> Self {
    Self {
      domain,
      server_config: Arc::new(server_config),
      client_config: Arc::new(client_config),
      _marker: std::marker::PhantomData,
    }
  }
}

impl<R: Runtime> StreamLayer for Tls<R> {
  type Listener = TlsListener<R>;
  type Stream = TlsStream<R>;

  async fn connect(&self, addr: SocketAddr) -> io::Result<Self::Stream> {
    let connector = TlsConnector::from(self.client_config.clone());
    let conn = <<R::Net as Net>::TcpStream as TcpStream>::connect(addr).await?;
    let stream = connector.connect(self.domain.clone(), conn).await?;
    Ok(TlsStream {
      stream: TlsStreamKind::Client(stream),
    })
  }

  async fn bind(&self, addr: SocketAddr) -> io::Result<Self::Listener> {
    <<R::Net as Net>::TcpListener as TcpListener>::bind(addr)
      .await
      .map(|ln| TlsListener {
        ln,
        acceptor: TlsAcceptor::from(self.server_config.clone()),
      })
  }
}

/// Listener of the TLS stream layer
pub struct TlsListener<R: Runtime> {
  ln: <R::Net as Net>::TcpListener,
  acceptor: TlsAcceptor,
}

impl<R: Runtime> Listener for TlsListener<R> {
  type Stream = TlsStream<R>;

  async fn accept(&self) -> io::Result<(Self::Stream, std::net::SocketAddr)> {
    let (conn, addr) = self.ln.accept().await?;
    let stream = self.acceptor.accept(conn).await?;
    Ok((
      TlsStream {
        stream: TlsStreamKind::Server(stream),
      },
      addr,
    ))
  }

  fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
    self.ln.local_addr()
  }
}

#[pin_project::pin_project]
enum TlsStreamKind<R: Runtime> {
  Client(#[pin] client::TlsStream<<R::Net as Net>::TcpStream>),
  Server(#[pin] server::TlsStream<<R::Net as Net>::TcpStream>),
}

impl<R: Runtime> TlsStreamKind<R> {
  fn split(
    self,
  ) -> (
    TlsStreamOwnedReadHalfKind<R>,
    TlsStreamOwnedWriteHalfKind<R>,
  ) {
    match self {
      Self::Client(s) => {
        let (r, w) = futures::AsyncReadExt::split(s);
        (
          TlsStreamOwnedReadHalfKind::Client(r),
          TlsStreamOwnedWriteHalfKind::Client(w),
        )
      }
      Self::Server(s) => {
        let (r, w) = futures::AsyncReadExt::split(s);
        (
          TlsStreamOwnedReadHalfKind::Server(r),
          TlsStreamOwnedWriteHalfKind::Server(w),
        )
      }
    }
  }
}

impl<R: Runtime> AsyncRead for TlsStreamKind<R> {
  fn poll_read(
    self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut [u8],
  ) -> Poll<io::Result<usize>> {
    match self.get_mut() {
      Self::Client(s) => Pin::new(s).poll_read(cx, buf),
      Self::Server(s) => Pin::new(s).poll_read(cx, buf),
    }
  }
}

impl<R: Runtime> AsyncWrite for TlsStreamKind<R> {
  fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
    match self.get_mut() {
      Self::Client(s) => Pin::new(s).poll_write(cx, buf),
      Self::Server(s) => Pin::new(s).poll_write(cx, buf),
    }
  }

  fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    match self.get_mut() {
      Self::Client(s) => Pin::new(s).poll_flush(cx),
      Self::Server(s) => Pin::new(s).poll_flush(cx),
    }
  }

  fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    match self.get_mut() {
      Self::Client(s) => Pin::new(s).poll_close(cx),
      Self::Server(s) => Pin::new(s).poll_close(cx),
    }
  }
}

enum TlsStreamOwnedReadHalfKind<R: Runtime> {
  Client(futures::io::ReadHalf<client::TlsStream<<R::Net as Net>::TcpStream>>),
  Server(futures::io::ReadHalf<server::TlsStream<<R::Net as Net>::TcpStream>>),
}

impl<R: Runtime> AsyncRead for TlsStreamOwnedReadHalfKind<R> {
  fn poll_read(
    self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut [u8],
  ) -> Poll<io::Result<usize>> {
    match self.get_mut() {
      // If it's the Client variant
      Self::Client(read_half) => {
        // Delegate to the poll_read method of the ReadHalf
        // We use Pin::new to satisfy the pinning requirements
        Pin::new(read_half).poll_read(cx, buf)
      }
      // If it's the Server variant
      Self::Server(read_half) => {
        // Delegate to the poll_read method of the ReadHalf
        Pin::new(read_half).poll_read(cx, buf)
      }
    }
  }
}

/// The owned read half of the connection
#[pin_project::pin_project]
pub struct TlsStreamOwnedReadHalf<R: Runtime> {
  #[pin]
  inner: TlsStreamOwnedReadHalfKind<R>,
}

impl<R: Runtime> AsyncRead for TlsStreamOwnedReadHalf<R> {
  fn poll_read(
    self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut [u8],
  ) -> Poll<io::Result<usize>> {
    self.project().inner.poll_read(cx, buf)
  }
}

enum TlsStreamOwnedWriteHalfKind<R: Runtime> {
  Client(futures::io::WriteHalf<client::TlsStream<<R::Net as Net>::TcpStream>>),
  Server(futures::io::WriteHalf<server::TlsStream<<R::Net as Net>::TcpStream>>),
}

impl<R: Runtime> AsyncWrite for TlsStreamOwnedWriteHalfKind<R> {
  fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
    match self.get_mut() {
      // If it's the Client variant
      Self::Client(write_half) => {
        // Delegate to the poll_write method of the WriteHalf
        // We use Pin::new to satisfy the pinning requirements
        Pin::new(write_half).poll_write(cx, buf)
      }
      // If it's the Server variant
      Self::Server(write_half) => {
        // Delegate to the poll_write method of the WriteHalf
        Pin::new(write_half).poll_write(cx, buf)
      }
    }
  }

  fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    match self.get_mut() {
      // If it's the Client variant
      Self::Client(write_half) => {
        // Delegate to the poll_flush method of the WriteHalf
        // We use Pin::new to satisfy the pinning requirements
        Pin::new(write_half).poll_flush(cx)
      }
      // If it's the Server variant
      Self::Server(write_half) => {
        // Delegate to the poll_flush method of the WriteHalf
        Pin::new(write_half).poll_flush(cx)
      }
    }
  }

  fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    match self.get_mut() {
      // If it's the Client variant
      Self::Client(write_half) => {
        // Delegate to the poll_close method of the WriteHalf
        // We use Pin::new to satisfy the pinning requirements
        Pin::new(write_half).poll_close(cx)
      }
      // If it's the Server variant
      Self::Server(write_half) => {
        // Delegate to the poll_close method of the WriteHalf
        Pin::new(write_half).poll_close(cx)
      }
    }
  }
}

/// The owned write half of the connection
#[pin_project::pin_project]
pub struct TlsStreamOwnedWriteHalf<R: Runtime> {
  #[pin]
  inner: TlsStreamOwnedWriteHalfKind<R>,
}

impl<R: Runtime> AsyncWrite for TlsStreamOwnedWriteHalf<R> {
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

/// The options used to connect remote node.
#[derive(Debug, Clone)]
pub struct TlsStreamOptions {
  /// The TLS configuration.
  pub config: Arc<ClientConfig>,
  /// The domain name of the server.
  pub domain: rustls::ServerName,
}

/// TLS connection of the TCP stream layer.
#[pin_project::pin_project]
pub struct TlsStream<R: Runtime> {
  #[pin]
  stream: TlsStreamKind<R>,
}

impl<R: Runtime> AsyncRead for TlsStream<R> {
  fn poll_read(
    self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut [u8],
  ) -> Poll<io::Result<usize>> {
    self.project().stream.poll_read(cx, buf)
  }
}

impl<R: Runtime> AsyncWrite for TlsStream<R> {
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

impl<R: Runtime> Connection for TlsStream<R> {
  type OwnedReadHalf = TlsStreamOwnedReadHalf<R>;

  type OwnedWriteHalf = TlsStreamOwnedWriteHalf<R>;

  fn set_write_timeout(&self, timeout: Option<Duration>) {
    match self {
      Self {
        stream: TlsStreamKind::Client(s),
      } => s.get_ref().0.set_write_timeout(timeout),
      Self {
        stream: TlsStreamKind::Server(s),
      } => s.get_ref().0.set_write_timeout(timeout),
    }
  }

  fn write_timeout(&self) -> Option<Duration> {
    match self {
      Self {
        stream: TlsStreamKind::Client(s),
      } => s.get_ref().0.write_timeout(),
      Self {
        stream: TlsStreamKind::Server(s),
      } => s.get_ref().0.write_timeout(),
    }
  }

  fn set_read_timeout(&self, timeout: Option<Duration>) {
    match self {
      Self {
        stream: TlsStreamKind::Client(s),
      } => s.get_ref().0.set_read_timeout(timeout),
      Self {
        stream: TlsStreamKind::Server(s),
      } => s.get_ref().0.set_read_timeout(timeout),
    }
  }

  fn read_timeout(&self) -> Option<Duration> {
    match self {
      Self {
        stream: TlsStreamKind::Client(s),
      } => s.get_ref().0.read_timeout(),
      Self {
        stream: TlsStreamKind::Server(s),
      } => s.get_ref().0.read_timeout(),
    }
  }

  fn into_split(self) -> (Self::OwnedReadHalf, Self::OwnedWriteHalf) {
    let (r, w) = self.stream.split();
    (
      TlsStreamOwnedReadHalf { inner: r },
      TlsStreamOwnedWriteHalf { inner: w },
    )
  }
}
