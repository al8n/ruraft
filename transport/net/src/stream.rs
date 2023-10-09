use std::{io, net::SocketAddr, time::Duration};

use futures::{AsyncRead, AsyncWrite};

#[async_trait::async_trait]
pub trait Listener: Send + Sync + 'static {
  type Stream: Connection;

  async fn bind(addr: SocketAddr) -> io::Result<Self>
  where
    Self: Sized;

  async fn accept(&self) -> io::Result<(Self::Stream, SocketAddr)>;

  fn local_addr(&self) -> io::Result<SocketAddr>;
}

#[async_trait::async_trait]
pub trait Connection: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static {
  async fn connect(addr: SocketAddr) -> io::Result<Self>
  where
    Self: Sized;

  fn set_timeout(&self, timeout: Option<Duration>) {
    self.set_write_timeout(timeout);
    self.set_read_timeout(timeout);
  }

  fn timeout(&self) -> (Option<Duration>, Option<Duration>) {
    (self.read_timeout(), self.write_timeout())
  }

  fn set_write_timeout(&self, timeout: Option<Duration>);

  fn write_timeout(&self) -> Option<Duration>;

  fn set_read_timeout(&self, timeout: Option<Duration>);

  fn read_timeout(&self) -> Option<Duration>;
}

/// Used with the [`NetTransport`](super::NetTransport) to provide
/// the low level stream abstraction.
pub trait StreamLayer: Send + Sync + 'static {
  /// The listener type
  type Listener: Listener<Stream = Self::Stream>;
  /// The connection type
  type Stream: Connection;
}
