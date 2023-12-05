use std::{future::Future, io, net::SocketAddr, time::Duration};

use futures::{AsyncRead, AsyncWrite};

/// Represents a network listener.
///
/// This trait defines the operations required for a network listener that can bind to an address,
/// accept incoming connections, and query its local address.
pub trait Listener: Send + Sync + 'static {
  /// The type of the network stream associated with this listener.
  type Stream: Connection;

  /// Options for configuring the listener binding.
  type Options: Send + Sync + 'static;

  /// Binds the listener to a given socket address.
  fn bind(
    addr: SocketAddr,
    bind_opts: Self::Options,
  ) -> impl Future<Output = io::Result<Self>> + Send
  where
    Self: Sized;

  /// Accepts an incoming connection.
  fn accept(&self) -> impl Future<Output = io::Result<(Self::Stream, SocketAddr)>> + Send;

  /// Retrieves the local socket address of the listener.
  fn local_addr(&self) -> io::Result<SocketAddr>;
}

/// Represents a network connection.
///
/// This trait encapsulates functionality for a network connection that supports asynchronous
/// read/write operations and can be split into separate read and write halves.
pub trait Connection: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static {
  /// The type representing the owned read half of the connection.
  type OwnedReadHalf: AsyncRead + Unpin + Send + Sync + 'static;
  /// The type representing the owned write half of the connection.
  type OwnedWriteHalf: AsyncWrite + Unpin + Send + Sync + 'static;

  /// Options for configuring the connection connecting.
  type Options: Clone + Send + Sync + 'static;

  /// Establishes a connection to a specified socket address.
  fn connect(
    addr: SocketAddr,
    opts: Self::Options,
  ) -> impl Future<Output = io::Result<Self>> + Send
  where
    Self: Sized;

  /// Sets the read and write timeout for the connection.
  fn set_timeout(&self, timeout: Option<Duration>) {
    self.set_write_timeout(timeout);
    self.set_read_timeout(timeout);
  }

  /// Retrieves the read and write timeout for the connection.
  fn timeout(&self) -> (Option<Duration>, Option<Duration>) {
    (self.read_timeout(), self.write_timeout())
  }

  /// Sets the write timeout for the connection.
  fn set_write_timeout(&self, timeout: Option<Duration>);

  /// Retrieves the write timeout for the connection.
  fn write_timeout(&self) -> Option<Duration>;

  /// Sets the read timeout for the connection.
  fn set_read_timeout(&self, timeout: Option<Duration>);

  /// Retrieves the read timeout for the connection.
  fn read_timeout(&self) -> Option<Duration>;

  /// Splits the connection into separate read and write halves.
  fn into_split(self) -> (Self::OwnedReadHalf, Self::OwnedWriteHalf);
}

/// A trait defining the necessary components for a stream-based network layer.
///
/// This trait is used in conjunction with [`NetTransport`](super::NetTransport) to provide
/// an abstraction over the underlying network stream. It specifies the types for listeners
/// and connections that operate on this stream.
pub trait StreamLayer: Send + Sync + 'static {
  /// The listener type for the network stream.
  type Listener: Listener<Stream = Self::Stream>;

  /// The connection type for the network stream.
  type Stream: Connection;
}
