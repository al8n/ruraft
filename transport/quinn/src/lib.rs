use std::{
  io,
  net::SocketAddr,
  pin::Pin,
  sync::{atomic::Ordering, Arc},
  task::{Context, Poll},
  time::Duration,
};

use agnostic::{net::Net, Runtime};
use futures::{AsyncRead, AsyncWrite, Future};
use nodecraft::resolver::AddressResolver;
use quinn::{ClientConfig, Endpoint, EndpointConfig, RecvStream, SendStream, ServerConfig};
use ruraft_net::{stream::*, NetTransport};
use atomic_time::AtomicDuration;

/// Quinn transport
pub type QuinnTransport<I, A, D, W> =
  NetTransport<I, A, D, Quinn<<A as AddressResolver>::Runtime>, W>;

/// Quinn stream layer
pub struct Quinn<R> {
  endpoint: Endpoint,
  client_config: ClientConfig,
  server_name: String,
  _marker: std::marker::PhantomData<R>,
}

impl<R: Runtime> Quinn<R> {
  pub fn new(
    server_name: String,
    addr: SocketAddr,
    endpoint_config: EndpointConfig,
    client_config: ClientConfig,
    server_config: ServerConfig,
  ) -> io::Result<Self> {
    let socket = std::net::UdpSocket::bind(addr)?;
    Endpoint::new(
      endpoint_config,
      Some(server_config),
      socket,
      Arc::new(<<R::Net as Net>::Quinn as Default>::default()),
    )
    .map(|endpoint| Self {
      server_name,
      endpoint,
      client_config,
      _marker: std::marker::PhantomData,
    })
  }
}

impl<R: Runtime> StreamLayer for Quinn<R> {
  type Listener = QuinnListener<R>;

  type Stream = QuinnStream<R>;

  async fn connect(&self, addr: SocketAddr) -> io::Result<Self::Stream> {
    match self
      .endpoint
      .connect_with(self.client_config.clone(), addr, &self.server_name)
      .map_err(|e| io::Error::new(io::ErrorKind::ConnectionRefused, e))?
      .await
    {
      Ok(conn) => match conn.accept_bi().await {
        Ok((s, r)) => Ok(QuinnStream::new(r, s)),
        Err(e) => Err(io::Error::new(io::ErrorKind::ConnectionRefused, e)),
      },
      Err(e) => Err(io::Error::new(io::ErrorKind::ConnectionRefused, e)),
    }
  }

  async fn bind(&mut self, _addr: SocketAddr) -> io::Result<Self::Listener> {
    Ok(QuinnListener {
      endpoint: self.endpoint.clone(),
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
