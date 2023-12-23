use std::{
  io,
  net::SocketAddr,
  pin::Pin,
  sync::atomic::Ordering,
  task::{Context, Poll},
  time::Duration,
};

use agnostic::Runtime;
use bytes::Bytes;
use futures::{AsyncRead, AsyncWrite, Future};
use nodecraft::resolver::AddressResolver;
use quic::{
  client::Connect,
  stream::{ReceiveStream, SendStream},
};
use ruraft_net::{stream::*, NetTransport};
use ruraft_utils::duration::AtomicDuration;
pub use s2n_quic as quic;
pub use s2n_quic::{Client, Server};

/// [`s2n-quic`][s2n_quic] transport, which only support `tokio` async runtime.
pub type S2nTransport<I, A, D, W> = NetTransport<I, A, D, S2n<<A as AddressResolver>::Runtime>, W>;

/// [`s2n-quic`] stream layer implementation, which only support `tokio` async runtime.
pub struct S2n<R> {
  server: Option<Server>,
  client: Client,
  server_name: String,
  _marker: std::marker::PhantomData<R>,
}

impl<R> S2n<R> {
  pub fn new(server_name: impl Into<String>, server: Server, client: Client) -> io::Result<Self> {
    Ok(Self {
      server_name: server_name.into(),
      server: Some(server),
      client,
      _marker: std::marker::PhantomData,
    })
  }
}

impl<R: Runtime> StreamLayer for S2n<R> {
  type Listener = S2nListener<R>;

  type Stream = S2nStream<R>;

  async fn connect(&self, addr: SocketAddr) -> io::Result<Self::Stream> {
    let connect = Connect::new(addr).with_server_name(self.server_name.as_str());
    let mut conn = self
      .client
      .connect(connect)
      .await
      .and_then(|mut conn| conn.keep_alive(true).map(|_| conn))
      .map_err(|e| io::Error::new(io::ErrorKind::ConnectionRefused, e))?;

    let (r, s) = conn.open_bidirectional_stream().await?.split();
    Ok(S2nStream {
      read_stream: S2nStreamOwnedReadHalf {
        stream: r,
        timeout: AtomicDuration::new(Duration::ZERO),
        _marker: std::marker::PhantomData,
      },
      write_stream: S2nStreamOwnedWriteHalf {
        stream: s,
        timeout: AtomicDuration::new(Duration::ZERO),
        _marker: std::marker::PhantomData,
      },
    })
  }

  async fn bind(&mut self, _addr: SocketAddr) -> io::Result<Self::Listener> {
    let srv = self.server.take();
    let srv =
      srv.ok_or_else(|| io::Error::new(io::ErrorKind::Other, "already bind to s2n server"))?;
    Ok(S2nListener {
      server: srv,
      _marker: std::marker::PhantomData,
    })
  }
}

pub struct S2nListener<R> {
  server: Server,
  _marker: std::marker::PhantomData<R>,
}

impl<R: Runtime> Listener for S2nListener<R> {
  type Stream = S2nStream<R>;

  async fn accept(&mut self) -> io::Result<(Self::Stream, SocketAddr)> {
    match self.server.accept().await {
      Some(mut conn) => {
        let remote = conn
          .remote_addr()
          .map_err(|e| io::Error::new(io::ErrorKind::ConnectionRefused, e))?;
        match conn.accept_bidirectional_stream().await {
          Ok(Some(stream)) => {
            let (r, s) = stream.split();
            Ok((
              S2nStream {
                read_stream: S2nStreamOwnedReadHalf {
                  stream: r,
                  timeout: AtomicDuration::new(Duration::ZERO),
                  _marker: std::marker::PhantomData,
                },
                write_stream: S2nStreamOwnedWriteHalf {
                  stream: s,
                  timeout: AtomicDuration::new(Duration::ZERO),
                  _marker: std::marker::PhantomData,
                },
              },
              remote,
            ))
          }
          Ok(None) => Err(io::Error::new(
            io::ErrorKind::ConnectionRefused,
            "connection closed",
          )),
          Err(e) => Err(io::Error::new(io::ErrorKind::ConnectionRefused, e)),
        }
      }
      None => Err(io::Error::new(
        io::ErrorKind::ConnectionAborted,
        "server closed",
      )),
    }
  }

  fn local_addr(&self) -> io::Result<SocketAddr> {
    self.server.local_addr()
  }
}

pub struct S2nStreamOwnedReadHalf<R> {
  stream: ReceiveStream,
  timeout: AtomicDuration,
  _marker: std::marker::PhantomData<R>,
}

impl<R: Runtime> AsyncRead for S2nStreamOwnedReadHalf<R> {
  fn poll_read(
    self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut [u8],
  ) -> Poll<Result<usize, io::Error>> {
    let self_ref = Pin::into_inner(self);
    let stream = &mut self_ref.stream;
    let timeout = self_ref.timeout.load(Ordering::Acquire);

    if timeout.is_zero() {
      let fut = stream.receive();
      futures::pin_mut!(fut);
      match fut.poll(cx) {
        Poll::Ready(Ok(Some(res))) => {
          buf.copy_from_slice(&res);
          Poll::Ready(Ok(res.len()))
        }
        Poll::Ready(Ok(None)) => Poll::Ready(Ok(0)),
        Poll::Ready(Err(e)) => Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, e))),
        Poll::Pending => Poll::Pending,
      }
    } else {
      let fut = R::timeout(timeout, stream.receive());
      futures::pin_mut!(fut);
      match fut.poll(cx) {
        Poll::Ready(Ok(rst)) => match rst {
          Ok(Some(res)) => {
            buf.copy_from_slice(&res);
            Poll::Ready(Ok(res.len()))
          }
          Ok(None) => Poll::Ready(Ok(0)),
          Err(e) => Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, e))),
        },
        Poll::Ready(Err(e)) => Poll::Ready(Err(io::Error::new(io::ErrorKind::TimedOut, e))),
        Poll::Pending => Poll::Pending,
      }
    }
  }
}

pub struct S2nStreamOwnedWriteHalf<R> {
  stream: SendStream,
  timeout: AtomicDuration,
  _marker: std::marker::PhantomData<R>,
}

impl<R: Runtime> AsyncWrite for S2nStreamOwnedWriteHalf<R> {
  fn poll_write(
    self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &[u8],
  ) -> Poll<Result<usize, io::Error>> {
    let self_ref = Pin::into_inner(self);
    let stream = &mut self_ref.stream;
    let timeout = self_ref.timeout.load(Ordering::Acquire);
    let n = buf.len();
    let data = Bytes::copy_from_slice(buf);
    if timeout.is_zero() {
      let fut = stream.send(data);
      futures::pin_mut!(fut);
      match fut.poll(cx) {
        Poll::Ready(Ok(_)) => Poll::Ready(Ok(n)),
        Poll::Ready(Err(e)) => Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, e))),
        Poll::Pending => Poll::Pending,
      }
    } else {
      let fut = R::timeout(timeout, stream.send(data));
      futures::pin_mut!(fut);
      match fut.poll(cx) {
        Poll::Ready(Ok(rst)) => match rst {
          Ok(_) => Poll::Ready(Ok(n)),
          Err(e) => Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, e))),
        },
        Poll::Ready(Err(e)) => Poll::Ready(Err(io::Error::new(io::ErrorKind::TimedOut, e))),
        Poll::Pending => Poll::Pending,
      }
    }
  }

  fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
    let self_ref = Pin::into_inner(self);
    let stream = &mut self_ref.stream;
    stream
      .poll_flush(cx)
      .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
  }

  fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    let self_ref = Pin::into_inner(self);
    let stream = &mut self_ref.stream;
    stream
      .poll_close(cx)
      .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
  }
}

#[pin_project::pin_project]
pub struct S2nStream<R> {
  #[pin]
  read_stream: S2nStreamOwnedReadHalf<R>,
  #[pin]
  write_stream: S2nStreamOwnedWriteHalf<R>,
}

impl<R: Runtime> AsyncRead for S2nStream<R> {
  fn poll_read(
    self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut [u8],
  ) -> Poll<Result<usize, io::Error>> {
    self.project().read_stream.poll_read(cx, buf)
  }
}

impl<R: Runtime> AsyncWrite for S2nStream<R> {
  fn poll_write(
    self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &[u8],
  ) -> Poll<Result<usize, io::Error>> {
    self.project().write_stream.poll_write(cx, buf)
  }

  fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
    self.project().write_stream.poll_flush(cx)
  }

  fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    self.project().write_stream.poll_close(cx)
  }
}

impl<R: Runtime> Connection for S2nStream<R> {
  type OwnedReadHalf = S2nStreamOwnedReadHalf<R>;

  type OwnedWriteHalf = S2nStreamOwnedWriteHalf<R>;

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
      .read_stream
      .timeout
      .store(timeout.unwrap_or(Duration::ZERO), Ordering::Release)
  }

  fn read_timeout(&self) -> Option<Duration> {
    let t = self.read_stream.timeout.load(Ordering::Acquire);
    if t == Duration::ZERO {
      None
    } else {
      Some(t)
    }
  }

  fn into_split(self) -> (Self::OwnedReadHalf, Self::OwnedWriteHalf) {
    (self.read_stream, self.write_stream)
  }
}
