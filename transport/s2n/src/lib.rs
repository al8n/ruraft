//! QUIC transport implementation (based on amazon's [`s2n`](https://crates.io/crates/s2n-quic)) for [`ruraft`](https://github.com/al8n/ruraft).
#![deny(warnings, missing_docs)]
#![forbid(unsafe_code)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

use std::{
  io,
  net::SocketAddr,
  pin::Pin,
  sync::atomic::Ordering,
  task::{Context, Poll},
  time::Duration,
};

use agnostic::Runtime;
use atomic_time::AtomicDuration;
use bytes::Bytes;
use futures::{AsyncRead, AsyncWrite, Future};
use nodecraft::resolver::AddressResolver;
use quic::{
  client::Connect,
  stream::{ReceiveStream, SendStream},
};
use ruraft_net::{stream::*, NetTransport};
pub use s2n_quic as quic;
pub use s2n_quic::{Client, Server};

/// [`s2n-quic`][s2n_quic] transport, which only support `tokio` async runtime.
pub type S2nTransport<I, A, D, W> = NetTransport<I, A, D, S2n<<A as AddressResolver>::Runtime>, W>;



/// Options for [`S2n`] stream layer.
#[derive(Clone)]
pub struct S2nOptions {
  server_name: String,
  // tls_server_config: quic::provider::tls::default::rustls::rustls::ServerConfig,
  // tls_client_config: quic::provider::tls::rustls::rustls::ClientConfig,
}

impl S2nOptions {
  /// Create a new S2nOptions
  pub fn new(name: impl Into<String>) -> Self {
    Self {
      server_name: name.into(),
    }
  }
}


/// s2n stream layer implementation
pub struct S2n<R> {
  opts: S2nOptions,
  client: Client,
  _marker: std::marker::PhantomData<R>,
}

impl<R> S2n<R> {
  /// Create a new `s2n`] stream layer.
  pub fn new(opts: S2nOptions) -> io::Result<Self> {
    Ok(Self {
      opts,
      client: Client::builder().with_io("0.0.0.0:0").map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?.start().map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
      _marker: std::marker::PhantomData,
    })
  }
}

impl<R: Runtime> StreamLayer for S2n<R>
{
  type Listener = S2nListener<R>;

  type Stream = S2nStream<R>;

  async fn connect(&self, addr: SocketAddr) -> io::Result<Self::Stream> {
    let connect = Connect::new(addr).with_server_name(self.opts.server_name.as_str());
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

  async fn bind(&mut self, addr: SocketAddr) -> io::Result<Self::Listener> {
    let srv = Server::builder()
      // .with_tls(self.options.tls_server_config.clone())?
      .with_io(addr).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
      .start()
      .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(S2nListener {
      server: srv,
      _marker: std::marker::PhantomData,
    })
  }
}

/// [`s2n-quic`] listener implementation, which only support `tokio` async runtime.
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

/// S2n stream layer owned read half
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

/// S2n stream layer owned write half
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

/// S2n stream
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

/// Exports unit tests to let users test transport implementation based on this crate.
#[cfg(any(feature = "test", test))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "test", test))))]
pub mod tests {
  pub use super::testcases::quinn::*;
}

#[cfg(any(feature = "test", test))]
mod testcases {
  use super::*;
  use futures::Future;
  use ruraft_net::{
    resolver::SocketAddrResolver, tests, tests_mod, wire::LpeWire, Header, ProtocolVersion,
  };
  use smol_str::SmolStr;
  use std::{
    net::SocketAddr,
    sync::atomic::{AtomicU16, Ordering},
  };

  static PORT: AtomicU16 = AtomicU16::new(19090);

  fn header1() -> Header<SmolStr, SocketAddr> {
    let addr = format!("127.0.0.1:{}", PORT.fetch_add(1, Ordering::SeqCst));
    Header::new(
      ProtocolVersion::V1,
      SmolStr::new("header1"),
      addr.parse().unwrap(),
    )
  }

  fn header2() -> Header<SmolStr, SocketAddr> {
    Header::new(
      ProtocolVersion::V1,
      SmolStr::new("header2"),
      "127.0.0.1:0".parse().unwrap(),
    )
  }

  async fn stream_layer<R: Runtime>() -> S2n<R> {
    S2n::<R>::new(S2nOptions::new("localhost")).unwrap()
  }

  tests_mod!(quinn::Quinn::stream_layer);
}