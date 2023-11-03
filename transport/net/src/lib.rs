//!
#![forbid(unsafe_code)]
// #![deny(missing_docs)]

/// Stream layer abstraction.
pub mod stream;
use stream::{Connection, Listener, StreamLayer};

use std::{
  collections::HashMap,
  future::Future,
  net::SocketAddr,
  sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
  },
  time::{Duration, Instant},
};

use agnostic::Runtime;
use async_lock::Mutex;
use futures::{
  io::{BufReader, BufWriter},
  AsyncRead, AsyncWriteExt, FutureExt,
};
use ruraft_core::{options::ProtocolVersion, transport::*};
use wg::AsyncWaitGroup;

/// Network [`Wire`](ruraft_core::transport::Wire) implementors.
pub mod wire;

mod pipeline;
pub use pipeline::*;

/// The default TimeoutScale in a [`NetworkTransport`].
pub const DEFAULT_TIMEOUT_SCALE: usize = 256 * 1024; // 256KB

/// The default value used for pipelining configuration
/// if a zero value is passed. See https://github.com/hashicorp/raft/pull/541
/// for rationale. Note, if this is changed we should update the doc comments
/// below for [`NetTransportOptions`].
pub const DEFAULT_MAX_INFLIGHT_REQUESTS: usize = 2;

/// The size of the buffer we will use for reading RPC requests into
/// on followers
const CONN_RECEIVE_BUFFER_SIZE: usize = 256 * 1024; // 256KB

/// The size of the buffer we will use for sending RPC request data from
/// the leader to followers.
const CONN_SEND_BUFFER_SIZE: usize = 256 * 1024; // 256KB

/// a property of our current pipelining
/// implementation and must not be changed unless we change the invariants of
/// that implementation. Roughly speaking even with a zero-length in-flight
/// buffer we still allow 2 requests to be in-flight before we block because we
/// only block after sending and the receiving task always unblocks the
/// chan right after first send. This is a constant just to provide context
/// rather than a magic number in a few places we have to check invariants to
/// avoid panics etc.
const MIN_IN_FLIGHT_FOR_PIPELINING: usize = 2;

/// Represents errors specific to the [`NetTransport`].
///
/// This enum captures a comprehensive set of errors that can arise when using the `NetTransport`.
/// It categorizes these errors based on their origin, whether from the `Id`, `AddressResolver`,
/// `Wire` operations or more general transport-level concerns.
#[derive(thiserror::Error)]
pub enum Error<I: Id, R: AddressResolver, W: Wire> {
  /// Errors arising from the node identifier (`Id`) operations.
  #[error("{0}")]
  Id(I::Error),

  /// Errors originating from the transformation of node addresses.
  #[error("{0}")]
  Address(<R::Address as Transformable>::Error),

  /// Errors from the address resolver operations.
  #[error("{0}")]
  Resolver(R::Error),

  /// Errors related to encoding and decoding using the `Wire` trait.
  #[error("{0}")]
  Wire(W::Error),

  /// Error thrown when trying to operate on a transport that has already been terminated.
  #[error("transport already shutdown")]
  AlreadyShutdown,

  /// Error signifying that the append pipeline has been closed.
  #[error("append pipeline closed")]
  PipelingShutdown,

  /// Error indicating a failure to forward the request to the raft system.
  #[error("failed to forward request to raft")]
  Dispatch,

  /// Error indicating that the received response was not as expected.
  #[error("unexpected response {actual}, expected {expected}")]
  UnexpectedResponse {
    expected: &'static str,
    actual: &'static str,
  },

  /// Error returned from a remote node during communication.
  #[error("remote: {0}")]
  Remote(String),

  /// General I/O related errors.
  #[error("{0}")]
  IO(#[from] std::io::Error),

  /// Allows defining custom error messages.
  #[error("{0}")]
  Custom(String),
}

impl<I: Id, R: AddressResolver, W: Wire> core::fmt::Debug for Error<I, R, W> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    core::fmt::Display::fmt(&self, f)
  }
}

impl<I: Id, R: AddressResolver, W: Wire> TransportError for Error<I, R, W> {
  type Id = I;

  type Resolver = R;

  type Wire = W;

  fn id(err: <I as Transformable>::Error) -> Self
  where
    Self: Sized,
  {
    Self::Id(err)
  }

  fn address(err: <<R as AddressResolver>::Address as Transformable>::Error) -> Self
  where
    Self: Sized,
  {
    Self::Address(err)
  }

  fn resolver(err: <R as AddressResolver>::Error) -> Self
  where
    Self: Sized,
  {
    Self::Resolver(err)
  }

  fn wire(err: <W as Wire>::Error) -> Self {
    Self::Wire(err)
  }

  fn io(err: std::io::Error) -> Self
  where
    Self: Sized,
  {
    Self::IO(err)
  }

  fn custom<T>(msg: T) -> Self
  where
    Self: Sized,
    T: core::fmt::Display,
  {
    Self::Custom(msg.to_string())
  }
}

/// Encapsulates configuration for the network transport layer.
#[viewit::viewit]
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct NetTransportOptions<I: Id, A: Address> {
  /// The protocol version to use for encoding/decoding messages.
  protocol_version: ProtocolVersion,

  /// The local ID of the server we are running on.
  header: Header<I, A>,

  /// The address for the network transport layer bind to, if not set,
  /// will use the address of the header to resolve an address by using
  /// the [`AddressResolver`].
  #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
  advertise_addr: Option<SocketAddr>,

  /// Controls how many connections we will pool
  max_pool: usize,

  /// Controls the pipelining "optimization" when replicating
  /// entries to followers.
  ///
  /// Setting this to 1 explicitly disables pipelining since no overlapping of
  /// request processing is allowed. If set to 1 the pipelining code path is
  /// skipped entirely and every request is entirely synchronous.
  ///
  /// If zero is set (or left as default), [`DEFAULT_MAX_INFLIGHT_REQUESTS`] is used which
  /// is currently `2`. A value of `2` overlaps the preparation and sending of the
  /// next request while waiting for the previous response, but avoids additional
  /// queuing.
  ///
  /// Historically this was internally fixed at (effectively) 130 however
  /// performance testing has shown that in practice the pipelining optimization
  /// combines badly with batching and actually has a very large negative impact
  /// on commit latency when throughput is high, whilst having very little
  /// benefit on latency or throughput in any other case! See
  /// [#541](https://github.com/hashicorp/raft/pull/541) for more analysis of the
  /// performance impacts.
  ///
  /// Increasing this beyond `2` is likely to be beneficial only in very
  /// high-latency network conditions. HashiCorp doesn't recommend using our own
  /// products this way.
  max_inflight_requests: usize,

  /// Used to apply I/O deadlines. For InstallSnapshot, we multiply
  /// the timeout by (SnapshotSize / TimeoutScale).
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  timeout: Duration,
}

impl<I, A> NetTransportOptions<I, A>
where
  I: Id,
  A: Address,
{
  /// Create a new [`NetTransportOptions`] with default values.
  #[inline]
  pub const fn new(header: Header<I, A>) -> Self {
    Self {
      max_pool: 3,
      max_inflight_requests: DEFAULT_MAX_INFLIGHT_REQUESTS,
      timeout: Duration::from_secs(10),
      header,
      advertise_addr: None,
      protocol_version: ProtocolVersion::V1,
    }
  }
}

/// Provides a TCP based transport that can be
/// used to communicate with Raft on remote machines.
///
/// This transport is very simple and lightweight. Each request is
/// framed by sending a byte that indicates the message type, followed
/// by the [`Wire`] encoded request.
///
/// The response is an error string followed by the response object,
/// both are encoded using [`Wire`].
///
/// InstallSnapshot is special, in that after the request we stream
/// the entire state. That socket is not re-used as the connection state
/// is not known if there is an error.
pub struct NetTransport<I, R, S, W>
where
  I: Id + Send + Sync + 'static,
  R: AddressResolver,
  R::Address: Send + Sync + 'static,
  <<<R as AddressResolver>::Runtime as Runtime>::Sleep as Future>::Output: Send,
  S: StreamLayer,
  W: Wire<Id = I, Address = <R as AddressResolver>::Address>,
{
  shutdown: Arc<AtomicBool>,
  shutdown_tx: async_channel::Sender<()>,
  local_header: Header<I, <R as AddressResolver>::Address>,
  advertise_addr: SocketAddr,
  consumer: RpcConsumer<I, <R as AddressResolver>::Address>,
  resolver: R,
  wg: AsyncWaitGroup,
  conn_pool: Mutex<HashMap<<R as AddressResolver>::Address, S::Stream>>,
  conn_size: AtomicUsize,
  protocol_version: ProtocolVersion,
  max_pool: usize,
  max_inflight_requests: usize,
  timeout: Duration,
  _w: std::marker::PhantomData<W>,
}

impl<I, R, S, W> Transport for NetTransport<I, R, S, W>
where
  I: Id + Send + Sync + 'static,
  R: AddressResolver,
  R::Address: Send + Sync + 'static,

  <<<R as AddressResolver>::Runtime as Runtime>::Sleep as Future>::Output: Send,
  S: StreamLayer,
  W: Wire<Id = I, Address = <R as AddressResolver>::Address>,
{
  type Error = Error<Self::Id, Self::Resolver, Self::Wire>;
  type Runtime = <Self::Resolver as AddressResolver>::Runtime;
  type Options = NetTransportOptions<Self::Id, <Self::Resolver as AddressResolver>::Address>;

  type Id = I;

  // type Pipeline: AppendEntriesPipeline<Runtime = Self::Runtime>;

  type Resolver = R;

  type Wire = W;

  fn consumer(&self) -> RpcConsumer<Self::Id, <Self::Resolver as AddressResolver>::Address> {
    self.consumer.clone()
  }

  fn local_addr(&self) -> &<Self::Resolver as AddressResolver>::Address {
    self.local_header.addr()
  }

  fn local_id(&self) -> &Self::Id {
    self.local_header.id()
  }

  fn advertise_addr(&self) -> SocketAddr {
    self.advertise_addr
  }

  fn resolver(&self) -> &<Self as ruraft_core::transport::Transport>::Resolver {
    &self.resolver
  }

  async fn new(resolver: Self::Resolver, opts: Self::Options) -> Result<Self, Self::Error> {
    let (shutdown_tx, shutdown_rx) = async_channel::unbounded();
    let advertise_addr = if let Some(addr) = opts.advertise_addr {
      addr
    } else {
      tracing::warn!(target = "ruraft.net.transport", "advertise address is not set, will use the resolver to resolve the advertise address according to the header");
      resolver
        .resolve(opts.header.addr())
        .await
        .map_err(<Self::Error as TransportError>::resolver)?
    };
    let auto_port = advertise_addr.port() == 0;

    let ln = <S::Listener as Listener>::bind(advertise_addr)
      .await
      .map_err(|e| {
        tracing::error!(target = "ruraft.net.transport", err=%e, "failed to bind listener");
        Error::IO(e)
      })?;

    let advertise_addr = if auto_port {
      let addr = ln.local_addr()?;
      tracing::warn!(target = "ruraft.net.transport", local_addr=%addr, "listening on automatically assigned port {}", addr.port());
      addr
    } else {
      advertise_addr
    };

    let shutdown = Arc::new(AtomicBool::new(false));
    let wg = AsyncWaitGroup::from(1);
    let (producer, consumer) = rpc::<Self::Id, <Self::Resolver as AddressResolver>::Address>();
    let request_handler = RequestHandler {
      ln,
      local_header: opts.header.clone(),
      producer,
      shutdown: shutdown.clone(),
      shutdown_rx,
      wg: wg.clone(),
    };
    <Self::Runtime as Runtime>::spawn_detach(RequestHandler::<
      I,
      <Self::Resolver as AddressResolver>::Address,
      S,
    >::run::<Self::Resolver, Self::Wire>(
      request_handler
    ));

    Ok(Self {
      shutdown,
      shutdown_tx,
      local_header: opts.header,
      advertise_addr,
      resolver,
      consumer,
      conn_pool: Mutex::new(HashMap::with_capacity(opts.max_pool)),
      conn_size: AtomicUsize::new(0),
      protocol_version: opts.protocol_version,
      wg,
      max_pool: opts.max_pool,
      max_inflight_requests: if opts.max_inflight_requests == 0 {
        DEFAULT_MAX_INFLIGHT_REQUESTS
      } else {
        opts.max_inflight_requests
      },
      timeout: opts.timeout,
      _w: std::marker::PhantomData,
    })
  }

  async fn append_entries(
    &self,
    req: AppendEntriesRequest<Self::Id, <Self::Resolver as AddressResolver>::Address>,
  ) -> Result<
    AppendEntriesResponse<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    Self::Error,
  > {
    let header_addr = req.header().addr().clone();
    let addr = self
      .resolver
      .resolve(&header_addr)
      .await
      .map_err(<Self::Error as TransportError>::resolver)?;
    let req = Request::append_entries(req);
    let mut conn = BufReader::new(self.send(addr, req).await?);
    let resp = <Self::Wire as Wire>::decode_response(&mut conn)
      .await
      .map_err(<Self::Error as TransportError>::wire)?;
    match resp {
      Response::Error(err) => Err(Error::Remote(err.error)),
      Response::AppendEntries(resp) => {
        self.return_conn(conn.into_inner(), header_addr).await;
        Ok(resp)
      }
      kind => Err(Error::UnexpectedResponse {
        expected: "AppendEntries",
        actual: kind.description(),
      }),
    }
  }

  async fn vote(
    &self,
    req: VoteRequest<Self::Id, <Self::Resolver as AddressResolver>::Address>,
  ) -> Result<VoteResponse<Self::Id, <Self::Resolver as AddressResolver>::Address>, Self::Error> {
    let header_addr = req.header().addr().clone();
    let addr = self
      .resolver
      .resolve(&header_addr)
      .await
      .map_err(<Self::Error as TransportError>::resolver)?;
    let req = Request::vote(req);
    let mut conn = BufReader::new(self.send(addr, req).await?);
    let resp = <Self::Wire as Wire>::decode_response(&mut conn)
      .await
      .map_err(<Self::Error as TransportError>::wire)?;
    match resp {
      Response::Error(err) => Err(Error::Remote(err.error)),
      Response::Vote(resp) => {
        self.return_conn(conn.into_inner(), header_addr).await;
        Ok(resp)
      }
      kind => Err(Error::UnexpectedResponse {
        expected: "Vote",
        actual: kind.description(),
      }),
    }
  }

  async fn install_snapshot(
    &self,
    req: InstallSnapshotRequest<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    source: impl AsyncRead + Send,
  ) -> Result<
    InstallSnapshotResponse<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    Self::Error,
  > {
    let header_addr = req.header().addr().clone();
    let addr = self
      .resolver
      .resolve(&header_addr)
      .await
      .map_err(<Self::Error as TransportError>::resolver)?;
    let req = Request::install_snapshot(req);
    let conn = self.send(addr, req).await?;
    let mut w = BufWriter::with_capacity(CONN_SEND_BUFFER_SIZE, conn);
    futures::io::copy(source, &mut w).await?;
    let mut conn = BufReader::new(w.into_inner());
    let resp = <Self::Wire as Wire>::decode_response(&mut conn)
      .await
      .map_err(<Self::Error as TransportError>::wire)?;
    match resp {
      Response::Error(err) => Err(Error::Remote(err.error)),
      Response::InstallSnapshot(resp) => {
        self.return_conn(conn.into_inner(), header_addr).await;
        Ok(resp)
      }
      kind => Err(Error::UnexpectedResponse {
        expected: "InstallSnapshot",
        actual: kind.description(),
      }),
    }
  }

  async fn timeout_now(
    &self,
    req: TimeoutNowRequest<Self::Id, <Self::Resolver as AddressResolver>::Address>,
  ) -> Result<TimeoutNowResponse<Self::Id, <Self::Resolver as AddressResolver>::Address>, Self::Error>
  {
    let header_addr = req.header().addr().clone();
    let addr = self
      .resolver
      .resolve(&header_addr)
      .await
      .map_err(<Self::Error as TransportError>::resolver)?;
    let req = Request::timeout_now(req);
    let mut conn = BufReader::new(self.send(addr, req).await?);
    let resp = <Self::Wire as Wire>::decode_response(&mut conn)
      .await
      .map_err(<Self::Error as TransportError>::wire)?;
    match resp {
      Response::Error(err) => Err(Error::Remote(err.error)),
      Response::TimeoutNow(resp) => {
        self.return_conn(conn.into_inner(), header_addr).await;
        Ok(resp)
      }
      kind => Err(Error::UnexpectedResponse {
        expected: "TimeoutNow",
        actual: kind.description(),
      }),
    }
  }

  async fn heartbeat(
    &self,
    req: HeartbeatRequest<Self::Id, <Self::Resolver as AddressResolver>::Address>,
  ) -> Result<HeartbeatResponse<Self::Id, <Self::Resolver as AddressResolver>::Address>, Self::Error>
  {
    let header_addr = req.header().addr().clone();
    let addr = self
      .resolver
      .resolve(&header_addr)
      .await
      .map_err(<Self::Error as TransportError>::resolver)?;
    let req = Request::heartbeat(req);
    let mut conn = BufReader::new(self.send(addr, req).await?);
    let resp = <Self::Wire as Wire>::decode_response(&mut conn)
      .await
      .map_err(<Self::Error as TransportError>::wire)?;
    match resp {
      Response::Error(err) => Err(Error::Remote(err.error)),
      Response::Heartbeat(resp) => {
        self.return_conn(conn.into_inner(), header_addr).await;
        Ok(resp)
      }
      kind => Err(Error::UnexpectedResponse {
        expected: "Heartbeat",
        actual: kind.description(),
      }),
    }
  }

  async fn shutdown(&self) -> Result<(), Self::Error> {
    if self.shutdown.load(Ordering::Acquire) {
      return Ok(());
    }
    self.shutdown.store(true, Ordering::Release);
    self.shutdown_tx.close();
    self.wg.wait().await;
    Ok(())
  }
}

impl<I, R, S, W> NetTransport<I, R, S, W>
where
  I: Id + Send + Sync + 'static,
  R: AddressResolver,
  R::Address: Send + Sync + 'static,
  <<<R as AddressResolver>::Runtime as Runtime>::Sleep as Future>::Output: Send,
  S: StreamLayer,
  W: Wire<Id = I, Address = <R as AddressResolver>::Address>,
{
  async fn return_conn(&self, conn: S::Stream, addr: <R as AddressResolver>::Address) {
    if self.shutdown.load(Ordering::Acquire)
      && self.conn_size.load(Ordering::Acquire) > self.max_pool
    {
      return;
    }

    {
      let mut pool = self.conn_pool.lock().await;
      if pool.len() >= self.max_pool {
        return;
      }
      pool.insert(addr, conn);
    }

    self.conn_size.fetch_add(1, Ordering::AcqRel);
  }

  async fn send(
    &self,
    target: SocketAddr,
    req: Request<I, <R as AddressResolver>::Address>,
  ) -> Result<S::Stream, <Self as Transport>::Error> {
    // Get a connection
    let mut conn = {
      let mut pool = self.conn_pool.lock().await;
      match pool.remove(req.header().addr()) {
        Some(conn) => {
          self.conn_size.fetch_sub(1, Ordering::AcqRel);
          conn
        }
        None => {
          let conn = <S::Stream as Connection>::connect(target).await?;
          if !self.timeout.is_zero() {
            conn.set_timeout(Some(self.timeout));
          }
          conn
        }
      }
    };

    let data = <<Self as Transport>::Wire as Wire>::encode_request(&req)
      .map_err(<<Self as Transport>::Error as TransportError>::wire)?;
    conn.write_all(data.as_ref()).await?;
    conn.flush().await?;

    Ok(conn)
  }
}

impl<I, R, S, W> Drop for NetTransport<I, R, S, W>
where
  I: Id + Send + Sync + 'static,
  R: AddressResolver,
  R::Address: Send + Sync + 'static,
  <<<R as AddressResolver>::Runtime as Runtime>::Sleep as Future>::Output: Send,
  S: StreamLayer,
  W: Wire<Id = I, Address = <R as AddressResolver>::Address>,
{
  fn drop(&mut self) {
    use pollster::FutureExt as _;
    let _ = self.shutdown().block_on();
  }
}

/// Used to handle connection from remote peers.
struct RequestHandler<I: Id, A: Address, S: StreamLayer> {
  ln: S::Listener,
  local_header: Header<I, A>,
  producer: RpcProducer<I, A>,
  shutdown: Arc<AtomicBool>,
  shutdown_rx: async_channel::Receiver<()>,
  wg: AsyncWaitGroup,
}

impl<I, A, S> RequestHandler<I, A, S>
where
  I: Id + Send + Sync + 'static,
  A: Address + Send + Sync + 'static,
  S: StreamLayer,
{
  async fn run<Resolver: AddressResolver, W: Wire<Id = I, Address = A>>(self)
  where
    <Resolver as AddressResolver>::Runtime: Runtime,
    <<<Resolver as AddressResolver>::Runtime as Runtime>::Sleep as Future>::Output: Send,
  {
    const BASE_DELAY: Duration = Duration::from_millis(5);
    const MAX_DELAY: Duration = Duration::from_secs(1);

    scopeguard::defer!(self.wg.done());

    let mut loop_delay = Duration::ZERO;
    loop {
      futures::select! {
        res = self.ln.accept().fuse() => {
          // Accept incoming connections
          match res {
            Ok((conn, addr)) => {
              // No error, reset loop delay
              loop_delay = Duration::ZERO;

              tracing::debug!(target = "ruraft.net.transport", remote = %addr, "accepted connection");

              // Spawn a task to handle the connection
              let producer = self.producer.clone();
              let shutdown_rx = self.shutdown_rx.clone();
              let local_header = self.local_header.clone();
              let wg = self.wg.add(1);
              <<Resolver as AddressResolver>::Runtime as agnostic::Runtime>::spawn_detach(async move {
                if Self::handle_connection::<Resolver, W>(conn, producer, shutdown_rx, local_header)
                  .await
                  .is_err()
                {
                  // We do not need to log error here, error has been logged in handle_connection
                  tracing::error!(target = "ruraft.net.transport", remote = %addr, "failed to handle connection");
                }
                wg.done();
              });
            }
            Err(e) => {
              if loop_delay.is_zero() {
                loop_delay = BASE_DELAY;
              } else {
                loop_delay *= 2;
              }

              if loop_delay > MAX_DELAY {
                loop_delay = MAX_DELAY;
              }

              if !self.shutdown.load(Ordering::Acquire) {
                tracing::error!(target = "ruraft.net.transport", err=%e, "failed to accept connection");
              }

              futures::select! {
                _ = <<Resolver as AddressResolver>::Runtime as agnostic::Runtime>::sleep(loop_delay).fuse() => continue,
                _ = self.shutdown_rx.recv().fuse() => {
                  tracing::debug!(target = "ruraft.net.transport", "received shutdown signal, exit request handler task...");
                  return;
                },
              }
            }
          };
        }
        _ = self.shutdown_rx.recv().fuse() => {
          tracing::debug!(target = "ruraft.net.transport", "received shutdown signal, exit request handler task...");
          return;
        }
      }
    }
  }

  async fn handle_connection<Resolver: AddressResolver, W: Wire<Id = I, Address = A>>(
    conn: S::Stream,
    producer: RpcProducer<I, A>,
    shutdown_rx: async_channel::Receiver<()>,
    local_header: Header<I, A>,
  ) -> Result<(), Error<I, Resolver, W>>
  where
    <Resolver as AddressResolver>::Runtime: Runtime,
    <<<Resolver as AddressResolver>::Runtime as Runtime>::Sleep as Future>::Output: Send,
  {
    let mut r = BufReader::with_capacity(CONN_RECEIVE_BUFFER_SIZE, conn);

    let _get_type_start = Instant::now();

    // TODO: metrics
    // measuring the time to get the first byte separately because the heartbeat conn will hang out here
    // for a good while waiting for a heartbeat whereas the append entries/rpc conn should not.

    #[cfg(feature = "metrics")]
    let decode_start = Instant::now();
    // Get the request meta
    let req = <W as Wire>::decode_request(&mut r)
      .await
      .map_err(Error::Wire)?;
    #[cfg(feature = "metrics")]
    {
      let decode_label = req.decode_label();
      metrics::histogram!(decode_label, decode_start.elapsed().as_millis() as f64);
    }

    #[cfg(feature = "metrics")]
    let process_start = Instant::now();
    #[cfg(feature = "metrics")]
    let enqueue_label = req.enqueue_label();
    #[cfg(feature = "metrics")]
    let respond_label = req.respond_label();

    let resp = if let Request::Heartbeat(_) = &req {
      Response::heartbeat(HeartbeatResponse::new(local_header))
    } else {
      let (tx, handle) = Rpc::<I, A>::new(req);
      futures::select! {
        res = producer.send(tx).fuse() => {
          #[cfg(feature = "metrics")]
          metrics::histogram!(enqueue_label, process_start.elapsed().as_millis() as f64);

          match res {
            Ok(_) => {
              #[cfg(feature = "metrics")]
              let resp_wait_start = Instant::now();

              futures::select! {
                res = handle.fuse() => {
                  #[cfg(feature = "metrics")]
                  metrics::histogram!(respond_label, resp_wait_start.elapsed().as_millis() as f64);

                  match res {
                    Ok(resp) => resp,
                    Err(e) => {
                      tracing::error!(target = "ruraft.net.transport", err=%e, "canceled command");
                      Response::error(ErrorResponse::new(local_header, e.to_string()))
                    },
                  }
                },
                _ = shutdown_rx.recv().fuse() => return Err(Error::AlreadyShutdown),
              }
            },
            Err(e) => {
              tracing::error!(target = "ruraft.net.transport", err=%e, "failed to send dispatch request");
              return Err(Error::Dispatch);
            },
          }
        },
        _ = shutdown_rx.recv().fuse() => return Err(Error::AlreadyShutdown),
      }
    };
    let resp = W::encode_response(&resp).map_err(|e| {
      tracing::error!(target = "ruraft.net.transport", err=%e, "failed to encode response");
      Error::Wire(e)
    })?;

    r.into_inner().write_all(resp.as_ref()).await.map_err(|e| {
      tracing::error!(target = "ruraft.net.transport", err=%e, "failed to send response");
      Error::IO(e)
    })
  }
}

#[cfg(feature = "metrics")]
trait RequestMetricsExt {
  fn enqueue_label(&self) -> &'static str;

  fn respond_label(&self) -> &'static str;

  fn decode_label(&self) -> &'static str;
}

#[cfg(feature = "metrics")]
impl<I: Id, A: Address> RequestMetricsExt for Request<I, A> {
  fn enqueue_label(&self) -> &'static str {
    match self {
      Self::AppendEntries(_) => "ruraft.net.rpc.enqueue.append_entries",
      Self::Vote(_) => "ruraft.net.rpc.enqueue.vote",
      Self::InstallSnapshot(_) => "ruraft.net.rpc.enqueue.install_snapshot",
      Self::TimeoutNow(_) => "ruraft.net.rpc.enqueue.timeout_now",
      Self::Heartbeat(_) => "ruraft.net.rpc.enqueue.heartbeat",
      _ => unreachable!(),
    }
  }

  fn respond_label(&self) -> &'static str {
    match self {
      Self::AppendEntries(_) => "ruraft.net.rpc.respond.append_entries",
      Self::Vote(_) => "ruraft.net.rpc.respond.vote",
      Self::InstallSnapshot(_) => "ruraft.net.rpc.respond.install_snapshot",
      Self::TimeoutNow(_) => "ruraft.net.rpc.respond.timeout_now",
      Self::Heartbeat(_) => "ruraft.net.rpc.respond.heartbeat",
      _ => unreachable!(),
    }
  }

  fn decode_label(&self) -> &'static str {
    match self {
      Self::AppendEntries(_) => "ruraft.net.rpc.decode.append_entries",
      Self::Vote(_) => "ruraft.net.rpc.decode.vote",
      Self::InstallSnapshot(_) => "ruraft.net.rpc.decode.install_snapshot",
      Self::TimeoutNow(_) => "ruraft.net.rpc.decode.timeout_now",
      Self::Heartbeat(_) => "ruraft.net.rpc.decode.heartbeat",
      _ => unreachable!(),
    }
  }
}

// #[cfg(any(test, feature = "test"))]
// pub(super) mod tests {
//   use nodecraft::{NodeId, resolver::socket_addr::SocketAddrResolver};
//   use ruraft_core::storage::{Log, LogKind};

//   use super::*;

//   pub trait NodeIdExt {
//     fn random() -> Self;
//   }

//   impl NodeIdExt for NodeId {
//     fn random() -> Self {
//       use rand::Rng;
//       use rand::distributions::Alphanumeric;
//       let rand_string: String = rand::thread_rng()
//         .sample_iter(&Alphanumeric)
//         .take(32) // Adjust this to the desired length
//         .map(char::from)
//         .collect();

//       NodeId::new(rand_string)
//     }
//   }

//   async fn make_transport<R>() -> NetTransport<NodeId, SocketAddrResolver<Runtime = R>, SomeWire>
//   where
//     R: Runtime,
//     <R::Sleep as Future>::Output: Send,
//   {
//     let opts = NetTransportOptions::new(NodeId::random(), "127.0.0.1:0".parse().unwrap());
//     NetTransport::<R>::new(SocketAddrResolver::default(), opts).await.unwrap()
//   }

//   fn make_append_req(id: ServerId, addr: SocketAddr) -> AppendEntriesRequest {
//     AppendEntriesRequest {
//       header: Header::new(ProtocolVersion::V1, id, addr),
//       term: 10,
//       prev_log_entry: 100,
//       prev_log_term: 4,
//       entries: vec![Log::new(101, 4, LogKind::Noop)],
//       leader_commit: 90,
//     }
//   }

//   fn make_append_resp(id: ServerId, addr: SocketAddr) -> AppendEntriesResponse {
//     AppendEntriesResponse {
//       header: Header::new(ProtocolVersion::V1, id, addr),
//       term: 4,
//       last_log: 90,
//       success: true,
//       no_retry_backoff: false,
//     }
//   }

//   pub async fn test_net_transport_start_stop<R: Runtime>()
//   where
//     <R::Sleep as Future>::Output: Send,
//   {
//     let trans = NetTransport::<R>::new(NetTransportOptions::new(
//       NodeId::random(),
//       "127.0.0.1:0".parse().unwrap(),
//     ))
//     .await
//     .unwrap();

//     trans.shutdown().await.unwrap();
//   }

//   pub async fn test_net_transport_append_entries<R>()
//   where
//     R: Runtime,
//     <R::Sleep as Future>::Output: Send,
//   {
//     let trans1 = NetTransport::<R>::new(NetTransportOptions::new(
//       NodeId::random(),
//       "127.0.0.1:0".parse().unwrap(),
//     ))
//     .await
//     .unwrap();

//     let trans1_addr = trans1.local_addr();
//     let args = make_append_req(trans1.local_id().clone(), trans1_addr);
//     let expected_resp = make_append_resp(trans1.local_id().clone(), trans1_addr);
//     let mut consumer = trans1.consumer();
//     let resp = expected_resp.clone();

//     R::spawn_detach(async move {
//       use futures::StreamExt;

//       futures::select! {
//         req = consumer.next().fuse() => {
//           let req = req.unwrap();
//           let Ok(_) = req.respond(Response::append_entries(ProtocolVersion::V1, resp)) else {
//             panic!("unexpected respond fail");
//           };
//         },
//         _ = R::sleep(Duration::from_millis(200)).fuse() => {
//           panic!("timeout");
//         },
//       }
//     });

//     let trans2 = NetTransport::<R>::new(NetTransportOptions::new(
//       NodeId::random(),
//       "127.0.0.1:0".parse().unwrap(),
//     ))
//     .await
//     .unwrap();

//     let res = trans2.append_entries(args).await.unwrap();
//     assert_eq!(res, expected_resp);
//   }
// }
