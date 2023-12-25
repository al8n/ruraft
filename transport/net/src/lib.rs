//! A high level abstraction for the network transport layer of [`ruraft`](https://github.com/al8n/ruraft),
//! which helps the developers to implement the network transport layer for [`ruraft`](https://github.com/al8n/ruraft) easily.
//!
//! # Usage
//!
//! Please see how those network transport layer implementations are implemented based on this crate:
//!
//! 1. [`ruraft-tcp`](https://github.com/al8n/ruraft/tree/main/transport/tcp).
//! 2. [`ruraft-quinn`](https://github.com/al8n/ruraft/tree/main/transport/quinn).
//! 3. [`ruraft-s2n`](https://github.com/al8n/ruraft/tree/main/transport/s2n).
//!
#![forbid(unsafe_code)]
#![allow(clippy::type_complexity)]
#![deny(missing_docs, warnings)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
/// Stream layer abstraction.
pub mod stream;
use arc_swap::ArcSwapOption;
use nodecraft::CheapClone;
use ruraft_utils::io::LimitedReader;
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
use ruraft_core::{options::ProtocolVersion, transport::*, Data, HeartbeatHandler, Node};
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
// TODO(al8n): change it back to 2 when async_channel::bounded supports zero cap.
const MIN_IN_FLIGHT_FOR_PIPELINING: usize = 3;

/// Represents errors specific to the [`NetTransport`].
///
/// This enum captures a comprehensive set of errors that can arise when using the `NetTransport`.
/// It categorizes these errors based on their origin, whether from the `Id`, `AddressResolver`,
/// `Wire` operations or more general transport-level concerns.
#[derive(thiserror::Error)]
pub enum Error<I: Id, A: AddressResolver, W: Wire> {
  /// Errors arising from the node identifier (`Id`) operations.
  #[error("{0}")]
  Id(I::Error),

  /// Errors originating from the transformation of node addresses.
  #[error("{0}")]
  Address(<A::Address as Transformable>::Error),

  /// Errors from the address resolver operations.
  #[error("{0}")]
  Resolver(A::Error),

  /// Errors related to encoding and decoding using the `Wire` trait.
  #[error("{0}")]
  Wire(W::Error),

  /// Error thrown when trying to operate on a transport that has already been terminated.
  #[error("transport already shutdown")]
  AlreadyShutdown,

  /// Error thrown when trying to replicate logs in pipeline mode, but got rejected.
  #[error("pipeline replication not supported, reach max in-flight bound")]
  PipelineReplicationNotSupported(usize),

  /// Error signifying that the append pipeline has been closed.
  #[error("append pipeline closed")]
  PipelineShutdown,

  /// Error indicating a failure to forward the request to the raft system.
  #[error("failed to forward request to raft")]
  Dispatch,

  /// Error indicating that the received response was not as expected.
  #[error("unexpected response {actual}, expected {expected}")]
  UnexpectedResponse {
    /// Expected response type.
    expected: &'static str,
    /// Actual response type.
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

impl<I: Id, A: AddressResolver, W: Wire> TransportError for Error<I, A, W> {
  type Id = I;

  type Resolver = A;

  type Wire = W;

  fn id(err: <I as Transformable>::Error) -> Self
  where
    Self: Sized,
  {
    Self::Id(err)
  }

  fn address(err: <<A as AddressResolver>::Address as Transformable>::Error) -> Self
  where
    Self: Sized,
  {
    Self::Address(err)
  }

  fn resolver(err: <A as AddressResolver>::Error) -> Self
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

  fn with_message(self, msg: std::borrow::Cow<'static, str>) -> Self {
    Self::Custom(msg.into_owned())
  }

  fn is_pipeline_replication_not_supported(&self) -> bool {
    matches!(self, Self::PipelineReplicationNotSupported(_))
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
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct NetTransportOptions<I: Id, A: Address> {
  /// The local header of the header of the network transport.
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Returns the header of the network transport.")
    ),
    setter(attrs(doc = "Sets the header of the network transport."))
  )]
  header: Header<I, A>,

  /// The address for the network transport layer bind to, if not set,
  /// will use the address of the header to resolve an address by using
  /// the [`AddressResolver`].
  #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
  #[viewit(
    getter(
      const,
      attrs(doc = "Returns the socket address of this network transport layer bind to.")
    ),
    setter(attrs(doc = "Sets the socket address of this network transport layer bind to."))
  )]
  advertise_addr: Option<SocketAddr>,

  /// Controls how many connections we will pool
  #[viewit(
    getter(const, attrs(doc = "Returns how many connections we will pool.")),
    setter(attrs(doc = "Sets how many connections we will pool."))
  )]
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
  #[viewit(
    getter(const, attrs(doc = "Returns the max inflight requests.")),
    setter(attrs(doc = "Sets the max inflight requests."))
  )]
  max_inflight_requests: usize,

  /// Used to apply I/O deadlines. For InstallSnapshot, we multiply
  /// the timeout by (SnapshotSize / TimeoutScale).
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  #[viewit(
    getter(const, attrs(doc = "Returns the timeout used to apply I/O deadlines.")),
    setter(attrs(doc = "Sets the timeout used to apply I/O deadlines."))
  )]
  timeout: Duration,
}

impl<I, A> Clone for NetTransportOptions<I, A>
where
  I: Id,
  A: Address,
{
  fn clone(&self) -> Self {
    Self {
      max_pool: self.max_pool,
      max_inflight_requests: self.max_inflight_requests,
      timeout: self.timeout,
      header: self.header.clone(),
      advertise_addr: self.advertise_addr,
    }
  }
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
pub struct NetTransport<I, A, D, S, W>
where
  A: AddressResolver,

  S: StreamLayer,
{
  shutdown: Arc<AtomicBool>,
  shutdown_tx: async_channel::Sender<()>,
  local_header: Header<I, <A as AddressResolver>::Address>,
  advertise_addr: SocketAddr,
  consumer: RpcConsumer<
    I,
    <A as AddressResolver>::Address,
    D,
    LimitedReader<BufReader<<S::Stream as Connection>::OwnedReadHalf>>,
  >,
  resolver: A,
  wg: AsyncWaitGroup,
  conn_pool: Mutex<HashMap<<A as AddressResolver>::Address, S::Stream>>,
  conn_size: AtomicUsize,
  protocol_version: ProtocolVersion,
  max_pool: usize,
  max_inflight_requests: usize,
  timeout: Duration,
  stream_layer: S,
  heartbeat_handler: Arc<ArcSwapOption<HeartbeatHandler<I, A::Address>>>,
  _w: std::marker::PhantomData<W>,
}

impl<I, A, D, S, W> NetTransport<I, A, D, S, W>
where
  I: Id,

  A: AddressResolver<ResolvedAddress = SocketAddr>,

  <<<A as AddressResolver>::Runtime as Runtime>::Sleep as Future>::Output: Send,
  D: Data,
  S: StreamLayer,
  W: Wire<Id = I, Address = <A as AddressResolver>::Address, Data = D>,
{
  /// Create a new [`NetTransport`].
  pub async fn new(
    resolver: A,
    mut stream_layer: S,
    opts: NetTransportOptions<I, A::Address>,
  ) -> Result<Self, Error<I, A, W>> {
    let (shutdown_tx, shutdown_rx) = async_channel::unbounded();
    let advertise_addr = if let Some(addr) = opts.advertise_addr {
      addr
    } else {
      tracing::warn!(target = "ruraft.net.transport", "advertise address is not set, will use the resolver to resolve the advertise address according to the header");
      resolver
        .resolve(opts.header.from().addr())
        .await
        .map_err(<Error<_, _, _> as TransportError>::resolver)?
    };
    let auto_port = advertise_addr.port() == 0;

    let ln = stream_layer.bind(advertise_addr).await.map_err(|e| {
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
    let (producer, consumer) = rpc();
    let heartbeat_handler = Arc::new(ArcSwapOption::from_pointee(None));
    let request_handler = RequestHandler {
      ln,
      local_header: opts.header.clone(),
      producer,
      shutdown: shutdown.clone(),
      shutdown_rx,
      wg: wg.clone(),
      heartbeat_handler: heartbeat_handler.clone(),
    };
    <A::Runtime as Runtime>::spawn_detach(RequestHandler::<I, A::Address, D, S>::run::<A, W>(
      request_handler,
    ));

    Ok(Self {
      shutdown,
      shutdown_tx,
      advertise_addr,
      resolver,
      consumer,
      conn_pool: Mutex::new(HashMap::with_capacity(opts.max_pool)),
      conn_size: AtomicUsize::new(0),
      protocol_version: opts.header.protocol_version(),
      local_header: opts.header,
      wg,
      max_pool: opts.max_pool,
      max_inflight_requests: if opts.max_inflight_requests == 0 {
        DEFAULT_MAX_INFLIGHT_REQUESTS
      } else {
        opts.max_inflight_requests
      },
      timeout: opts.timeout,
      stream_layer,
      heartbeat_handler,
      _w: std::marker::PhantomData,
    })
  }
}

impl<I, A, D, S, W> Transport for NetTransport<I, A, D, S, W>
where
  I: Id,

  A: AddressResolver<ResolvedAddress = SocketAddr>,

  <<<A as AddressResolver>::Runtime as Runtime>::Sleep as Future>::Output: Send,
  D: Data,
  S: StreamLayer,
  W: Wire<Id = I, Address = <A as AddressResolver>::Address, Data = D>,
{
  type Error = Error<Self::Id, Self::Resolver, Self::Wire>;
  type Runtime = <Self::Resolver as AddressResolver>::Runtime;

  type Id = I;

  type Pipeline = NetAppendEntriesPipeline<Self::Id, Self::Resolver, Self::Data, S, Self::Wire>;

  type Resolver = A;

  type Wire = W;

  type Data = D;

  type SnapshotInstaller = LimitedReader<BufReader<<S::Stream as Connection>::OwnedReadHalf>>;

  fn consumer(
    &self,
  ) -> RpcConsumer<
    Self::Id,
    <Self::Resolver as AddressResolver>::Address,
    Self::Data,
    Self::SnapshotInstaller,
  > {
    self.consumer.clone()
  }

  fn local_addr(&self) -> &<Self::Resolver as AddressResolver>::Address {
    self.local_header.from().addr()
  }

  fn local_id(&self) -> &Self::Id {
    self.local_header.from().id()
  }

  fn version(&self) -> ProtocolVersion {
    self.protocol_version
  }

  fn advertise_addr(&self) -> &<Self::Resolver as AddressResolver>::ResolvedAddress {
    &self.advertise_addr
  }

  fn resolver(&self) -> &Self::Resolver {
    &self.resolver
  }

  fn set_heartbeat_handler(
    &self,
    handler: Option<HeartbeatHandler<Self::Id, <Self::Resolver as AddressResolver>::Address>>,
  ) {
    self.heartbeat_handler.store(handler.map(Arc::new));
  }

  async fn append_entries_pipeline(
    &self,
    target: Node<Self::Id, <Self::Resolver as AddressResolver>::Address>,
  ) -> Result<Self::Pipeline, Self::Error> {
    if self.max_inflight_requests < MIN_IN_FLIGHT_FOR_PIPELINING {
      // Pipelining is disabled since no more than one request can be outstanding
      // at once. Skip the whole code path and use synchronous requests
      return Err(
        Error::<Self::Id, Self::Resolver, Self::Wire>::PipelineReplicationNotSupported(
          self.max_inflight_requests,
        ),
      );
    }

    // Get a connection
    let target_addr = target.addr().cheap_clone();
    let addr = self
      .resolver
      .resolve(&target_addr)
      .await
      .map_err(<Self::Error as TransportError>::resolver)?;

    let conn = self
      .stream_layer
      .connect(addr)
      .await
      .map_err(<Self::Error as TransportError>::io)?;

    Ok(NetAppendEntriesPipeline::new(
      conn,
      self.max_inflight_requests,
      self.timeout,
    ))
  }

  async fn append_entries(
    &self,
    target: &Node<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    req: AppendEntriesRequest<Self::Id, <Self::Resolver as AddressResolver>::Address, Self::Data>,
  ) -> Result<
    AppendEntriesResponse<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    Self::Error,
  > {
    let target_addr: <A as AddressResolver>::Address = target.addr().cheap_clone();
    let addr = self
      .resolver
      .resolve(&target_addr)
      .await
      .map_err(<Self::Error as TransportError>::resolver)?;
    let req = Request::append_entries(req);
    let mut conn = BufReader::new(self.send(addr, req).await?);
    let resp = <Self::Wire as Wire>::decode_response_from_reader(&mut conn)
      .await
      .map_err(|e| {
        <Self::Error as TransportError>::wire(<<Self::Wire as Wire>::Error as WireError>::io(e))
      })?;
    match resp {
      Response::Error(err) => Err(Error::Remote(err.error)),
      Response::AppendEntries(resp) => {
        self.return_conn(conn.into_inner(), target_addr).await;
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
    target: &Node<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    req: VoteRequest<Self::Id, <Self::Resolver as AddressResolver>::Address>,
  ) -> Result<VoteResponse<Self::Id, <Self::Resolver as AddressResolver>::Address>, Self::Error> {
    let target_addr: <A as AddressResolver>::Address = target.addr().cheap_clone();
    let addr = self
      .resolver
      .resolve(&target_addr)
      .await
      .map_err(<Self::Error as TransportError>::resolver)?;
    let req = Request::vote(req);
    let mut conn = BufReader::new(self.send(addr, req).await?);
    let resp = <Self::Wire as Wire>::decode_response_from_reader(&mut conn)
      .await
      .map_err(|e| {
        <Self::Error as TransportError>::wire(<<Self::Wire as Wire>::Error as WireError>::io(e))
      })?;
    match resp {
      Response::Error(err) => Err(Error::Remote(err.error)),
      Response::Vote(resp) => {
        self.return_conn(conn.into_inner(), target_addr).await;
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
    target: &Node<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    req: InstallSnapshotRequest<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    source: impl AsyncRead + Send + Unpin,
  ) -> Result<
    InstallSnapshotResponse<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    Self::Error,
  > {
    let target_addr: <A as AddressResolver>::Address = target.addr().cheap_clone();
    let addr = self
      .resolver
      .resolve(&target_addr)
      .await
      .map_err(<Self::Error as TransportError>::resolver)?;
    let req = Request::install_snapshot(req);
    let conn = self.send(addr, req).await?;
    let mut w = BufWriter::with_capacity(CONN_SEND_BUFFER_SIZE, conn);
    futures::io::copy(source, &mut w).await?;
    let mut conn = BufReader::new(w.into_inner());
    let resp = <Self::Wire as Wire>::decode_response_from_reader(&mut conn)
      .await
      .map_err(|e| {
        <Self::Error as TransportError>::wire(<<Self::Wire as Wire>::Error as WireError>::io(e))
      })?;

    match resp {
      Response::Error(err) => Err(Error::Remote(err.error)),
      Response::InstallSnapshot(resp) => {
        self.return_conn(conn.into_inner(), target_addr).await;
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
    target: &Node<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    req: TimeoutNowRequest<Self::Id, <Self::Resolver as AddressResolver>::Address>,
  ) -> Result<TimeoutNowResponse<Self::Id, <Self::Resolver as AddressResolver>::Address>, Self::Error>
  {
    let target_addr: <A as AddressResolver>::Address = target.addr().cheap_clone();
    let addr = self
      .resolver
      .resolve(&target_addr)
      .await
      .map_err(<Self::Error as TransportError>::resolver)?;
    let req = Request::timeout_now(req);
    let mut conn = BufReader::new(self.send(addr, req).await?);
    let resp = <Self::Wire as Wire>::decode_response_from_reader(&mut conn)
      .await
      .map_err(|e| {
        <Self::Error as TransportError>::wire(<<Self::Wire as Wire>::Error as WireError>::io(e))
      })?;
    match resp {
      Response::Error(err) => Err(Error::Remote(err.error)),
      Response::TimeoutNow(resp) => {
        self.return_conn(conn.into_inner(), target_addr).await;
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
    target: &Node<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    req: HeartbeatRequest<Self::Id, <Self::Resolver as AddressResolver>::Address>,
  ) -> Result<HeartbeatResponse<Self::Id, <Self::Resolver as AddressResolver>::Address>, Self::Error>
  {
    let target_addr: <A as AddressResolver>::Address = target.addr().cheap_clone();
    let addr = self
      .resolver
      .resolve(&target_addr)
      .await
      .map_err(<Self::Error as TransportError>::resolver)?;
    let req = Request::heartbeat(req);
    let mut conn = BufReader::new(self.send(addr, req).await?);
    let resp = <Self::Wire as Wire>::decode_response_from_reader(&mut conn)
      .await
      .map_err(|e| {
        <Self::Error as TransportError>::wire(<<Self::Wire as Wire>::Error as WireError>::io(e))
      })?;
    match resp {
      Response::Error(err) => Err(Error::Remote(err.error)),
      Response::Heartbeat(resp) => {
        self.return_conn(conn.into_inner(), target_addr).await;
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

impl<I, A, D, S, W> NetTransport<I, A, D, S, W>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  <<<A as AddressResolver>::Runtime as Runtime>::Sleep as Future>::Output: Send,
  D: Data,
  S: StreamLayer,
  W: Wire<Id = I, Address = <A as AddressResolver>::Address, Data = D>,
{
  async fn return_conn(&self, conn: S::Stream, addr: <A as AddressResolver>::Address) {
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
    req: Request<I, <A as AddressResolver>::Address, D>,
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
          let conn = self.stream_layer.connect(target).await?;
          if !self.timeout.is_zero() {
            conn.set_timeout(Some(self.timeout));
          }
          conn
        }
      }
    };

    <<Self as Transport>::Wire as Wire>::encode_request_to_writer(&req, &mut conn)
      .await
      .map_err(|e| {
        <<Self as Transport>::Error as TransportError>::wire(<W as Wire>::Error::io(e))
      })?;
    conn.flush().await?;

    Ok(conn)
  }
}

impl<I, A, D, S, W> Drop for NetTransport<I, A, D, S, W>
where
  A: AddressResolver,
  S: StreamLayer,
{
  fn drop(&mut self) {
    if self.shutdown.load(Ordering::Acquire) {
      return;
    }
    self.shutdown.store(true, Ordering::Release);
    self.shutdown_tx.close();
    self.wg.block_wait();
  }
}

/// Used to handle connection from remote peers.
struct RequestHandler<I, A, D, S: StreamLayer> {
  ln: S::Listener,
  local_header: Header<I, A>,
  producer:
    RpcProducer<I, A, D, LimitedReader<BufReader<<S::Stream as Connection>::OwnedReadHalf>>>,
  shutdown: Arc<AtomicBool>,
  shutdown_rx: async_channel::Receiver<()>,
  heartbeat_handler: Arc<ArcSwapOption<HeartbeatHandler<I, A>>>,
  wg: AsyncWaitGroup,
}

impl<I, A, D, S> RequestHandler<I, A, D, S>
where
  I: Id,
  A: Address,
  D: Data,
  S: StreamLayer,
{
  async fn run<Resolver: AddressResolver, W: Wire<Id = I, Address = A, Data = D>>(mut self)
  where
    <Resolver as AddressResolver>::Runtime: Runtime,
    <<<Resolver as AddressResolver>::Runtime as Runtime>::Sleep as Future>::Output: Send,
  {
    const BASE_DELAY: Duration = Duration::from_millis(5);
    const MAX_DELAY: Duration = Duration::from_secs(1);

    let wg = self.wg;
    scopeguard::defer!(wg.done());

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
              let wg = wg.add(1);
              let heartbeat_handler = self.heartbeat_handler.clone();
              <<Resolver as AddressResolver>::Runtime as agnostic::Runtime>::spawn_detach(async move {
                if Self::handle_connection::<Resolver, W>(
                  &heartbeat_handler,
                  conn,
                  producer,
                  shutdown_rx,
                  local_header
                )
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

  async fn handle_connection<Resolver: AddressResolver, W: Wire<Id = I, Address = A, Data = D>>(
    heartbeat_handler: &ArcSwapOption<HeartbeatHandler<I, A>>,
    conn: S::Stream,
    producer: RpcProducer<
      I,
      A,
      D,
      LimitedReader<BufReader<<S::Stream as Connection>::OwnedReadHalf>>,
    >,

    shutdown_rx: async_channel::Receiver<()>,
    local_header: Header<I, A>,
  ) -> Result<(), Error<I, Resolver, W>>
  where
    <Resolver as AddressResolver>::Runtime: Runtime,
    <<<Resolver as AddressResolver>::Runtime as Runtime>::Sleep as Future>::Output: Send,
  {
    let (mut reader, writer) = {
      let (reader, writer) = conn.into_split();
      (
        BufReader::with_capacity(CONN_RECEIVE_BUFFER_SIZE, reader),
        BufWriter::with_capacity(CONN_SEND_BUFFER_SIZE, writer),
      )
    };

    // TODO: metrics
    // measuring the time to get the first byte separately because the heartbeat conn will hang out here
    // for a good while waiting for a heartbeat whereas the append entries/rpc conn should not.

    #[cfg(feature = "metrics")]
    let decode_start = Instant::now();
    // Get the request meta
    let req = <W as Wire>::decode_request_from_reader(&mut reader)
      .await
      .map_err(|e| Error::Wire(<<W as Wire>::Error as WireError>::io(e)))?;
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

    let mut is_heartbeat = false;
    let (rpc, handle) = match &req {
      Request::InstallSnapshot(ireq) => {
        let lr = LimitedReader::new(*ireq.size(), reader);
        Rpc::new(req, Some(lr))
      }
      Request::Heartbeat(_) => {
        is_heartbeat = true;
        Rpc::new(req, None)
      }
      _ => Rpc::new(req, None),
    };

    // Check for heartbeat fast-path
    if is_heartbeat {
      let handler = heartbeat_handler.load();
      if let Some(h) = handler.as_ref() {
        let (sender, req, _) = rpc.into_components();
        if let Request::Heartbeat(req) = req {
          h.handle_heartbeat(local_header.clone(), req, sender).await;
        } else {
          unreachable!();
        }

        return Self::wait_and_send_response(
          writer,
          local_header,
          handle,
          shutdown_rx,
          respond_label,
        )
        .await;
      }
    }

    // Dispatch the RPC
    futures::select! {
      res = producer.send(rpc).fuse() => {
        match res {
          Ok(_) => {},
          Err(e) => {
            tracing::error!(target = "ruraft.net.transport", err=%e, "failed to send dispatch request");
            return Err(Error::Dispatch);
          },
        }
      },
      _ = shutdown_rx.recv().fuse() => return Err(Error::AlreadyShutdown),
    }

    #[cfg(feature = "metrics")]
    metrics::histogram!(enqueue_label, process_start.elapsed().as_millis() as f64);

    // Wait for response
    Self::wait_and_send_response(writer, local_header, handle, shutdown_rx, respond_label).await
  }

  async fn wait_and_send_response<
    Resolver: AddressResolver,
    W: Wire<Id = I, Address = A, Data = D>,
  >(
    mut writer: BufWriter<<S::Stream as Connection>::OwnedWriteHalf>,
    local_header: Header<I, A>,
    handle: RpcHandle<I, A>,
    shutdown_rx: async_channel::Receiver<()>,
    #[cfg(feature = "metrics")] respond_label: &'static str,
  ) -> Result<(), Error<I, Resolver, W>>
  where
    <Resolver as AddressResolver>::Runtime: Runtime,
    <<<Resolver as AddressResolver>::Runtime as Runtime>::Sleep as Future>::Output: Send,
  {
    #[cfg(feature = "metrics")]
    let resp_wait_start = Instant::now();
    #[cfg(feature = "metrics")]
    scopeguard::defer!(metrics::histogram!(
      respond_label,
      resp_wait_start.elapsed().as_millis() as f64
    ));

    let resp = futures::select! {
      res = handle.fuse() => {
        match res {
          Ok(resp) => {
            resp

          },
          Err(e) => {
            tracing::error!(target = "ruraft.net.transport", err=%e, "canceled command");
            Response::error(ErrorResponse::new(local_header, e.to_string()))
          },
        }
      },
      _ = shutdown_rx.recv().fuse() => return Err(Error::AlreadyShutdown),
    };

    W::encode_response_to_writer(&resp, &mut writer)
      .await
      .map_err(|e| {
        tracing::error!(target = "ruraft.net.transport", err=%e, "failed to send response");
        Error::Wire(<W as Wire>::Error::io(e))
      })?;

    writer.flush().await.map_err(|e| {
      tracing::error!(target = "ruraft.net.transport", err=%e, "failed to flush response");
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
impl<I, A, D> RequestMetricsExt for Request<I, A, D> {
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
