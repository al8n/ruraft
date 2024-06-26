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

/// Exports unit tests to let users test transport implementation based on this crate.
#[cfg(any(feature = "test", test))]
pub mod tests;

/// Stream layer abstraction.
pub mod stream;
use arc_swap::ArcSwapOption;
use concurrent_queue::ConcurrentQueue;
use nodecraft::CheapClone;
use ruraft_utils::io::LimitedReader;
use stream::{Connection, Listener, StreamLayer};

use std::{
  collections::HashMap,
  future::Future,
  net::SocketAddr,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
  time::Duration,
};

#[cfg(feature = "metrics")]
use std::time::Instant;

use agnostic::{Runtime, RuntimeLite};
use async_lock::Mutex;
use futures::{
  io::{BufReader, BufWriter},
  AsyncRead, AsyncWriteExt, FutureExt,
};
use ruraft_core::Node;

pub use ruraft_core::{options::ProtocolVersion, transport::*};

/// Re-exports [`ruraft-wire`](ruraft_wire).
pub mod wire {
  pub use ruraft_core::transport::{Wire, WireError};
  pub use ruraft_wire::{Error as LpeWireError, ErrorKind as LpeWireErrorKind, LpeWire};
}

/// Re-exports [`nodecraft`]'s address resolver.
pub mod resolver {
  pub use nodecraft::resolver::{address, dns, socket_addr};
}

mod pipeline;
pub use pipeline::*;

/// The default TimeoutScale in a [`NetworkTransport`].
pub const DEFAULT_TIMEOUT_SCALE: usize = 256 * 1024; // 256KB

/// The default value used for pipelining configuration
/// if a zero value is passed. See https://github.com/hashicorp/raft/pull/541
/// for rationale. Note, if this is changed we should update the doc comments
/// below for [`NetTransportOptions`].
pub const DEFAULT_MAX_INFLIGHT_REQUESTS: usize = 3;

/// The size of the buffer we will use for reading RPC requests into
/// on followers
const CONN_RECEIVE_BUFFER_SIZE: usize = 256 * 1024; // 256KB

/// The size of the buffer we will use for sending RPC request data from
/// the leader to followers.
const CONN_SEND_BUFFER_SIZE: usize = 256 * 1024; // 256KB

/// a property of our current pipelining
/// implementation and must not be changed unless we change the invariants of
/// that implementation. Roughly speaking even with a zero-length in-flight
/// buffer we still allow 3 requests to be in-flight before we block because we
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
#[viewit::viewit(getters(style = "move"), setters(prefix = "with"))]
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct NetTransportOptions {
  /// Controls how many connections we will pool
  #[viewit(
    getter(const, attrs(doc = "Returns how many connections we will pool.")),
    setter(attrs(doc = "Sets how many connections we will pool. (Builder pattern)"))
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
    setter(attrs(doc = "Sets the max inflight requests. (Builder pattern)"))
  )]
  max_inflight_requests: usize,

  /// Used to apply I/O deadlines. For InstallSnapshot, we multiply
  /// the timeout by (SnapshotSize / TimeoutScale).
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  #[viewit(
    getter(const, attrs(doc = "Returns the timeout used to apply I/O deadlines.")),
    setter(attrs(doc = "Sets the timeout used to apply I/O deadlines. (Builder pattern)"))
  )]
  timeout: Duration,
}

impl Default for NetTransportOptions {
  fn default() -> Self {
    Self::new()
  }
}

impl NetTransportOptions {
  /// Create a new [`NetTransportOptions`] with default values.
  #[inline]
  pub const fn new() -> Self {
    Self {
      max_pool: 3,
      max_inflight_requests: DEFAULT_MAX_INFLIGHT_REQUESTS,
      timeout: Duration::from_secs(10),
    }
  }

  /// Set the max pooled connections.
  #[inline]
  pub fn set_max_pool(&mut self, max_pool: usize) {
    self.max_pool = max_pool;
  }

  /// Set the max inflight requests.
  #[inline]
  pub fn set_max_inflight_requests(&mut self, max_inflight_requests: usize) {
    self.max_inflight_requests = max_inflight_requests;
  }

  /// Set the timeout used to apply I/O deadlines.
  #[inline]
  pub fn set_timeout(&mut self, timeout: Duration) {
    self.timeout = timeout;
  }
}

struct StreamContext {
  mu: spin::Mutex<()>,
  queue: ConcurrentQueue<async_channel::Sender<()>>,
}

impl StreamContext {
  fn new() -> Arc<Self> {
    Arc::new(Self {
      queue: ConcurrentQueue::unbounded(),
      mu: spin::Mutex::new(()),
    })
  }

  fn cancel(&self) {
    let _guard = self.mu.lock();
    while let Ok(tx) = self.queue.pop() {
      tx.close();
    }
  }

  fn renew(&self) {
    let _guard = self.mu.lock();
    // checkpoint, prevent new comming tx be poped out
    let mut size = self.queue.len();
    while size > 0 {
      if let Ok(tx) = self.queue.pop() {
        tx.close();
      }
      size -= 1;
    }
  }

  fn push(&self, tx: async_channel::Sender<()>) {
    if let Err(t) = self.queue.push(tx) {
      t.into_inner().close();
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
pub struct NetTransport<I, A, S, W, R>
where
  A: AddressResolver,
  S: StreamLayer,
{
  #[cfg(test)]
  shutdown_rx: async_channel::Receiver<()>,
  shutdown: Arc<AtomicBool>,
  shutdown_tx: async_channel::Sender<()>,
  local_header: Header<I, <A as AddressResolver>::Address>,
  bind_addr: SocketAddr,
  consumer: RpcConsumer<I, <A as AddressResolver>::Address>,
  resolver: A,
  conn_pool: Mutex<HashMap<<A as AddressResolver>::Address, smallvec::SmallVec<[S::Stream; 2]>>>,
  protocol_version: ProtocolVersion,
  max_pool: usize,
  max_inflight_requests: usize,
  timeout: Duration,
  stream_layer: S,
  heartbeat_handler: Arc<ArcSwapOption<HeartbeatHandler<I, A::Address>>>,
  stream_ctx: Arc<StreamContext>,
  _w: std::marker::PhantomData<(W, R)>,
}

impl<I, A, S, W, R> NetTransport<I, A, S, W, R>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,

  S: StreamLayer,
  W: Wire<Id = I, Address = <A as AddressResolver>::Address>,
  R: Runtime,
{
  /// Create a new [`NetTransport`].
  pub async fn new(
    local_header: Header<I, A::Address>,
    bind_addr: A::ResolvedAddress,
    resolver: A,
    stream_layer: S,
    opts: NetTransportOptions,
  ) -> Result<Self, Error<I, A, W>> {
    let (shutdown_tx, shutdown_rx) = async_channel::unbounded();
    let auto_port = bind_addr.port() == 0;

    let ln = stream_layer.bind(bind_addr).await.map_err(|e| {
      tracing::error!(target = "ruraft.net.transport", bind_addr=%bind_addr, err=%e, "failed to bind listener");
      Error::IO(e)
    })?;

    let bind_addr = if auto_port {
      let addr = ln.local_addr()?;
      tracing::warn!(target = "ruraft.net.transport", local_addr=%addr, "listening on automatically assigned port {}", addr.port());
      addr
    } else {
      bind_addr
    };

    tracing::info!(
      target = "ruraft.net.transport",
      "advertise to {}",
      bind_addr
    );

    let shutdown = Arc::new(AtomicBool::new(false));
    let (producer, consumer) = rpc();
    let heartbeat_handler = Arc::new(ArcSwapOption::from_pointee(None));
    let stream_ctx = StreamContext::new();
    let request_handler = RequestHandler {
      ln,
      local_header: local_header.clone(),
      producer,
      shutdown: shutdown.clone(),
      #[cfg(test)]
      shutdown_rx: shutdown_rx.clone(),
      #[cfg(not(test))]
      shutdown_rx,
      stream_ctx: stream_ctx.clone(),
      heartbeat_handler: heartbeat_handler.clone(),
    };
    <A::Runtime as RuntimeLite>::spawn_detach(RequestHandler::<I, A::Address, S>::run::<A, W>(
      request_handler,
    ));

    Ok(Self {
      #[cfg(test)]
      shutdown_rx,
      shutdown,
      shutdown_tx,
      bind_addr,
      resolver,
      consumer,
      conn_pool: Mutex::new(HashMap::with_capacity(opts.max_pool)),
      protocol_version: local_header.protocol_version(),
      local_header,
      max_pool: opts.max_pool,
      max_inflight_requests: if opts.max_inflight_requests == 0 {
        DEFAULT_MAX_INFLIGHT_REQUESTS
      } else {
        opts.max_inflight_requests
      },
      timeout: opts.timeout,
      stream_layer,
      stream_ctx,
      heartbeat_handler,
      _w: std::marker::PhantomData,
    })
  }

  /// Closes the current streams.
  pub async fn close_streams(&self) {
    let mut pool = self.conn_pool.lock().await;

    // Close all the connections in the connection pool and then remove their
    // entry.
    pool.clear();

    self.stream_ctx.renew();
  }
}

impl<I, A, S, W, R> Transport for NetTransport<I, A, S, W, R>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
  S: StreamLayer,
  W: Wire<Id = I, Address = <A as AddressResolver>::Address>,
  R: Runtime,
{
  type Error = Error<Self::Id, Self::Resolver, Self::Wire>;
  type Runtime = R;

  type Id = I;

  type Pipeline = NetAppendEntriesPipeline<Self::Id, Self::Resolver, S, Self::Wire>;

  type Resolver = A;

  type Wire = W;

  fn consumer(&self) -> RpcConsumer<Self::Id, <Self::Resolver as AddressResolver>::Address> {
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

  fn bind_addr(&self) -> &<Self::Resolver as AddressResolver>::ResolvedAddress {
    &self.bind_addr
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
    req: AppendEntriesRequest<Self::Id, <Self::Resolver as AddressResolver>::Address>,
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
    data: impl AsyncRead + Send + Unpin,
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
    futures::io::copy(data, &mut w).await?;
    w.flush().await?;

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
    self.stream_ctx.cancel();
    Ok(())
  }
}

impl<I, A, S, W, R> NetTransport<I, A, S, W, R>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,

  S: StreamLayer,
  W: Wire<Id = I, Address = <A as AddressResolver>::Address>,
  R: Runtime,
{
  async fn return_conn(&self, conn: S::Stream, addr: <A as AddressResolver>::Address) {
    if self.shutdown.load(Ordering::Acquire) {
      return;
    }

    {
      let mut pool = self.conn_pool.lock().await;
      match pool.entry(addr) {
        std::collections::hash_map::Entry::Occupied(mut entry) => {
          let value = entry.get_mut();
          if value.len() < self.max_pool {
            entry.get_mut().push(conn);
          }
        }
        std::collections::hash_map::Entry::Vacant(entry) => {
          entry.insert(smallvec::smallvec![conn]);
        }
      }
    }
  }

  async fn send(
    &self,
    target: SocketAddr,
    req: Request<I, <A as AddressResolver>::Address>,
  ) -> Result<S::Stream, <Self as Transport>::Error> {
    // Get a connection
    let mut conn = {
      let mut pool = self.conn_pool.lock().await;
      match pool.get_mut(req.header().addr()) {
        Some(conns) if !conns.is_empty() => conns.pop().unwrap(),
        _ => {
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

impl<I, A, S, W, R> Drop for NetTransport<I, A, S, W, R>
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
    self.stream_ctx.cancel();
  }
}

/// Used to handle connection from remote peers.
struct RequestHandler<I, A, S: StreamLayer> {
  ln: S::Listener,
  local_header: Header<I, A>,
  producer: RpcProducer<I, A>,
  shutdown: Arc<AtomicBool>,
  shutdown_rx: async_channel::Receiver<()>,
  heartbeat_handler: Arc<ArcSwapOption<HeartbeatHandler<I, A>>>,
  stream_ctx: Arc<StreamContext>,
}

impl<I, A, S> RequestHandler<I, A, S>
where
  I: Id,
  A: Address,
  S: StreamLayer,
{
  async fn run<Resolver: AddressResolver, W: Wire<Id = I, Address = A>>(self)
  where
    <Resolver as AddressResolver>::Runtime: Runtime,
  {
    const BASE_DELAY: Duration = Duration::from_millis(5);
    const MAX_DELAY: Duration = Duration::from_secs(1);

    let mut loop_delay = Duration::ZERO;
    loop {
      futures::select! {
        res = self.ln.accept().fuse() => {
          // Accept incoming connections
          match res {
            Ok((conn, addr)) => {
              // No error, reset loop delay
              loop_delay = Duration::ZERO;

              tracing::debug!(target = "ruraft.net.transport", local = %self.local_header.addr(), remote = %addr, "accepted connection");

              // Spawn a task to handle the connection
              let producer = self.producer.clone();
              let shutdown_rx = self.shutdown_rx.clone();
              let local_header = self.local_header.clone();
              let heartbeat_handler = self.heartbeat_handler.clone();

              let (tx, rx) = async_channel::bounded(1);
              self.stream_ctx.push(tx);
              <<Resolver as AddressResolver>::Runtime as RuntimeLite>::spawn_detach(async move {
                Self::handle_connection::<Resolver, W>(
                  rx,
                  &heartbeat_handler,
                  conn,
                  producer,
                  shutdown_rx,
                  local_header
                ).await;
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
                _ = <<Resolver as AddressResolver>::Runtime as RuntimeLite>::sleep(loop_delay).fuse() => continue,
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
    ctx: async_channel::Receiver<()>,
    heartbeat_handler: &ArcSwapOption<HeartbeatHandler<I, A>>,
    conn: S::Stream,
    producer: RpcProducer<I, A>,
    shutdown_rx: async_channel::Receiver<()>,
    local_header: Header<I, A>,
  ) where
    <Resolver as AddressResolver>::Runtime: Runtime,
  {
    let (reader, mut writer) = {
      let (reader, writer) = conn.into_split();
      (
        BufReader::with_capacity(CONN_RECEIVE_BUFFER_SIZE, reader),
        BufWriter::with_capacity(CONN_SEND_BUFFER_SIZE, writer),
      )
    };

    let mut reader = Some(reader);
    loop {
      futures::select! {
        _ = ctx.recv().fuse() => {
          tracing::debug!(target = "ruraft.net.transport", "stream layer is closed");
          return;
        },
        default => {},
      }

      match Self::handle_command::<Resolver, W>(
        reader.take().unwrap(),
        &mut writer,
        heartbeat_handler,
        producer.clone(),
        shutdown_rx.clone(),
        local_header.clone(),
      )
      .await
      {
        Ok(r) => {
          if r.is_some() {
            reader = r;
          } else {
            if let Err(e) = writer.flush().await {
              tracing::error!(target = "ruraft.net.transport", err=%e, "failed to flush response");
              return;
            }
            return;
          }
        }
        Err(e) => {
          if let Error::IO(ref e) = e {
            if e.kind() != std::io::ErrorKind::UnexpectedEof {
              tracing::error!(target = "ruraft.net.transport", err=%e, "failed to decode incoming command");
            }
          }
          return;
        }
      }

      if let Err(e) = writer.flush().await {
        tracing::error!(target = "ruraft.net.transport", err=%e, "failed to flush response");
        return;
      }
    }
  }

  async fn handle_command<Resolver: AddressResolver, W: Wire<Id = I, Address = A>>(
    mut reader: BufReader<<S::Stream as Connection>::OwnedReadHalf>,
    writer: &mut BufWriter<<S::Stream as Connection>::OwnedWriteHalf>,
    heartbeat_handler: &ArcSwapOption<HeartbeatHandler<I, A>>,
    producer: RpcProducer<I, A>,
    shutdown_rx: async_channel::Receiver<()>,
    local_header: Header<I, A>,
  ) -> Result<Option<BufReader<<S::Stream as Connection>::OwnedReadHalf>>, Error<I, Resolver, W>>
  {
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
      metrics::histogram!(decode_label).record(decode_start.elapsed().as_millis() as f64);
    }

    #[cfg(feature = "metrics")]
    let process_start = Instant::now();
    #[cfg(feature = "metrics")]
    let enqueue_label = req.enqueue_label();
    #[cfg(feature = "metrics")]
    let respond_label = req.respond_label();

    let mut is_heartbeat = false;
    let ((rpc, handle), reader) = match &req {
      Request::InstallSnapshot(ireq) => {
        let lr = LimitedReader::new(*ireq.size(), reader);
        (Rpc::new(req, Some(lr)), None)
      }
      Request::Heartbeat(_) => {
        is_heartbeat = true;
        (
          Rpc::new::<LimitedReader<BufReader<<S::Stream as Connection>::OwnedReadHalf>>>(req, None),
          None,
        )
      }
      Request::AppendEntries(_) => (
        Rpc::new::<LimitedReader<BufReader<<S::Stream as Connection>::OwnedReadHalf>>>(req, None),
        Some(reader),
      ),
      _ => (
        Rpc::new::<LimitedReader<BufReader<<S::Stream as Connection>::OwnedReadHalf>>>(req, None),
        None,
      ),
    };

    // Check for heartbeat fast-path
    if is_heartbeat {
      let handler = heartbeat_handler.load();
      if let Some(h) = handler.as_ref() {
        let (sender, req, _) = rpc.into_components();
        if let Request::Heartbeat(req) = req {
          (h)(local_header.clone(), req, sender).await;
        } else {
          unreachable!();
        }

        return Self::wait_and_send_response(
          writer,
          local_header,
          handle,
          shutdown_rx,
          #[cfg(feature = "metrics")]
          respond_label,
        )
        .await
        .map(|_| reader);
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
    metrics::histogram!(enqueue_label).record(process_start.elapsed().as_millis() as f64);

    // Wait for response
    Self::wait_and_send_response(
      writer,
      local_header,
      handle,
      shutdown_rx,
      #[cfg(feature = "metrics")]
      respond_label,
    )
    .await
    .map(|_| reader)
  }

  async fn wait_and_send_response<Resolver: AddressResolver, W: Wire<Id = I, Address = A>>(
    mut writer: &mut BufWriter<<S::Stream as Connection>::OwnedWriteHalf>,
    local_header: Header<I, A>,
    handle: RpcHandle<I, A>,
    shutdown_rx: async_channel::Receiver<()>,
    #[cfg(feature = "metrics")] respond_label: &'static str,
  ) -> Result<(), Error<I, Resolver, W>> {
    #[cfg(feature = "metrics")]
    let resp_wait_start = Instant::now();
    #[cfg(feature = "metrics")]
    scopeguard::defer!(
      metrics::histogram!(respond_label).record(resp_wait_start.elapsed().as_millis() as f64)
    );

    let resp = futures::select! {
      res = handle.fuse() => {
        match res {
          Ok(resp) => resp,
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
impl<I, A> RequestMetricsExt for Request<I, A> {
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
