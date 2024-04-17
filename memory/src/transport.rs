#![allow(clippy::type_complexity, missing_docs)]

use std::{
  collections::HashMap,
  sync::Arc,
  time::{Duration, SystemTime},
};

use agnostic_lite::RuntimeLite;
use async_lock::{Mutex, RwLock};
use futures::{io::Cursor, Future, FutureExt};
use ruraft_core::{
  options::ProtocolVersion,
  transport::{
    rpc, AddressResolver, AppendEntriesPipeline, Id, PipelineAppendEntriesResponse, Request,
    Response, Rpc, RpcConsumer, RpcHandle, RpcProducer, Transformable, Transport, TransportError,
    Wire,
  },
  CheapClone,
};

pub use self::address::MemoryAddress;
mod address;
pub use self::resolver::MemoryAddressResolver;
mod resolver;

/// Represents errors specific to the [`MemoryTransport`].
///
/// This enum captures a comprehensive set of errors that can arise when using the `MemoryTransport`.
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

  /// Error indicating that the received response was not as expected.
  #[error("unexpected response {actual}, expected {expected}")]
  UnexpectedResponse {
    /// Expected response type.
    expected: &'static str,
    /// Actual response type.
    actual: &'static str,
  },

  /// The memory address is not in the [`MemoryTransport`]'s routing table.
  #[error("failed to connect to peer: {0}")]
  ConnectionRefused(A::Address),

  /// Send [`Request`] to peer memory transport timeout
  #[error("send timed out")]
  SendTimeout,

  /// Receive [`Response`] from peer memory transport timeout
  #[error("command timed out")]
  CommandTimeout,

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

impl<I: Id, A: AddressResolver, W: Wire> core::fmt::Debug for Error<I, A, W> {
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

  fn address(err: <A::Address as Transformable>::Error) -> Self
  where
    Self: Sized,
  {
    Self::Address(err)
  }

  fn resolver(err: A::Error) -> Self
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

struct PipelineInflight<I, A> {
  /// The term of the request
  term: u64,

  /// The highest log index of the [`AppendEntriesRequest`]'s entries
  highest_log_index: Option<u64>,

  /// The number of entries in the [`AppendEntriesRequest`]'s
  num_entries: usize,

  /// The time that the original request was started
  start: SystemTime,
  handle: RpcHandle<I, A>,
}

/// Append entries pipeline used by [`MemoryTransport`].
pub struct MemoryTransportPipeline<I, A, W>
where
  I: Id,
  A: AddressResolver,
  W: Wire,
{
  trans: MemoryTransport<I, A, W>,
  peer: MemoryTransport<I, A, W>,
  peer_addr: A::Address,
  inprogress_tx: async_channel::Sender<PipelineInflight<I, A::Address>>,
  finish_rx:
    async_channel::Receiver<Result<PipelineAppendEntriesResponse<I, A::Address>, Error<I, A, W>>>,
  shutdown: Arc<Mutex<bool>>,
  shutdown_tx: async_channel::Sender<()>,
  shutdown_rx: async_channel::Receiver<()>,
}

impl<I, A, W> Clone for MemoryTransportPipeline<I, A, W>
where
  I: Id,
  A: AddressResolver,
  W: Wire,
{
  fn clone(&self) -> Self {
    Self {
      trans: self.trans.clone(),
      peer: self.peer.clone(),
      peer_addr: self.peer_addr.cheap_clone(),
      inprogress_tx: self.inprogress_tx.clone(),
      finish_rx: self.finish_rx.clone(),
      shutdown: self.shutdown.clone(),
      shutdown_tx: self.shutdown_tx.clone(),
      shutdown_rx: self.shutdown_rx.clone(),
    }
  }
}

impl<I, A, W> MemoryTransportPipeline<I, A, W>
where
  I: Id,
  A: AddressResolver,

  W: Wire<Id = I, Address = A::Address>,
{
  fn new(
    trans: MemoryTransport<I, A, W>,
    peer: MemoryTransport<I, A, W>,
    addr: A::Address,
  ) -> Self {
    let (shutdown_tx, shutdown_rx) = async_channel::bounded(1);
    let (finish_tx, finish_rx) = async_channel::bounded(16);
    let (inprogress_tx, inprogress_rx) = async_channel::bounded(16);

    let this = Self {
      trans,
      peer,
      peer_addr: addr,
      finish_rx,
      shutdown: Arc::new(Mutex::new(false)),
      shutdown_tx,
      shutdown_rx: shutdown_rx.clone(),
      inprogress_tx,
    };

    <A::Runtime as RuntimeLite>::spawn_detach(Self::decode_response(
      inprogress_rx,
      finish_tx,
      this.trans.timeout,
      shutdown_rx,
    ));
    this
  }

  async fn decode_response(
    inprogress_rx: async_channel::Receiver<PipelineInflight<I, A::Address>>,
    finish_tx: async_channel::Sender<
      Result<PipelineAppendEntriesResponse<I, A::Address>, Error<I, A, W>>,
    >,
    timeout: Duration,
    shutdown_rx: async_channel::Receiver<()>,
  ) {
    let timeout = if timeout != Duration::ZERO {
      timeout
    } else {
      Duration::from_millis(u64::MAX)
    };

    loop {
      futures::select! {
        task = inprogress_rx.recv().fuse() => {
          match task {
            Ok(task) => {
              futures::select! {
                resp = task.handle.fuse() => {
                  if let Ok(resp) = resp {
                    match resp {
                      Response::Error(err) => {
                        futures::select! {
                          res = finish_tx.send(Err(Error::Remote(err.into()))).fuse() => {
                            if res.is_err() {
                              return;
                            }
                          }
                          _ = shutdown_rx.recv().fuse() => return,
                        }
                      }
                      Response::AppendEntries(resp) => {
                        let resp = PipelineAppendEntriesResponse {
                          term: task.term,
                          highest_log_index: task.highest_log_index,
                          num_entries: task.num_entries,
                          start: task.start,
                          resp
                        };

                        futures::select! {
                          res = finish_tx.send(Ok(resp)).fuse() => {
                            if res.is_err() {
                              return;
                            }
                          }
                          _ = shutdown_rx.recv().fuse() => return,
                        }
                      }
                      resp => {
                        futures::select! {
                          res = finish_tx.send(Err(Error::UnexpectedResponse {
                            expected: "AppendEntries",
                            actual: resp.description(),
                          })).fuse() => {
                            if res.is_err() {
                              return;
                            }
                          }
                          _ = shutdown_rx.recv().fuse() => return,
                        }
                      }
                    }
                  }
                }
                _ = <A::Runtime as RuntimeLite>::sleep(timeout).fuse() => {
                  futures::select! {
                    res = finish_tx.send(Err(Error::CommandTimeout)).fuse() => {
                      if res.is_err() {
                        return;
                      }
                    }
                    _ = shutdown_rx.recv().fuse() => return,
                  }
                },
                _ = shutdown_rx.recv().fuse() => return,
              }
            }
            Err(_) => return,
          }
        }
        _ = shutdown_rx.recv().fuse() => return,
      }
    }
  }
}

impl<I, A, W> AppendEntriesPipeline for MemoryTransportPipeline<I, A, W>
where
  I: Id,

  A: AddressResolver,

  <<A::Runtime as RuntimeLite>::Sleep as Future>::Output: Send + 'static,

  W: Wire<Id = I, Address = A::Address>,
{
  type Error = Error<I, A, W>;
  type Id = I;
  type Address = A::Address;

  fn consumer(
    &self,
  ) -> impl futures::prelude::Stream<
    Item = Result<
      ruraft_core::transport::PipelineAppendEntriesResponse<Self::Id, Self::Address>,
      Self::Error,
    >,
  > + Send
       + 'static {
    self.finish_rx.clone()
  }

  async fn append_entries(
    &mut self,
    req: ruraft_core::transport::AppendEntriesRequest<Self::Id, Self::Address>,
  ) -> Result<(), Self::Error> {
    let term = req.term;
    let num_entries = req.entries().len();
    let highest_log_index = if num_entries == 0 {
      None
    } else {
      req.entries().last().map(|e| e.index())
    };

    let (rpc, handle) = Rpc::new::<Cursor<Vec<u8>>>(Request::AppendEntries(req), None);

    // Check if we have been already shutdown, otherwise the random choose
    // made by select statement below might pick consumerCh even if
    // shutdownCh was closed.
    if *self.shutdown.lock().await {
      return Err(Error::PipelineShutdown);
    }

    if self.trans.timeout > Duration::ZERO {
      futures::select! {
        _ = self.peer.producer.send(rpc).fuse() => {},
        _ = <A::Runtime as RuntimeLite>::sleep(self.trans.timeout).fuse() => return Err(Error::CommandTimeout),
        _ = self.shutdown_rx.recv().fuse() => return Err(Error::PipelineShutdown),
      }
    } else {
      futures::select! {
        _ = self.peer.producer.send(rpc).fuse() => {},
        _ = self.shutdown_rx.recv().fuse() => return Err(Error::PipelineShutdown),
      }
    }

    // Send to be decoded
    futures::select! {
      rst = self.inprogress_tx.send(PipelineInflight {
        handle,
        term,
        highest_log_index,
        num_entries,
        start: SystemTime::now(),
      }).fuse() => {
        if rst.is_err() {
          return Err(Error::PipelineShutdown);
        }

        Ok(())
      },
      _ = self.shutdown_rx.recv().fuse() => Err(Error::PipelineShutdown),
    }
  }

  async fn close(self) -> Result<(), Self::Error> {
    let mut mu = self.shutdown.lock().await;
    if *mu {
      return Ok(());
    }

    *mu = true;
    self.shutdown_tx.close();
    Ok(())
  }
}

/// Options for the [`MemoryTransport`].
#[viewit::viewit(setters(prefix = "with"))]
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MemoryTransportOptions<I> {
  /// The identifier of the node.
  id: I,
  /// The protocol version of the memory transport.
  version: ProtocolVersion,
  /// The given timeout
  /// will be used to decide how long to wait for a connected peer to process the
  /// RPCs that we're sending it. See also [`MemoryTransport::connect`] and [`MemoryTransport::consumer`].
  ///
  /// Defaults to 500ms.
  ///
  /// [`MemoryTransport::connect`]: struct.MemoryTransport.html#method.connect
  /// [`MemoryTransport::consumer`]: struct.MemoryTransport.html#method.consumer
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde::option"))]
  timeout: Option<Duration>,
}

impl<I> MemoryTransportOptions<I> {
  /// Returns a new [`MemoryTransportOptions`] with the given node identifier and default configurations.
  pub const fn new(id: I) -> Self {
    Self {
      id,
      version: ProtocolVersion::V1,
      timeout: Some(Duration::from_millis(500)),
    }
  }
}

struct MemoryTransportInner<I, A, W>
where
  I: Id,
  A: AddressResolver,
  W: Wire,
{
  peers: HashMap<A::Address, MemoryTransport<I, A, W>>,
  pipelines: Vec<MemoryTransportPipeline<I, A, W>>,
}

/// Implements the [`Transport`] trait, to allow Raft to be
/// tested in-memory without going over a network.
pub struct MemoryTransport<I, A, W>
where
  I: Id,
  A: AddressResolver,
  W: Wire,
{
  inner: Arc<RwLock<MemoryTransportInner<I, A, W>>>,
  timeout: Duration,
  bind_address: A::ResolvedAddress,
  local_address: A::Address,
  local_id: I,
  resolver: Arc<A>,
  consumer: RpcConsumer<I, A::Address>,
  producer: RpcProducer<I, A::Address>,
  protocol_version: ProtocolVersion,
}

impl<I, A, W> core::fmt::Debug for MemoryTransport<I, A, W>
where
  I: Id,
  A: AddressResolver,
  W: Wire,
{
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("MemoryTransportInner")
      .field("local_id", &self.local_id)
      .field("local_address", &self.local_address)
      .finish()
  }
}

impl<I, A, W> Clone for MemoryTransport<I, A, W>
where
  I: Id,
  A: AddressResolver,
  W: Wire,
{
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
      timeout: self.timeout,
      bind_address: self.bind_address.cheap_clone(),
      resolver: self.resolver.clone(),
      local_address: self.local_address.cheap_clone(),
      local_id: self.local_id.clone(),
      consumer: self.consumer.clone(),
      producer: self.producer.clone(),
      protocol_version: self.protocol_version,
    }
  }
}

impl<I, A, W> MemoryTransport<I, A, W>
where
  I: Id,
  A: AddressResolver<Address = MemoryAddress>,
  W: Wire,
{
  /// Initialize a new transport
  /// and generates a random local address if none is specified
  pub async fn new(resolver: A, opts: MemoryTransportOptions<I>) -> Result<Self, Error<I, A, W>> {
    let (tx, rx) = rpc();
    let addr = MemoryAddress::new();
    let resolved = resolver.resolve(&addr).await.map_err(Error::Resolver)?;
    Ok(Self {
      inner: Arc::new(RwLock::new(MemoryTransportInner {
        peers: HashMap::new(),
        pipelines: vec![],
      })),
      timeout: opts.timeout.unwrap_or(Duration::from_millis(500)),
      bind_address: resolved,
      resolver: Arc::new(resolver),
      local_id: opts.id,
      local_address: addr,
      consumer: rx,
      producer: tx,
      protocol_version: opts.version,
    })
  }

  /// Update the local id for the transport.
  pub fn set_local_id(&mut self, id: I) {
    self.local_id = id;
  }
}

impl<I, A, W> MemoryTransport<I, A, W>
where
  I: Id,
  A: AddressResolver,
  W: Wire,
{
  /// Initialize a new transport and
  /// with specified local address and [`MemoryTransportOptions`].
  pub async fn with_address(
    addr: A::Address,
    resolver: A,
    opts: MemoryTransportOptions<I>,
  ) -> Result<Self, Error<I, A, W>> {
    let (tx, rx) = rpc();
    let bind_addr = resolver.resolve(&addr).await.map_err(Error::Resolver)?;

    Ok(Self {
      inner: Arc::new(RwLock::new(MemoryTransportInner {
        peers: HashMap::new(),
        pipelines: vec![],
      })),
      timeout: opts.timeout.unwrap_or(Duration::from_millis(500)),
      bind_address: bind_addr,
      resolver: Arc::new(resolver),
      local_id: opts.id,
      local_address: addr,
      consumer: rx,
      producer: tx,
      protocol_version: opts.version,
    })
  }

  /// Used to connect this transport to another transport for
  /// a given peer name. This allows for local routing.
  pub async fn connect(&self, peer: A::Address, t: Self) {
    let mut inner = self.inner.write().await;
    inner.peers.insert(peer, t);
  }
}

impl<I, A, W> MemoryTransport<I, A, W>
where
  I: Id,

  A: AddressResolver,

  <<A::Runtime as RuntimeLite>::Sleep as Future>::Output: Send + 'static,

  W: Wire<Id = I, Address = A::Address>,
{
  /// Used to remove the ability to route to a given peer.
  pub async fn disconnect(&self, peer: &A::Address) {
    let mut inner: async_lock::RwLockWriteGuard<'_, MemoryTransportInner<I, A, W>> =
      self.inner.write().await;
    inner.peers.remove(peer);

    // Disconnect any pipelines
    let mut n = inner.pipelines.len();
    let mut idx = 0;

    while idx < n {
      if inner.pipelines[idx].peer_addr.eq(peer) {
        inner.pipelines.swap(idx, n - 1);
        n -= 1;
      } else {
        idx += 1;
      }
    }

    for pipe in inner.pipelines.drain(..n) {
      let _ = pipe.close().await;
    }
  }

  /// Used to remove all routes to peers.
  pub async fn disconnect_all(&self) {
    let mut inner = self.inner.write().await;
    inner.peers.clear();

    for pipe in core::mem::take(&mut inner.pipelines) {
      let _ = pipe.close().await;
    }
  }
}

impl<I, A, W> MemoryTransport<I, A, W>
where
  I: Id,

  A: AddressResolver,

  W: Wire<Id = I, Address = A::Address>,
{
  async fn make_rpc(
    &self,
    target: A::Address,
    req: Request<I, A::Address>,
    conn: Option<Cursor<Vec<u8>>>,
    timeout: Duration,
  ) -> Result<Response<I, A::Address>, Error<I, A, W>> {
    let inner = self.inner.read().await;
    match inner.peers.get(&target) {
      None => Err(Error::ConnectionRefused(target)),
      Some(peer) => {
        // Send the RPC over
        let (rpc, handle) = Rpc::new(req, conn);

        futures::select! {
          _ = peer.producer.send(rpc).fuse() => {}
          _ = <A::Runtime as RuntimeLite>::sleep(timeout).fuse() => return Err(Error::SendTimeout),
        }

        // Wait for a response
        futures::select! {
          resp = handle.fuse() => {
            let resp = resp.unwrap();
            if let Response::Error(err) = resp {
              return Err(Error::Remote(err.into()));
            }

            Ok(resp)
          }
          _ = <A::Runtime as RuntimeLite>::sleep(timeout).fuse() => Err(Error::CommandTimeout),
        }
      }
    }
  }
}

impl<I, A, W> Transport for MemoryTransport<I, A, W>
where
  I: Id,

  A: AddressResolver,

  <<A::Runtime as RuntimeLite>::Sleep as Future>::Output: Send + 'static,

  W: Wire<Id = I, Address = A::Address>,
{
  type Error = Error<I, A, W>;

  type Runtime = A::Runtime;

  type Id = I;

  type Pipeline = MemoryTransportPipeline<I, A, W>;

  type Resolver = A;

  type Wire = W;

  fn consumer(&self) -> RpcConsumer<Self::Id, <Self::Resolver as AddressResolver>::Address> {
    self.consumer.clone()
  }

  fn local_addr(&self) -> &<Self::Resolver as AddressResolver>::Address {
    &self.local_address
  }

  fn local_id(&self) -> &Self::Id {
    &self.local_id
  }

  fn version(&self) -> ruraft_core::options::ProtocolVersion {
    self.protocol_version
  }

  fn set_heartbeat_handler(
    &self,
    _handler: Option<
      ruraft_core::transport::HeartbeatHandler<
        Self::Id,
        <Self::Resolver as AddressResolver>::Address,
      >,
    >,
  ) {
  }

  fn bind_addr(&self) -> &<Self::Resolver as AddressResolver>::ResolvedAddress {
    &self.bind_address
  }

  fn resolver(&self) -> &Self::Resolver {
    &self.resolver
  }

  async fn append_entries_pipeline(
    &self,
    target: ruraft_core::Node<Self::Id, <Self::Resolver as AddressResolver>::Address>,
  ) -> Result<Self::Pipeline, Self::Error> {
    let mut inner = self.inner.write().await;
    let addr = target.addr();
    let peer = inner
      .peers
      .get(addr)
      .ok_or(Error::ConnectionRefused(addr.cheap_clone()))?;
    let pipe = MemoryTransportPipeline::new(self.clone(), peer.clone(), addr.cheap_clone());
    inner.pipelines.push(pipe.clone());
    Ok(pipe)
  }

  async fn append_entries(
    &self,
    target: &ruraft_core::Node<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    req: ruraft_core::transport::AppendEntriesRequest<
      Self::Id,
      <Self::Resolver as AddressResolver>::Address,
    >,
  ) -> Result<
    ruraft_core::transport::AppendEntriesResponse<
      Self::Id,
      <Self::Resolver as AddressResolver>::Address,
    >,
    Self::Error,
  > {
    self
      .make_rpc(
        target.addr().cheap_clone(),
        Request::append_entries(req),
        None,
        10 * self.timeout,
      )
      .await
      .map(|resp| resp.unwrap_append_entries())
  }

  async fn vote(
    &self,
    target: &ruraft_core::Node<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    req: ruraft_core::transport::VoteRequest<
      Self::Id,
      <Self::Resolver as AddressResolver>::Address,
    >,
  ) -> Result<
    ruraft_core::transport::VoteResponse<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    Self::Error,
  > {
    self
      .make_rpc(
        target.addr().cheap_clone(),
        Request::vote(req),
        None,
        10 * self.timeout,
      )
      .await
      .map(|resp| resp.unwrap_vote())
  }

  async fn install_snapshot(
    &self,
    target: &ruraft_core::Node<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    req: ruraft_core::transport::InstallSnapshotRequest<
      Self::Id,
      <Self::Resolver as AddressResolver>::Address,
    >,
    mut source: impl futures::AsyncRead + Send + Unpin,
  ) -> Result<
    ruraft_core::transport::InstallSnapshotResponse<
      Self::Id,
      <Self::Resolver as AddressResolver>::Address,
    >,
    Self::Error,
  > {
    use futures::AsyncReadExt;

    let mut buffer = Vec::new();
    source.read_to_end(&mut buffer).await?;
    self
      .make_rpc(
        target.addr().cheap_clone(),
        Request::install_snapshot(req),
        Some(Cursor::new(buffer)),
        10 * self.timeout,
      )
      .await
      .map(|resp| resp.unwrap_install_snapshot())
  }

  async fn timeout_now(
    &self,
    target: &ruraft_core::Node<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    req: ruraft_core::transport::TimeoutNowRequest<
      Self::Id,
      <Self::Resolver as AddressResolver>::Address,
    >,
  ) -> Result<
    ruraft_core::transport::TimeoutNowResponse<
      Self::Id,
      <Self::Resolver as AddressResolver>::Address,
    >,
    Self::Error,
  > {
    self
      .make_rpc(
        target.addr().cheap_clone(),
        Request::timeout_now(req),
        None,
        10 * self.timeout,
      )
      .await
      .map(|resp| resp.unwrap_timeout_now())
  }

  async fn heartbeat(
    &self,
    target: &ruraft_core::Node<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    req: ruraft_core::transport::HeartbeatRequest<
      Self::Id,
      <Self::Resolver as AddressResolver>::Address,
    >,
  ) -> Result<
    ruraft_core::transport::HeartbeatResponse<
      Self::Id,
      <Self::Resolver as AddressResolver>::Address,
    >,
    Self::Error,
  > {
    self
      .make_rpc(
        target.addr().cheap_clone(),
        Request::heartbeat(req),
        None,
        10 * self.timeout,
      )
      .await
      .map(|resp| resp.unwrap_heartbeat())
  }

  async fn shutdown(&self) -> Result<(), Self::Error> {
    self.disconnect_all().await;
    Ok(())
  }
}

#[cfg(any(feature = "test", test))]
pub(super) mod tests {
  use std::marker::PhantomData;

  use futures::StreamExt;
  use ruraft_core::{
    log_batch,
    transport::{Address, AppendEntriesRequest, AppendEntriesResponse, WireError},
    Node,
  };

  use super::*;

  struct NoopWireError;

  impl WireError for NoopWireError {
    fn custom<T>(_msg: T) -> Self
    where
      T: core::fmt::Display,
    {
      unreachable!()
    }

    fn io(_err: std::io::Error) -> Self {
      unreachable!()
    }
  }

  impl core::fmt::Display for NoopWireError {
    fn fmt(&self, _f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
      unreachable!()
    }
  }

  impl core::fmt::Debug for NoopWireError {
    fn fmt(&self, _f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
      unreachable!()
    }
  }

  impl std::error::Error for NoopWireError {}

  struct NoopWire<I, A>(PhantomData<(I, A)>);

  impl<I: Id, A: Address> Wire for NoopWire<I, A>
  where
    I: Id,
    A: Send + Sync + 'static,
  {
    type Error = NoopWireError;

    type Id = I;

    type Address = A;

    type Bytes = Vec<u8>;

    fn encode_request(_req: &Request<Self::Id, Self::Address>) -> Result<Self::Bytes, Self::Error> {
      unreachable!()
    }

    fn encode_response(
      _resp: &Response<Self::Id, Self::Address>,
    ) -> Result<Self::Bytes, Self::Error> {
      unreachable!()
    }

    async fn encode_request_to_writer(
      _req: &Request<Self::Id, Self::Address>,
      _writer: impl futures::prelude::AsyncWrite + Send + Unpin,
    ) -> std::io::Result<()> {
      unreachable!()
    }

    async fn encode_response_to_writer(
      _resp: &Response<Self::Id, Self::Address>,
      _writer: impl futures::prelude::AsyncWrite + Send + Unpin,
    ) -> std::io::Result<()> {
      unreachable!()
    }

    fn decode_request(_src: &[u8]) -> Result<Request<Self::Id, Self::Address>, Self::Error> {
      todo!()
    }

    fn decode_response(_src: &[u8]) -> Result<Response<Self::Id, Self::Address>, Self::Error> {
      todo!()
    }

    async fn decode_request_from_reader(
      _reader: impl futures::prelude::AsyncRead + Send + Unpin,
    ) -> std::io::Result<Request<Self::Id, Self::Address>> {
      todo!()
    }

    async fn decode_response_from_reader(
      _reader: impl futures::prelude::AsyncRead + Send + Unpin,
    ) -> std::io::Result<Response<Self::Id, Self::Address>> {
      todo!()
    }
  }

  /// [`MemoryTransport`] test
  ///
  /// - Description:
  ///   - Test that the [`MemoryTransport`] can send and receive messages, and the timeout also works.
  pub async fn memory_transport_write_timeout<I, A>(
    id1: I,
    addr1: A::Address,
    resolver1: A,
    id2: I,
    addr2: A::Address,
    resolver2: A,
  ) where
    I: Id,
    A: AddressResolver,
  {
    // InmemTransport should timeout if the other end has gone away
    // when it tries to send a request.
    // Use unbuffered channels so that we can see the write failing
    // without having to contrive to fill up the buffer first.
    let timeout = Duration::from_millis(10);

    let t1 = MemoryTransport::<_, _, NoopWire<I, A::Address>>::with_address(
      addr1,
      resolver1,
      MemoryTransportOptions::new(id1.clone()).with_timeout(Some(timeout)),
    )
    .await
    .unwrap();
    let t2 = MemoryTransport::<_, _, NoopWire<I, A::Address>>::with_address(
      addr2.clone(),
      resolver2,
      MemoryTransportOptions::new(id2).with_timeout(Some(timeout)),
    )
    .await
    .unwrap();

    t1.connect(addr2.clone(), t2.clone()).await;

    let (stop_tx, stop_rx) = async_channel::bounded::<()>(1);
    let (stopped_tx, stopped_rx) = async_channel::bounded::<()>(1);
    let t2_header = t2.header();

    <A::Runtime as RuntimeLite>::spawn_detach(async move {
      let consumer = t2.consumer();
      futures::pin_mut!(consumer);
      let mut i = 0;
      let h = t2.header();
      loop {
        futures::select! {
          _ = stop_rx.recv().fuse() => {
            stopped_tx.close();
            return;
          },
          rpc = consumer.next().fuse() => {
            let rpc = rpc.unwrap();
            i += 1;
            rpc.respond(Response::AppendEntries(
              AppendEntriesResponse {
                term:1,
                success:true,
                header: h.clone(),
                last_log: i,
                no_retry_backoff: false,
              })
            ).unwrap();
          },
        }
      }
    });

    // Sanity check that sending is working before stopping the
    // responder.
    let t1_header = t1.header();
    let target = Node::new(t2_header.id().clone(), t2_header.addr().clone());
    let resp = t1
      .append_entries(
        &target,
        AppendEntriesRequest {
          header: t1_header.clone(),
          term: 0,
          prev_log_entry: 0,
          prev_log_term: 0,
          entries: log_batch![],
          leader_commit: 0,
        },
      )
      .await
      .unwrap();
    assert_eq!(resp.last_log, 1);

    stop_tx.close();

    futures::select! {
      _ = stopped_rx.recv().fuse() => {},
      _ = <A::Runtime as RuntimeLite>::sleep(Duration::from_secs(1)).fuse() => panic!("timed out waiting for responder to stop"),
    }

    let err = t1
      .append_entries(
        &target,
        AppendEntriesRequest {
          header: t1_header,
          term: 0,
          prev_log_entry: 0,
          prev_log_term: 0,
          entries: log_batch![],
          leader_commit: 0,
        },
      )
      .await
      .unwrap_err();
    assert!(matches!(err, Error::CommandTimeout));
  }
}
