use core::mem;

use std::{
  collections::HashMap,
  future::Future,
  io,
  net::SocketAddr,
  sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
  },
  time::{Duration, Instant},
};

use agnostic::{
  net::{Net, TcpListener, TcpStream},
  Runtime,
};
use async_lock::Mutex;
use futures::{
  io::{BufReader, BufWriter},
  AsyncRead, AsyncReadExt, AsyncWriteExt, FutureExt,
};
use ruraft_core::{
  options::ProtocolVersion,
  transport::{Address, Id},
};
use wg::AsyncWaitGroup;

use super::*;

/// Network encoder and decoder based on [`rmp_serde`].
pub mod rmp;

mod pipeline;
pub use pipeline::*;

const ENCODED_HEADER_SIZE: usize = mem::size_of::<ProtocolVersion>()
  + 1 // kind tag
  + mem::size_of::<u32>() // header length
  + mem::size_of::<u32>(); // req/resp length

fn encode_message_header<I: Id, A: Address>(
  protocol_version: ProtocolVersion,
  tag: u8,
  id: &I,
  address: &A,
) -> [u8; ENCODED_HEADER_SIZE] {
  let id_encoded_len = id.encoded_len();
  let address_encoded_len = address.encoded_len();
  let header_len = ENCODED_HEADER_SIZE + id_encoded_len + address_encoded_len;

  let mut buf = [0u8; ENCODED_HEADER_SIZE];
  let mut cur = 0;
  buf[0] = protocol_version as u8;
  cur += 1;
  buf[1] = tag;
  cur += 1;
  buf[cur..cur + mem::size_of::<u32>()].copy_from_slice(&(header_len as u32).to_be_bytes());
  cur += mem::size_of::<u32>();
  // We do not add req/resp length here, because we do not know the length of req/resp yet.
  // req/resp length will be updated by the caller.
  buf
}

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

/// The error returned by the [`NetTransport`].
#[derive(thiserror::Error)]
pub enum Error<I: Id, R: AddressResolver, E: Encoder, D: Decoder> {
  #[error("{0}")]
  Id(I::Error),
  #[error("{0}")]
  Address(<R::Address as Transformable>::Error),
  #[error("{0}")]
  Resolver(R::Error),
  #[error("{0}")]
  Encode(E::Error),
  #[error("{0}")]
  Decode(D::Error),
  /// Returned when operations on a transport are
  /// invoked after it's been terminated.
  #[error("transport already shutdown")]
  AlreadyShutdown,
  /// Returned when the pipeline is closed
  #[error("append pipeline closed")]
  PipelingShutdown,
  /// Returned when fail to forward the request to the raft
  #[error("failed to forward request to raft")]
  Dispatch,
  /// Returned when the response is unexpected
  #[error("unexpected response {actual}, expected {expected}")]
  UnexpectedResponse {
    expected: &'static str,
    actual: &'static str,
  },
  /// Returned when the remote response error
  #[error("remote: {0}")]
  Remote(String),
  /// An error about the I/O
  #[error("{0}")]
  IO(#[from] std::io::Error),
}

impl<I: Id, R: AddressResolver, E: Encoder, D: Decoder> core::fmt::Debug for Error<I, R, E, D> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    core::fmt::Display::fmt(&self, f)
  }
}

impl<I: Id, R: AddressResolver, E: Encoder, D: Decoder> ruraft_core::transport::Error
  for Error<I, R, E, D>
{
  type Id = I;

  type Resolver = R;

  type Encoder = E;

  type Decoder = D;

  fn id(err: <I as Transformable>::Error) -> Self
  where
    Self: Sized,
  {
    todo!()
  }

  fn address(err: <<R as AddressResolver>::Address as Transformable>::Error) -> Self
  where
    Self: Sized,
  {
    todo!()
  }

  fn resolver(err: <R as AddressResolver>::Error) -> Self
  where
    Self: Sized,
  {
    todo!()
  }

  fn encoder(err: <E as Encoder>::Error) -> Self
  where
    Self: Sized,
  {
    todo!()
  }

  fn decoder(err: <D as Decoder>::Error) -> Self
  where
    Self: Sized,
  {
    todo!()
  }

  fn io(err: std::io::Error) -> Self
  where
    Self: Sized,
  {
    todo!()
  }

  fn custom<T>(msg: T) -> Self
  where
    Self: Sized,
    T: core::fmt::Display,
  {
    todo!()
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
/// by the MsgPack encoded request.
///
/// The response is an error string followed by the response object,
/// both are encoded using MsgPack.
///
/// InstallSnapshot is special, in that after the request we stream
/// the entire state. That socket is not re-used as the connection state
/// is not known if there is an error.
pub struct NetTransport<I, R, E, D>
where
  I: Id + Send + Sync + 'static,
  R: AddressResolver,
  R::Address: Send + Sync + 'static,
  <R as AddressResolver>::Runtime: Runtime,
  <<<R as AddressResolver>::Runtime as Runtime>::Sleep as Future>::Output: Send,
  E: Encoder,
  D: Decoder,
{
  shutdown: Arc<AtomicBool>,
  shutdown_tx: async_channel::Sender<()>,
  local_header: Header<I, <R as AddressResolver>::Address>,
  advertise_addr: SocketAddr,
  consumer: RequestConsumer<I, <R as AddressResolver>::Address>,
  resolver: R,
  wg: AsyncWaitGroup,
  conn_pool: Mutex<
    HashMap<
      <R as AddressResolver>::Address,
      <<<R as AddressResolver>::Runtime as Runtime>::Net as Net>::TcpStream,
    >,
  >,
  conn_size: AtomicUsize,
  protocol_version: ProtocolVersion,
  max_pool: usize,
  max_inflight_requests: usize,
  timeout: Duration,
  _encoder: std::marker::PhantomData<E>,
  _decoder: std::marker::PhantomData<D>,
}

#[async_trait::async_trait]
impl<I, R, E, D> Transport for NetTransport<I, R, E, D>
where
  I: Id + Send + Sync + 'static,
  R: AddressResolver,
  R::Address: Send + Sync + 'static,
  <R as AddressResolver>::Runtime: Runtime,
  <<<R as AddressResolver>::Runtime as Runtime>::Sleep as Future>::Output: Send,
  E: Encoder,
  D: Decoder,
{
  type Error = Error<Self::Id, Self::Resolver, Self::Encoder, Self::Decoder>;
  type Runtime = <Self::Resolver as AddressResolver>::Runtime;
  type Options = NetTransportOptions<Self::Id, <Self::Resolver as AddressResolver>::Address>;

  type Id = I;

  // type Pipeline: AppendPipeline<Runtime = Self::Runtime>;

  type Resolver = R;

  type Encoder = E;

  type Decoder = D;

  fn consumer(&self) -> RequestConsumer<Self::Id, <Self::Resolver as AddressResolver>::Address> {
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
      resolver.resolve(opts.header.addr()).await?
    };
    let auto_port = advertise_addr.port() == 0;

    let ln = <<<R as Runtime>::Net as Net>::TcpListener as TcpListener>::bind(opts.advertise_addr)
      .await
      .map_err(|e| {
        tracing::error!(target = "ruraft.net.transport", err=%e, "failed to bind listener");
        Error::IO(e)
      })?;

    let addr = if auto_port {
      let addr = ln.local_addr()?;
      tracing::warn!(target = "ruraft.net.transport", local_addr=%addr, "listening on automatically assigned port {}", addr.port());
      addr
    } else {
      opts.advertise_addr
    };

    let shutdown = Arc::new(AtomicBool::new(false));
    let wg = AsyncWaitGroup::from(1);
    let (producer, consumer) = command();
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
      Self::Runtime,
    >::run(request_handler));

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
      _encoder: std::marker::PhantomData,
      _decoder: std::marker::PhantomData,
    })
  }

  async fn append_entries(
    &self,
    req: AppendEntriesRequest<Self::Id, <Self::Resolver as AddressResolver>::Address>,
  ) -> Result<
    AppendEntriesResponse<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    Self::Error,
  > {
    let addr = req.header().addr();
    let conn = self
      .send(Request::append_entries(self.protocol_version, req))
      .await?;
    self
      .receive(conn, addr, CommandResponseKind::AppendEntries)
      .await
  }

  async fn vote(
    &self,
    req: VoteRequest<Self::Id, <Self::Resolver as AddressResolver>::Address>,
  ) -> Result<VoteResponse<Self::Id, <Self::Resolver as AddressResolver>::Address>, Self::Error> {
    let addr = req.header().addr();
    let conn: <<R as Runtime>::Net as Net>::TcpStream =
      self.send(Request::vote(self.protocol_version, req)).await?;
    self.receive(conn, addr, CommandResponseKind::Vote).await
  }

  async fn install_snapshot(
    &self,
    req: InstallSnapshotRequest<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    source: impl AsyncRead + Send,
  ) -> Result<
    InstallSnapshotResponse<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    Self::Error,
  > {
    let addr = req.header().addr();
    let conn = self
      .send(Request::install_snapshot(self.protocol_version, req))
      .await?;
    let mut w = BufWriter::with_capacity(CONN_SEND_BUFFER_SIZE, conn);
    futures::io::copy(source, &mut w).await?;
    self
      .receive(w.into_inner(), addr, CommandResponseKind::InstallSnapshot)
      .await
  }

  async fn timeout_now(
    &self,
    req: TimeoutNowRequest<Self::Id, <Self::Resolver as AddressResolver>::Address>,
  ) -> Result<TimeoutNowResponse<Self::Id, <Self::Resolver as AddressResolver>::Address>, Self::Error>
  {
    let addr = req.header().addr();
    let conn = self
      .send(Request::timeout_now(self.protocol_version, req))
      .await?;
    self
      .receive(conn, addr, CommandResponseKind::TimeoutNow)
      .await
  }

  async fn heartbeat(
    &self,
    req: HeartbeatRequest<Self::Id, <Self::Resolver as AddressResolver>::Address>,
  ) -> Result<HeartbeatResponse<Self::Id, <Self::Resolver as AddressResolver>::Address>, Self::Error>
  {
    let addr = req.header().addr();
    let conn = self
      .send(Request::heartbeat(self.protocol_version, req))
      .await?;
    self
      .receive(conn, addr, CommandResponseKind::Heartbeat)
      .await
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

impl<I, R, E, D> NetTransport<I, R, E, D>
where
  I: Id + Send + Sync + 'static,
  R: AddressResolver,
  R::Address: Send + Sync + 'static,
  <R as AddressResolver>::Runtime: Runtime,
  <<<R as AddressResolver>::Runtime as Runtime>::Sleep as Future>::Output: Send,
  E: Encoder,
  D: Decoder,
{
  async fn receive<Res: serde::de::DeserializeOwned>(
    &self,
    conn: <<<R as AddressResolver>::Runtime as Runtime>::Net as Net>::TcpStream,
    addr: SocketAddr,
    expected_kind: CommandResponseKind,
  ) -> Result<Res, <Self as Transport>::Error> {
    // let mut r = BufReader::new(conn);
    // let mut header = [0u8; HEADER_SIZE];
    // r.read_exact(&mut header).await?;
    // let kind = CommandResponseKind::try_from(header[0])?;
    // let version = ProtocolVersion::from_u8(header[1])?;
    // let len = u32::from_be_bytes([header[2], header[3], header[4], header[5]]) as usize;
    // let mut data = vec![0; len];
    // r.read_exact(&mut data).await?;
    // match kind {
    //   CommandResponseKind::Err => {
    //     let res = decode::<ErrorResponse>(version, &data)?;
    //     Err(Error::Remote(res.error))
    //   }
    //   kind if kind == expected_kind => {
    //     let res = decode::<Res>(version, &data)?;
    //     self.return_conn(r.into_inner(), addr).await;
    //     Ok(res)
    //   }
    //   kind => Err(Error::UnexpectedResponse {
    //     expected: expected_kind.as_str(),
    //     actual: kind.as_str(),
    //   }),
    // }
    todo!()
  }

  async fn return_conn(
    &self,
    conn: <<<R as AddressResolver>::Runtime as Runtime>::Net as Net>::TcpStream,
    addr: <R as AddressResolver>::Address,
  ) {
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

    self.conn_size.fetch_add(1, Ordering::Release);
  }

  async fn send(
    &self,
    req: Request<I, <R as AddressResolver>::Address>,
  ) -> Result<
    <<<R as AddressResolver>::Runtime as Runtime>::Net as Net>::TcpStream,
    <Self as Transport>::Error,
  > {
    // Get a connection
    let mut conn = {
      let mut pool = self.conn_pool.lock().await;
      match pool.remove(&req.header().addr()) {
        Some(conn) => {
          self.conn_size.fetch_sub(1, Ordering::Release);
          conn
        }
        None => {
          let socket_addr = self.resolver.resolve(req.header().addr()).await?;
          let conn = <<<<R as AddressResolver>::Runtime as Runtime>::Net as Net>::TcpStream as TcpStream>::connect(socket_addr).await?;
          if !self.timeout.is_zero() {
            conn.set_timeout(Some(self.timeout));
          }
          conn
        }
      }
    };

    // let data = req.encode()?;
    // conn.write_all(&data).await?;
    // conn.flush().await?;

    // Ok(conn)
    todo!()
  }
}

impl<I, R, E, D> Drop for NetTransport<I, R, E, D>
where
  I: Id + Send + Sync + 'static,
  R: AddressResolver,
  R::Address: Send + Sync + 'static,
  <R as AddressResolver>::Runtime: Runtime,
  <<<R as AddressResolver>::Runtime as Runtime>::Sleep as Future>::Output: Send,
  E: Encoder,
  D: Decoder,
{
  fn drop(&mut self) {
    use pollster::FutureExt as _;
    let _ = self.shutdown().block_on();
  }
}

/// Used to handle connection from remote peers.
struct RequestHandler<I: Id, A: Address, R: Runtime> {
  ln: <R::Net as agnostic::net::Net>::TcpListener,
  local_header: Header<I, A>,
  producer: RequestProducer<I, A>,
  shutdown: Arc<AtomicBool>,
  shutdown_rx: async_channel::Receiver<()>,
  wg: AsyncWaitGroup,
}

impl<I, A, R> RequestHandler<I, A, R>
where
  I: Id + Send + 'static,
  A: Address + Send + 'static,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
{
  async fn run(self) {
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
              R::spawn_detach(async move {
                if Self::handle_connection(conn, producer, shutdown_rx, local_header)
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
                _ = R::sleep(loop_delay).fuse() => continue,
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

  async fn handle_connection(
    conn: <R::Net as agnostic::net::Net>::TcpStream,
    producer: RequestProducer<I, A>,
    shutdown_rx: async_channel::Receiver<()>,
    local_header: Header<I, A>,
  ) -> Result<(), <Self as Transport>::Error> {
    // let mut r = BufReader::with_capacity(CONN_RECEIVE_BUFFER_SIZE, conn);

    // let _get_type_start = Instant::now();

    // // TODO: metrics
    // // measuring the time to get the first byte separately because the heartbeat conn will hang out here
    // // for a good while waiting for a heartbeat whereas the append entries/rpc conn should not.

    // let _decode_start = Instant::now();
    // // Get the request meta
    // let mut header = [0u8; HEADER_SIZE];
    // r.read_exact(&mut header).await?;
    // let kind = CommandKind::try_from(header[0])?;
    // let version = ProtocolVersion::from_u8(header[1])?;
    // let msg_len = u32::from_be_bytes([header[2], header[3], header[4], header[5]]) as usize;
    // let mut buf = vec![0; msg_len];
    // r.read_exact(&mut buf).await?;

    // let req = match kind {
    //   CommandKind::AppendEntries => {
    //     let req = decode(version, &buf)?;
    //     // TODO: metrics
    //     Request::append_entries(version, req)
    //   }
    //   CommandKind::Vote => {
    //     let req = decode(version, &buf)?;
    //     // TODO: metrics
    //     Request::vote(version, req)
    //   }
    //   CommandKind::InstallSnapshot => {
    //     let req = decode(version, &buf)?;
    //     // TODO: metrics
    //     Request::install_snapshot(version, req)
    //   }
    //   CommandKind::TimeoutNow => {
    //     let req = decode(version, &buf)?;
    //     // TODO: metrics
    //     Request::timeout_now(version, req)
    //   }
    //   CommandKind::Heartbeat => {
    //     let req = decode(version, &buf)?;
    //     // TODO: metrics
    //     Request::heartbeat(version, req)
    //   }
    //   _ => {
    //     unreachable!();
    //   }
    // };

    // // TODO: metrics

    // let _process_start = Instant::now();
    // let resp = if let RequestKind::Heartbeat(_) = req.kind {
    //   Response::heartbeat(version, local_header)
    // } else {
    //   let (tx, handle) = Command::new(req);
    //   futures::select! {
    //     res = producer.send(tx).fuse() => {
    //       match res {
    //         Ok(_) => {
    //           futures::select! {
    //             res = handle.fuse() => {
    //               match res {
    //                 Ok(resp) => resp,
    //                 Err(e) => {
    //                   tracing::error!(target = "ruraft.net.transport", err=%e, "canceled command");
    //                   Response::error(version, local_header, e.to_string())
    //                 },
    //               }
    //             },
    //             _ = shutdown_rx.recv().fuse() => return Err(Error::AlreadyShutdown),
    //           }
    //         },
    //         Err(e) => {
    //           tracing::error!(target = "ruraft.net.transport", err=%e, "failed to send dispatch request");
    //           return Err(Error::Dispatch);
    //         },
    //       }
    //     },
    //     _ = shutdown_rx.recv().fuse() => return Err(Error::AlreadyShutdown),
    //   }
    // };
    // let resp = resp.encode().map_err(|e| {
    //   tracing::error!(target = "ruraft.net.transport", err=%e, "failed to encode response");
    //   Error::IO(e)
    // })?;

    // r.into_inner().write_all(&resp).await.map_err(|e| {
    //   tracing::error!(target = "ruraft.net.transport", err=%e, "failed to send response");
    //   Error::IO(e)
    // })
    todo!()
  }
}

#[repr(u8)]
#[non_exhaustive]
pub(super) enum CommandKind {
  AppendEntries = 0,
  Vote = 1,
  InstallSnapshot = 2,
  TimeoutNow = 3,
  Heartbeat = 4,
}

impl TryFrom<u8> for CommandKind {
  type Error = io::Error;

  fn try_from(value: u8) -> Result<Self, Self::Error> {
    match () {
      () if CommandKind::AppendEntries as u8 == value => Ok(Self::AppendEntries),
      () if CommandKind::Vote as u8 == value => Ok(Self::Vote),
      () if CommandKind::InstallSnapshot as u8 == value => Ok(Self::InstallSnapshot),
      () if CommandKind::TimeoutNow as u8 == value => Ok(Self::TimeoutNow),
      () if CommandKind::Heartbeat as u8 == value => Ok(Self::Heartbeat),
      _ => Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!("unknown command type: {value}"),
      )),
    }
  }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
#[non_exhaustive]
pub(super) enum CommandResponseKind {
  AppendEntries = 0,
  Vote = 1,
  InstallSnapshot = 2,
  TimeoutNow = 3,
  Heartbeat = 4,
  Err = 5,
}

impl CommandResponseKind {
  pub(super) const fn as_str(&self) -> &'static str {
    match self {
      Self::AppendEntries => "AppendEntries",
      Self::Vote => "Vote",
      Self::InstallSnapshot => "InstallSnapshot",
      Self::TimeoutNow => "TimeoutNow",
      Self::Heartbeat => "Heartbeat",
      Self::Err => "Error",
    }
  }
}

impl core::fmt::Display for CommandResponseKind {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::AppendEntries => write!(f, "AppendEntries"),
      Self::Vote => write!(f, "Vote"),
      Self::InstallSnapshot => write!(f, "InstallSnapshot"),
      Self::TimeoutNow => write!(f, "TimeoutNow"),
      Self::Heartbeat => write!(f, "Heartbeat"),
      Self::Err => write!(f, "Error"),
    }
  }
}

impl TryFrom<u8> for CommandResponseKind {
  type Error = io::Error;

  fn try_from(value: u8) -> Result<Self, Self::Error> {
    match () {
      () if Self::AppendEntries as u8 == value => Ok(Self::AppendEntries),
      () if Self::Vote as u8 == value => Ok(Self::Vote),
      () if Self::InstallSnapshot as u8 == value => Ok(Self::InstallSnapshot),
      () if Self::TimeoutNow as u8 == value => Ok(Self::TimeoutNow),
      () if Self::Heartbeat as u8 == value => Ok(Self::Heartbeat),
      () if Self::Err as u8 == value => Ok(Self::Err),
      _ => Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!("unknown command response type: {value}"),
      )),
    }
  }
}

// #[cfg(any(test, feature = "test"))]
// pub(super) mod tests {
//   use crate::storage::{Log, LogKind};

//   use super::*;

//   async fn make_transport<R>() -> NetTransport<R>
//   where
//     R: Runtime,
//     <R::Sleep as Future>::Output: Send,
//   {
//     let opts = NetTransportOptions::new(ServerId::random(), "127.0.0.1:0".parse().unwrap());
//     NetTransport::<R>::new(opts).await.unwrap()
//   }

//   fn make_append_req(id: ServerId, addr: SocketAddr) -> AppendEntriesRequest {
//     AppendEntriesRequest {
//       header: Header::new(id, addr),
//       term: 10,
//       prev_log_entry: 100,
//       prev_log_term: 4,
//       entries: vec![Log::crate_new(101, 4, LogKind::Noop)],
//       leader_commit: 90,
//     }
//   }

//   fn make_append_resp(id: ServerId, addr: SocketAddr) -> AppendEntriesResponse {
//     AppendEntriesResponse {
//       header: Header::new(id, addr),
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
//       ServerId::random(),
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
//       ServerId::random(),
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
//       ServerId::random(),
//       "127.0.0.1:0".parse().unwrap(),
//     ))
//     .await
//     .unwrap();

//     let res = trans2.append_entries(args).await.unwrap();
//     assert_eq!(res, expected_resp);
//   }
// }
