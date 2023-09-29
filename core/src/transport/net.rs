use std::{
  collections::HashMap,
  future::Future,
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
  AsyncReadExt, AsyncWriteExt, FutureExt,
};

use serde::{Deserialize, Serialize};
use wg::AsyncWaitGroup;

use crate::options::ProtocolVersion;

use super::*;

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
#[derive(Debug, thiserror::Error)]
pub enum Error {
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
    expected: CommandResponseKind,
    actual: CommandResponseKind,
  },
  /// Returned when the remote response error
  #[error("remote: {0}")]
  Remote(String),
  /// An error about the I/O
  #[error("{0}")]
  IO(#[from] std::io::Error),
}

/// Encapsulates configuration for the network transport layer.
#[viewit::viewit]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetTransportOptions {
  /// The protocol version to use for encoding/decoding messages.
  protocol_version: ProtocolVersion,

  /// The local ID of the server we are running on.
  id: ServerId,

  /// The address for the network transport layer bind to.
  bind_addr: SocketAddr,

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
  #[serde(with = "humantime_serde")]
  timeout: Duration,
}

impl NetTransportOptions {
  /// Create a new [`NetTransportOptions`] with default values.
  #[inline]
  pub const fn new(id: ServerId, bind_addr: SocketAddr) -> Self {
    Self {
      max_pool: 3,
      max_inflight_requests: DEFAULT_MAX_INFLIGHT_REQUESTS,
      timeout: Duration::from_secs(10),
      id,
      bind_addr,
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
pub struct NetTransport<R: Runtime> {
  shutdown: Arc<AtomicBool>,
  shutdown_tx: async_channel::Sender<()>,
  local_header: Header,
  consumer: CommandConsumer<Error>,
  wg: AsyncWaitGroup,
  conn_pool: Mutex<HashMap<SocketAddr, <R::Net as Net>::TcpStream>>,
  conn_size: AtomicUsize,
  protocol_version: ProtocolVersion,
  max_pool: usize,
  max_inflight_requests: usize,
  timeout: Duration,
}

#[async_trait::async_trait]
impl<R> Transport for NetTransport<R>
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
{
  type Error = Error;
  type Runtime = R;
  type Options = NetTransportOptions;

  fn consumer(&self) -> CommandConsumer<Self::Error> {
    self.consumer.clone()
  }

  fn local_header(&self) -> &Header {
    &self.local_header
  }

  async fn new(opts: Self::Options) -> Result<Self, Self::Error> {
    let (shutdown_tx, shutdown_rx) = async_channel::unbounded();
    let auto_port = opts.bind_addr.port() == 0;

    let ln = <<<R as Runtime>::Net as Net>::TcpListener as TcpListener>::bind(opts.bind_addr)
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
      opts.bind_addr
    };

    let local_header = Header::new(opts.id, addr);
    let shutdown = Arc::new(AtomicBool::new(false));
    let wg = AsyncWaitGroup::from(1);
    let (producer, consumer) = command();
    let request_handler = RequestHandler {
      ln,
      local_header: local_header.clone(),
      producer,
      shutdown: shutdown.clone(),
      shutdown_rx,
      wg: wg.clone(),
    };
    R::spawn_detach(RequestHandler::<R>::run(request_handler));

    Ok(Self {
      shutdown,
      shutdown_tx,
      local_header,
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
    })
  }

  async fn append_entries(
    &self,
    req: AppendEntriesRequest,
  ) -> Result<AppendEntriesResponse, Self::Error> {
    let addr = req.header().addr;
    let conn = self
      .send(Request::append_entries(self.protocol_version, req))
      .await?;
    self
      .receive(conn, addr, CommandResponseKind::AppendEntries)
      .await
  }

  async fn vote(&self, req: VoteRequest) -> Result<VoteResponse, Self::Error> {
    let addr = req.header().addr;
    let conn: <<R as Runtime>::Net as Net>::TcpStream =
      self.send(Request::vote(self.protocol_version, req)).await?;
    self.receive(conn, addr, CommandResponseKind::Vote).await
  }

  async fn install_snapshot(
    &self,
    req: InstallSnapshotRequest,
    source: impl AsyncRead + Send,
  ) -> Result<InstallSnapshotResponse, Self::Error> {
    let addr = req.header().addr;
    let conn = self
      .send(Request::install_snapshot(self.protocol_version, req))
      .await?;
    let mut w = BufWriter::with_capacity(CONN_SEND_BUFFER_SIZE, conn);
    futures::io::copy(source, &mut w).await?;
    self
      .receive(w.into_inner(), addr, CommandResponseKind::InstallSnapshot)
      .await
  }

  async fn timeout_now(&self, req: TimeoutNowRequest) -> Result<TimeoutNowResponse, Self::Error> {
    let addr = req.header().addr;
    let conn = self
      .send(Request::timeout_now(self.protocol_version, req))
      .await?;
    self
      .receive(conn, addr, CommandResponseKind::TimeoutNow)
      .await
  }

  async fn heartbeat(&self, req: HeartbeatRequest) -> Result<HeartbeatResponse, Self::Error> {
    let addr = req.header().addr;
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

impl<R> NetTransport<R>
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
{
  async fn receive<Res: serde::de::DeserializeOwned>(
    &self,
    conn: <R::Net as Net>::TcpStream,
    addr: SocketAddr,
    expected_kind: CommandResponseKind,
  ) -> Result<Res, Error> {
    let mut r = BufReader::new(conn);
    let mut header = [0u8; HEADER_SIZE];
    r.read_exact(&mut header).await?;
    let kind = CommandResponseKind::try_from(header[0])?;
    let version = ProtocolVersion::from_u8(header[1])?;
    let len = u32::from_be_bytes([header[2], header[3], header[4], header[5]]) as usize;
    let mut data = Vec::with_capacity(len);
    r.read_exact(&mut data).await?;

    match kind {
      CommandResponseKind::Err => {
        let res = decode::<ErrorResponse>(version, &data)?;
        Err(Error::Remote(res.error))
      }
      kind if kind == expected_kind => {
        let res = decode::<Res>(version, &data)?;
        self.return_conn(r.into_inner(), addr).await;
        Ok(res)
      }
      kind => Err(Error::UnexpectedResponse {
        expected: expected_kind,
        actual: kind,
      }),
    }
  }

  async fn return_conn(&self, conn: <R::Net as Net>::TcpStream, addr: SocketAddr) {
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

  async fn send(&self, req: Request) -> Result<<R::Net as Net>::TcpStream, Error> {
    // Get a connection
    let mut conn = {
      let mut pool = self.conn_pool.lock().await;
      match pool.remove(&req.header().addr()) {
        Some(conn) => {
          self.conn_size.fetch_sub(1, Ordering::Release);
          conn
        }
        None => {
          let conn = <<R::Net as Net>::TcpStream as TcpStream>::connect(req.header().addr).await?;
          if !self.timeout.is_zero() {
            conn.set_timeout(Some(self.timeout));
          }
          conn
        }
      }
    };

    let data = req.encode()?;
    conn.write_all(&data).await?;
    conn.flush().await?;

    Ok(conn)
  }
}

/// Used to handle connection from remote peers.
struct RequestHandler<R: Runtime> {
  ln: <R::Net as agnostic::net::Net>::TcpListener,
  local_header: Header,
  producer: CommandProducer<Error>,
  shutdown: Arc<AtomicBool>,
  shutdown_rx: async_channel::Receiver<()>,
  wg: AsyncWaitGroup,
}

impl<R> RequestHandler<R>
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
{
  async fn run(self) {
    const BASE_DELAY: Duration = Duration::from_millis(5);
    const MAX_DELAY: Duration = Duration::from_secs(1);

    let mut loop_delay = Duration::ZERO;
    loop {
      // Accept incoming connections
      match self.ln.accept().await {
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

          if !self.shutdown.load(Ordering::Acquire) {
            tracing::error!(target = "ruraft.net.transport", err=%e, "failed to accept connection");
          }

          futures::select! {
            _ = R::sleep(loop_delay).fuse() => continue,
            _ = self.shutdown_rx.recv().fuse() => return,
          }
        }
      };
    }
  }

  async fn handle_connection(
    conn: <R::Net as agnostic::net::Net>::TcpStream,
    producer: CommandProducer<Error>,
    shutdown_rx: async_channel::Receiver<()>,
    local_header: Header,
  ) -> Result<(), Error> {
    let mut r = BufReader::with_capacity(CONN_RECEIVE_BUFFER_SIZE, conn);

    let _get_type_start = Instant::now();

    // TODO: metrics
    // measuring the time to get the first byte separately because the heartbeat conn will hang out here
    // for a good while waiting for a heartbeat whereas the append entries/rpc conn should not.

    let _decode_start = Instant::now();
    // Get the request meta
    let mut header = [0u8; HEADER_SIZE];
    r.read_exact(&mut header).await?;
    let kind = CommandKind::try_from(header[0])?;
    let version = ProtocolVersion::from_u8(header[1])?;
    let msg_len = u32::from_be_bytes([header[2], header[3], header[4], header[5]]) as usize;
    let mut buf = Vec::with_capacity(msg_len);
    r.read_exact(&mut buf).await?;

    let req = match kind {
      CommandKind::AppendEntries => {
        let req = decode(version, &buf)?;
        // TODO: metrics
        Request::append_entries(version, req)
      }
      CommandKind::Vote => {
        let req = decode(version, &buf)?;
        // TODO: metrics
        Request::vote(version, req)
      }
      CommandKind::InstallSnapshot => {
        let req = decode(version, &buf)?;
        // TODO: metrics
        Request::install_snapshot(version, req)
      }
      CommandKind::TimeoutNow => {
        let req = decode(version, &buf)?;
        // TODO: metrics
        Request::timeout_now(version, req)
      }
      CommandKind::Heartbeat => {
        let req = decode(version, &buf)?;
        // TODO: metrics
        Request::heartbeat(version, req)
      }
    };

    // TODO: metrics

    let _process_start = Instant::now();
    let resp = if let RequestKind::Heartbeat(_) = req.kind {
      Response::heartbeat(version, local_header)
    } else {
      let (tx, handle) = Command::<Error>::new(req);
      futures::select! {
        res = producer.send(tx).fuse() => {
          match res {
            Ok(_) => {
              futures::select! {
                res = handle.fuse() => {
                  match res {
                    Ok(resp) => resp,
                    Err(e) => {
                      tracing::error!(target = "ruraft.net.transport", err=%e, "canceled command");
                      Response::error(version, local_header, e.to_string())
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

    let resp = resp.encode().map_err(|e| {
      tracing::error!(target = "ruraft.net.transport", err=%e, "failed to encode response");
      Error::IO(e)
    })?;

    BufWriter::new(r.into_inner())
      .write_all(&resp)
      .await
      .map_err(|e| {
        tracing::error!(target = "ruraft.net.transport", err=%e, "failed to send response");
        Error::IO(e)
      })
  }
}
