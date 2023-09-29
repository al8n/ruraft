use std::{
  future::Future,
  io, mem,
  net::SocketAddr,
  pin::Pin,
  task::{Context, Poll},
};

use futures::{channel::oneshot, Stream};
use serde::{Deserialize, Serialize};

use crate::{
  membership::{Membership, ServerId},
  options::{ProtocolVersion, SnapshotVersion},
  storage::Log,
};

pub(super) const HEADER_SIZE: usize =
  mem::size_of::<ProtocolVersion>() + mem::size_of::<CommandKind>() + mem::size_of::<u32>();

/// A common sub-structure used to pass along protocol version and
/// other information about the cluster.
#[viewit::viewit(
  vis_all = "pub(crate)",
  getters(vis_all = "pub"),
  setters(vis_all = "pub")
)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Header {
  /// The [`ServerId`] of the node sending the RPC Request or Response
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Get the server id of the request or response"),
    ),
    setter(attrs(doc = "Set the server id of the request or response"),)
  )]
  id: ServerId,
  /// The addr of the node sending the RPC Request or Response
  #[viewit(
    getter(
      const,
      attrs(doc = "Get the target address of the request or response"),
    ),
    setter(attrs(doc = "Set the target address of the request or response"),)
  )]
  addr: SocketAddr,
}

impl Header {
  /// Create a new [`Header`] with the given [`ServerId`] and [`SocketAddr`]
  #[inline]
  pub const fn new(id: ServerId, addr: SocketAddr) -> Self {
    Self { id, addr }
  }
}

/// The command used to append entries to the
/// replicated log.
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
  /// The header of the request
  #[viewit(getter(const))]
  header: Header,

  /// Provide the current term and leader
  term: u64,

  /// Provide the previous entries for integrity checking
  prev_log_entry: u64,
  /// Provide the previous term for integrity checking
  prev_log_term: u64,

  /// New entries to commit
  entries: Vec<Log>,

  /// Commit index on the leader
  leader_commit: u64,
}

/// The response returned from an
/// [`AppendEntriesRequest`].
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]

pub struct AppendEntriesResponse {
  /// The header of the response
  #[viewit(getter(const))]
  header: Header,

  /// Newer term if leader is out of date
  term: u64,

  /// A hint to help accelerate rebuilding slow nodes
  last_log: u64,

  /// We may not succeed if we have a conflicting entry
  success: bool,

  /// There are scenarios where this request didn't succeed
  /// but there's no need to wait/back-off the next attempt.
  no_retry_backoff: bool,
}

/// The command used by a candidate to ask a Raft peer
/// for a vote in an election.
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VoteRequest {
  /// The header of the request
  #[viewit(getter(const))]
  header: Header,

  /// The term of the candidate
  term: u64,

  /// The index of the candidate's last log entry
  last_log_index: u64,

  /// The term of the candidate's last log entry
  last_log_term: u64,

  /// Used to indicate to peers if this vote was triggered by a leadership
  /// transfer. It is required for leadership transfer to work, because servers
  /// wouldn't vote otherwise if they are aware of an existing leader.
  leadership_transfer: bool,
}

/// The command used by a candidate to ask a Raft peer
/// for a vote in an election.
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VoteResponse {
  /// The header of the response
  #[viewit(getter(const))]
  header: Header,

  /// Newer term if leader is out of date.
  term: u64,

  /// Is the vote granted.
  granted: bool,
}

/// The command sent to a Raft peer to bootstrap its
/// log (and state machine) from a snapshot on another peer.
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstallSnapshotRequest {
  /// The header of the request
  #[viewit(getter(const))]
  header: Header,

  /// The snapshot version
  snapshot_version: SnapshotVersion,

  /// The term
  term: u64,

  /// The last index included in the snapshot
  last_log_index: u64,
  /// The last term included in the snapshot
  last_log_term: u64,

  /// Cluster membership.
  membership: Membership,

  /// Log index where [`Membership`] entry was originally written.
  membership_index: u64,

  /// Size of the snapshot
  size: u64,
}

/// The response returned from an
/// [`InstallSnapshotRequest`].
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InstallSnapshotResponse {
  /// The header of the response
  #[viewit(getter(const))]
  header: Header,

  /// The term
  term: u64,

  /// Successfully install the snapshot or not.
  success: bool,
}

/// The command used by a leader to signal another server to
/// start an election.
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TimeoutNowRequest {
  /// The header of the request
  #[viewit(getter(const))]
  header: Header,
}

/// The response to [`TimeoutNowRequest`].
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TimeoutNowResponse {
  /// The header of the response
  #[viewit(getter(const))]
  header: Header,
}

/// The heartbeat command.
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct HeartbeatRequest {
  /// The header of the request
  #[viewit(getter(const))]
  header: Header,

  /// Provide the current term and leader
  term: u64,
}

/// The response returned from an
/// [`HeartbeatRequest`].
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]

pub struct HeartbeatResponse {
  /// The header of the response
  #[viewit(getter(const))]
  header: Header,
}

/// The response returned from an
/// [`HeartbeatRequest`].
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ErrorResponse {
  /// The header of the response
  #[viewit(getter(const))]
  header: Header,
  /// The error message
  error: String,
}

impl ProtocolVersion {
  pub(crate) const fn header_offset(&self) -> usize {
    match self {
      ProtocolVersion::V1 => {
        mem::size_of::<Self>() + mem::size_of::<CommandKind>() + mem::size_of::<u32>()
      }
    }
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

macro_rules! encode {
  (v1::$ty:ident { $expr: expr }) => {{
    let mut buf = Vec::with_capacity(128);
    buf.push({ $expr });
    buf.push(ProtocolVersion::V1 as u8);

    // Reserve length for the message.
    buf.extend_from_slice(&[0, 0, 0, 0]);

    rmp_serde::encode::write(&mut buf, $ty)
      .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let encoded_len = buf.len() - OFFSET;
    buf[2..OFFSET].copy_from_slice(&(encoded_len as u32).to_be_bytes());
    Ok(buf)
  }};
}

impl ProtocolVersion {
  pub(super) fn from_u8(val: u8) -> io::Result<Self> {
    match val {
      1 => Ok(Self::V1),
      _ => Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!("unknown protocol version: {val}"),
      )),
    }
  }
}

/// Request to be sent to the Raft node.
#[non_exhaustive]
pub enum RequestKind {
  AppendEntries(AppendEntriesRequest),
  Vote(VoteRequest),
  InstallSnapshot(InstallSnapshotRequest),
  TimeoutNow(TimeoutNowRequest),
  Heartbeat(HeartbeatRequest),
}

pub struct Request {
  pub(super) protocol_version: ProtocolVersion,
  pub(super) kind: RequestKind,
}

impl Request {
  pub(super) const fn append_entries(version: ProtocolVersion, req: AppendEntriesRequest) -> Self {
    Self {
      protocol_version: version,
      kind: RequestKind::AppendEntries(req),
    }
  }

  pub(super) const fn vote(version: ProtocolVersion, req: VoteRequest) -> Self {
    Self {
      protocol_version: version,
      kind: RequestKind::Vote(req),
    }
  }

  pub(super) const fn install_snapshot(
    version: ProtocolVersion,
    req: InstallSnapshotRequest,
  ) -> Self {
    Self {
      protocol_version: version,
      kind: RequestKind::InstallSnapshot(req),
    }
  }

  pub(super) const fn timeout_now(version: ProtocolVersion, req: TimeoutNowRequest) -> Self {
    Self {
      protocol_version: version,
      kind: RequestKind::TimeoutNow(req),
    }
  }

  pub(super) const fn heartbeat(version: ProtocolVersion, req: HeartbeatRequest) -> Self {
    Self {
      protocol_version: version,
      kind: RequestKind::Heartbeat(req),
    }
  }

  pub const fn header(&self) -> &Header {
    match &self.kind {
      RequestKind::AppendEntries(req) => req.header(),
      RequestKind::Vote(req) => req.header(),
      RequestKind::InstallSnapshot(req) => req.header(),
      RequestKind::TimeoutNow(req) => req.header(),
      RequestKind::Heartbeat(req) => req.header(),
    }
  }

  pub fn encode(&self) -> io::Result<Vec<u8>> {
    match self.protocol_version {
      ProtocolVersion::V1 => {
        const OFFSET: usize = ProtocolVersion::V1.header_offset();

        match &self.kind {
          RequestKind::AppendEntries(req) => encode!(v1::req { CommandKind::AppendEntries as u8 }),
          RequestKind::Vote(req) => encode!(v1::req {CommandKind::Vote as u8 }),
          RequestKind::InstallSnapshot(req) => {
            encode!(v1::req { CommandKind::InstallSnapshot as u8 })
          }
          RequestKind::TimeoutNow(req) => encode!(v1::req { CommandKind::TimeoutNow as u8 }),
          RequestKind::Heartbeat(req) => encode!(v1::req { CommandKind::Heartbeat as u8 }),
        }
      }
    }
  }
}

pub(super) fn decode<T>(protocol_version: ProtocolVersion, src: &[u8]) -> io::Result<T>
where
  T: serde::de::DeserializeOwned,
{
  match protocol_version {
    ProtocolVersion::V1 => {
      rmp_serde::decode::from_slice(src).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
  }
}

/// Response from the Raft node
#[non_exhaustive]
pub enum ResponseKind {
  AppendEntries(AppendEntriesResponse),
  Vote(VoteResponse),
  InstallSnapshot(InstallSnapshotResponse),
  TimeoutNow(TimeoutNowResponse),
  Heartbeat(HeartbeatResponse),
  Error(ErrorResponse),
}

pub struct Response {
  protocol_version: ProtocolVersion,
  kind: ResponseKind,
}

impl Response {
  const fn append_entries(version: ProtocolVersion, res: AppendEntriesResponse) -> Self {
    Self {
      protocol_version: version,
      kind: ResponseKind::AppendEntries(res),
    }
  }

  const fn vote(version: ProtocolVersion, res: VoteResponse) -> Self {
    Self {
      protocol_version: version,
      kind: ResponseKind::Vote(res),
    }
  }

  const fn install_snapshot(version: ProtocolVersion, res: InstallSnapshotResponse) -> Self {
    Self {
      protocol_version: version,
      kind: ResponseKind::InstallSnapshot(res),
    }
  }

  const fn timeout_now(version: ProtocolVersion, res: TimeoutNowResponse) -> Self {
    Self {
      protocol_version: version,
      kind: ResponseKind::TimeoutNow(res),
    }
  }

  pub(super) const fn heartbeat(version: ProtocolVersion, header: Header) -> Self {
    Self {
      protocol_version: version,
      kind: ResponseKind::Heartbeat(HeartbeatResponse { header }),
    }
  }

  pub(super) const fn error(version: ProtocolVersion, header: Header, error: String) -> Self {
    Self {
      protocol_version: version,
      kind: ResponseKind::Error(ErrorResponse { header, error }),
    }
  }

  /// Returns the header of the response.
  #[inline]
  pub const fn header(&self) -> &Header {
    match &self.kind {
      ResponseKind::AppendEntries(res) => res.header(),
      ResponseKind::Vote(res) => res.header(),
      ResponseKind::InstallSnapshot(res) => res.header(),
      ResponseKind::TimeoutNow(res) => res.header(),
      ResponseKind::Heartbeat(res) => res.header(),
      ResponseKind::Error(res) => res.header(),
    }
  }

  pub fn encode(&self) -> io::Result<Vec<u8>> {
    match self.protocol_version {
      ProtocolVersion::V1 => {
        const OFFSET: usize =
          mem::size_of::<CommandKind>() + mem::size_of::<ProtocolVersion>() + mem::size_of::<u32>();

        match &self.kind {
          ResponseKind::AppendEntries(res) => {
            encode!(v1::res { CommandResponseKind::AppendEntries as u8 })
          }
          ResponseKind::Vote(res) => encode!(v1::res { CommandResponseKind::Vote as u8 }),
          ResponseKind::InstallSnapshot(res) => {
            encode!(v1::res { CommandResponseKind::InstallSnapshot as u8 })
          }
          ResponseKind::TimeoutNow(res) => {
            encode!(v1::res { CommandResponseKind::TimeoutNow as u8 })
          }
          ResponseKind::Heartbeat(res) => encode!(v1::res { CommandResponseKind::Heartbeat as u8 }),
          ResponseKind::Error(res) => encode!(v1::res { CommandResponseKind::Err as u8 }),
        }
      }
    }
  }
}

/// Errors returned by the [`CommandHandle`].
#[derive(Debug, thiserror::Error)]
pub enum CommandHandleError<E>
where
  E: std::error::Error + Send + Sync + 'static,
{
  /// Returned when the command is handled by the Raft but got an error
  #[error("{0}")]
  Err(E),
  /// Returned when the command is cancelled
  #[error("{0}")]
  Canceled(#[from] oneshot::Canceled),
}

/// A future for getting the corresponding response from the Raft.
#[pin_project::pin_project]
#[repr(transparent)]
pub struct CommandHandle<E>
where
  E: std::error::Error + Send + Sync + 'static,
{
  #[pin]
  rx: oneshot::Receiver<Result<Response, E>>,
}

impl<E> CommandHandle<E>
where
  E: std::error::Error + Send + Sync + 'static,
{
  pub(crate) fn new(rx: oneshot::Receiver<Result<Response, E>>) -> Self {
    Self { rx }
  }
}

impl<E> Future for CommandHandle<E>
where
  E: std::error::Error + Send + Sync + 'static,
{
  type Output = Result<Response, CommandHandleError<E>>;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    // Using Pin::as_mut to get a Pin<&mut Receiver>.
    let this = self.project();

    // Now, poll the receiver directly
    this.rx.poll(cx).map(|res| match res {
      Ok(res) => res.map_err(CommandHandleError::Err),
      Err(e) => Err(From::from(e)),
    })
  }
}

/// The struct is used to interact with the Raft.
pub struct Command<E>
where
  E: std::error::Error + Send + Sync + 'static,
{
  req: Request,
  tx: oneshot::Sender<Result<Response, E>>,
}

impl<E> Command<E>
where
  E: std::error::Error + Send + Sync + 'static,
{
  /// Create a new command from the given request.
  pub fn new(req: Request) -> (Self, CommandHandle<E>) {
    let (tx, rx) = oneshot::channel();
    (Self { req, tx }, CommandHandle::new(rx))
  }

  /// Returns the [`Request`] of this command.
  #[inline]
  pub const fn request(&self) -> &Request {
    &self.req
  }

  /// Set the [`Request`] in builder pattern
  #[inline]
  pub fn with_request(mut self, req: Request) -> Self {
    self.req = req;
    self
  }

  /// Set the [`Request`] for this command
  #[inline]
  pub fn set_request(&mut self, req: Request) {
    self.req = req;
  }
}

/// Command stream is a consumer for Raft node to consume requests
/// from remote nodes
#[pin_project::pin_project]
#[derive(Debug)]
pub struct CommandConsumer<E>
where
  E: std::error::Error + Send + Sync + 'static,
{
  #[pin]
  rx: async_channel::Receiver<Command<E>>,
}

impl<E> Clone for CommandConsumer<E>
where
  E: std::error::Error + Send + Sync + 'static,
{
  fn clone(&self) -> Self {
    Self {
      rx: self.rx.clone(),
    }
  }
}

impl<E> Stream for CommandConsumer<E>
where
  E: std::error::Error + Send + Sync + 'static,
{
  type Item = <async_channel::Receiver<Command<E>> as Stream>::Item;

  fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    <async_channel::Receiver<Command<E>> as Stream>::poll_next(self.project().rx, cx)
  }
}

/// A producer for [`Command`]s
pub struct CommandProducer<E>
where
  E: std::error::Error + Send + Sync + 'static,
{
  tx: async_channel::Sender<Command<E>>,
}

impl<E> Clone for CommandProducer<E>
where
  E: std::error::Error + Send + Sync + 'static,
{
  fn clone(&self) -> Self {
    Self {
      tx: self.tx.clone(),
    }
  }
}

impl<E> CommandProducer<E>
where
  E: std::error::Error + Send + Sync + 'static,
{
  /// Produce a command for processing
  pub async fn send(
    &self,
    command: Command<E>,
  ) -> Result<(), async_channel::SendError<Command<E>>> {
    self.tx.send(command).await
  }
}

/// Returns unbounded command producer and command consumer.
pub fn command<E>() -> (CommandProducer<E>, CommandConsumer<E>)
where
  E: std::error::Error + Send + Sync + 'static,
{
  let (tx, rx) = async_channel::unbounded();
  (CommandProducer { tx }, CommandConsumer { rx })
}
