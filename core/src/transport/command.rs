use std::{
  future::Future,
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

/// A common sub-structure used to pass along protocol version and
/// other information about the cluster.
#[viewit::viewit(
  vis_all = "pub(crate)",
  getters(vis_all = "pub"),
  setters(vis_all = "pub")
)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Header {
  /// The version of the protocol the sender is speaking
  #[viewit(
    getter(
      const,
      attrs(doc = "Get the protocol version of the request or response"),
    ),
    setter(attrs(doc = "Set the protocol version of the request or response"),)
  )]
  protocol_version: ProtocolVersion,
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
    Self {
      protocol_version: ProtocolVersion::V1,
      id,
      addr,
    }
  }
}

/// The command used to append entries to the
/// replicated log.
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
  /// The header of the request
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
  header: Header,
}

/// The response to [`TimeoutNowRequest`].
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TimeoutNowResponse {
  /// The header of the response
  header: Header,
}

/// Request to be sent to the Raft node.
pub enum Request {
  AppendEntries(AppendEntriesRequest),
  Vote(VoteRequest),
  InstallSnapshot(InstallSnapshotRequest),
  TimeoutNow(TimeoutNowRequest),
}

impl Request {
  pub fn header(&self) -> &Header {
    match self {
      Self::AppendEntries(req) => req.header(),
      Self::Vote(req) => req.header(),
      Self::InstallSnapshot(req) => req.header(),
      Self::TimeoutNow(req) => req.header(),
    }
  }
}

/// Response from the Raft node
pub enum Response {
  AppendEntries(AppendEntriesResponse),
  Vote(VoteResponse),
  InstallSnapshot(InstallSnapshotResponse),
  TimeoutNow(TimeoutNowResponse),
}

impl Response {
  /// Returns the header of the response.
  #[inline]
  pub fn header(&self) -> &Header {
    match self {
      Self::AppendEntries(res) => res.header(),
      Self::Vote(res) => res.header(),
      Self::InstallSnapshot(res) => res.header(),
      Self::TimeoutNow(res) => res.header(),
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
