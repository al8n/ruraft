use std::{
  future::Future,
  pin::Pin,
  task::{Context, Poll},
  time::Instant,
};

use futures::{channel::oneshot, Stream};

use crate::{
  membership::Membership,
  options::{ProtocolVersion, SnapshotVersion},
  storage::Log,
  Data, Node,
};

use super::{Address, Id};

pub use async_channel::{RecvError, TryRecvError};

/// A common sub-structure used to pass along protocol version and
/// other information about the cluster.
#[viewit::viewit(
  vis_all = "pub(crate)",
  getters(vis_all = "pub"),
  setters(vis_all = "pub")
)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Header<I, A> {
  /// The protocol version of the request or response
  #[viewit(
    getter(
      const,
      attrs(doc = "Get the protocol version of the request or response"),
    ),
    setter(attrs(doc = "Set the protocol version of the request or response"),)
  )]
  protocol_version: ProtocolVersion,

  /// The id of the node sending the RPC Request or Response
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Get the node of the request or response"),
    ),
    setter(attrs(doc = "Set the node of the request or response"),)
  )]
  from: Node<I, A>,
}

impl<I, A> Header<I, A> {
  /// Create a new [`Header`] with the given `id` and `addr`.
  #[inline]
  pub fn new(version: ProtocolVersion, id: I, addr: A) -> Self {
    Self {
      protocol_version: version,
      from: Node::new(id, addr),
    }
  }
}

impl<I, A> From<(ProtocolVersion, Node<I, A>)> for Header<I, A> {
  #[inline]
  fn from((version, from): (ProtocolVersion, Node<I, A>)) -> Self {
    Self {
      protocol_version: version,
      from,
    }
  }
}

/// The command used to append entries to the
/// replicated log.
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AppendEntriesRequest<I, A, D: Data> {
  /// The header of the request
  #[viewit(getter(const))]
  header: Header<I, A>,

  /// Provide the current term and leader
  term: u64,

  /// Provide the previous entries for integrity checking
  prev_log_entry: u64,
  /// Provide the previous term for integrity checking
  prev_log_term: u64,

  /// New entries to commit
  entries: Vec<Log<I, A, D>>,

  /// Commit index on the leader
  leader_commit: u64,
}

impl<I: Id, A: Address, D: Data> AppendEntriesRequest<I, A, D> {
  /// Create a new [`AppendEntriesRequest`] with the given `id` and `addr` and `version`. Other fields
  /// are set to their default values.
  pub fn new(version: ProtocolVersion, id: I, addr: A) -> Self {
    Self {
      header: Header {
        protocol_version: version,
        from: Node::new(id, addr),
      },
      term: 0,
      prev_log_entry: 0,
      prev_log_term: 0,
      entries: Vec::new(),
      leader_commit: 0,
    }
  }
}

/// The response returned from an
/// [`AppendEntriesRequest`].
#[viewit::viewit(setters(prefix = "with"))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]

pub struct AppendEntriesResponse<I, A> {
  /// The header of the response
  #[viewit(getter(const))]
  header: Header<I, A>,

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

impl<I: Id, A: Address> AppendEntriesResponse<I, A> {
  pub fn new(version: ProtocolVersion, id: I, addr: A) -> Self {
    Self {
      header: Header::new(version, id, addr),
      term: 0,
      last_log: 0,
      success: false,
      no_retry_backoff: false,
    }
  }
}

/// The response returned by a pipeline.
///
/// The difference between this and [`AppendEntriesResponse`] is that this
/// keeps some extra information:
///
/// 1. the time that the append request was started
/// 2. the original request's `term`
/// 3. the number of entries the original request has
/// 4. highest log index of the original request's entries
#[viewit::viewit(getters(vis_all = "pub"), setters(prefix = "with"))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PipelineAppendEntriesResponse<I: Id, A: Address> {
  /// The term of the request
  term: u64,

  /// The highest log index of the [`AppendEntriesRequest`]'s entries
  highest_log_index: Option<u64>,

  /// The number of entries in the [`AppendEntriesRequest`]'s
  num_entries: usize,

  /// The time that the original request was started
  #[cfg_attr(feature = "serde", serde(with = "crate::utils::serde_instant"))]
  start: Instant,

  /// The response of the [`AppendEntriesRequest`]
  #[viewit(getter(const, style = "ref"))]
  resp: AppendEntriesResponse<I, A>,
}

/// The command used by a candidate to ask a Raft peer
/// for a vote in an election.
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct VoteRequest<I, A> {
  /// The header of the request
  #[viewit(getter(const))]
  header: Header<I, A>,

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
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct VoteResponse<I, A> {
  /// The header of the response
  #[viewit(getter(const))]
  header: Header<I, A>,

  /// Newer term if leader is out of date.
  term: u64,

  /// Is the vote granted.
  granted: bool,
}

/// The command sent to a Raft peer to bootstrap its
/// log (and state machine) from a snapshot on another peer.
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct InstallSnapshotRequest<I: Id, A: Address> {
  /// The header of the request
  #[viewit(getter(const))]
  header: Header<I, A>,

  /// The snapshot version
  snapshot_version: SnapshotVersion,

  /// The term
  term: u64,

  /// The last index included in the snapshot
  last_log_index: u64,
  /// The last term included in the snapshot
  last_log_term: u64,

  /// Cluster membership.
  membership: Membership<I, A>,

  /// Log index where [`Membership`] entry was originally written.
  membership_index: u64,

  /// Size of the snapshot
  size: u64,
}

/// The response returned from an
/// [`InstallSnapshotRequest`].
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct InstallSnapshotResponse<I, A> {
  /// The header of the response
  #[viewit(getter(const))]
  header: Header<I, A>,

  /// The term
  term: u64,

  /// Successfully install the snapshot or not.
  success: bool,
}

/// The command used by a leader to signal another server to
/// start an election.
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TimeoutNowRequest<I, A> {
  /// The header of the request
  #[viewit(getter(const))]
  header: Header<I, A>,
}

/// The response to [`TimeoutNowRequest`].
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TimeoutNowResponse<I, A> {
  /// The header of the response
  #[viewit(getter(const))]
  header: Header<I, A>,
}

/// The heartbeat command.
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct HeartbeatRequest<I, A> {
  /// The header of the request
  #[viewit(getter(const))]
  header: Header<I, A>,

  /// Provide the current term and leader
  term: u64,
}

/// The response returned from an
/// [`HeartbeatRequest`].
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct HeartbeatResponse<I, A> {
  /// The header of the response
  #[viewit(getter(const))]
  header: Header<I, A>,

  /// We may not succeed if we have a conflicting entry
  success: bool,
}

impl<I: Id, A: Address> HeartbeatResponse<I, A> {
  /// Create a new HeartbeatResponse
  pub const fn new(header: Header<I, A>, success: bool) -> Self {
    Self { header, success }
  }
}

/// The response returned from an
/// [`HeartbeatRequest`].
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ErrorResponse<I, A> {
  /// The header of the response
  #[viewit(getter(const))]
  header: Header<I, A>,
  /// The error message
  error: String,
}

impl<I: Id, A: Address> ErrorResponse<I, A> {
  /// Create a new ErrorResponse
  pub const fn new(header: Header<I, A>, error: String) -> Self {
    Self { header, error }
  }
}

// macro_rules! encode {
//   (v1::$ty:ident { $expr: expr }) => {{
//     let mut buf = Vec::with_capacity(128);
//     buf.push({ $expr });
//     buf.push(ProtocolVersion::V1 as u8);

//     // Reserve length for the message.
//     buf.extend_from_slice(&[0, 0, 0, 0]);

//     rmp_serde::encode::write(&mut buf, $ty)
//       .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
//     let encoded_len = buf.len() - OFFSET;
//     buf[2..OFFSET].copy_from_slice(&(encoded_len as u32).to_be_bytes());
//     Ok(buf)
//   }};
// }

/// Request to be sent to the Raft node.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Request<I: Id, A: Address, D: Data> {
  AppendEntries(AppendEntriesRequest<I, A, D>),
  Vote(VoteRequest<I, A>),
  InstallSnapshot(InstallSnapshotRequest<I, A>),
  TimeoutNow(TimeoutNowRequest<I, A>),
  Heartbeat(HeartbeatRequest<I, A>),
}

impl<I: Id, A: Address, D: Data> Request<I, A, D> {
  /// Returns the tag of this request kind for encoding/decoding.
  #[inline]
  pub const fn tag(&self) -> u8 {
    match self {
      Self::AppendEntries(_) => 0,
      Self::Vote(_) => 1,
      Self::InstallSnapshot(_) => 2,
      Self::TimeoutNow(_) => 3,
      Self::Heartbeat(_) => 4,
    }
  }
}

impl<I: Id, A: Address, D: Data> From<AppendEntriesRequest<I, A, D>> for Request<I, A, D> {
  fn from(req: AppendEntriesRequest<I, A, D>) -> Self {
    Self::AppendEntries(req)
  }
}

impl<I: Id, A: Address, D: Data> From<VoteRequest<I, A>> for Request<I, A, D> {
  fn from(req: VoteRequest<I, A>) -> Self {
    Self::Vote(req)
  }
}

impl<I: Id, A: Address, D: Data> From<InstallSnapshotRequest<I, A>> for Request<I, A, D> {
  fn from(req: InstallSnapshotRequest<I, A>) -> Self {
    Self::InstallSnapshot(req)
  }
}

impl<I: Id, A: Address, D: Data> From<TimeoutNowRequest<I, A>> for Request<I, A, D> {
  fn from(req: TimeoutNowRequest<I, A>) -> Self {
    Self::TimeoutNow(req)
  }
}

impl<I: Id, A: Address, D: Data> From<HeartbeatRequest<I, A>> for Request<I, A, D> {
  fn from(req: HeartbeatRequest<I, A>) -> Self {
    Self::Heartbeat(req)
  }
}

// #[derive(Debug)]
// pub struct Request<I: Id, A: Address> {
//   pub(crate) kind: Request<I, A>,
// }

impl<I: Id, A: Address, D: Data> Request<I, A, D> {
  pub const fn append_entries(req: AppendEntriesRequest<I, A, D>) -> Self {
    Self::AppendEntries(req)
  }

  pub const fn vote(req: VoteRequest<I, A>) -> Self {
    Self::Vote(req)
  }

  pub const fn install_snapshot(req: InstallSnapshotRequest<I, A>) -> Self {
    Self::InstallSnapshot(req)
  }

  pub const fn timeout_now(req: TimeoutNowRequest<I, A>) -> Self {
    Self::TimeoutNow(req)
  }

  pub const fn heartbeat(req: HeartbeatRequest<I, A>) -> Self {
    Self::Heartbeat(req)
  }

  /// Returns the header of the request
  pub const fn header(&self) -> &Header<I, A> {
    match self {
      Self::AppendEntries(req) => req.header(),
      Self::Vote(req) => req.header(),
      Self::InstallSnapshot(req) => req.header(),
      Self::TimeoutNow(req) => req.header(),
      Self::Heartbeat(req) => req.header(),
    }
  }

  /// Returns the protocol version of the request.
  #[inline]
  pub const fn protocol_version(&self) -> ProtocolVersion {
    self.header().protocol_version
  }

  // pub(super) fn encode(&self) -> io::Result<Vec<u8>> {
  //   match self.protocol_version {
  //     ProtocolVersion::V1 => {
  //       const OFFSET: usize = ProtocolVersion::V1.header_offset();

  //       match &self.kind {
  //         Request::AppendEntries(req) => encode!(v1::req { CommandKind::AppendEntries as u8 }),
  //         Request::Vote(req) => encode!(v1::req {CommandKind::Vote as u8 }),
  //         Request::InstallSnapshot(req) => {
  //           encode!(v1::req { CommandKind::InstallSnapshot as u8 })
  //         }
  //         Request::TimeoutNow(req) => encode!(v1::req { CommandKind::TimeoutNow as u8 }),
  //         Request::Heartbeat(req) => encode!(v1::req { CommandKind::Heartbeat as u8 }),
  //       }
  //     }
  //   }
  // }
}

// pub(super) fn decode<T>(protocol_src: &[u8]) -> io::Result<T>
// where
//   T: serde::de::DeserializeOwned,
// {
//   match protocol_version {
//     ProtocolVersion::V1 => {
//       rmp_serde::decode::from_slice(src).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
//     }
//   }
// }

/// Response from the Raft node
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Response<I, A> {
  AppendEntries(AppendEntriesResponse<I, A>),
  Vote(VoteResponse<I, A>),
  InstallSnapshot(InstallSnapshotResponse<I, A>),
  TimeoutNow(TimeoutNowResponse<I, A>),
  Heartbeat(HeartbeatResponse<I, A>),
  Error(ErrorResponse<I, A>),
}

impl<I: Id, A: Address> Response<I, A> {
  /// Returns the tag of this request kind for encoding/decoding.
  #[inline]
  pub const fn tag(&self) -> u8 {
    match self {
      Self::AppendEntries(_) => 0,
      Self::Vote(_) => 1,
      Self::InstallSnapshot(_) => 2,
      Self::TimeoutNow(_) => 3,
      Self::Heartbeat(_) => 4,
      Self::Error(_) => 255,
    }
  }

  #[inline]
  pub const fn description(&self) -> &'static str {
    match self {
      Self::AppendEntries(_) => "AppendEntries",
      Self::Vote(_) => "Vote",
      Self::InstallSnapshot(_) => "InstallSnapshot",
      Self::TimeoutNow(_) => "TimeoutNow",
      Self::Heartbeat(_) => "Heartbeat",
      Self::Error(_) => "Error",
    }
  }
}

impl<I: Id, A: Address> From<AppendEntriesResponse<I, A>> for Response<I, A> {
  fn from(resp: AppendEntriesResponse<I, A>) -> Self {
    Self::AppendEntries(resp)
  }
}

impl<I: Id, A: Address> From<VoteResponse<I, A>> for Response<I, A> {
  fn from(resp: VoteResponse<I, A>) -> Self {
    Self::Vote(resp)
  }
}

impl<I: Id, A: Address> From<InstallSnapshotResponse<I, A>> for Response<I, A> {
  fn from(resp: InstallSnapshotResponse<I, A>) -> Self {
    Self::InstallSnapshot(resp)
  }
}

impl<I: Id, A: Address> From<TimeoutNowResponse<I, A>> for Response<I, A> {
  fn from(resp: TimeoutNowResponse<I, A>) -> Self {
    Self::TimeoutNow(resp)
  }
}

impl<I: Id, A: Address> From<HeartbeatResponse<I, A>> for Response<I, A> {
  fn from(resp: HeartbeatResponse<I, A>) -> Self {
    Self::Heartbeat(resp)
  }
}

impl<I: Id, A: Address> From<ErrorResponse<I, A>> for Response<I, A> {
  fn from(resp: ErrorResponse<I, A>) -> Self {
    Self::Error(resp)
  }
}

impl<I: Id, A: Address> Response<I, A> {
  pub const fn append_entries(resp: AppendEntriesResponse<I, A>) -> Self {
    Self::AppendEntries(resp)
  }

  pub const fn vote(resp: VoteResponse<I, A>) -> Self {
    Self::Vote(resp)
  }

  pub const fn install_snapshot(resp: InstallSnapshotResponse<I, A>) -> Self {
    Self::InstallSnapshot(resp)
  }

  pub const fn timeout_now(resp: TimeoutNowResponse<I, A>) -> Self {
    Self::TimeoutNow(resp)
  }

  pub const fn heartbeat(resp: HeartbeatResponse<I, A>) -> Self {
    Self::Heartbeat(resp)
  }

  pub const fn error(resp: ErrorResponse<I, A>) -> Self {
    Self::Error(resp)
  }

  /// Returns the header of the response.
  pub const fn header(&self) -> &Header<I, A> {
    match self {
      Self::AppendEntries(res) => res.header(),
      Self::Vote(res) => res.header(),
      Self::InstallSnapshot(res) => res.header(),
      Self::TimeoutNow(res) => res.header(),
      Self::Heartbeat(res) => res.header(),
      Self::Error(res) => res.header(),
    }
  }
}

/// Errors returned by the [`RpcHandle`].
#[derive(Debug, thiserror::Error)]
pub enum RpcHandleError {
  /// Returned when the command is cancelled
  #[error("{0}")]
  Canceled(#[from] oneshot::Canceled),
}

/// A future for getting the corresponding response from the Raft.
#[pin_project::pin_project]
#[repr(transparent)]
pub struct RpcHandle<I, A> {
  #[pin]
  rx: oneshot::Receiver<Response<I, A>>,
}

impl<I: Id, A: Address> RpcHandle<I, A> {
  fn new(rx: oneshot::Receiver<Response<I, A>>) -> Self {
    Self { rx }
  }
}

impl<I: Id, A: Address> Future for RpcHandle<I, A> {
  type Output = Result<Response<I, A>, RpcHandleError>;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    // Using Pin::as_mut to get a Pin<&mut Receiver>.
    let this = self.project();

    // Now, poll the receiver directly
    this.rx.poll(cx).map(|res| match res {
      Ok(res) => Ok(res),
      Err(e) => Err(From::from(e)),
    })
  }
}

pub enum RpcKind<I, A, D, R> {
  AppendEntries(AppendEntriesRequest<I, A, D>),
  Vote(VoteRequest<I, A>),
  InstallSnapshot(InstallSnapshotRequest<I, A>),
  TimeoutNow(TimeoutNowRequest<I, A>),
  Heartbeat(HeartbeatRequest<I, A>),
}

/// The struct is used to interact with the Raft.
pub struct Rpc<I: Id, A: Address, D: Data> {
  req: Request<I, A, D>,
  tx: oneshot::Sender<Response<I, A>>,
}

impl<I: Id, A: Address, D: Data> Rpc<I, A, D> {
  /// Create a new command from the given request.
  pub fn new(req: Request<I, A, D>) -> (Self, RpcHandle<I, A>) {
    let (tx, rx) = oneshot::channel();
    (Self { req, tx }, RpcHandle::new(rx))
  }

  /// Returns the header of the request.
  pub fn header(&self) -> &Header<I, A> {
    self.req.header()
  }

  /// Respond to the request, if the remote half is closed
  /// then the response will be returned back as an error.
  pub fn respond(self, resp: Response<I, A>) -> Result<(), Response<I, A>> {
    self.tx.send(resp)
  }

  pub(crate) fn into_components(self) -> (oneshot::Sender<Response<I, A>>, Request<I, A, D>) {
    (self.tx, self.req)
  }
}

/// Command stream is a consumer for Raft node to consume requests
/// from remote nodes
#[pin_project::pin_project]
#[derive(Debug)]
pub struct RpcConsumer<I: Id, A: Address, D: Data> {
  #[pin]
  rx: async_channel::Receiver<Rpc<I, A, D>>,
}

impl<I: Id, A: Address, D: Data> Clone for RpcConsumer<I, A, D> {
  fn clone(&self) -> Self {
    Self {
      rx: self.rx.clone(),
    }
  }
}

impl<I: Id, A: Address, D: Data> Stream for RpcConsumer<I, A, D> {
  type Item = <async_channel::Receiver<Rpc<I, A, D>> as Stream>::Item;

  fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    <async_channel::Receiver<Rpc<I, A, D>> as Stream>::poll_next(self.project().rx, cx)
  }
}

impl<I: Id, A: Address, D: Data> RpcConsumer<I, A, D> {
  /// Receives a [`Rpc`] from the consumer.
  pub async fn recv(&self) -> Result<Rpc<I, A, D>, RecvError> {
    self.rx.recv().await
  }

  /// Attempts to receive a [`Rpc`] from the consumer.
  ///
  /// If the consumer is empty, or empty and closed, this method returns an error
  pub fn try_recv(&self) -> Result<Rpc<I, A, D>, TryRecvError> {
    self.rx.try_recv()
  }
}

/// A producer for [`Rpc`]s
pub struct RpcProducer<I: Id, A: Address, D: Data> {
  tx: async_channel::Sender<Rpc<I, A, D>>,
}

impl<I: Id, A: Address, D: Data> Clone for RpcProducer<I, A, D> {
  fn clone(&self) -> Self {
    Self {
      tx: self.tx.clone(),
    }
  }
}

impl<I: Id, A: Address, D: Data> RpcProducer<I, A, D> {
  /// Produce a command for processing
  pub async fn send(
    &self,
    req: Rpc<I, A, D>,
  ) -> Result<(), async_channel::SendError<Rpc<I, A, D>>> {
    self.tx.send(req).await
  }
}

/// Returns unbounded command producer and command consumer.
pub fn rpc<I: Id, A: Address, D: Data>() -> (RpcProducer<I, A, D>, RpcConsumer<I, A, D>) {
  let (tx, rx) = async_channel::unbounded();
  (RpcProducer { tx }, RpcConsumer { rx })
}
