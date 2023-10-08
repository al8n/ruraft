use std::{
  future::Future,
  pin::Pin,
  task::{Context, Poll},
};

use futures::{channel::oneshot, Stream};

use crate::{
  membership::Membership,
  options::{ProtocolVersion, SnapshotVersion},
  storage::Log,
};

use super::{Address, Id};

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
      attrs(doc = "Get the server id of the request or response"),
    ),
    setter(attrs(doc = "Set the server id of the request or response"),)
  )]
  id: I,
  /// The addr of the node sending the RPC Request or Response
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Get the target address of the request or response"),
    ),
    setter(attrs(doc = "Set the target address of the request or response"),)
  )]
  addr: A,
}

impl<I: Id, A: Address> Header<I, A> {
  /// Create a new [`Header`] with the given `id` and `addr`.
  #[inline]
  pub const fn new(version: ProtocolVersion, id: I, addr: A) -> Self {
    Self {
      protocol_version: version,
      id,
      addr,
    }
  }
}

/// The command used to append entries to the
/// replicated log.
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AppendEntriesRequest<I: Id, A: Address> {
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
  entries: Vec<Log<I, A>>,

  /// Commit index on the leader
  leader_commit: u64,
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
  pub const fn new(version: ProtocolVersion, id: I, addr: A) -> Self {
    Self {
      header: Header::new(version, id, addr),
      term: 0,
      last_log: 0,
      success: false,
      no_retry_backoff: false,
    }
  }
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
}

impl<I: Id, A: Address> HeartbeatResponse<I, A> {
  /// Create a new HeartbeatResponse
  pub const fn new(header: Header<I, A>) -> Self {
    Self {
      header,
    }
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
    Self {
      header,
      error,
    }
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
pub enum Request<I: Id, A: Address> {
  AppendEntries(AppendEntriesRequest<I, A>),
  Vote(VoteRequest<I, A>),
  InstallSnapshot(InstallSnapshotRequest<I, A>),
  TimeoutNow(TimeoutNowRequest<I, A>),
  Heartbeat(HeartbeatRequest<I, A>),
}

impl<I: Id, A: Address> Request<I, A> {
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

impl<I: Id, A: Address> From<AppendEntriesRequest<I, A>> for Request<I, A> {
  fn from(req: AppendEntriesRequest<I, A>) -> Self {
    Self::AppendEntries(req)
  }
}

impl<I: Id, A: Address> From<VoteRequest<I, A>> for Request<I, A> {
  fn from(req: VoteRequest<I, A>) -> Self {
    Self::Vote(req)
  }
}

impl<I: Id, A: Address> From<InstallSnapshotRequest<I, A>> for Request<I, A> {
  fn from(req: InstallSnapshotRequest<I, A>) -> Self {
    Self::InstallSnapshot(req)
  }
}

impl<I: Id, A: Address> From<TimeoutNowRequest<I, A>> for Request<I, A> {
  fn from(req: TimeoutNowRequest<I, A>) -> Self {
    Self::TimeoutNow(req)
  }
}

impl<I: Id, A: Address> From<HeartbeatRequest<I, A>> for Request<I, A> {
  fn from(req: HeartbeatRequest<I, A>) -> Self {
    Self::Heartbeat(req)
  }
}

// #[derive(Debug)]
// pub struct Request<I: Id, A: Address> {
//   pub(crate) kind: Request<I, A>,
// }

impl<I: Id, A: Address> Request<I, A> {
  pub const fn append_entries(req: AppendEntriesRequest<I, A>) -> Self {
    Self::AppendEntries(req)
  }

  pub const fn vote(req: VoteRequest<I, A>) -> Self {
    Self::Vote(req)
  }

  pub const fn install_snapshot(
    req: InstallSnapshotRequest<I, A>,
  ) -> Self {
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

  pub const fn install_snapshot(
    resp: InstallSnapshotResponse<I, A>,
  ) -> Self {
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

  // pub fn encode(&self) -> io::Result<Vec<u8>> {
  //   match self.protocol_version {
  //     ProtocolVersion::V1 => {
  //       const OFFSET: usize =
  //         mem::size_of::<CommandKind>() + mem::size_of::<ProtocolVersion>() + mem::size_of::<u32>();

  //       match &self.kind {
  //         Response::AppendEntries(res) => {
  //           encode!(v1::res { CommandResponse::AppendEntries as u8 })
  //         }
  //         Response::Vote(res) => encode!(v1::res { CommandResponse::Vote as u8 }),
  //         Response::InstallSnapshot(res) => {
  //           encode!(v1::res { CommandResponse::InstallSnapshot as u8 })
  //         }
  //         Response::TimeoutNow(res) => {
  //           encode!(v1::res { CommandResponse::TimeoutNow as u8 })
  //         }
  //         Response::Heartbeat(res) => encode!(v1::res { CommandResponse::Heartbeat as u8 }),
  //         Response::Error(res) => encode!(v1::res { CommandResponse::Err as u8 }),
  //       }
  //     }
  //   }
  // }
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

/// The struct is used to interact with the Raft.
pub struct Rpc<I: Id, A: Address> {
  req: Request<I, A>,
  tx: oneshot::Sender<Response<I, A>>,
}

impl<I: Id, A: Address> Rpc<I, A> {
  /// Create a new command from the given request.
  pub fn new(req: Request<I, A>) -> (Self, RpcHandle<I, A>) {
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

  pub(crate) fn into_components(self) -> (oneshot::Sender<Response<I, A>>, Request<I, A>) {
    (self.tx, self.req)
  }
}

/// Command stream is a consumer for Raft node to consume requests
/// from remote nodes
#[pin_project::pin_project]
#[derive(Debug)]
pub struct RpcConsumer<I: Id, A: Address> {
  #[pin]
  rx: async_channel::Receiver<Rpc<I, A>>,
}

impl<I: Id, A: Address> Clone for RpcConsumer<I, A> {
  fn clone(&self) -> Self {
    Self {
      rx: self.rx.clone(),
    }
  }
}

impl<I: Id, A: Address> Stream for RpcConsumer<I, A> {
  type Item = <async_channel::Receiver<Rpc<I, A>> as Stream>::Item;

  fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    <async_channel::Receiver<Rpc<I, A>> as Stream>::poll_next(self.project().rx, cx)
  }
}

/// A producer for [`Rpc`]s
pub struct RpcProducer<I: Id, A: Address> {
  tx: async_channel::Sender<Rpc<I, A>>,
}

impl<I: Id, A: Address> Clone for RpcProducer<I, A> {
  fn clone(&self) -> Self {
    Self {
      tx: self.tx.clone(),
    }
  }
}

impl<I: Id, A: Address> RpcProducer<I, A> {
  /// Produce a command for processing
  pub async fn send(&self, req: Rpc<I, A>) -> Result<(), async_channel::SendError<Rpc<I, A>>> {
    self.tx.send(req).await
  }
}

/// Returns unbounded command producer and command consumer.
pub fn rpc<I: Id, A: Address>() -> (RpcProducer<I, A>, RpcConsumer<I, A>) {
  let (tx, rx) = async_channel::unbounded();
  (RpcProducer { tx }, RpcConsumer { rx })
}
