use std::{
  io,
  pin::Pin,
  task::{Context, Poll},
};

use futures::{channel::oneshot, Stream};
use serde::{Deserialize, Serialize};

use crate::{
  membership::Membership,
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
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Header<Id, Address> {
  /// The id of the node sending the RPC Request or Response
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Get the server id of the request or response"),
    ),
    setter(attrs(doc = "Set the server id of the request or response"),)
  )]
  id: Id,
  /// The addr of the node sending the RPC Request or Response
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Get the target address of the request or response"),
    ),
    setter(attrs(doc = "Set the target address of the request or response"),)
  )]
  addr: Address,
}

impl<Id, Address> Header<Id, Address> {
  /// Create a new [`Header`] with the given `id` and `addr`.
  #[inline]
  pub const fn new(id: Id, addr: Address) -> Self {
    Self { id, addr }
  }
}

/// The command used to append entries to the
/// replicated log.
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AppendEntriesRequest<Id, Address> {
  /// The header of the request
  #[viewit(getter(const))]
  header: Header<Id, Address>,

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
#[viewit::viewit(setters(prefix = "with"))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]

pub struct AppendEntriesResponse<Id, Address> {
  /// The header of the response
  #[viewit(getter(const))]
  header: Header<Id, Address>,

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

impl<Id, Address> AppendEntriesResponse<Id, Address> {
  pub const fn new(id: Id, addr: Address) -> Self {
    Self {
      header: Header::new(id, addr),
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
pub struct VoteRequest<Id, Address> {
  /// The header of the request
  #[viewit(getter(const))]
  header: Header<Id, Address>,

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
pub struct VoteResponse<Id, Address> {
  /// The header of the response
  #[viewit(getter(const))]
  header: Header<Id, Address>,

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
pub struct InstallSnapshotRequest<Id, Address> {
  /// The header of the request
  #[viewit(getter(const))]
  header: Header<Id, Address>,

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
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct InstallSnapshotResponse<Id, Address> {
  /// The header of the response
  #[viewit(getter(const))]
  header: Header<Id, Address>,

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
pub struct TimeoutNowRequest<Id, Address> {
  /// The header of the request
  #[viewit(getter(const))]
  header: Header<Id, Address>,
}

/// The response to [`TimeoutNowRequest`].
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TimeoutNowResponse<Id, Address> {
  /// The header of the response
  #[viewit(getter(const))]
  header: Header<Id, Address>,
}

/// The heartbeat command.
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct HeartbeatRequest<Id, Address> {
  /// The header of the request
  #[viewit(getter(const))]
  header: Header<Id, Address>,

  /// Provide the current term and leader
  term: u64,
}

/// The response returned from an
/// [`HeartbeatRequest`].
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct HeartbeatResponse<Id, Address> {
  /// The header of the response
  #[viewit(getter(const))]
  header: Header<Id, Address>,
}

/// The response returned from an
/// [`HeartbeatRequest`].
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ErrorResponse<Id, Address> {
  /// The header of the response
  #[viewit(getter(const))]
  header: Header<Id, Address>,
  /// The error message
  error: String,
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

/// Request to be sent to the Raft node.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum RequestKind<Id, Address> {
  AppendEntries(AppendEntriesRequest<Id, Address>),
  Vote(VoteRequest<Id, Address>),
  InstallSnapshot(InstallSnapshotRequest<Id, Address>),
  TimeoutNow(TimeoutNowRequest<Id, Address>),
  Heartbeat(HeartbeatRequest<Id, Address>),
}

impl<Id, Address> From<AppendEntriesRequest<Id, Address>> for RequestKind<Id, Address> {
  fn from(req: AppendEntriesRequest<Id, Address>) -> Self {
    Self::AppendEntries(req)
  }
}

impl<Id, Address> From<VoteRequest<Id, Address>> for RequestKind<Id, Address> {
  fn from(req: VoteRequest<Id, Address>) -> Self {
    Self::Vote(req)
  }
}

impl<Id, Address> From<InstallSnapshotRequest<Id, Address>> for RequestKind<Id, Address> {
  fn from(req: InstallSnapshotRequest<Id, Address>) -> Self {
    Self::InstallSnapshot(req)
  }
}

impl<Id, Address> From<TimeoutNowRequest<Id, Address>> for RequestKind<Id, Address> {
  fn from(req: TimeoutNowRequest<Id, Address>) -> Self {
    Self::TimeoutNow(req)
  }
}

impl<Id, Address> From<HeartbeatRequest<Id, Address>> for RequestKind<Id, Address> {
  fn from(req: HeartbeatRequest<Id, Address>) -> Self {
    Self::Heartbeat(req)
  }
}

#[derive(Debug)]
pub struct Request<Id, Address> {
  pub(crate) protocol_version: ProtocolVersion,
  pub(crate) kind: RequestKind<Id, Address>,
  pub(crate) tx: oneshot::Sender<Response<Id, Address>>,
}

impl<Id, Address> Request<Id, Address> {
  // pub const fn append_entries(version: ProtocolVersion, req: AppendEntriesRequest) -> Self {
  //   Self {
  //     protocol_version: version,
  //     kind: RequestKind::AppendEntries(req),
  //   }
  // }

  // pub const fn vote(version: ProtocolVersion, req: VoteRequest) -> Self {
  //   Self {
  //     protocol_version: version,
  //     kind: RequestKind::Vote(req),
  //   }
  // }

  // pub const fn install_snapshot(
  //   version: ProtocolVersion,
  //   req: InstallSnapshotRequest,
  // ) -> Self {
  //   Self {
  //     protocol_version: version,
  //     kind: RequestKind::InstallSnapshot(req),
  //   }
  // }

  // pub const fn timeout_now(version: ProtocolVersion, req: TimeoutNowRequest) -> Self {
  //   Self {
  //     protocol_version: version,
  //     kind: RequestKind::TimeoutNow(req),
  //   }
  // }

  // pub const fn heartbeat(version: ProtocolVersion, req: HeartbeatRequest) -> Self {
  //   Self {
  //     protocol_version: version,
  //     kind: RequestKind::Heartbeat(req),
  //   }
  // }

  pub const fn header(&self) -> &Header<Id, Address> {
    match &self.kind {
      RequestKind::AppendEntries(req) => req.header(),
      RequestKind::Vote(req) => req.header(),
      RequestKind::InstallSnapshot(req) => req.header(),
      RequestKind::TimeoutNow(req) => req.header(),
      RequestKind::Heartbeat(req) => req.header(),
    }
  }

  /// Respond to the request, if the remote half is closed
  /// then the response will be returned back as an error.
  pub fn respond(self, resp: Response<Id, Address>) -> Result<(), Response<Id, Address>> {
    self.tx.send(resp)
  }

  pub(crate) fn into_components(
    self,
  ) -> (
    oneshot::Sender<Response<Id, Address>>,
    RequestKind<Id, Address>,
  ) {
    (self.tx, self.kind)
  }

  // pub(super) fn encode(&self) -> io::Result<Vec<u8>> {
  //   match self.protocol_version {
  //     ProtocolVersion::V1 => {
  //       const OFFSET: usize = ProtocolVersion::V1.header_offset();

  //       match &self.kind {
  //         RequestKind::AppendEntries(req) => encode!(v1::req { CommandKind::AppendEntries as u8 }),
  //         RequestKind::Vote(req) => encode!(v1::req {CommandKind::Vote as u8 }),
  //         RequestKind::InstallSnapshot(req) => {
  //           encode!(v1::req { CommandKind::InstallSnapshot as u8 })
  //         }
  //         RequestKind::TimeoutNow(req) => encode!(v1::req { CommandKind::TimeoutNow as u8 }),
  //         RequestKind::Heartbeat(req) => encode!(v1::req { CommandKind::Heartbeat as u8 }),
  //       }
  //     }
  //   }
  // }
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
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ResponseKind<Id, Address> {
  AppendEntries(AppendEntriesResponse<Id, Address>),
  Vote(VoteResponse<Id, Address>),
  InstallSnapshot(InstallSnapshotResponse<Id, Address>),
  TimeoutNow(TimeoutNowResponse<Id, Address>),
  Heartbeat(HeartbeatResponse<Id, Address>),
  Error(ErrorResponse<Id, Address>),
}

impl<Id, Address> From<AppendEntriesResponse<Id, Address>> for ResponseKind<Id, Address> {
  fn from(resp: AppendEntriesResponse<Id, Address>) -> Self {
    Self::AppendEntries(resp)
  }
}

impl<Id, Address> From<VoteResponse<Id, Address>> for ResponseKind<Id, Address> {
  fn from(resp: VoteResponse<Id, Address>) -> Self {
    Self::Vote(resp)
  }
}

impl<Id, Address> From<InstallSnapshotResponse<Id, Address>> for ResponseKind<Id, Address> {
  fn from(resp: InstallSnapshotResponse<Id, Address>) -> Self {
    Self::InstallSnapshot(resp)
  }
}

impl<Id, Address> From<TimeoutNowResponse<Id, Address>> for ResponseKind<Id, Address> {
  fn from(resp: TimeoutNowResponse<Id, Address>) -> Self {
    Self::TimeoutNow(resp)
  }
}

impl<Id, Address> From<HeartbeatResponse<Id, Address>> for ResponseKind<Id, Address> {
  fn from(resp: HeartbeatResponse<Id, Address>) -> Self {
    Self::Heartbeat(resp)
  }
}

impl<Id, Address> From<ErrorResponse<Id, Address>> for ResponseKind<Id, Address> {
  fn from(resp: ErrorResponse<Id, Address>) -> Self {
    Self::Error(resp)
  }
}

#[derive(Debug, Clone)]
pub struct Response<Id, Address> {
  protocol_version: ProtocolVersion,
  kind: ResponseKind<Id, Address>,
}

impl<Id, Address> Response<Id, Address> {
  pub const fn append_entries(
    version: ProtocolVersion,
    resp: AppendEntriesResponse<Id, Address>,
  ) -> Self {
    Self {
      protocol_version: version,
      kind: ResponseKind::AppendEntries(resp),
    }
  }

  pub const fn vote(version: ProtocolVersion, resp: VoteResponse<Id, Address>) -> Self {
    Self {
      protocol_version: version,
      kind: ResponseKind::Vote(resp),
    }
  }

  pub const fn install_snapshot(
    version: ProtocolVersion,
    resp: InstallSnapshotResponse<Id, Address>,
  ) -> Self {
    Self {
      protocol_version: version,
      kind: ResponseKind::InstallSnapshot(resp),
    }
  }

  pub const fn timeout_now(
    version: ProtocolVersion,
    resp: TimeoutNowResponse<Id, Address>,
  ) -> Self {
    Self {
      protocol_version: version,
      kind: ResponseKind::TimeoutNow(resp),
    }
  }

  pub const fn heartbeat(version: ProtocolVersion, header: Header<Id, Address>) -> Self {
    Self {
      protocol_version: version,
      kind: ResponseKind::Heartbeat(HeartbeatResponse { header }),
    }
  }

  pub const fn error(version: ProtocolVersion, header: Header<Id, Address>, error: String) -> Self {
    Self {
      protocol_version: version,
      kind: ResponseKind::Error(ErrorResponse { header, error }),
    }
  }

  /// Returns the header of the response.
  pub const fn header(&self) -> &Header<Id, Address> {
    match &self.kind {
      ResponseKind::AppendEntries(res) => res.header(),
      ResponseKind::Vote(res) => res.header(),
      ResponseKind::InstallSnapshot(res) => res.header(),
      ResponseKind::TimeoutNow(res) => res.header(),
      ResponseKind::Heartbeat(res) => res.header(),
      ResponseKind::Error(res) => res.header(),
    }
  }

  // pub fn encode(&self) -> io::Result<Vec<u8>> {
  //   match self.protocol_version {
  //     ProtocolVersion::V1 => {
  //       const OFFSET: usize =
  //         mem::size_of::<CommandKind>() + mem::size_of::<ProtocolVersion>() + mem::size_of::<u32>();

  //       match &self.kind {
  //         ResponseKind::AppendEntries(res) => {
  //           encode!(v1::res { CommandResponseKind::AppendEntries as u8 })
  //         }
  //         ResponseKind::Vote(res) => encode!(v1::res { CommandResponseKind::Vote as u8 }),
  //         ResponseKind::InstallSnapshot(res) => {
  //           encode!(v1::res { CommandResponseKind::InstallSnapshot as u8 })
  //         }
  //         ResponseKind::TimeoutNow(res) => {
  //           encode!(v1::res { CommandResponseKind::TimeoutNow as u8 })
  //         }
  //         ResponseKind::Heartbeat(res) => encode!(v1::res { CommandResponseKind::Heartbeat as u8 }),
  //         ResponseKind::Error(res) => encode!(v1::res { CommandResponseKind::Err as u8 }),
  //       }
  //     }
  //   }
  // }
}

// /// Errors returned by the [`CommandHandle`].
// #[derive(Debug, thiserror::Error)]
// pub enum CommandHandleError {
//   /// Returned when the command is cancelled
//   #[error("{0}")]
//   Canceled(#[from] oneshot::Canceled),
// }

// /// A future for getting the corresponding response from the Raft.
// #[pin_project::pin_project]
// #[repr(transparent)]
// pub(super) struct CommandHandle<Id, Address> {
//   #[pin]
//   rx: oneshot::Receiver<Response<Id, Address>>,
// }

// impl<Id, Address> CommandHandle<Id, Address> {
//   pub(crate) fn new(rx: oneshot::Receiver<Response<Id, Address>>) -> Self {
//     Self { rx }
//   }
// }

// impl<Id, Address> Future for CommandHandle<Id, Address> {
//   type Output = Result<Response<Id, Address>, CommandHandleError>;

//   fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
//     // Using Pin::as_mut to get a Pin<&mut Receiver>.
//     let this = self.project();

//     // Now, poll the receiver directly
//     this.rx.poll(cx).map(|res| match res {
//       Ok(res) => Ok(res),
//       Err(e) => Err(From::from(e)),
//     })
//   }
// }

// /// The struct is used to interact with the Raft.
// pub struct Command {
//   req: Request,
//   tx: oneshot::Sender<Response>,
// }

// impl Command {
//   /// Create a new command from the given request.
//   pub(super) fn new(req: Request) -> (Self, CommandHandle) {
//     let (tx, rx) = oneshot::channel();
//     (Self { req, tx }, CommandHandle::new(rx))
//   }

//   /// Returns the header of the request.
//   pub fn header(&self) -> &Header {
//     self.req.header()
//   }

//   /// Respond to the request, if the remote half is closed
//   /// then the response will be returned back as an error.
//   pub fn respond(self, resp: Response) -> Result<(), Response> {
//     self.tx.send(resp)
//   }

//   pub(crate) fn into_components(self) -> (oneshot::Sender<Response>, Request) {
//     (self.tx, self.req)
//   }
// }

/// Command stream is a consumer for Raft node to consume requests
/// from remote nodes
#[pin_project::pin_project]
#[derive(Debug)]
pub struct RequestConsumer<Id, Address> {
  #[pin]
  rx: async_channel::Receiver<Request<Id, Address>>,
}

impl<Id, Address> Clone for RequestConsumer<Id, Address> {
  fn clone(&self) -> Self {
    Self {
      rx: self.rx.clone(),
    }
  }
}

impl<Id, Address> Stream for RequestConsumer<Id, Address> {
  type Item = <async_channel::Receiver<Request<Id, Address>> as Stream>::Item;

  fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    <async_channel::Receiver<Request<Id, Address>> as Stream>::poll_next(self.project().rx, cx)
  }
}

/// A producer for [`Request`]s
pub struct RequestProducer<Id, Address> {
  tx: async_channel::Sender<Request<Id, Address>>,
}

impl<Id, Address> Clone for RequestProducer<Id, Address> {
  fn clone(&self) -> Self {
    Self {
      tx: self.tx.clone(),
    }
  }
}

impl<Id, Address> RequestProducer<Id, Address> {
  /// Produce a command for processing
  pub async fn send(
    &self,
    command: Request<Id, Address>,
  ) -> Result<(), async_channel::SendError<Request<Id, Address>>> {
    self.tx.send(command).await
  }
}

/// Returns unbounded command producer and command consumer.
pub fn command<Id, Address>() -> (RequestProducer<Id, Address>, RequestConsumer<Id, Address>) {
  let (tx, rx) = async_channel::unbounded();
  (RequestProducer { tx }, RequestConsumer { rx })
}
