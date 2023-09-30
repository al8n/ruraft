use std::net::SocketAddr;

use agnostic::Runtime;

mod command;
pub use command::*;
mod net;
use futures::AsyncRead;
pub use net::*;

use crate::membership::ServerId;

/// Provides an interface for network transports
/// to allow Raft to communicate with other nodes.
#[async_trait::async_trait]
pub trait Transport: Send + Sync + 'static {
  /// Errors returned by the transport.
  type Error: std::error::Error + Send + Sync + 'static;

  /// The runtime used by the transport.
  type Runtime: Runtime;

  /// The configuration used to construct the transport.
  type Options: Send + Sync + 'static;

  /// Returns a stream that can be used to
  /// consume and respond to RPC requests.
  fn consumer(&self) -> CommandConsumer;

  /// Used to return our local addr to distinguish from our peers.
  fn local_addr(&self) -> SocketAddr;

  /// Used to return our local id to distinguish from our peers.
  fn local_id(&self) -> &ServerId;

  /// Returns a transport
  async fn new(opts: Self::Options) -> Result<Self, Self::Error>
  where
    Self: Sized;

  /// Sends the append entries requrest to the target node.
  async fn append_entries(
    &self,
    req: AppendEntriesRequest,
  ) -> Result<AppendEntriesResponse, Self::Error>;

  /// Sends the vote request to the target node.
  async fn vote(&self, req: VoteRequest) -> Result<VoteResponse, Self::Error>;

  /// Used to push a snapshot down to a follower.
  async fn install_snapshot(
    &self,
    req: InstallSnapshotRequest,
    source: impl AsyncRead + Send,
  ) -> Result<InstallSnapshotResponse, Self::Error>;

  /// Used to start a leadership transfer to the target node.
  async fn timeout_now(&self, req: TimeoutNowRequest) -> Result<TimeoutNowResponse, Self::Error>;

  /// Used to send a heartbeat to the target node.
  async fn heartbeat(&self, req: HeartbeatRequest) -> Result<HeartbeatResponse, Self::Error>;

  /// Shutdown the transport.
  async fn shutdown(&self) -> Result<(), Self::Error>;
}

// /// Used for pipelining [`AppendEntriesRequest`]s. It is used
// /// to increase the replication throughput by masking latency and better
// /// utilizing bandwidth.
// pub trait AppendPipeline {

// }

// /// Used to return information about a pipelined AppendEntries request.
// pub trait AppendFuture: Future<Output = io::Result<AppendEntriesResponse>> {
//   /// Returns the time that the append request was started.
// 	/// It is always OK to call this method.
//   fn start(&self) -> Instant;

//   /// Holds the parameters of the `append_entries` call.
// 	/// It is always OK to call this method.
//   fn request(&self) -> &AppendEntriesRequest;
// }

#[cfg(feature = "test")]
pub(super) mod tests {
  pub use super::net::tests::*;
}
