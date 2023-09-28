use std::net::SocketAddr;

use agnostic::Runtime;

mod command;
pub use command::*;

use crate::membership::ServerId;

/// Provides an interface for network transports
/// to allow Raft to communicate with other nodes.
#[async_trait::async_trait]
pub trait Transport: Send + Sync + 'static {
  /// Errors returned by the transport.
  type Error: std::error::Error + Send + Sync + 'static;
  /// The runtime used by the transport.
  type Runtime: Runtime;

  /// Returns a stream that can be used to
  /// consume and respond to RPC requests.
  fn consumer(&self) -> CommandConsumer<Self::Error>;

  /// Used to return our local address to distinguish from our peers.
  fn local_addr(&self) -> SocketAddr;

  /// Sends the append entries requrest to the target node.
  async fn append_entries(
    &self,
    id: &ServerId,
    target: SocketAddr,
    req: AppendEntriesRequest,
  ) -> Result<(), Self::Error>;

  /// Sends the vote request to the target node.
  async fn vote(
    &self,
    id: &ServerId,
    target: SocketAddr,
    req: VoteRequest,
  ) -> Result<(), Self::Error>;

  /// Used to push a snapshot down to a follower.
  async fn install_snapshot(
    &self,
    id: &ServerId,
    target: SocketAddr,
    req: InstallSnapshotRequest,
  ) -> Result<(), Self::Error>;

  /// Used to start a leadership transfer to the target node.
  async fn timeout_now(
    &self,
    id: &ServerId,
    target: SocketAddr,
    req: TimeoutNowRequest,
  ) -> Result<(), Self::Error>;
}
