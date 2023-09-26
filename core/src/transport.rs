/// Provides an interface for network transports
/// to allow Raft to communicate with other nodes.
#[async_trait::async_trait]
pub trait Transport {
  /// Errors returned by the transport.
  type Error: std::error::Error;
}
