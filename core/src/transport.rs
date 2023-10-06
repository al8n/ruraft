use agnostic::Runtime;
use futures::AsyncRead;

mod command;
pub use command::*;

pub use nodecraft::{resolver::NodeAddressResolver, NodeAddress, NodeId, Transformable};

/// Used to encode [`Request`] and [`Response`] to bytes for transmission.
pub trait Encoder: Send + Sync + 'static {
  /// The error type returned by the encoder.
  type Error: std::error::Error
    + From<<Self::NodeId as Transformable>::Error>
    + From<<Self::NodeAddress as Transformable>::Error>
    + Send
    + Sync
    + 'static;
  /// The id type used to identify nodes.
  type NodeId: NodeId;
  /// The address type of node.
  type NodeAddress: NodeAddress;
  /// The encoded result for sending
  type Bytes: AsRef<[u8]>;

  /// Encodes [`Request`] to [`Encoder::Bytes`] for transmission
  fn encode_request(
    req: &Request<Self::NodeId, Self::NodeAddress>,
  ) -> Result<Self::Bytes, Self::Error>;

  /// Encodes [`Response`] to [`Encoder::Bytes`] for transmission
  fn encode_response(
    resp: &Response<Self::NodeId, Self::NodeAddress>,
  ) -> Result<Self::Bytes, Self::Error>;
}

/// Used to decode [`Request`] and [`Response`] from a reader.
#[async_trait::async_trait]
pub trait Decoder: Send + Sync + 'static {
  /// The error type returned by the encoder.
  type Error: std::error::Error
    + From<<Self::NodeId as Transformable>::Error>
    + From<<Self::NodeAddress as Transformable>::Error>
    + Send
    + Sync
    + 'static;
  /// The id type used to identify nodes.
  type NodeId: NodeId;
  /// The address type of node.
  type NodeAddress: NodeAddress;

  /// Decodes [`Request`] from a reader.
  async fn decode_request(
    reader: impl AsyncRead + Unpin,
  ) -> Result<Request<Self::NodeId, Self::NodeAddress>, Self::Error>;

  /// Decodes [`Response`] from a reader.
  async fn decode_response(
    reader: impl AsyncRead + Unpin,
  ) -> Result<Response<Self::NodeId, Self::NodeAddress>, Self::Error>;
}

/// Used for pipelining [`AppendEntriesRequest`]s. It is used
/// to increase the replication throughput by masking latency and better
/// utilizing bandwidth.
#[async_trait::async_trait]
pub trait AppendPipeline {
  /// The error type returned by the resolver.
  type Error: std::error::Error + Send + Sync + 'static;
  /// The runtime used by the transport.
  type Runtime: Runtime;
  /// The id type used to identify nodes.
  type NodeId: NodeId;
  /// The address type of node.
  type NodeAddress: NodeAddress;

  /// The append entries response yield by the pipeline.
  type Item: AppendFuture<NodeId = Self::NodeId, NodeAddress = Self::NodeAddress>;

  /// Returns a stream that can be used to consume
  /// response futures when they are ready.
  // TODO(al8n): change the return type to `impl Stream<Item = Self::Item>
  // when `RPITIT` is stable
  fn consumer(&self) -> async_channel::Receiver<Self::Item>;

  /// Sends the append entries requrest to the target node.
  async fn append_entries(
    &self,
    req: AppendEntriesRequest<Self::NodeId, Self::NodeAddress>,
  ) -> Result<AppendEntriesResponse<Self::NodeId, Self::NodeAddress>, Self::Error>;

  /// Closes the pipeline and cancels all inflight requests
  async fn close(&self) -> Result<(), Self::Error>;
}

/// Used to return information about a pipelined [`AppendEntriesRequest`].
pub trait AppendFuture:
  std::future::Future<
    Output = std::io::Result<AppendEntriesResponse<Self::NodeId, Self::NodeAddress>>,
  > + Send
  + Sync
  + 'static
{
  /// The id type used to identify nodes.
  type NodeId: NodeId;
  /// The address type of node.
  type NodeAddress: NodeAddress;

  /// Returns the time that the append request was started.
  /// It is always OK to call this method.
  fn start(&self) -> std::time::Instant;
}

/// Communicating with other nodes through the network.
#[async_trait::async_trait]
pub trait Transport: Send + Sync + 'static {
  /// Errors returned by the transport.
  type Error: std::error::Error
    + From<<Self::Pipeline as AppendPipeline>::Error>
    + From<<Self::Resolver as NodeAddressResolver>::Error>
    + From<<Self::Encoder as Encoder>::Error>
    + From<<Self::Decoder as Decoder>::Error>
    + Send
    + Sync
    + 'static;

  /// The runtime used by the transport.
  type Runtime: Runtime;

  /// The configuration used to construct the transport.
  type Options: Send + Sync + 'static;

  /// The id type used to identify nodes.
  type NodeId: NodeId;

  /// The pipeline used to increase the replication throughput by masking latency and better
  /// utilizing bandwidth.
  type Pipeline: AppendPipeline<
    Runtime = Self::Runtime,
    NodeId = Self::NodeId,
    NodeAddress = <Self::Resolver as NodeAddressResolver>::NodeAddress,
  >;

  /// The node address resolver used to resolve a node address to a [`SocketAddr`].
  ///
  /// e.g., you can implement a DNS resolver, then the raft node can accept a domain like `www.foo.com`
  /// as the node address.
  type Resolver: NodeAddressResolver<Runtime = Self::Runtime>;

  /// The encoder used to encode [`Request`] or [`Response`] for data transmission.
  type Encoder: Encoder;

  /// The decoder used to decode [`Request`] or [`Response`] from data transmission.
  type Decoder: Decoder;

  /// Returns a stream that can be used to
  /// consume and respond to RPC requests.
  fn consumer(
    &self,
  ) -> RequestConsumer<Self::NodeId, <Self::Resolver as NodeAddressResolver>::NodeAddress>;

  /// Used to return our local addr to distinguish from our peers.
  fn local_addr(&self) -> &<Self::Resolver as NodeAddressResolver>::NodeAddress;

  /// Used to return our local id to distinguish from our peers.
  fn local_id(&self) -> &Self::NodeId;

  /// Returns the node address resolver for the transport
  fn resolver(&self) -> &Self::Resolver;

  /// Returns a transport
  async fn new(resolver: Self::Resolver, opts: Self::Options) -> Result<Self, Self::Error>
  where
    Self: Sized;

  /// Returns a [`AppendPipeline`] that can be used to pipeline
  /// [`AppendEntriesRequest`]s.
  async fn append_entries_pipeline(
    &self,
    id: Self::NodeId,
    target: <Self::Resolver as NodeAddressResolver>::NodeAddress,
  ) -> Result<Self::Pipeline, Self::Error>;

  /// Sends the append entries requrest to the target node.
  async fn append_entries(
    &self,
    req: AppendEntriesRequest<Self::NodeId, <Self::Resolver as NodeAddressResolver>::NodeAddress>,
  ) -> Result<
    AppendEntriesResponse<Self::NodeId, <Self::Resolver as NodeAddressResolver>::NodeAddress>,
    Self::Error,
  >;

  /// Sends the vote request to the target node.
  async fn vote(
    &self,
    req: VoteRequest<Self::NodeId, <Self::Resolver as NodeAddressResolver>::NodeAddress>,
  ) -> Result<
    VoteResponse<Self::NodeId, <Self::Resolver as NodeAddressResolver>::NodeAddress>,
    Self::Error,
  >;

  /// Used to push a snapshot down to a follower.
  async fn install_snapshot(
    &self,
    req: InstallSnapshotRequest<Self::NodeId, <Self::Resolver as NodeAddressResolver>::NodeAddress>,
    source: impl AsyncRead + Send,
  ) -> Result<
    InstallSnapshotResponse<Self::NodeId, <Self::Resolver as NodeAddressResolver>::NodeAddress>,
    Self::Error,
  >;

  /// Used to start a leadership transfer to the target node.
  async fn timeout_now(
    &self,
    req: TimeoutNowRequest<Self::NodeId, <Self::Resolver as NodeAddressResolver>::NodeAddress>,
  ) -> Result<
    TimeoutNowResponse<Self::NodeId, <Self::Resolver as NodeAddressResolver>::NodeAddress>,
    Self::Error,
  >;

  /// Used to send a heartbeat to the target node.
  async fn heartbeat(
    &self,
    req: HeartbeatRequest<Self::NodeId, <Self::Resolver as NodeAddressResolver>::NodeAddress>,
  ) -> Result<
    HeartbeatResponse<Self::NodeId, <Self::Resolver as NodeAddressResolver>::NodeAddress>,
    Self::Error,
  >;

  /// Shutdown the transport.
  async fn shutdown(&self) -> Result<(), Self::Error>;
}

#[cfg(feature = "test")]
pub(super) mod tests {
  // pub use super::net::tests::*;
}
