use std::net::SocketAddr;

use agnostic::Runtime;
use futures::AsyncRead;

mod command;
pub use command::*;

mod error;
pub use error::*;

pub use nodecraft::{resolver::AddressResolver, Address, Id, Transformable};

/// Represents errors that can arise during the wire encoding or decoding processes.
///
/// `WireError` provides a standard interface to wrap and differentiate between
/// errors related to node identifiers (`Id`), node addresses (`Address`), and custom error messages.
pub trait WireError: std::error::Error + Send + Sync + 'static {
  /// Represents the unique identifier for nodes.
  type Id: Id;

  /// Represents the network address associated with nodes.
  type Address: Address;

  /// Constructs an error instance from an `Id` transformation error.
  ///
  /// # Parameters
  /// * `err` - The error arising from the transformation of a node identifier.
  fn id(err: <Self::Id as Transformable>::Error) -> Self;

  /// Constructs an error instance from an `Address` transformation error.
  ///
  /// # Parameters
  /// * `err` - The error arising from the transformation of a node address.
  fn address(err: <Self::Address as Transformable>::Error) -> Self;

  /// Constructs a custom error instance from a provided message.
  ///
  /// # Parameters
  /// * `msg` - The custom error message to be associated with the error instance.
  fn custom<T>(msg: T) -> Self
  where
    T: core::fmt::Display;
}

/// Represents the ability to convert between high-level [`Request`] and [`Response`] structures
/// and their byte-array representations suitable for network transmission.
///
/// The trait ensures that implementations provide consistent encoding and decoding functionality,
/// accompanied by appropriate error handling.
#[async_trait::async_trait]
pub trait Wire: Send + Sync + 'static {
  /// Specifies the error type for encoding and decoding operations.
  ///
  /// This associated type provides detailed error categorization,
  /// including transformations of the `Id` and `Address` types.
  type Error: WireError<Id = Self::Id, Address = Self::Address>;

  /// Represents the unique identifier associated with nodes.
  type Id: Id;

  /// Denotes the network address format or specification used for nodes.
  type Address: Address;

  /// Represents the byte-array format produced after encoding,
  /// which is then suitable for transmission over the network.
  type Bytes: AsRef<[u8]> + Send + Sync + 'static;

  /// Encodes a [`Request`] into its byte-array representation.
  ///
  /// # Parameters
  /// * `req` - The `Request` instance to be encoded.
  ///
  /// # Returns
  /// * `Result` - Returns the encoded byte array or an error if the encoding process fails.
  fn encode_request(req: &Request<Self::Id, Self::Address>) -> Result<Self::Bytes, Self::Error>;

  /// Encodes a [`Response`] into its byte-array representation.
  ///
  /// # Parameters
  /// * `resp` - The `Response` instance to be encoded.
  ///
  /// # Returns
  /// * `Result` - Returns the encoded byte array or an error if the encoding process fails.
  fn encode_response(resp: &Response<Self::Id, Self::Address>) -> Result<Self::Bytes, Self::Error>;

  /// Decodes a [`Request`] instance from a provided asynchronous reader.
  ///
  /// # Parameters
  /// * `reader` - The asynchronous reader source containing the byte data of the `Request`.
  ///
  /// # Returns
  /// * `Result` - Returns the decoded `Request` or an error if the decoding process encounters issues.
  async fn decode_request(
    reader: impl AsyncRead + Unpin,
  ) -> Result<Request<Self::Id, Self::Address>, Self::Error>;

  /// Decodes a [`Response`] instance from a provided asynchronous reader.
  ///
  /// # Parameters
  /// * `reader` - The asynchronous reader source containing the byte data of the `Response`.
  ///
  /// # Returns
  /// * `Result` - Returns the decoded `Response` or an error if the decoding process encounters issues.
  async fn decode_response(
    reader: impl AsyncRead + Unpin,
  ) -> Result<Response<Self::Id, Self::Address>, Self::Error>;
}

/// Provides utilities to pipeline [`AppendEntriesRequest`]s, aiming to
/// enhance replication throughput by minimizing latency and maximizing bandwidth utilization.
#[async_trait::async_trait]
pub trait AppendPipeline {
  /// Specifies potential errors that can occur within the pipeline.
  type Error: std::error::Error + Send + Sync + 'static;

  /// The runtime environment or context in which the transport operations occur.
  type Runtime: Runtime;

  /// Unique identifier associated with nodes.
  type Id: Id;

  /// Network address representation of nodes.
  type Address: Address;

  /// Represents the pipeline's output or response to an appended entry.
  type Item: AppendFuture<Id = Self::Id, Address = Self::Address>;

  /// Retrieves a stream for consuming response futures once they are ready.
  // TODO(al8n): change the return type to `impl Stream<Item = Self::Item>
  // when `RPITIT` is stable
  fn consumer(&self) -> async_channel::Receiver<Self::Item>;

  /// Asynchronously appends entries to the target node and returns the associated response.
  async fn append_entries(
    &self,
    req: AppendEntriesRequest<Self::Id, Self::Address>,
  ) -> Result<AppendEntriesResponse<Self::Id, Self::Address>, Self::Error>;

  /// Gracefully closes the pipeline and terminates any in-flight requests.
  async fn close(&self) -> Result<(), Self::Error>;
}

/// Represents the anticipated response following an appended entry in the pipeline.
pub trait AppendFuture:
  std::future::Future<Output = std::io::Result<AppendEntriesResponse<Self::Id, Self::Address>>>
  + Send
  + Sync
  + 'static
{
  /// Unique identifier associated with nodes.
  type Id: Id;

  /// Network address representation of nodes.
  type Address: Address;

  /// Retrieves the timestamp indicating when the append request was initiated.
  fn start(&self) -> std::time::Instant;
}

/// Defines the capabilities and requirements for communication with other nodes across a network.
#[async_trait::async_trait]
pub trait Transport: Send + Sync + 'static {
  /// Errors that the transport can potentially return during operations.
  type Error: TransportError<Id = Self::Id, Resolver = Self::Resolver, Wire = Self::Wire>;

  /// Specifies the runtime environment for transport operations.
  type Runtime: Runtime;

  /// The configuration used to construct the transport.
  type Options: Send + Sync + 'static;

  /// Unique identifier for nodes.
  type Id: Id;

  // /// The pipeline used to increase the replication throughput by masking latency and better
  // /// utilizing bandwidth.
  // type Pipeline: AppendPipeline<
  //   Error = Self::Error,
  //   Runtime = Self::Runtime,
  //   Id = Self::Id,
  //   Address = <Self::Resolver as AddressResolver>::Address,
  // >;

  /// Resolves node addresses to concrete network addresses, like mapping a domain name to an IP.
  type Resolver: AddressResolver<Runtime = Self::Runtime>;

  /// Mechanism to encode and decode data for network transmission.
  type Wire: Wire<Id = Self::Id, Address = <Self::Resolver as AddressResolver>::Address>;

  /// Consumes and responds to incoming RPC requests.
  fn consumer(&self) -> RpcConsumer<Self::Id, <Self::Resolver as AddressResolver>::Address>;

  /// Provides the local network address, aiding in distinguishing this node from peers.
  fn local_addr(&self) -> &<Self::Resolver as AddressResolver>::Address;

  /// Provides the local unique identifier, helping in distinguishing this node from its peers.
  fn local_id(&self) -> &Self::Id;

  /// Provides the concrete network address for peers in the Raft cluster to communicate with.
  fn advertise_addr(&self) -> SocketAddr;

  /// Provides access to the node's address resolver.
  fn resolver(&self) -> &Self::Resolver;

  /// Returns a transport
  async fn new(resolver: Self::Resolver, opts: Self::Options) -> Result<Self, Self::Error>
  where
    Self: Sized;

  // /// Returns a [`AppendPipeline`] that can be used to pipeline
  // /// [`AppendEntriesRequest`]s.
  // async fn append_entries_pipeline(
  //   &self,
  //   id: Self::Id,
  //   target: <Self::Resolver as AddressResolver>::Address,
  // ) -> Result<Self::Pipeline, Self::Error>;

  /// Sends the append entries requrest to the target node.
  async fn append_entries(
    &self,
    req: AppendEntriesRequest<Self::Id, <Self::Resolver as AddressResolver>::Address>,
  ) -> Result<
    AppendEntriesResponse<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    Self::Error,
  >;

  /// Sends the vote request to the target node.
  async fn vote(
    &self,
    req: VoteRequest<Self::Id, <Self::Resolver as AddressResolver>::Address>,
  ) -> Result<VoteResponse<Self::Id, <Self::Resolver as AddressResolver>::Address>, Self::Error>;

  /// Used to push a snapshot down to a follower.
  async fn install_snapshot(
    &self,
    req: InstallSnapshotRequest<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    source: impl AsyncRead + Send,
  ) -> Result<
    InstallSnapshotResponse<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    Self::Error,
  >;

  /// Used to start a leadership transfer to the target node.
  async fn timeout_now(
    &self,
    req: TimeoutNowRequest<Self::Id, <Self::Resolver as AddressResolver>::Address>,
  ) -> Result<TimeoutNowResponse<Self::Id, <Self::Resolver as AddressResolver>::Address>, Self::Error>;

  /// Used to send a heartbeat to the target node.
  async fn heartbeat(
    &self,
    req: HeartbeatRequest<Self::Id, <Self::Resolver as AddressResolver>::Address>,
  ) -> Result<HeartbeatResponse<Self::Id, <Self::Resolver as AddressResolver>::Address>, Self::Error>;

  /// Shutdown the transport.
  async fn shutdown(&self) -> Result<(), Self::Error>;
}

#[cfg(feature = "test")]
pub(super) mod tests {
  // pub use super::net::tests::*;
}
