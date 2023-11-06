use std::{future::Future, net::SocketAddr};

use agnostic::Runtime;
use futures::{AsyncRead, Stream};

mod rpc;
use nodecraft::CheapClone;
pub use rpc::*;

mod error;
pub use error::*;

pub use nodecraft::{resolver::AddressResolver, Address, Id, Transformable};

use crate::{options::ProtocolVersion, Data, Node};

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

  /// The log entry's type-specific data, which will be applied to a user [`FinateStateMachine`](crate::FinateStateMachine).
  type Data: Data;

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
  fn encode_request(
    req: &Request<Self::Id, Self::Address, Self::Data>,
  ) -> Result<Self::Bytes, Self::Error>;

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
  fn decode_request(
    reader: impl AsyncRead + Unpin,
  ) -> impl Future<Output = Result<Request<Self::Id, Self::Address, Self::Data>, Self::Error>> + Send;

  /// Decodes a [`Response`] instance from a provided asynchronous reader.
  ///
  /// # Parameters
  /// * `reader` - The asynchronous reader source containing the byte data of the `Response`.
  ///
  /// # Returns
  /// * `Result` - Returns the decoded `Response` or an error if the decoding process encounters issues.
  fn decode_response(
    reader: impl AsyncRead + Unpin,
  ) -> impl Future<Output = Result<Response<Self::Id, Self::Address>, Self::Error>> + Send;
}

/// Provides utilities to pipeline [`AppendEntriesRequest`]s, aiming to
/// enhance replication throughput by minimizing latency and maximizing bandwidth utilization.
pub trait AppendEntriesPipeline: Send + Sync + 'static {
  /// Specifies potential errors that can occur within the pipeline.
  type Error: std::error::Error + Send + Sync + 'static;

  /// Unique identifier associated with nodes.
  type Id: Id + Send + Sync + 'static;

  /// Network address representation of nodes.
  type Address: Address + Send + Sync + 'static;

  /// The log entry's type-specific data, which will be applied to a user [`FinateStateMachine`](crate::FinateStateMachine).
  type Data: Data;

  /// Represents the pipeline's output or response to an appended entry.
  type Response: AppendEntriesPipelineFuture<
    Id = Self::Id,
    Address = Self::Address,
    Pipeline = Self,
  >;

  /// Retrieves a stream for consuming response futures once they are ready.
  fn consumer(
    &self,
  ) -> impl Stream<Item = Result<PipelineAppendEntriesResponse<Self::Id, Self::Address>, Self::Error>>
       + Send
       + 'static;

  /// Asynchronously appends entries to the target node and returns the associated response.
  fn append_entries(
    &self,
    req: AppendEntriesRequest<Self::Id, Self::Address, Self::Data>,
  ) -> impl Future<Output = Result<Self::Response, Self::Error>> + Send;

  /// Gracefully closes the pipeline and terminates any in-flight requests.
  fn close(self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Represents the anticipated response following an appended entry in the pipeline.
pub trait AppendEntriesPipelineFuture:
  std::future::Future<
    Output = Result<
      PipelineAppendEntriesResponse<Self::Id, Self::Address>,
      <Self::Pipeline as AppendEntriesPipeline>::Error,
    >,
  > + Send
  + Sync
  + 'static
{
  type Pipeline: AppendEntriesPipeline<Id = Self::Id, Address = Self::Address, Response = Self>;

  /// Unique identifier associated with nodes.
  type Id: Id;

  /// Network address representation of nodes.
  type Address: Address;

  /// Retrieves the timestamp indicating when the append request was initiated.
  fn start(&self) -> std::time::Instant;
}

/// Defines the capabilities and requirements for communication with other nodes across a network.
pub trait Transport: Send + Sync + 'static {
  /// Errors that the transport can potentially return during operations.
  type Error: TransportError<Id = Self::Id, Resolver = Self::Resolver, Wire = Self::Wire>;

  /// Specifies the runtime environment for transport operations.
  type Runtime: Runtime;

  /// The configuration used to construct the transport.
  type Options: Send + Sync + 'static;

  /// Unique identifier for nodes.
  type Id: Id + CheapClone + Send + Sync + 'static;

  /// The log entry's type-specific data, which will be applied to a user [`FinateStateMachine`](crate::FinateStateMachine).
  type Data: Data;

  /// The pipeline used to increase the replication throughput by masking latency and better
  /// utilizing bandwidth.
  type Pipeline: AppendEntriesPipeline<
    Error = Self::Error,
    Id = Self::Id,
    Address = <Self::Resolver as AddressResolver>::Address,
    Data = Self::Data,
  >;

  /// Resolves node addresses to concrete network addresses, like mapping a domain name to an IP.
  type Resolver: AddressResolver<Runtime = Self::Runtime>;

  /// Mechanism to encode and decode data for network transmission.
  type Wire: Wire<
    Id = Self::Id,
    Address = <Self::Resolver as AddressResolver>::Address,
    Data = Self::Data,
  >;

  /// Consumes and responds to incoming RPC requests.
  fn consumer(
    &self,
  ) -> RpcConsumer<Self::Id, <Self::Resolver as AddressResolver>::Address, Self::Data>;

  /// Provides the local network address, aiding in distinguishing this node from peers.
  fn local_addr(&self) -> &<Self::Resolver as AddressResolver>::Address;

  /// Provides the local unique identifier, helping in distinguishing this node from its peers.
  fn local_id(&self) -> &Self::Id;

  /// Provides the protocol version used by the transport.
  fn version(&self) -> ProtocolVersion;

  /// Provides the header used for all RPC requests.
  fn header(&self) -> Header<Self::Id, <Self::Resolver as AddressResolver>::Address> {
    Header::new(
      self.version(),
      self.local_id().clone(),
      self.local_addr().clone(),
    )
  }

  /// Provides the concrete network address for peers in the Raft cluster to communicate with.
  fn advertise_addr(&self) -> SocketAddr;

  /// Provides access to the node's address resolver.
  fn resolver(&self) -> &Self::Resolver;

  /// Returns a transport
  fn new(
    resolver: Self::Resolver,
    opts: Self::Options,
  ) -> impl Future<Output = Result<Self, Self::Error>> + Send
  where
    Self: Sized;

  /// Returns a [`AppendEntriesPipeline`] that can be used to pipeline
  /// [`AppendEntriesRequest`]s.
  fn append_entries_pipeline(
    &self,
    target: Node<Self::Id, <Self::Resolver as AddressResolver>::Address>,
  ) -> impl Future<Output = Result<Self::Pipeline, Self::Error>> + Send;

  /// Sends the append entries requrest to the target node.
  fn append_entries(
    &self,
    target: &Node<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    req: AppendEntriesRequest<Self::Id, <Self::Resolver as AddressResolver>::Address, Self::Data>,
  ) -> impl Future<
    Output = Result<
      AppendEntriesResponse<Self::Id, <Self::Resolver as AddressResolver>::Address>,
      Self::Error,
    >,
  > + Send;

  /// Sends the vote request to the target node.
  fn vote(
    &self,
    target: &Node<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    req: VoteRequest<Self::Id, <Self::Resolver as AddressResolver>::Address>,
  ) -> impl Future<
    Output = Result<
      VoteResponse<Self::Id, <Self::Resolver as AddressResolver>::Address>,
      Self::Error,
    >,
  > + Send;

  /// Used to push a snapshot down to a follower.
  fn install_snapshot(
    &self,
    target: &Node<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    req: InstallSnapshotRequest<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    source: impl AsyncRead + Send,
  ) -> impl Future<
    Output = Result<
      InstallSnapshotResponse<Self::Id, <Self::Resolver as AddressResolver>::Address>,
      Self::Error,
    >,
  > + Send;

  /// Used to start a leadership transfer to the target node.
  fn timeout_now(
    &self,
    target: &Node<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    req: TimeoutNowRequest<Self::Id, <Self::Resolver as AddressResolver>::Address>,
  ) -> impl Future<
    Output = Result<
      TimeoutNowResponse<Self::Id, <Self::Resolver as AddressResolver>::Address>,
      Self::Error,
    >,
  > + Send;

  /// Used to send a heartbeat to the target node.
  fn heartbeat(
    &self,
    target: &Node<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    req: HeartbeatRequest<Self::Id, <Self::Resolver as AddressResolver>::Address>,
  ) -> impl Future<
    Output = Result<
      HeartbeatResponse<Self::Id, <Self::Resolver as AddressResolver>::Address>,
      Self::Error,
    >,
  > + Send;

  /// Shutdown the transport.
  fn shutdown(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

#[cfg(feature = "test")]
pub(super) mod tests {
  // pub use super::net::tests::*;
}
