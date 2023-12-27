use std::future::Future;

use agnostic::Runtime;
use futures::{AsyncRead, AsyncWrite, Stream};

mod rpc;
pub use rpc::*;

mod error;
pub use error::*;

pub use nodecraft::{resolver::AddressResolver, Address, Id, Transformable};

use crate::{options::ProtocolVersion, Data, HeartbeatHandler, Node};

/// Represents errors that can arise during the wire encoding or decoding processes.
///
/// `WireError` provides a standard interface to wrap and differentiate between
/// errors related to node identifiers (`Id`), node addresses (`Address`), and custom error messages.
pub trait WireError: std::error::Error + Send + Sync + 'static {
  /// Creates a new error instance from an IO error.
  fn io(err: std::io::Error) -> Self;

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
  // type Error: WireError<Id = Self::Id, Address = Self::Address>;
  type Error: WireError;

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
  fn encode_request(
    req: &Request<Self::Id, Self::Address, Self::Data>,
  ) -> Result<Self::Bytes, Self::Error>;

  /// Encodes a [`Response`] into its byte-array representation.
  fn encode_response(resp: &Response<Self::Id, Self::Address>) -> Result<Self::Bytes, Self::Error>;

  /// Encodes a [`Request`] into its bytes representation to a writer.
  fn encode_request_to_writer(
    req: &Request<Self::Id, Self::Address, Self::Data>,
    writer: impl AsyncWrite + Send + Unpin,
  ) -> impl Future<Output = std::io::Result<()>> + Send;

  /// Encodes a [`Response`] into its bytes representation to a writer.
  fn encode_response_to_writer(
    resp: &Response<Self::Id, Self::Address>,
    writer: impl AsyncWrite + Send + Unpin,
  ) -> impl Future<Output = std::io::Result<()>> + Send;

  /// Decodes a [`Request`] instance from a provided source slice.
  fn decode_request(
    src: &[u8],
  ) -> Result<Request<Self::Id, Self::Address, Self::Data>, Self::Error>;

  /// Decodes a [`Response`] instance from a provided source slice.
  fn decode_response(src: &[u8]) -> Result<Response<Self::Id, Self::Address>, Self::Error>;

  /// Decodes a [`Request`] instance from a provided asynchronous reader.
  fn decode_request_from_reader(
    reader: impl AsyncRead + Send + Unpin,
  ) -> impl Future<Output = std::io::Result<Request<Self::Id, Self::Address, Self::Data>>> + Send;

  /// Decodes a [`Response`] instance from a provided asynchronous reader.
  fn decode_response_from_reader(
    reader: impl AsyncRead + Send + Unpin,
  ) -> impl Future<Output = std::io::Result<Response<Self::Id, Self::Address>>> + Send;
}

/// Provides utilities to pipeline [`AppendEntriesRequest`]s, aiming to
/// enhance replication throughput by minimizing latency and maximizing bandwidth utilization.
pub trait AppendEntriesPipeline: Send + Sync + 'static {
  /// Specifies potential errors that can occur within the pipeline.
  type Error: std::error::Error + Send + Sync + 'static;

  /// Unique identifier associated with nodes.
  type Id: Id;

  /// Network address representation of nodes.
  type Address: Address;

  /// The log entry's type-specific data, which will be applied to a user [`FinateStateMachine`](crate::FinateStateMachine).
  type Data: Data;

  /// Retrieves a stream for consuming response futures once they are ready.
  fn consumer(
    &self,
  ) -> impl Stream<Item = Result<PipelineAppendEntriesResponse<Self::Id, Self::Address>, Self::Error>>
       + Send
       + 'static;

  /// Asynchronously appends entries to the target node and returns the associated response.
  fn append_entries(
    &mut self,
    req: AppendEntriesRequest<Self::Id, Self::Address, Self::Data>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Gracefully closes the pipeline and terminates any in-flight requests.
  fn close(self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
/// Defines the capabilities and requirements for communication with other nodes across a network.
pub trait Transport: Send + Sync + 'static {
  /// Errors that the transport can potentially return during operations.
  type Error: TransportError<Id = Self::Id, Resolver = Self::Resolver, Wire = Self::Wire>;

  /// Specifies the runtime environment for transport operations.
  type Runtime: Runtime;

  /// Unique identifier for nodes.
  type Id: Id;

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

  /// Represents the type used by the transport to install the snapshot from remote.
  type SnapshotInstaller: AsyncRead + Unpin + Send + 'static;

  /// Consumes and responds to incoming RPC requests.
  fn consumer(
    &self,
  ) -> RpcConsumer<
    Self::Id,
    <Self::Resolver as AddressResolver>::Address,
    Self::Data,
    Self::SnapshotInstaller,
  >;

  /// Provides the local unique identifier, helping in distinguishing this node from its peers.
  fn local_id(&self) -> &Self::Id;

  /// Provides the local network address, aiding in distinguishing this node from peers.
  fn local_addr(&self) -> &<Self::Resolver as AddressResolver>::Address;

  /// Provides the concrete network address for peers in the Raft cluster to communicate with.
  fn advertise_addr(&self) -> &<Self::Resolver as AddressResolver>::ResolvedAddress;

  /// Provides the protocol version used by the transport.
  fn version(&self) -> ProtocolVersion;

  /// Setup a Heartbeat handler
  /// as a fast-pass. This can be used to avoid head-of-line blocking from
  /// disk IO. If a [`Transport`] does not support this, it can simply
  /// ignore the call, and push the heartbeat onto the [`RpcResponseSender`].
  fn set_heartbeat_handler(
    &self,
    handler: Option<HeartbeatHandler<Self::Id, <Self::Resolver as AddressResolver>::Address>>,
  );

  /// Provides the header used for all RPC requests.
  fn header(&self) -> Header<Self::Id, <Self::Resolver as AddressResolver>::Address> {
    Header::new(
      self.version(),
      self.local_id().clone(),
      self.local_addr().clone(),
    )
  }

  /// Provides access to the node's address resolver.
  fn resolver(&self) -> &Self::Resolver;

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
    source: impl AsyncRead + Send + Unpin,
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

/// Exports unit tests to let users test transport implementation.
#[cfg(any(feature = "test", test))]
pub mod tests {
  use std::{future::Future, time::Duration};

  use futures::{FutureExt, StreamExt};

  use crate::{
    membership::{MembershipBuilder, Server, ServerSuffrage},
    options::SnapshotVersion,
    storage::{Log, LogKind},
  };

  use super::*;

  fn make_append_req<I: Id, A: Address, D: Data>(id: I, addr: A) -> AppendEntriesRequest<I, A, D> {
    AppendEntriesRequest {
      header: Header::new(ProtocolVersion::V1, id, addr),
      term: 10,
      prev_log_entry: 100,
      prev_log_term: 4,
      entries: vec![Log::__crate_new(101, 4, LogKind::Noop)],
      leader_commit: 90,
    }
  }

  fn make_append_resp<I: Id, A: Address>(id: I, addr: A) -> AppendEntriesResponse<I, A> {
    AppendEntriesResponse {
      header: Header::new(ProtocolVersion::V1, id, addr),
      term: 4,
      last_log: 90,
      success: true,
      no_retry_backoff: false,
    }
  }

  /// Test [`Transport::set_heartbeat_handler`](Transport::set_heartbeat_handler).
  pub async fn heartbeat_fastpath<T: Transport>(_t1: T, _t2: T) {
    todo!()
  }

  /// Test [`Transport::append_entries`](Transport::append_entries).
  pub async fn append_entries<T: Transport>(trans1: T, trans2: T)
  where
    <<<T::Resolver as AddressResolver>::Runtime as Runtime>::Sleep as Future>::Output:
      Send + 'static,
  {
    let trans1_header = trans1.header();
    let args = make_append_req(trans1_header.id().clone(), trans1_header.addr().clone());
    let expected_resp = make_append_resp(trans1_header.id().clone(), trans1_header.addr().clone());
    let consumer = trans1.consumer();
    let resp = expected_resp.clone();

    <<T::Resolver as AddressResolver>::Runtime as Runtime>::spawn_detach(async move {
      futures::pin_mut!(consumer);

      futures::select! {
        req = consumer.next().fuse() => {
          let req = req.unwrap();
          let Ok(_) = req.respond(Response::append_entries(resp)) else {
            panic!("unexpected respond fail");
          };
        },
        _ = <<T::Resolver as AddressResolver>::Runtime as Runtime>::sleep(Duration::from_millis(200)).fuse() => {
          panic!("timeout");
        },
      }
    });

    let res = trans2
      .append_entries(trans1_header.from(), args)
      .await
      .unwrap();
    assert_eq!(res, expected_resp);
  }

  /// Test [`Transport::append_entries_pipeline`](Transport::append_entries_pipeline).
  pub async fn append_entries_pipeline<T: Transport>(trans1: T, trans2: T)
  where
    T::Data: core::fmt::Debug + PartialEq,
    <<<T::Resolver as AddressResolver>::Runtime as Runtime>::Sleep as Future>::Output:
      Send + 'static,
  {
    let trans1_consumer = trans1.consumer();
    let trans1_header = trans1.header();

    // Make the RPC request
    let args = make_append_req(trans1_header.id().clone(), trans1_header.addr().clone());
    let args1 = args.clone();
    let resp = make_append_resp(trans1_header.id().clone(), trans1_header.addr().clone());
    let resp1 = resp.clone();

    // Listen for a request
    <<T::Resolver as AddressResolver>::Runtime as Runtime>::spawn_detach(async move {
      futures::pin_mut!(trans1_consumer);
      for _ in 0..10 {
        futures::select! {
          req = trans1_consumer.next().fuse() => {
            let req = req.unwrap();
            // Verify the command
            if let Request::AppendEntries(req) = req.request() {
              assert_eq!(req, &args);
            } else {
              panic!("unexpected request");
            }
            let Ok(_) = req.respond(Response::append_entries(resp.clone())) else {
              panic!("unexpected respond fail");
            };
          },
          _ = <<T::Resolver as AddressResolver>::Runtime as Runtime>::sleep(Duration::from_millis(12200)).fuse() => {
            panic!("timeout");
          },
        }
      }
    });

    // Transport 2 makes outbound request
    let mut pipeline = trans2
      .append_entries_pipeline(trans1_header.from().clone())
      .await
      .unwrap();

    for _ in 0..10 {
      pipeline.append_entries(args1.clone()).await.unwrap();
    }

    let pc = pipeline.consumer();
    futures::pin_mut!(pc);
    for _ in 0..10 {
      futures::select! {
        res = pc.next().fuse() => {
          let res = res.unwrap().unwrap();
          assert_eq!(res.response(), &resp1);
        },
        _ = <<T::Resolver as AddressResolver>::Runtime as Runtime>::sleep(Duration::from_millis(200)).fuse() => {
          panic!("timeout");
        },
      }
    }
    pipeline.close().await.unwrap();
  }

  /// Test [`Transport::vote`](Transport::vote).
  pub async fn vote<T: Transport>(
    trans1: T,
    trans2: T,
    fake_vote_target: Header<T::Id, <T::Resolver as AddressResolver>::Address>,
  ) where
    <<<T::Resolver as AddressResolver>::Runtime as Runtime>::Sleep as Future>::Output:
      Send + 'static,
  {
    let trans1_consumer = trans1.consumer();

    let args = VoteRequest {
      header: fake_vote_target.clone(),
      term: 20,
      last_log_index: 100,
      last_log_term: 19,
      leadership_transfer: false,
    };
    let args1 = args.clone();

    let resp = VoteResponse {
      header: trans1.header().clone(),
      term: 100,
      granted: false,
    };
    let resp1 = resp.clone();

    <<T::Resolver as AddressResolver>::Runtime as Runtime>::spawn_detach(async move {
      futures::pin_mut!(trans1_consumer);
      futures::select! {
        req = trans1_consumer.next().fuse() => {
          let req = req.unwrap();
          // Verify the command
          if let Request::Vote(req) = req.request() {
            assert_eq!(req, &args);
          } else {
            panic!("unexpected request");
          }

          let Ok(_) = req.respond(Response::vote(resp.clone())) else {
            panic!("unexpected respond fail");
          };
        },
        _ = <<T::Resolver as AddressResolver>::Runtime as Runtime>::sleep(Duration::from_millis(200)).fuse() => {
          panic!("timeout");
        },
      }
    });

    let res = trans2.vote(trans1.header().from(), args1).await.unwrap();
    assert_eq!(res, resp1);
  }

  /// Test [`Transport::install_snapshot`](Transport::install_snapshot).
  pub async fn install_snapshot<T: Transport>(
    trans1: T,
    trans2: T,
    fake_target: Header<T::Id, <T::Resolver as AddressResolver>::Address>,
  ) where
    <<<T::Resolver as AddressResolver>::Runtime as Runtime>::Sleep as Future>::Output:
      Send + 'static,
  {
    use futures::AsyncReadExt;

    let trans1_consumer = trans1.consumer();

    let args = InstallSnapshotRequest {
      header: fake_target.clone(),
      term: 10,
      last_log_index: 100,
      last_log_term: 9,
      size: 10,
      snapshot_version: SnapshotVersion::V1,
      membership: {
        let mut builder = MembershipBuilder::new();
        builder
          .insert(Server::from_node(
            fake_target.from().clone(),
            ServerSuffrage::Voter,
          ))
          .unwrap();
        builder.build().unwrap()
      },
      membership_index: 1,
    };
    let args1 = args.clone();

    let resp = InstallSnapshotResponse {
      header: trans1.header().clone(),
      term: 10,
      success: true,
    };
    let resp1 = resp.clone();

    <<T::Resolver as AddressResolver>::Runtime as Runtime>::spawn_detach(async move {
      futures::pin_mut!(trans1_consumer);
      futures::select! {
        req = trans1_consumer.next().fuse() => {
          let mut req = req.unwrap();
          // Verify the command
          if let Request::InstallSnapshot(req) = req.request() {
            assert_eq!(req, &args);
          } else {
            panic!("unexpected request");
          }

          // Try to read the bytes
          let mut buf = [0; 10];
          let reader = req.reader_mut().unwrap();
          reader.read_exact(&mut buf).await.unwrap();

          // Compare
          assert_eq!(buf, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);

          let Ok(_) = req.respond(Response::install_snapshot(resp.clone())) else {
            panic!("unexpected respond fail");
          };
        },
        _ = <<T::Resolver as AddressResolver>::Runtime as Runtime>::sleep(Duration::from_millis(200)).fuse() => {
          panic!("timeout");
        },
      }
    });

    let buf: [u8; 10] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    let resp = trans2
      .install_snapshot(trans1.header().from(), args1, futures::io::Cursor::new(buf))
      .await
      .unwrap();
    assert_eq!(resp, resp1);
  }
}
