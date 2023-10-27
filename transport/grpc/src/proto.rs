#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Header {
  /// ProtocolVerison is the version of the protocol the sender is speaking
  #[prost(int32, tag = "1")]
  pub protocol_version: i32,
  /// The node id of the node sending the RPC Request or Response
  #[prost(bytes = "vec", tag = "2")]
  pub id: ::prost::alloc::vec::Vec<u8>,
  /// Addr is the ServerAddr of the node sending theRPC Request or Response
  #[prost(bytes = "vec", tag = "3")]
  pub addr: ::prost::alloc::vec::Vec<u8>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Log {
  /// The index of the log entry.
  #[prost(uint64, tag = "1")]
  pub index: u64,
  /// The election term of the log entry.
  #[prost(uint64, tag = "2")]
  pub term: u64,
  /// The type of the log entry.
  #[prost(uint32, tag = "3")]
  pub r#type: u32,
  /// The log entries type-specific data.
  #[prost(bytes = "vec", tag = "4")]
  pub data: ::prost::alloc::vec::Vec<u8>,
  /// Extensions holds an opaque byte slice of information for middleware. It
  /// is up to the client of the library to properly modify this as it adds
  /// layers and remove those layers when appropriate. This value is a part of
  /// the log, so very large values could cause timing issues.
  ///
  /// N.B. It is _up to the client_ to handle upgrade paths. For instance if
  /// using this with go-raftchunking, the client should ensure that all Raft
  /// peers are using a version that can handle that extension before ever
  /// actually triggering chunking behavior. It is sometimes sufficient to
  /// ensure that non-leaders are upgraded first, then the current leader is
  /// upgraded, but a leader changeover during this process could lead to
  /// trouble, so gating extension behavior via some flag in the client
  /// program is also a good idea.
  #[prost(bytes = "vec", tag = "5")]
  pub extensions: ::prost::alloc::vec::Vec<u8>,
  /// AppendedAt stores the time the leader first appended this log to it's
  /// LogStore. Followers will observe the leader's time. It is not used for
  /// coordination or as part of the replication protocol at all. It exists only
  /// to provide operational information for example how many seconds worth of
  /// logs are present on the leader which might impact follower's ability to
  /// catch up after restoring a large snapshot. We should never rely on this
  /// being in the past when appending on a follower or reading a log back since
  /// the clock skew can mean a follower could see a log with a future timestamp.
  /// In general too the leader is not required to persist the log before
  /// delivering to followers although the current implementation happens to do
  /// this.
  /// Time validity bounds.
  #[prost(uint64, tag = "6")]
  pub appended_at: u64,
}
/// AppendEntriesRequest is the command used to append entries to the
/// replicated log.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AppendEntriesRequest {
  #[prost(message, optional, tag = "1")]
  pub header: ::core::option::Option<Header>,
  /// Provide the current term and leader
  #[prost(uint64, tag = "2")]
  pub term: u64,
  /// Provide the previous entries index for integrity checking
  #[prost(uint64, tag = "3")]
  pub previous_log_entry: u64,
  /// PreviousLogTerm is the previous logs term value for integrity checking
  #[prost(uint64, tag = "4")]
  pub previous_log_term: u64,
  /// New entries to commit
  #[prost(message, repeated, tag = "5")]
  pub entries: ::prost::alloc::vec::Vec<Log>,
  /// Commit index on the leader
  #[prost(uint64, tag = "6")]
  pub leader_commit_index: u64,
}
/// AppendEntriesResponse is the response returned from an
/// AppendEntriesRequest.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AppendEntriesResponse {
  #[prost(message, optional, tag = "1")]
  pub header: ::core::option::Option<Header>,
  /// Newer term if leader is out of date
  #[prost(uint64, tag = "2")]
  pub term: u64,
  /// Last Log is a hint to help accelerate rebuilding slow nodes
  #[prost(uint64, tag = "3")]
  pub last_log: u64,
  /// We may not succeed if we have a conflicting entry
  #[prost(bool, tag = "4")]
  pub success: bool,
  /// There are scenarios where this request didn't succeed
  /// but there's no need to wait/back-off the next attempt.
  #[prost(bool, tag = "5")]
  pub no_retry_backoff: bool,
}
/// RequestVoteRequest is the command used to request a vote.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RequestVoteRequest {
  #[prost(message, optional, tag = "1")]
  pub header: ::core::option::Option<Header>,
  /// Provide the term
  #[prost(uint64, tag = "2")]
  pub term: u64,
  /// The requesting servers last log index
  #[prost(uint64, tag = "3")]
  pub last_log_index: u64,
  /// The requesting servers last log term
  #[prost(uint64, tag = "4")]
  pub last_log_term: u64,
  /// Used to indicate to peers if this vote was triggered by a leadership
  /// transfer. It is required for leadership transfer to work, because servers
  /// wouldn't vote otherwise if they are aware of an existing leader.
  #[prost(bool, tag = "5")]
  pub leadership_transfer: bool,
}
/// RequestVoteResponse is the data returned from RequestVote RPC
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RequestVoteResponse {
  #[prost(message, optional, tag = "1")]
  pub header: ::core::option::Option<Header>,
  /// Provide the term
  #[prost(uint64, tag = "2")]
  pub term: u64,
  /// Was the vote granted
  #[prost(bool, tag = "3")]
  pub granted: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InstallSnapshotRequest {
  #[prost(oneof = "install_snapshot_request::Message", tags = "1, 2")]
  pub message: ::core::option::Option<install_snapshot_request::Message>,
}
/// Nested message and enum types in `InstallSnapshotRequest`.
pub mod install_snapshot_request {
  #[allow(clippy::derive_partial_eq_without_eq)]
  #[derive(Clone, PartialEq, ::prost::Oneof)]
  pub enum Message {
    #[prost(message, tag = "1")]
    Metadata(super::InstallSnapshotMetadata),
    #[prost(message, tag = "2")]
    Chunk(super::InstallSnapshotChunk),
  }
}
/// InstallSnapshotMetadata is the initial install snapshot request
/// arguments. This will be the first message sent in the InstallSnapshot RPC.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InstallSnapshotMetadata {
  #[prost(message, optional, tag = "1")]
  pub header: ::core::option::Option<Header>,
  #[prost(int32, tag = "2")]
  pub snapshot_version: i32,
  /// The current term of the leader
  #[prost(uint64, tag = "3")]
  pub term: u64,
  /// The last log index represented in the snapshot
  #[prost(uint64, tag = "4")]
  pub last_log_index: u64,
  /// The last log term represented in the snapshot
  #[prost(uint64, tag = "5")]
  pub last_log_term: u64,
  /// Contains cluster membership information.
  #[prost(bytes = "vec", tag = "6")]
  pub membership: ::prost::alloc::vec::Vec<u8>,
  /// The log index where the Membership data was
  /// originally written.
  #[prost(uint64, tag = "7")]
  pub membership_index: u64,
  /// Size is the number of bytes of snapshot data to be sent.
  #[prost(int64, tag = "8")]
  pub size: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InstallSnapshotChunk {
  #[prost(bytes = "vec", tag = "1")]
  pub data: ::prost::alloc::vec::Vec<u8>,
}
/// InstallSnapshotResponse is the data returned from RequestVote RPC
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InstallSnapshotResponse {
  #[prost(message, optional, tag = "1")]
  pub header: ::core::option::Option<Header>,
  /// Provide the term
  #[prost(uint64, tag = "2")]
  pub term: u64,
  /// Success represents whether the snapshot was installed successfully.
  #[prost(bool, tag = "3")]
  pub success: bool,
}
/// TimeoutNowRequest is the arguments for the TimeoutNow RPC request
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TimeoutNowRequest {
  #[prost(message, optional, tag = "1")]
  pub header: ::core::option::Option<Header>,
}
/// TimeoutNowResponse is the response message for a TimeoutNow RPC request
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TimeoutNowResponse {
  #[prost(message, optional, tag = "1")]
  pub header: ::core::option::Option<Header>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HeartbeatRequest {
  #[prost(message, optional, tag = "1")]
  pub header: ::core::option::Option<Header>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HeartbeatResponse {
  #[prost(message, optional, tag = "1")]
  pub header: ::core::option::Option<Header>,
}
/// Generated client implementations.
pub mod transport_client {
  #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
  use tonic::codegen::http::Uri;
  use tonic::codegen::*;
  #[derive(Debug, Clone)]
  pub struct TransportClient<T> {
    inner: tonic::client::Grpc<T>,
  }
  impl TransportClient<tonic::transport::Channel> {
    /// Attempt to create a new client by connecting to a given endpoint.
    pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
    where
      D: TryInto<tonic::transport::Endpoint>,
      D::Error: Into<StdError>,
    {
      let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
      Ok(Self::new(conn))
    }
  }
  impl<T> TransportClient<T>
  where
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
  {
    pub fn new(inner: T) -> Self {
      let inner = tonic::client::Grpc::new(inner);
      Self { inner }
    }
    pub fn with_origin(inner: T, origin: Uri) -> Self {
      let inner = tonic::client::Grpc::with_origin(inner, origin);
      Self { inner }
    }
    pub fn with_interceptor<F>(
      inner: T,
      interceptor: F,
    ) -> TransportClient<InterceptedService<T, F>>
    where
      F: tonic::service::Interceptor,
      T::ResponseBody: Default,
      T: tonic::codegen::Service<
        http::Request<tonic::body::BoxBody>,
        Response = http::Response<
          <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
        >,
      >,
      <T as tonic::codegen::Service<http::Request<tonic::body::BoxBody>>>::Error:
        Into<StdError> + Send + Sync,
    {
      TransportClient::new(InterceptedService::new(inner, interceptor))
    }
    /// Compress requests with the given encoding.
    ///
    /// This requires the server to support it otherwise it might respond with an
    /// error.
    #[must_use]
    pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
      self.inner = self.inner.send_compressed(encoding);
      self
    }
    /// Enable decompressing responses.
    #[must_use]
    pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
      self.inner = self.inner.accept_compressed(encoding);
      self
    }
    /// Limits the maximum size of a decoded message.
    ///
    /// Default: `4MB`
    #[must_use]
    pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
      self.inner = self.inner.max_decoding_message_size(limit);
      self
    }
    /// Limits the maximum size of an encoded message.
    ///
    /// Default: `usize::MAX`
    #[must_use]
    pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
      self.inner = self.inner.max_encoding_message_size(limit);
      self
    }
    pub async fn append_entries(
      &mut self,
      request: impl tonic::IntoRequest<super::AppendEntriesRequest>,
    ) -> std::result::Result<tonic::Response<super::AppendEntriesResponse>, tonic::Status> {
      self.inner.ready().await.map_err(|e| {
        tonic::Status::new(
          tonic::Code::Unknown,
          format!("Service was not ready: {}", e.into()),
        )
      })?;
      let codec = tonic::codec::ProstCodec::default();
      let path = http::uri::PathAndQuery::from_static("/proto.Transport/AppendEntries");
      let mut req = request.into_request();
      req
        .extensions_mut()
        .insert(GrpcMethod::new("proto.Transport", "AppendEntries"));
      self.inner.unary(req, path, codec).await
    }
    pub async fn append_entries_pipeline(
      &mut self,
      request: impl tonic::IntoStreamingRequest<Message = super::AppendEntriesRequest>,
    ) -> std::result::Result<
      tonic::Response<tonic::codec::Streaming<super::AppendEntriesResponse>>,
      tonic::Status,
    > {
      self.inner.ready().await.map_err(|e| {
        tonic::Status::new(
          tonic::Code::Unknown,
          format!("Service was not ready: {}", e.into()),
        )
      })?;
      let codec = tonic::codec::ProstCodec::default();
      let path = http::uri::PathAndQuery::from_static("/proto.Transport/AppendEntriesPipeline");
      let mut req = request.into_streaming_request();
      req
        .extensions_mut()
        .insert(GrpcMethod::new("proto.Transport", "AppendEntriesPipeline"));
      self.inner.streaming(req, path, codec).await
    }
    pub async fn request_vote(
      &mut self,
      request: impl tonic::IntoRequest<super::RequestVoteRequest>,
    ) -> std::result::Result<tonic::Response<super::RequestVoteResponse>, tonic::Status> {
      self.inner.ready().await.map_err(|e| {
        tonic::Status::new(
          tonic::Code::Unknown,
          format!("Service was not ready: {}", e.into()),
        )
      })?;
      let codec = tonic::codec::ProstCodec::default();
      let path = http::uri::PathAndQuery::from_static("/proto.Transport/RequestVote");
      let mut req = request.into_request();
      req
        .extensions_mut()
        .insert(GrpcMethod::new("proto.Transport", "RequestVote"));
      self.inner.unary(req, path, codec).await
    }
    pub async fn install_snapshot(
      &mut self,
      request: impl tonic::IntoStreamingRequest<Message = super::InstallSnapshotRequest>,
    ) -> std::result::Result<
      tonic::Response<tonic::codec::Streaming<super::InstallSnapshotResponse>>,
      tonic::Status,
    > {
      self.inner.ready().await.map_err(|e| {
        tonic::Status::new(
          tonic::Code::Unknown,
          format!("Service was not ready: {}", e.into()),
        )
      })?;
      let codec = tonic::codec::ProstCodec::default();
      let path = http::uri::PathAndQuery::from_static("/proto.Transport/InstallSnapshot");
      let mut req = request.into_streaming_request();
      req
        .extensions_mut()
        .insert(GrpcMethod::new("proto.Transport", "InstallSnapshot"));
      self.inner.streaming(req, path, codec).await
    }
    pub async fn timeout_now(
      &mut self,
      request: impl tonic::IntoRequest<super::TimeoutNowRequest>,
    ) -> std::result::Result<tonic::Response<super::TimeoutNowResponse>, tonic::Status> {
      self.inner.ready().await.map_err(|e| {
        tonic::Status::new(
          tonic::Code::Unknown,
          format!("Service was not ready: {}", e.into()),
        )
      })?;
      let codec = tonic::codec::ProstCodec::default();
      let path = http::uri::PathAndQuery::from_static("/proto.Transport/TimeoutNow");
      let mut req = request.into_request();
      req
        .extensions_mut()
        .insert(GrpcMethod::new("proto.Transport", "TimeoutNow"));
      self.inner.unary(req, path, codec).await
    }
  }
}
/// Generated server implementations.
pub mod transport_server {
  #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
  use tonic::codegen::*;
  /// Generated trait containing gRPC methods that should be implemented for use with TransportServer.
  #[async_trait]
  pub trait Transport: Send + Sync + 'static {
    async fn append_entries(
      &self,
      request: tonic::Request<super::AppendEntriesRequest>,
    ) -> std::result::Result<tonic::Response<super::AppendEntriesResponse>, tonic::Status>;
    /// Server streaming response type for the AppendEntriesPipeline method.
    type AppendEntriesPipelineStream: tonic::codegen::tokio_stream::Stream<
        Item = std::result::Result<super::AppendEntriesResponse, tonic::Status>,
      > + Send
      + 'static;
    async fn append_entries_pipeline(
      &self,
      request: tonic::Request<tonic::Streaming<super::AppendEntriesRequest>>,
    ) -> std::result::Result<tonic::Response<Self::AppendEntriesPipelineStream>, tonic::Status>;
    async fn request_vote(
      &self,
      request: tonic::Request<super::RequestVoteRequest>,
    ) -> std::result::Result<tonic::Response<super::RequestVoteResponse>, tonic::Status>;
    /// Server streaming response type for the InstallSnapshot method.
    type InstallSnapshotStream: tonic::codegen::tokio_stream::Stream<
        Item = std::result::Result<super::InstallSnapshotResponse, tonic::Status>,
      > + Send
      + 'static;
    async fn install_snapshot(
      &self,
      request: tonic::Request<tonic::Streaming<super::InstallSnapshotRequest>>,
    ) -> std::result::Result<tonic::Response<Self::InstallSnapshotStream>, tonic::Status>;
    async fn timeout_now(
      &self,
      request: tonic::Request<super::TimeoutNowRequest>,
    ) -> std::result::Result<tonic::Response<super::TimeoutNowResponse>, tonic::Status>;
  }
  #[derive(Debug)]
  pub struct TransportServer<T: Transport> {
    inner: _Inner<T>,
    accept_compression_encodings: EnabledCompressionEncodings,
    send_compression_encodings: EnabledCompressionEncodings,
    max_decoding_message_size: Option<usize>,
    max_encoding_message_size: Option<usize>,
  }
  struct _Inner<T>(Arc<T>);
  impl<T: Transport> TransportServer<T> {
    pub fn new(inner: T) -> Self {
      Self::from_arc(Arc::new(inner))
    }
    pub fn from_arc(inner: Arc<T>) -> Self {
      let inner = _Inner(inner);
      Self {
        inner,
        accept_compression_encodings: Default::default(),
        send_compression_encodings: Default::default(),
        max_decoding_message_size: None,
        max_encoding_message_size: None,
      }
    }
    pub fn with_interceptor<F>(inner: T, interceptor: F) -> InterceptedService<Self, F>
    where
      F: tonic::service::Interceptor,
    {
      InterceptedService::new(Self::new(inner), interceptor)
    }
    /// Enable decompressing requests with the given encoding.
    #[must_use]
    pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
      self.accept_compression_encodings.enable(encoding);
      self
    }
    /// Compress responses with the given encoding, if the client supports it.
    #[must_use]
    pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
      self.send_compression_encodings.enable(encoding);
      self
    }
    /// Limits the maximum size of a decoded message.
    ///
    /// Default: `4MB`
    #[must_use]
    pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
      self.max_decoding_message_size = Some(limit);
      self
    }
    /// Limits the maximum size of an encoded message.
    ///
    /// Default: `usize::MAX`
    #[must_use]
    pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
      self.max_encoding_message_size = Some(limit);
      self
    }
  }
  impl<T, B> tonic::codegen::Service<http::Request<B>> for TransportServer<T>
  where
    T: Transport,
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
  {
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = std::convert::Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
      Poll::Ready(Ok(()))
    }
    fn call(&mut self, req: http::Request<B>) -> Self::Future {
      let inner = self.inner.clone();
      match req.uri().path() {
        "/proto.Transport/AppendEntries" => {
          #[allow(non_camel_case_types)]
          struct AppendEntriesSvc<T: Transport>(pub Arc<T>);
          impl<T: Transport> tonic::server::UnaryService<super::AppendEntriesRequest>
            for AppendEntriesSvc<T>
          {
            type Response = super::AppendEntriesResponse;
            type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
            fn call(
              &mut self,
              request: tonic::Request<super::AppendEntriesRequest>,
            ) -> Self::Future {
              let inner = Arc::clone(&self.0);
              let fut = async move { <T as Transport>::append_entries(&inner, request).await };
              Box::pin(fut)
            }
          }
          let accept_compression_encodings = self.accept_compression_encodings;
          let send_compression_encodings = self.send_compression_encodings;
          let max_decoding_message_size = self.max_decoding_message_size;
          let max_encoding_message_size = self.max_encoding_message_size;
          let inner = self.inner.clone();
          let fut = async move {
            let inner = inner.0;
            let method = AppendEntriesSvc(inner);
            let codec = tonic::codec::ProstCodec::default();
            let mut grpc = tonic::server::Grpc::new(codec)
              .apply_compression_config(accept_compression_encodings, send_compression_encodings)
              .apply_max_message_size_config(max_decoding_message_size, max_encoding_message_size);
            let res = grpc.unary(method, req).await;
            Ok(res)
          };
          Box::pin(fut)
        }
        "/proto.Transport/AppendEntriesPipeline" => {
          #[allow(non_camel_case_types)]
          struct AppendEntriesPipelineSvc<T: Transport>(pub Arc<T>);
          impl<T: Transport> tonic::server::StreamingService<super::AppendEntriesRequest>
            for AppendEntriesPipelineSvc<T>
          {
            type Response = super::AppendEntriesResponse;
            type ResponseStream = T::AppendEntriesPipelineStream;
            type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
            fn call(
              &mut self,
              request: tonic::Request<tonic::Streaming<super::AppendEntriesRequest>>,
            ) -> Self::Future {
              let inner = Arc::clone(&self.0);
              let fut =
                async move { <T as Transport>::append_entries_pipeline(&inner, request).await };
              Box::pin(fut)
            }
          }
          let accept_compression_encodings = self.accept_compression_encodings;
          let send_compression_encodings = self.send_compression_encodings;
          let max_decoding_message_size = self.max_decoding_message_size;
          let max_encoding_message_size = self.max_encoding_message_size;
          let inner = self.inner.clone();
          let fut = async move {
            let inner = inner.0;
            let method = AppendEntriesPipelineSvc(inner);
            let codec = tonic::codec::ProstCodec::default();
            let mut grpc = tonic::server::Grpc::new(codec)
              .apply_compression_config(accept_compression_encodings, send_compression_encodings)
              .apply_max_message_size_config(max_decoding_message_size, max_encoding_message_size);
            let res = grpc.streaming(method, req).await;
            Ok(res)
          };
          Box::pin(fut)
        }
        "/proto.Transport/RequestVote" => {
          #[allow(non_camel_case_types)]
          struct RequestVoteSvc<T: Transport>(pub Arc<T>);
          impl<T: Transport> tonic::server::UnaryService<super::RequestVoteRequest> for RequestVoteSvc<T> {
            type Response = super::RequestVoteResponse;
            type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
            fn call(&mut self, request: tonic::Request<super::RequestVoteRequest>) -> Self::Future {
              let inner = Arc::clone(&self.0);
              let fut = async move { <T as Transport>::request_vote(&inner, request).await };
              Box::pin(fut)
            }
          }
          let accept_compression_encodings = self.accept_compression_encodings;
          let send_compression_encodings = self.send_compression_encodings;
          let max_decoding_message_size = self.max_decoding_message_size;
          let max_encoding_message_size = self.max_encoding_message_size;
          let inner = self.inner.clone();
          let fut = async move {
            let inner = inner.0;
            let method = RequestVoteSvc(inner);
            let codec = tonic::codec::ProstCodec::default();
            let mut grpc = tonic::server::Grpc::new(codec)
              .apply_compression_config(accept_compression_encodings, send_compression_encodings)
              .apply_max_message_size_config(max_decoding_message_size, max_encoding_message_size);
            let res = grpc.unary(method, req).await;
            Ok(res)
          };
          Box::pin(fut)
        }
        "/proto.Transport/InstallSnapshot" => {
          #[allow(non_camel_case_types)]
          struct InstallSnapshotSvc<T: Transport>(pub Arc<T>);
          impl<T: Transport> tonic::server::StreamingService<super::InstallSnapshotRequest>
            for InstallSnapshotSvc<T>
          {
            type Response = super::InstallSnapshotResponse;
            type ResponseStream = T::InstallSnapshotStream;
            type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
            fn call(
              &mut self,
              request: tonic::Request<tonic::Streaming<super::InstallSnapshotRequest>>,
            ) -> Self::Future {
              let inner = Arc::clone(&self.0);
              let fut = async move { <T as Transport>::install_snapshot(&inner, request).await };
              Box::pin(fut)
            }
          }
          let accept_compression_encodings = self.accept_compression_encodings;
          let send_compression_encodings = self.send_compression_encodings;
          let max_decoding_message_size = self.max_decoding_message_size;
          let max_encoding_message_size = self.max_encoding_message_size;
          let inner = self.inner.clone();
          let fut = async move {
            let inner = inner.0;
            let method = InstallSnapshotSvc(inner);
            let codec = tonic::codec::ProstCodec::default();
            let mut grpc = tonic::server::Grpc::new(codec)
              .apply_compression_config(accept_compression_encodings, send_compression_encodings)
              .apply_max_message_size_config(max_decoding_message_size, max_encoding_message_size);
            let res = grpc.streaming(method, req).await;
            Ok(res)
          };
          Box::pin(fut)
        }
        "/proto.Transport/TimeoutNow" => {
          #[allow(non_camel_case_types)]
          struct TimeoutNowSvc<T: Transport>(pub Arc<T>);
          impl<T: Transport> tonic::server::UnaryService<super::TimeoutNowRequest> for TimeoutNowSvc<T> {
            type Response = super::TimeoutNowResponse;
            type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
            fn call(&mut self, request: tonic::Request<super::TimeoutNowRequest>) -> Self::Future {
              let inner = Arc::clone(&self.0);
              let fut = async move { <T as Transport>::timeout_now(&inner, request).await };
              Box::pin(fut)
            }
          }
          let accept_compression_encodings = self.accept_compression_encodings;
          let send_compression_encodings = self.send_compression_encodings;
          let max_decoding_message_size = self.max_decoding_message_size;
          let max_encoding_message_size = self.max_encoding_message_size;
          let inner = self.inner.clone();
          let fut = async move {
            let inner = inner.0;
            let method = TimeoutNowSvc(inner);
            let codec = tonic::codec::ProstCodec::default();
            let mut grpc = tonic::server::Grpc::new(codec)
              .apply_compression_config(accept_compression_encodings, send_compression_encodings)
              .apply_max_message_size_config(max_decoding_message_size, max_encoding_message_size);
            let res = grpc.unary(method, req).await;
            Ok(res)
          };
          Box::pin(fut)
        }
        _ => Box::pin(async move {
          Ok(
            http::Response::builder()
              .status(200)
              .header("grpc-status", "12")
              .header("content-type", "application/grpc")
              .body(empty_body())
              .unwrap(),
          )
        }),
      }
    }
  }
  impl<T: Transport> Clone for TransportServer<T> {
    fn clone(&self) -> Self {
      let inner = self.inner.clone();
      Self {
        inner,
        accept_compression_encodings: self.accept_compression_encodings,
        send_compression_encodings: self.send_compression_encodings,
        max_decoding_message_size: self.max_decoding_message_size,
        max_encoding_message_size: self.max_encoding_message_size,
      }
    }
  }
  impl<T: Transport> Clone for _Inner<T> {
    fn clone(&self) -> Self {
      Self(Arc::clone(&self.0))
    }
  }
  impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
      write!(f, "{:?}", self.0)
    }
  }
  impl<T: Transport> tonic::server::NamedService for TransportServer<T> {
    const NAME: &'static str = "proto.Transport";
  }
}
