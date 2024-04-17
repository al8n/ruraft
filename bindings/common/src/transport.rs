use std::{
  pin::Pin,
  task::{Context, Poll},
};

use agnostic::Runtime;
use futures::{AsyncRead, Future, StreamExt};
use nodecraft::{
  resolver::dns::{DnsResolver, DnsResolverOptions},
  NodeAddress, NodeId,
};
use ruraft_core::{options::ProtocolVersion, transport::*, Data, Node};
use ruraft_tcp::{net::resolver::dns::read_resolv_conf, Tcp, TcpTransport};

#[cfg(feature = "tls")]
use ruraft_tcp::tls::*;

#[cfg(feature = "native-tls")]
use ruraft_tcp::native_tls::*;
use ruraft_wire::LpeWire;

mod options;
pub use options::*;

pub struct SupportedAppendEntriesPipelineConsumer<W: Wire, R: Runtime>(
  Pin<
    Box<
      dyn futures::Stream<
          Item = Result<
            PipelineAppendEntriesResponse<NodeId, NodeAddress>,
            ruraft_tcp::net::Error<NodeId, DnsResolver<R>, W>,
          >,
        > + Send
        + 'static,
    >,
  >,
);

impl<W: Wire, R: Runtime> futures::Stream for SupportedAppendEntriesPipelineConsumer<W, R> {
  type Item = Result<
    PipelineAppendEntriesResponse<NodeId, NodeAddress>,
    ruraft_tcp::net::Error<NodeId, DnsResolver<R>, W>,
  >;

  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    self.0.poll_next_unpin(cx)
  }
}

#[derive(derive_more::From)]
pub enum SupportedAppendEntriesPipeline<
  W: Wire<Id = NodeId, Address = NodeAddress, Data = D>,
  R: Runtime,
> {
  Tcp(ruraft_tcp::net::NetAppendEntriesPipeline<NodeId, DnsResolver<R>, D, ruraft_tcp::Tcp<R>, W>),
  #[cfg(feature = "tls")]
  Tls(
    ruraft_tcp::net::NetAppendEntriesPipeline<
      NodeId,
      DnsResolver<R>,
      D,
      ruraft_tcp::tls::Tls<R>,
      W,
    >,
  ),
  #[cfg(feature = "native-tls")]
  NativeTls(
    ruraft_tcp::net::NetAppendEntriesPipeline<
      NodeId,
      DnsResolver<R>,
      D,
      ruraft_tcp::native_tls::NativeTls<R>,
      W,
    >,
  ),
}

impl<D: Data, W: Wire<Id = NodeId, Address = NodeAddress, Data = D>, R: Runtime>
  AppendEntriesPipeline for SupportedAppendEntriesPipeline<D, W, R>
where
  <R::Sleep as Future>::Output: Send + 'static,
{
  type Error = ruraft_tcp::net::Error<Self::Id, DnsResolver<R>, W>;

  type Id = NodeId;

  type Address = NodeAddress;

  type Data = D;

  fn consumer(
    &self,
  ) -> impl futures::Stream<
    Item = Result<PipelineAppendEntriesResponse<Self::Id, Self::Address>, Self::Error>,
  > + Send
       + 'static {
    match self {
      Self::Tcp(p) => SupportedAppendEntriesPipelineConsumer(Box::pin(p.consumer())),
      #[cfg(feature = "tls")]
      Self::Tls(p) => SupportedAppendEntriesPipelineConsumer(Box::pin(p.consumer())),
      #[cfg(feature = "native-tls")]
      Self::NativeTls(p) => SupportedAppendEntriesPipelineConsumer(Box::pin(p.consumer())),
    }
  }

  async fn append_entries(
    &mut self,
    req: AppendEntriesRequest<Self::Id, Self::Address>,
  ) -> Result<(), Self::Error> {
    match self {
      Self::Tcp(p) => p.append_entries(req).await,
      #[cfg(feature = "tls")]
      Self::Tls(p) => p.append_entries(req).await,
      #[cfg(feature = "native-tls")]
      Self::NativeTls(p) => p.append_entries(req).await,
    }
  }

  async fn close(self) -> Result<(), Self::Error> {
    match self {
      Self::Tcp(p) => p.close().await,
      #[cfg(feature = "tls")]
      Self::Tls(p) => p.close().await,
      #[cfg(feature = "native-tls")]
      Self::NativeTls(p) => p.close().await,
    }
  }
}

#[derive(derive_more::From)]
pub enum SupportedTransport<D, R: Runtime> {
  Tcp(TcpTransport<NodeId, DnsResolver<R>, D, LpeWire<NodeId, NodeAddress, D>>),
  #[cfg(feature = "tls")]
  Tls(TlsTransport<NodeId, DnsResolver<R>, D, LpeWire<NodeId, NodeAddress, D>>),
  #[cfg(feature = "native-tls")]
  NativeTls(NativeTlsTransport<NodeId, DnsResolver<R>, D, LpeWire<NodeId, NodeAddress, D>>),
}

impl<D, R: Runtime> SupportedTransport<D, R>
where
  <R::Sleep as Future>::Output: Send + 'static,
{
  pub async fn new(opts: SupportedTransportOptions) -> Result<Self, <Self as Transport>::Error> {
    let resl_conf = |path: Option<&std::path::PathBuf>| match path {
      Some(resolv_conf) => {
        let resolv_conf = read_resolv_conf(resolv_conf)?;
        DnsResolver::<R>::new(Some(
          DnsResolverOptions::new()
            .with_resolver_config(resolv_conf.0)
            .with_resolver_opts(resolv_conf.1),
        ))
        .map_err(<<Self as Transport>::Error as TransportError>::resolver)
      }
      None => {
        DnsResolver::new(None).map_err(<<Self as Transport>::Error as TransportError>::resolver)
      }
    };
    match opts {
      SupportedTransportOptions::Tcp(opts) => {
        let resolver = resl_conf(opts.resolv_conf.as_ref())?;
        TcpTransport::new(
          opts.header,
          opts.bind_addr,
          resolver,
          Tcp::new(),
          opts.transport_options,
        )
        .await
        .map(Self::Tcp)
      }
      #[cfg(feature = "native-tls")]
      SupportedTransportOptions::NativeTls(opts) => {
        let resolver = resl_conf(opts.resolv_conf())?;
        let identity = opts
          .identity
          .try_into()
          .map_err(<<Self as Transport>::Error as TransportError>::custom)?;

        let acceptor = ruraft_tcp::native_tls::TlsAcceptor::from(
          ruraft_tcp::native_tls::native_tls::TlsAcceptor::new(identity)
            .map_err(<<Self as Transport>::Error as TransportError>::custom)?,
        );
        let connector =
          ruraft_tcp::native_tls::TlsConnector::new().danger_accept_invalid_certs(true);

        NativeTlsTransport::new(
          opts.opts.header,
          opts.opts.bind_addr,
          resolver,
          NativeTls::new(opts.domain, acceptor, connector),
          opts.opts.transport_options,
        )
        .await
        .map(Self::NativeTls)
      }
      #[cfg(feature = "tls")]
      SupportedTransportOptions::Tls(opts) => {
        let resolver = resl_conf(opts.resolv_conf())?;
        let cfg = opts
          .server_config
          .into_server_config()
          .map_err(<<Self as Transport>::Error as TransportError>::custom)?;
        let acceptor = ruraft_tcp::tls::TlsAcceptor::from(std::sync::Arc::new(cfg));
        let cfg = opts
          .client_config
          .into_client_config()
          .map_err(<<Self as Transport>::Error as TransportError>::custom)?;
        let connector = ruraft_tcp::tls::TlsConnector::from(std::sync::Arc::new(cfg));
        let domain_name = ruraft_tcp::tls::ServerName::try_from(opts.domain)
          .map_err(<<Self as Transport>::Error as TransportError>::custom)?;
        TlsTransport::new(
          opts.opts.header,
          opts.opts.bind_addr,
          resolver,
          Tls::new(domain_name, acceptor, connector),
          opts.opts.transport_options,
        )
        .await
        .map(Self::Tls)
      }
    }
  }
}

impl<D: Data, R: Runtime> Transport for SupportedTransport<D, R>
where
  <R::Sleep as Future>::Output: Send + 'static,
{
  type Error = ruraft_tcp::net::Error<Self::Id, Self::Resolver, Self::Wire>;

  type Runtime = R;

  type Id = NodeId;

  type Data = D;

  type Pipeline = SupportedAppendEntriesPipeline<Self::Wire, Self::Runtime>;

  type Resolver = DnsResolver<Self::Runtime>;

  type Wire = LpeWire<NodeId, NodeAddress, D>;

  fn consumer(&self) -> RpcConsumer<Self::Id, <Self::Resolver as AddressResolver>::Address> {
    match self {
      Self::Tcp(t) => t.consumer(),
      #[cfg(feature = "tls")]
      Self::Tls(t) => t.consumer(),
      #[cfg(feature = "native-tls")]
      Self::NativeTls(t) => t.consumer(),
    }
  }

  fn local_id(&self) -> &Self::Id {
    match self {
      Self::Tcp(t) => t.local_id(),
      #[cfg(feature = "tls")]
      Self::Tls(t) => t.local_id(),
      #[cfg(feature = "native-tls")]
      Self::NativeTls(t) => t.local_id(),
    }
  }

  fn local_addr(&self) -> &<Self::Resolver as AddressResolver>::Address {
    match self {
      Self::Tcp(t) => t.local_addr(),
      #[cfg(feature = "tls")]
      Self::Tls(t) => t.local_addr(),
      #[cfg(feature = "native-tls")]
      Self::NativeTls(t) => t.local_addr(),
    }
  }

  fn bind_addr(&self) -> &<Self::Resolver as AddressResolver>::ResolvedAddress {
    match self {
      Self::Tcp(t) => t.bind_addr(),
      #[cfg(feature = "tls")]
      Self::Tls(t) => t.bind_addr(),
      #[cfg(feature = "native-tls")]
      Self::NativeTls(t) => t.bind_addr(),
    }
  }

  fn version(&self) -> ProtocolVersion {
    match self {
      Self::Tcp(t) => t.version(),
      #[cfg(feature = "tls")]
      Self::Tls(t) => t.version(),
      #[cfg(feature = "native-tls")]
      Self::NativeTls(t) => t.version(),
    }
  }

  fn set_heartbeat_handler(
    &self,
    handler: Option<HeartbeatHandler<Self::Id, <Self::Resolver as AddressResolver>::Address>>,
  ) {
    match self {
      Self::Tcp(t) => t.set_heartbeat_handler(handler),
      #[cfg(feature = "tls")]
      Self::Tls(t) => t.set_heartbeat_handler(handler),
      #[cfg(feature = "native-tls")]
      Self::NativeTls(t) => t.set_heartbeat_handler(handler),
    }
  }

  fn resolver(&self) -> &Self::Resolver {
    match self {
      Self::Tcp(t) => t.resolver(),
      #[cfg(feature = "tls")]
      Self::Tls(t) => t.resolver(),
      #[cfg(feature = "native-tls")]
      Self::NativeTls(t) => t.resolver(),
    }
  }

  async fn append_entries_pipeline(
    &self,
    target: Node<Self::Id, <Self::Resolver as AddressResolver>::Address>,
  ) -> Result<Self::Pipeline, Self::Error> {
    match self {
      Self::Tcp(t) => t.append_entries_pipeline(target).await.map(Into::into),
      #[cfg(feature = "tls")]
      Self::Tls(t) => t.append_entries_pipeline(target).await.map(Into::into),
      #[cfg(feature = "native-tls")]
      Self::NativeTls(t) => t.append_entries_pipeline(target).await.map(Into::into),
    }
  }

  async fn append_entries(
    &self,
    target: &Node<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    req: AppendEntriesRequest<Self::Id, <Self::Resolver as AddressResolver>::Address>,
  ) -> Result<
    AppendEntriesResponse<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    Self::Error,
  > {
    match self {
      Self::Tcp(t) => t.append_entries(target, req).await,
      #[cfg(feature = "tls")]
      Self::Tls(t) => t.append_entries(target, req).await,
      #[cfg(feature = "native-tls")]
      Self::NativeTls(t) => t.append_entries(target, req).await,
    }
  }

  async fn vote(
    &self,
    target: &Node<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    req: VoteRequest<Self::Id, <Self::Resolver as AddressResolver>::Address>,
  ) -> Result<VoteResponse<Self::Id, <Self::Resolver as AddressResolver>::Address>, Self::Error> {
    match self {
      Self::Tcp(t) => t.vote(target, req).await,
      #[cfg(feature = "tls")]
      Self::Tls(t) => t.vote(target, req).await,
      #[cfg(feature = "native-tls")]
      Self::NativeTls(t) => t.vote(target, req).await,
    }
  }

  async fn install_snapshot(
    &self,
    target: &Node<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    req: InstallSnapshotRequest<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    source: impl AsyncRead + Send + Unpin,
  ) -> Result<
    InstallSnapshotResponse<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    Self::Error,
  > {
    match self {
      Self::Tcp(t) => t.install_snapshot(target, req, source).await,
      #[cfg(feature = "tls")]
      Self::Tls(t) => t.install_snapshot(target, req, source).await,
      #[cfg(feature = "native-tls")]
      Self::NativeTls(t) => t.install_snapshot(target, req, source).await,
    }
  }

  /// Used to start a leadership transfer to the target node.
  async fn timeout_now(
    &self,
    target: &Node<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    req: TimeoutNowRequest<Self::Id, <Self::Resolver as AddressResolver>::Address>,
  ) -> Result<TimeoutNowResponse<Self::Id, <Self::Resolver as AddressResolver>::Address>, Self::Error>
  {
    match self {
      Self::Tcp(t) => t.timeout_now(target, req).await,
      #[cfg(feature = "tls")]
      Self::Tls(t) => t.timeout_now(target, req).await,
      #[cfg(feature = "native-tls")]
      Self::NativeTls(t) => t.timeout_now(target, req).await,
    }
  }

  /// Used to send a heartbeat to the target node.
  async fn heartbeat(
    &self,
    target: &Node<Self::Id, <Self::Resolver as AddressResolver>::Address>,
    req: HeartbeatRequest<Self::Id, <Self::Resolver as AddressResolver>::Address>,
  ) -> Result<HeartbeatResponse<Self::Id, <Self::Resolver as AddressResolver>::Address>, Self::Error>
  {
    match self {
      Self::Tcp(t) => t.heartbeat(target, req).await,
      #[cfg(feature = "tls")]
      Self::Tls(t) => t.heartbeat(target, req).await,
      #[cfg(feature = "native-tls")]
      Self::NativeTls(t) => t.heartbeat(target, req).await,
    }
  }

  /// Shutdown the transport.
  async fn shutdown(&self) -> Result<(), Self::Error> {
    match self {
      Self::Tcp(t) => t.shutdown().await,
      #[cfg(feature = "tls")]
      Self::Tls(t) => t.shutdown().await,
      #[cfg(feature = "native-tls")]
      Self::NativeTls(t) => t.shutdown().await,
    }
  }
}
