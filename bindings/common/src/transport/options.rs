use std::{net::SocketAddr, path::PathBuf};

use nodecraft::{NodeAddress, NodeId};
use ruraft_core::transport::Header;
#[cfg(feature = "tls")]
use std::sync::Arc;

#[cfg(feature = "tls")]
pub type Array = ::smallvec::SmallVec<[u8; 64]>;

use ruraft_tcp::net::NetTransportOptions;

#[derive(Debug, Clone, derive_more::From)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(untagged, rename_all = "snake_case"))]
pub enum SupportedTransportOptions {
  Tcp(TcpTransportOptions),
  #[cfg(feature = "native-tls")]
  NativeTls(NativeTlsTransportOptions),
  #[cfg(feature = "tls")]
  Tls(TlsTransportOptions),
}

impl SupportedTransportOptions {
  pub fn tcp(opts: TcpTransportOptions) -> Self {
    Self::Tcp(opts)
  }

  #[cfg(feature = "native-tls")]
  pub fn native_tls(opts: NativeTlsTransportOptions) -> Self {
    Self::NativeTls(opts)
  }

  #[cfg(feature = "tls")]
  pub fn tls(opts: TlsTransportOptions) -> Self {
    Self::Tls(opts)
  }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TcpTransportOptions {
  pub(super) resolv_conf: Option<PathBuf>,
  #[cfg_attr(feature = "serde", serde(flatten))]
  pub(super) transport_options: NetTransportOptions,
  pub(super) header: Header<NodeId, NodeAddress>,
  pub(super) bind_addr: SocketAddr,
}

impl TcpTransportOptions {
  #[cfg(unix)]
  pub fn new(header: Header<NodeId, NodeAddress>, bind_addr: SocketAddr) -> Self {
    Self {
      resolv_conf: Some(PathBuf::from("/etc/resolv.conf")),
      transport_options: NetTransportOptions::new(),
      header,
      bind_addr,
    }
  }

  #[cfg(windows)]
  pub fn new(header: Header<NodeId, NodeAddress>, bind_addr: SocketAddr) -> Self {
    Self {
      resolv_conf: Some(PathBuf::from(
        "C:\\Windows\\System32\\drivers\\etc\\resolv.conf",
      )),
      transport_options: NetTransportOptions::new(),
      header,
      bind_addr,
    }
  }

  #[cfg(not(any(windows, unix)))]
  pub fn new(header: Header<NodeId, NodeAddress>, bind_addr: SocketAddr) -> Self {
    Self {
      resolv_conf: None,
      transport_options: NetTransportOptions::new(),
      header,
      bind_addr,
    }
  }

  /// Sets the address to bind to.
  pub fn set_bind_addr(&mut self, bind_addr: SocketAddr) {
    self.bind_addr = bind_addr;
  }

  /// Returns the address to bind to.
  pub fn bind_addr(&self) -> &SocketAddr {
    &self.bind_addr
  }

  /// Sets the header used to identify the node.
  pub fn set_header(&mut self, header: Header<NodeId, NodeAddress>) {
    self.header = header;
  }

  /// Returns the header used to identify the node.
  pub fn header(&self) -> &Header<NodeId, NodeAddress> {
    &self.header
  }

  /// Sets the path to the resolv.conf file, this is used for DNS address resolve.
  /// If you can make sure all addresses you used in the
  /// Raft cluster is a socket address, then you can ignore this option.
  pub fn set_resolv_conf(&mut self, resolv_conf: Option<PathBuf>) {
    self.resolv_conf = resolv_conf;
  }

  /// Returns the path to the resolv.conf file.
  pub fn resolv_conf(&self) -> Option<&PathBuf> {
    self.resolv_conf.as_ref()
  }

  /// Sets the maximum number of connections to keep in the connection pool.
  pub fn set_max_pool(&mut self, max_pool: usize) {
    self.transport_options.set_max_pool(max_pool);
  }

  /// Returns the maximum number of connections to keep in the connection pool.
  pub fn max_pool(&self) -> usize {
    self.transport_options.max_pool()
  }

  /// Sets the maximum number of in-flight append entries requests.
  pub fn set_max_inflight_requests(&mut self, max_idle: usize) {
    self.transport_options.set_max_inflight_requests(max_idle);
  }

  /// Returns the maximum number of in-flight append entries requests.
  pub fn max_inflight_requests(&self) -> usize {
    self.transport_options.max_inflight_requests()
  }

  /// Set the timeout used to apply I/O deadlines.
  pub fn set_timeout(&mut self, timeout: std::time::Duration) {
    self.transport_options.set_timeout(timeout);
  }

  /// Returns the timeout used to apply I/O deadlines.
  pub fn timeout(&self) -> std::time::Duration {
    self.transport_options.timeout()
  }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
#[cfg(feature = "native-tls")]
pub enum Identity {
  Pkcs12 { cert: Array, password: String },
  Pkcs8 { cert: Array, key: Array },
}

#[cfg(feature = "native-tls")]
impl Identity {
  pub fn pkcs12(cert: Array, password: String) -> Self {
    Identity::Pkcs12 { cert, password }
  }

  pub fn pkcs8(cert: Array, key: Array) -> Self {
    Identity::Pkcs8 { cert, key }
  }
}

#[cfg(feature = "native-tls")]
impl TryFrom<Identity> for ruraft_tcp::native_tls::native_tls::Identity {
  type Error = ruraft_tcp::native_tls::native_tls::Error;

  fn try_from(value: Identity) -> Result<Self, Self::Error> {
    match value {
      Identity::Pkcs12 { cert, password } => {
        ruraft_tcp::native_tls::native_tls::Identity::from_pkcs12(&cert, &password)
      }
      Identity::Pkcs8 { cert, key } => {
        ruraft_tcp::native_tls::native_tls::Identity::from_pkcs8(&cert, &key)
      }
    }
  }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg(feature = "native-tls")]
pub struct NativeTlsTransportOptions {
  pub(super) domain: String,
  #[cfg_attr(feature = "serde", serde(flatten))]
  pub(super) opts: TcpTransportOptions,
  #[cfg_attr(feature = "serde", serde(flatten))]
  pub(super) identity: Identity,
}

#[cfg(feature = "native-tls")]
impl NativeTlsTransportOptions {
  /// Creates a new `NativeTlsTransportOptions` with the default configuration.
  ///
  /// Arguments:
  ///   domain: The domain name of the server.
  ///   identity: The identity used for TLS.
  pub fn new(domain: String, identity: Identity, opts: TcpTransportOptions) -> Self {
    Self {
      domain,
      opts,
      identity,
    }
  }

  /// Sets the address to bind to.
  pub fn set_bind_addr(&mut self, bind_addr: SocketAddr) {
    self.opts.set_bind_addr(bind_addr);
  }

  /// Returns the address to bind to.
  pub fn bind_addr(&self) -> &SocketAddr {
    self.opts.bind_addr()
  }

  /// Sets the header used to identify the node.
  pub fn set_header(&mut self, header: Header<NodeId, NodeAddress>) {
    self.opts.set_header(header);
  }

  /// Returns the header used to identify the node.
  pub fn header(&self) -> &Header<NodeId, NodeAddress> {
    self.opts.header()
  }

  /// Sets the domain name of the server.
  pub fn set_domain(&mut self, domain: String) {
    self.domain = domain;
  }

  /// Returns the domain name of the server.
  pub fn domain(&self) -> &String {
    &self.domain
  }

  /// Sets the identity used for TLS.
  pub fn set_identity(&mut self, identity: Identity) {
    self.identity = identity;
  }

  /// Returns the identity used for TLS.
  pub fn identity(&self) -> &Identity {
    &self.identity
  }

  /// Sets the path to the resolv.conf file, this is used for DNS address resolve.
  /// If you can make sure all addresses you used in the
  /// Raft cluster is a socket address, then you can ignore this option.
  pub fn set_resolv_conf(&mut self, resolv_conf: Option<PathBuf>) {
    self.opts.set_resolv_conf(resolv_conf);
  }

  /// Returns the path to the resolv.conf file.
  pub fn resolv_conf(&self) -> Option<&PathBuf> {
    self.opts.resolv_conf()
  }

  /// Sets the maximum number of connections to keep in the connection pool.
  pub fn set_max_pool(&mut self, max_pool: usize) {
    self.opts.set_max_pool(max_pool);
  }

  /// Returns the maximum number of connections to keep in the connection pool.
  pub fn max_pool(&self) -> usize {
    self.opts.max_pool()
  }

  /// Sets the maximum number of in-flight append entries requests.
  pub fn set_max_inflight_requests(&mut self, max_idle: usize) {
    self.opts.set_max_inflight_requests(max_idle);
  }

  /// Returns the maximum number of in-flight append entries requests.
  pub fn max_inflight_requests(&self) -> usize {
    self.opts.max_inflight_requests()
  }

  /// Set the timeout used to apply I/O deadlines.
  pub fn set_timeout(&mut self, timeout: std::time::Duration) {
    self.opts.set_timeout(timeout);
  }

  /// Returns the timeout used to apply I/O deadlines.
  pub fn timeout(&self) -> std::time::Duration {
    self.opts.timeout()
  }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
#[cfg(feature = "tls")]
pub enum PrivateKey {
  Pkcs1(Array),
  Pkcs8(Array),
  Sec1(Array),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg(feature = "tls")]
pub struct CertChainAndPrivateKey {
  cert_chain: Vec<Array>,
  private_key: PrivateKey,
}

#[cfg(feature = "tls")]
impl CertChainAndPrivateKey {
  pub fn new(cert_chain: Vec<Array>, pk: PrivateKey) -> Self {
    Self {
      cert_chain,
      private_key: pk,
    }
  }

  pub fn set_cert_chain(&mut self, cert_chain: Vec<Array>) {
    self.cert_chain = cert_chain;
  }

  pub fn cert_chain(&self) -> &Vec<Array> {
    &self.cert_chain
  }

  pub fn set_private_key(&mut self, pk: PrivateKey) {
    self.private_key = pk;
  }

  pub fn private_key(&self) -> &PrivateKey {
    &self.private_key
  }
}

#[derive(Clone, Debug)]
#[cfg(feature = "tls")]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TlsServerConfig {
  #[cfg_attr(feature = "serde", serde(skip))]
  client_cert_verifier:
    Option<Arc<dyn ruraft_tcp::tls::rustls::server::danger::ClientCertVerifier>>,
  #[cfg_attr(feature = "serde", serde(flatten))]
  cert_and_private_key: CertChainAndPrivateKey,
  ocsp: Option<Array>,
}

#[cfg(feature = "tls")]
impl TlsServerConfig {
  pub fn new(cert_chain: Vec<Array>, pk: PrivateKey) -> Self {
    Self {
      client_cert_verifier: None,
      cert_and_private_key: CertChainAndPrivateKey {
        cert_chain,
        private_key: pk,
      },
      ocsp: None,
    }
  }

  pub fn set_client_cert_verifier(
    &mut self,
    verifier: Option<Arc<dyn ruraft_tcp::tls::rustls::server::danger::ClientCertVerifier>>,
  ) {
    self.client_cert_verifier = verifier;
  }

  pub fn client_cert_verifier(
    &self,
  ) -> Option<&dyn ruraft_tcp::tls::rustls::server::danger::ClientCertVerifier> {
    self.client_cert_verifier.as_deref()
  }

  pub fn set_cert_chain_and_private_key(
    &mut self,
    cert_chain: Vec<Array>,
    private_key: PrivateKey,
  ) {
    self.cert_and_private_key = CertChainAndPrivateKey {
      cert_chain,
      private_key,
    };
  }

  pub fn cert_and_private_key(&self) -> &CertChainAndPrivateKey {
    &self.cert_and_private_key
  }

  pub fn set_ocsp(&mut self, ocsp: Option<Array>) {
    self.ocsp = ocsp;
  }

  pub fn ocsp(&self) -> Option<&Array> {
    self.ocsp.as_ref()
  }

  pub fn into_server_config(
    self,
  ) -> Result<ruraft_tcp::tls::rustls::ServerConfig, ruraft_tcp::tls::rustls::Error> {
    use ruraft_tcp::tls::rustls::{
      pki_types::{
        CertificateDer, PrivateKeyDer, PrivatePkcs1KeyDer, PrivatePkcs8KeyDer, PrivateSec1KeyDer,
      },
      ServerConfig,
    };
    let pk = match self.cert_and_private_key.private_key {
      PrivateKey::Pkcs8(pkcs8) => PrivateKeyDer::from(PrivatePkcs8KeyDer::from(pkcs8.to_vec())),
      PrivateKey::Pkcs1(pkcs12) => PrivateKeyDer::from(PrivatePkcs1KeyDer::from(pkcs12.to_vec())),
      PrivateKey::Sec1(sec1) => PrivateKeyDer::from(PrivateSec1KeyDer::from(sec1.to_vec())),
    };
    let cert_chain = self
      .cert_and_private_key
      .cert_chain
      .into_iter()
      .map(|c| CertificateDer::from(c.to_vec()))
      .collect();

    if let Some(verifier) = self.client_cert_verifier {
      if let Some(ocsp) = self.ocsp {
        ServerConfig::builder()
          .with_client_cert_verifier(verifier)
          .with_single_cert_with_ocsp(cert_chain, pk, ocsp.to_vec())
      } else {
        ServerConfig::builder()
          .with_client_cert_verifier(verifier)
          .with_single_cert(cert_chain, pk)
      }
    } else if let Some(ocsp) = self.ocsp {
      ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert_with_ocsp(cert_chain, pk, ocsp.to_vec())
    } else {
      ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, pk)
    }
  }
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg(feature = "tls")]
pub struct TlsClientConfig {
  #[cfg_attr(feature = "serde", serde(skip))]
  server_cert_verifier:
    Option<Arc<dyn ruraft_tcp::tls::rustls::client::danger::ServerCertVerifier>>,
  #[cfg_attr(feature = "serde", serde(flatten))]
  cert_and_private_key: Option<CertChainAndPrivateKey>,
  root_certs: Option<Vec<Array>>,
}

#[cfg(feature = "tls")]
impl Default for TlsClientConfig {
  fn default() -> Self {
    Self::new()
  }
}

#[cfg(feature = "tls")]
impl TlsClientConfig {
  pub fn new() -> Self {
    Self {
      cert_and_private_key: None,
      root_certs: None,
      server_cert_verifier: None,
    }
  }

  pub fn set_server_cert_verifier(
    &mut self,
    verifier: Option<Arc<dyn ruraft_tcp::tls::rustls::client::danger::ServerCertVerifier>>,
  ) {
    self.server_cert_verifier = verifier;
  }

  pub fn server_cert_verifier(
    &self,
  ) -> Option<&dyn ruraft_tcp::tls::rustls::client::danger::ServerCertVerifier> {
    self.server_cert_verifier.as_deref()
  }

  pub fn set_cert_chain_and_private_key(&mut self, cert_chain: Vec<Array>, pk: PrivateKey) {
    self.cert_and_private_key = Some(CertChainAndPrivateKey {
      cert_chain,
      private_key: pk,
    });
  }

  pub fn cert_chain_and_private_key(&self) -> Option<&CertChainAndPrivateKey> {
    self.cert_and_private_key.as_ref()
  }

  pub fn set_root_certs(&mut self, root_certs: Option<Vec<Array>>) {
    self.root_certs = root_certs;
  }

  pub fn root_certs(&self) -> Option<&Vec<Array>> {
    self.root_certs.as_ref()
  }

  pub fn into_client_config(
    self,
  ) -> Result<ruraft_tcp::tls::rustls::ClientConfig, ruraft_tcp::tls::rustls::Error> {
    use ruraft_tcp::tls::rustls::{
      pki_types::{
        CertificateDer, PrivateKeyDer, PrivatePkcs1KeyDer, PrivatePkcs8KeyDer, PrivateSec1KeyDer,
      },
      ClientConfig, RootCertStore,
    };

    match (self.cert_and_private_key, self.server_cert_verifier) {
      (None, None) => match self.root_certs {
        None => Ok(
          ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(ruraft_tcp::tls::NoopCertificateVerifier::new())
            .with_no_client_auth(),
        ),
        Some(root_certs) => {
          let mut root_store = RootCertStore::empty();
          for cert in root_certs {
            root_store.add(CertificateDer::from(cert.to_vec()))?;
          }
          Ok(
            ClientConfig::builder()
              .with_root_certificates(root_store)
              .with_no_client_auth(),
          )
        }
      },
      (None, Some(verifier)) => Ok(
        ClientConfig::builder()
          .dangerous()
          .with_custom_certificate_verifier(verifier)
          .with_no_client_auth(),
      ),
      (Some(auth_cert), verifier) => {
        let pk = match auth_cert.private_key {
          PrivateKey::Pkcs8(pkcs8) => PrivateKeyDer::from(PrivatePkcs8KeyDer::from(pkcs8.to_vec())),
          PrivateKey::Pkcs1(pkcs12) => {
            PrivateKeyDer::from(PrivatePkcs1KeyDer::from(pkcs12.to_vec()))
          }
          PrivateKey::Sec1(sec1) => PrivateKeyDer::from(PrivateSec1KeyDer::from(sec1.to_vec())),
        };
        let cert_chain = auth_cert
          .cert_chain
          .into_iter()
          .map(|c| CertificateDer::from(c.to_vec()))
          .collect();
        match verifier {
          None => {
            let mut root_store = RootCertStore::empty();
            if let Some(root_certs) = self.root_certs {
              for cert in root_certs {
                root_store.add(CertificateDer::from(cert.to_vec()))?;
              }
            }

            ClientConfig::builder()
              .with_root_certificates(root_store)
              .with_client_auth_cert(cert_chain, pk)
          }
          Some(verifier) => ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_client_auth_cert(cert_chain, pk),
        }
      }
    }
  }
}

#[derive(Debug, Clone)]
#[cfg(feature = "tls")]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TlsTransportOptions {
  pub(super) domain: String,
  #[cfg_attr(feature = "serde", serde(flatten))]
  pub(super) opts: TcpTransportOptions,
  pub(super) server_config: TlsServerConfig,
  pub(super) client_config: TlsClientConfig,
}

#[cfg(feature = "tls")]
impl TlsTransportOptions {
  pub fn new(
    domain: String,
    transport_opts: TcpTransportOptions,
    server_config: TlsServerConfig,
    client_config: TlsClientConfig,
  ) -> Self {
    Self {
      opts: transport_opts,
      server_config,
      client_config,
      domain,
    }
  }

  /// Sets the address to bind to.
  pub fn set_bind_addr(&mut self, bind_addr: SocketAddr) {
    self.opts.set_bind_addr(bind_addr);
  }

  /// Returns the address to bind to.
  pub fn bind_addr(&self) -> &SocketAddr {
    self.opts.bind_addr()
  }

  /// Sets the header used to identify the node.
  pub fn set_header(&mut self, header: Header<NodeId, NodeAddress>) {
    self.opts.set_header(header);
  }

  /// Returns the header used to identify the node.
  pub fn header(&self) -> &Header<NodeId, NodeAddress> {
    self.opts.header()
  }

  pub fn set_tls_server_cert_verifier(
    &mut self,
    verifier: Option<Arc<dyn ruraft_tcp::tls::rustls::server::danger::ClientCertVerifier>>,
  ) {
    self.server_config.set_client_cert_verifier(verifier);
  }

  pub fn tls_server_cert_verifier(
    &self,
  ) -> Option<&dyn ruraft_tcp::tls::rustls::server::danger::ClientCertVerifier> {
    self.server_config.client_cert_verifier()
  }

  pub fn set_tls_server_cert_chain_and_private_key(
    &mut self,
    cert_chain: Vec<Array>,
    pk: PrivateKey,
  ) {
    self
      .server_config
      .set_cert_chain_and_private_key(cert_chain, pk);
  }

  pub fn tls_server_cert_chain_and_private_key(&self) -> &CertChainAndPrivateKey {
    self.server_config.cert_and_private_key()
  }

  pub fn set_tls_server_ocsp(&mut self, ocsp: Option<Array>) {
    self.server_config.set_ocsp(ocsp);
  }

  pub fn tls_server_ocsp(&self) -> Option<&Array> {
    self.server_config.ocsp()
  }

  pub fn set_tls_client_cert_verifier(
    &mut self,
    verifier: Option<Arc<dyn ruraft_tcp::tls::rustls::client::danger::ServerCertVerifier>>,
  ) {
    self.client_config.set_server_cert_verifier(verifier);
  }

  pub fn tls_client_cert_verifier(
    &self,
  ) -> Option<&dyn ruraft_tcp::tls::rustls::client::danger::ServerCertVerifier> {
    self.client_config.server_cert_verifier()
  }

  pub fn set_tls_client_cert_chain_and_private_key(
    &mut self,
    cert_chain: Vec<Array>,
    pk: PrivateKey,
  ) {
    self
      .client_config
      .set_cert_chain_and_private_key(cert_chain, pk);
  }

  pub fn tls_client_cert_chain_and_private_key(&self) -> Option<&CertChainAndPrivateKey> {
    self.client_config.cert_chain_and_private_key()
  }

  pub fn set_tls_client_root_certificates(&mut self, root_certs: Option<Vec<Array>>) {
    self.client_config.set_root_certs(root_certs);
  }

  pub fn tls_client_root_certificates(&self) -> Option<&Vec<Array>> {
    self.client_config.root_certs()
  }

  pub fn set_domain(&mut self, domain: String) {
    self.domain = domain;
  }

  pub fn domain(&self) -> &String {
    &self.domain
  }

  pub fn set_resolv_conf(&mut self, resolv_conf: Option<PathBuf>) {
    self.opts.set_resolv_conf(resolv_conf);
  }

  pub fn resolv_conf(&self) -> Option<&PathBuf> {
    self.opts.resolv_conf()
  }

  pub fn set_max_pool(&mut self, max_pool: usize) {
    self.opts.set_max_pool(max_pool);
  }

  pub fn max_pool(&self) -> usize {
    self.opts.max_pool()
  }

  pub fn set_max_inflight_requests(&mut self, max_idle: usize) {
    self.opts.set_max_inflight_requests(max_idle);
  }

  pub fn max_inflight_requests(&self) -> usize {
    self.opts.max_inflight_requests()
  }

  pub fn set_timeout(&mut self, timeout: std::time::Duration) {
    self.opts.set_timeout(timeout);
  }

  pub fn timeout(&self) -> std::time::Duration {
    self.opts.timeout()
  }

  pub fn set_tls_server_config(&mut self, server_config: TlsServerConfig) {
    self.server_config = server_config;
  }

  pub fn tls_server_config(&self) -> &TlsServerConfig {
    &self.server_config
  }

  pub fn set_tls_client_config(&mut self, client_config: TlsClientConfig) {
    self.client_config = client_config;
  }

  pub fn tls_client_config(&self) -> &TlsClientConfig {
    &self.client_config
  }
}
