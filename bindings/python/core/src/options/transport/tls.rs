use std::{
  hash::{Hash, Hasher},
  net::SocketAddr,
  path::PathBuf,
};

use ::smallvec::SmallVec;
use pyo3::{
  exceptions::{PyIndexError, PyTypeError},
  types::PyModule,
  *,
};
use ruraft_bindings_common::transport::Array;

use crate::{options::PythonTcpTransportOptions, types::Header, Pyi};

/// Private key used for TLS.
#[derive(Debug, Clone, Eq, PartialEq, Hash, derive_more::From)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[pyclass(name = "PrivateKey")]
pub struct PythonPrivateKey(ruraft_bindings_common::transport::PrivateKey);

impl From<PythonPrivateKey> for ruraft_bindings_common::transport::PrivateKey {
  fn from(pk: PythonPrivateKey) -> Self {
    pk.0
  }
}

impl Pyi for PythonPrivateKey {
  fn pyi() -> std::borrow::Cow<'static, str> {
    r#"

class PrivateKey:
  @staticmethod
  def pkcs8(k: str) -> PrivateKey:...
  
  @staticmethod
  def pkcs1(k: str) -> PrivateKey:...
  
  @staticmethod
  def sec1(k: str) -> PrivateKey:...
  
  def __eq__(self, __value: PrivateKey) -> bool: ...
  
  def __ne__(self, __value: PrivateKey) -> bool: ...
  
  def __hash__(self) -> int: ...
  
  def __str__(self) -> str: ...
  
  def __repr__(self) -> str: ...

"#
    .into()
  }
}

#[pymethods]
impl PythonPrivateKey {
  #[staticmethod]
  pub fn pkcs8(key: Array) -> Self {
    Self(ruraft_bindings_common::transport::PrivateKey::Pkcs8(key))
  }

  #[staticmethod]
  pub fn pkcs1(key: Array) -> Self {
    Self(ruraft_bindings_common::transport::PrivateKey::Pkcs1(key))
  }

  #[staticmethod]
  pub fn sec1(key: Array) -> Self {
    Self(ruraft_bindings_common::transport::PrivateKey::Sec1(key))
  }

  fn __eq__(&self, other: &Self) -> bool {
    self.0 == other.0
  }

  fn __ne__(&self, other: &Self) -> bool {
    self.0 != other.0
  }

  fn __hash__(&self) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    self.0.hash(&mut hasher);
    hasher.finish()
  }

  fn __str__(&self) -> PyResult<String> {
    if cfg!(feature = "serde") {
      serde_json::to_string(&self.0).map_err(|e| PyTypeError::new_err(e.to_string()))
    } else {
      Ok(format!("{:?}", self.0))
    }
  }

  fn __repr__(&self) -> String {
    format!("{:?}", self.0)
  }
}

/// Certificate chain used for TLS.
#[derive(Debug, Clone, Eq, PartialEq, Hash, derive_more::From)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[pyclass(name = "CertChain")]
pub struct PythonCertChain(SmallVec<[Array; 2]>);

impl From<PythonCertChain> for SmallVec<[Array; 2]> {
  fn from(cert_chain: PythonCertChain) -> Self {
    cert_chain.0
  }
}

impl From<Vec<Array>> for PythonCertChain {
  fn from(cert_chain: Vec<Array>) -> Self {
    Self(SmallVec::from(cert_chain))
  }
}

impl Pyi for PythonCertChain {
  fn pyi() -> std::borrow::Cow<'static, str> {
    r#"

class CertChain:
  def __init__(self, cert: bytes) -> None: ...
  
  def push(self, cert: bytes) -> None: ...
  
  def remove(self, cert: bytes) -> None: ...
  
  def clear(self) -> None: ...
  
  def __len__(self) -> int: ...
  
  def __getitem__(self, __key: int) -> bytes: ...
  
  def __setitem__(self, __key: int, __value: bytes) -> None: ...
  
  def __delitem__(self, __key: int) -> None: ...
  
  def __contiains__(self, __key: bytes) -> bool: ...
  
  def __eq__(self, __value: CertChain) -> bool: ...
  
  def __ne__(self, __value: CertChain) -> bool: ...
  
  def __hash__(self) -> int: ...
  
  def __str__(self) -> str: ...
  
  def __repr__(self) -> str: ...

"#
    .into()
  }
}

#[pymethods]
impl PythonCertChain {
  #[new]
  pub fn new(cert: Array) -> Self {
    Self(SmallVec::from_elem(cert, 1))
  }

  pub fn push(&mut self, cert: Array) {
    self.0.push(cert)
  }

  pub fn remove(&mut self, cert: &[u8]) {
    self.0.retain(|c| c.as_slice() != cert)
  }

  pub fn clear(&mut self) {
    self.0.clear()
  }

  pub fn __len__(&self) -> usize {
    self.0.len()
  }

  pub fn __getitem__(&self, index: isize) -> PyResult<Array> {
    let len = self.0.len() as isize;
    if index < 0 {
      if -index > len {
        return Err(PyIndexError::new_err("index out of range"));
      }
      Ok(self.0[(len + index) as usize].clone())
    } else {
      if index >= len {
        return Err(PyIndexError::new_err("index out of range"));
      }
      Ok(self.0[index as usize].clone())
    }
  }

  pub fn __setitem__(&mut self, index: isize, value: Array) -> PyResult<()> {
    let len = self.0.len() as isize;
    if index < 0 {
      if -index > len {
        return Err(PyIndexError::new_err("index out of range"));
      }
      self.0[(len + index) as usize] = value;
    } else {
      if index >= len {
        return Err(PyIndexError::new_err("index out of range"));
      }
      self.0[index as usize] = value;
    }

    Ok(())
  }

  pub fn __delitem__(&mut self, index: isize) -> PyResult<()> {
    let len = self.0.len() as isize;
    if index < 0 {
      if -index > len {
        return Err(PyIndexError::new_err("index out of range"));
      }
      self.0.remove((len + index) as usize);
    } else {
      if index >= len {
        return Err(PyIndexError::new_err("index out of range"));
      }
      self.0.remove(index as usize);
    }

    Ok(())
  }

  pub fn __contains__(&self, cert: &[u8]) -> bool {
    self.0.iter().any(|c| c.as_slice() == cert)
  }

  fn __eq__(&self, other: &Self) -> bool {
    self.0 == other.0
  }

  fn __ne__(&self, other: &Self) -> bool {
    self.0 != other.0
  }

  fn __hash__(&self) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    self.0.hash(&mut hasher);
    hasher.finish()
  }

  fn __str__(&self) -> PyResult<String> {
    if cfg!(feature = "serde") {
      serde_json::to_string(&self.0).map_err(|e| PyTypeError::new_err(e.to_string()))
    } else {
      Ok(format!("{:?}", self.0))
    }
  }

  fn __repr__(&self) -> String {
    format!("{:?}", self.0)
  }
}

/// Certificate and private key used for TLS.
#[derive(Debug, Clone, Eq, PartialEq, Hash, derive_more::From)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[pyclass(name = "CertChainAndPrivateKey")]
pub struct PythonCertChainAndPrivateKey(ruraft_bindings_common::transport::CertChainAndPrivateKey);

impl From<PythonCertChainAndPrivateKey>
  for ruraft_bindings_common::transport::CertChainAndPrivateKey
{
  fn from(cert_and_pk: PythonCertChainAndPrivateKey) -> Self {
    cert_and_pk.0
  }
}

impl Pyi for PythonCertChainAndPrivateKey {
  fn pyi() -> std::borrow::Cow<'static, str> {
    r#"

class CertChainAndPrivateKey:
  @staticmethod
  def pkcs8(cert_chain: CertChain, private_key: bytes) -> CertChainAndPrivateKey:...
  
  @staticmethod
  def pkcs1(cert_chain: CertChain, private_key: bytes) -> CertChainAndPrivateKey:...
  
  @staticmethod
  def sec1(cert_chain: CertChain, private_key: bytes) -> CertChainAndPrivateKey:...

  @property
  def cert_chain(self) -> CertChain:...
  
  @cert_chain.setter
  def cert_chain(self, value: CertChain) -> None:...
  
  @property
  def private_key(self) -> PrivateKey:...
  
  @private_key.setter
  def private_key(self, value: PrivateKey) -> None:...

  def __eq__(self, __value: CertChainAndPrivateKey) -> bool: ...
  
  def __ne__(self, __value: CertChainAndPrivateKey) -> bool: ...
  
  def __hash__(self) -> int: ...
  
  def __str__(self) -> str: ...
  
  def __repr__(self) -> str: ...

"#
    .into()
  }
}

#[pymethods]
impl PythonCertChainAndPrivateKey {
  #[staticmethod]
  pub fn pkcs8(cert_chain: PythonCertChain, pk: Array) -> Self {
    Self(
      ruraft_bindings_common::transport::CertChainAndPrivateKey::new(
        cert_chain.0.to_vec(),
        PythonPrivateKey::pkcs8(pk).into(),
      ),
    )
  }

  #[staticmethod]
  pub fn pkcs1(cert_chain: PythonCertChain, pk: Array) -> Self {
    Self(
      ruraft_bindings_common::transport::CertChainAndPrivateKey::new(
        cert_chain.0.to_vec(),
        PythonPrivateKey::pkcs1(pk).into(),
      ),
    )
  }

  #[staticmethod]
  pub fn sec1(cert_chain: PythonCertChain, pk: Array) -> Self {
    Self(
      ruraft_bindings_common::transport::CertChainAndPrivateKey::new(
        cert_chain.0.to_vec(),
        PythonPrivateKey::sec1(pk).into(),
      ),
    )
  }

  pub fn set_cert_chain(&mut self, cert_chain: PythonCertChain) {
    self.0.set_cert_chain(cert_chain.0.to_vec())
  }

  pub fn cert_chain(&self) -> PythonCertChain {
    self.0.cert_chain().clone().into()
  }

  pub fn set_private_key(&mut self, pk: PythonPrivateKey) {
    self.0.set_private_key(pk.into())
  }

  pub fn private_key(&self) -> PythonPrivateKey {
    self.0.private_key().clone().into()
  }

  fn __eq__(&self, other: &Self) -> bool {
    self.0 == other.0
  }

  fn __ne__(&self, other: &Self) -> bool {
    self.0 != other.0
  }

  fn __hash__(&self) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    self.0.hash(&mut hasher);
    hasher.finish()
  }

  fn __str__(&self) -> PyResult<String> {
    if cfg!(feature = "serde") {
      serde_json::to_string(&self.0).map_err(|e| PyTypeError::new_err(e.to_string()))
    } else {
      Ok(format!("{:?}", self.0))
    }
  }

  fn __repr__(&self) -> String {
    format!("{:?}", self.0)
  }
}

/// TLS server configuration.
#[derive(Debug, Clone, derive_more::From)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[pyclass(name = "TlsServerConfig")]
pub struct PythonTlsServerConfig(ruraft_bindings_common::transport::TlsServerConfig);

impl From<PythonTlsServerConfig> for ruraft_bindings_common::transport::TlsServerConfig {
  fn from(config: PythonTlsServerConfig) -> Self {
    config.0
  }
}

#[pymethods]
impl PythonTlsServerConfig {
  #[new]
  pub fn new(cert_chain: PythonCertChain, pk: PythonPrivateKey) -> Self {
    Self(ruraft_bindings_common::transport::TlsServerConfig::new(
      cert_chain.0.to_vec(),
      pk.into(),
    ))
  }

  // pub fn set_client_cert_verifier(
  //   &mut self,
  //   verifier: Option<Arc<dyn ruraft_tcp::tls::rustls::server::danger::ClientCertVerifier>>,
  // ) {
  //   self.client_cert_verifier = verifier;
  // }

  // pub fn client_cert_verifier(
  //   &self,
  // ) -> Option<&dyn ruraft_tcp::tls::rustls::server::danger::ClientCertVerifier> {
  //   self.client_cert_verifier.as_deref()
  // }

  pub fn set_cert_chain_and_private_key(
    &mut self,
    cert_chain: PythonCertChain,
    private_key: PythonPrivateKey,
  ) {
    self
      .0
      .set_cert_chain_and_private_key(cert_chain.0.to_vec(), private_key.into());
  }

  pub fn cert_and_private_key(&self) -> PythonCertChainAndPrivateKey {
    self.0.cert_and_private_key().clone().into()
  }

  pub fn set_ocsp(&mut self, ocsp: Option<Array>) {
    self.0.set_ocsp(ocsp);
  }

  pub fn ocsp(&self) -> Option<Array> {
    self.0.ocsp().cloned()
  }

  fn __str__(&self) -> PyResult<String> {
    if cfg!(feature = "serde") {
      serde_json::to_string(&self.0).map_err(|e| PyTypeError::new_err(e.to_string()))
    } else {
      Ok(format!("{:?}", self.0))
    }
  }

  fn __repr__(&self) -> String {
    format!("{:?}", self.0)
  }
}

impl Pyi for PythonTlsServerConfig {
  fn pyi() -> std::borrow::Cow<'static, str> {
    r#"

class TlsServerConfig:
  def __init__(self, cert_chain: CertChain, pk: PrivateKey) -> None: ...
  
  @property
  def cert_chain_and_private_key(self) -> CertChainAndPrivateKey:...
  
  @cert_chain_and_private_key.setter
  def cert_chain_and_private_key(self, value: CertChainAndPrivateKey) -> None:...
  
  @property
  def ocsp(self) -> bytes:...
  
  @ocsp.setter
  def ocsp(self, value: bytes) -> None:...
  
  def __str__(self) -> str: ...
  
  def __repr__(self) -> str:...

"#
    .into()
  }
}

/// TLS client configuration.
#[derive(Debug, Clone, derive_more::From)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[pyclass(name = "TlsClientConfig")]
pub struct PythonTlsClientConfig(ruraft_bindings_common::transport::TlsClientConfig);

impl From<PythonTlsClientConfig> for ruraft_bindings_common::transport::TlsClientConfig {
  fn from(config: PythonTlsClientConfig) -> Self {
    config.0
  }
}

impl Pyi for PythonTlsClientConfig {
  fn pyi() -> std::borrow::Cow<'static, str> {
    r#"

class TlsClientConfig:
  def __init__(self) -> None: ...
  
  @property
  def cert_chain_and_private_key(self) -> CertChainAndPrivateKey:...
  
  @cert_chain_and_private_key.setter
  def cert_chain_and_private_key(self, value: CertChainAndPrivateKey) -> None:...
  
  @property
  def root_certs(self) -> Optional[CertChain]:...
  
  @root_certs.setter
  def root_certs(self, value: Optional[CertChain]) -> None:...
  
  def __str__(self) -> str: ...
  
  def __repr__(self) -> str:...

"#
    .into()
  }
}

#[pymethods]
impl PythonTlsClientConfig {
  #[new]
  pub fn new() -> Self {
    Self(ruraft_bindings_common::transport::TlsClientConfig::new())
  }

  pub fn set_cert_chain_and_private_key(
    &mut self,
    cert_chain: PythonCertChain,
    private_key: PythonPrivateKey,
  ) {
    self
      .0
      .set_cert_chain_and_private_key(cert_chain.0.to_vec(), private_key.into());
  }

  pub fn cert_and_private_key(&self) -> Option<PythonCertChainAndPrivateKey> {
    self.0.cert_chain_and_private_key().cloned().map(Into::into)
  }

  pub fn set_root_certs(&mut self, root_certs: Option<PythonCertChain>) {
    self.0.set_root_certs(root_certs.map(|c| c.0.to_vec()));
  }

  pub fn root_certs(&self) -> Option<PythonCertChain> {
    self.0.root_certs().cloned().map(Into::into)
  }

  fn __str__(&self) -> PyResult<String> {
    if cfg!(feature = "serde") {
      serde_json::to_string(&self.0).map_err(|e| PyTypeError::new_err(e.to_string()))
    } else {
      Ok(format!("{:?}", self.0))
    }
  }

  fn __repr__(&self) -> String {
    format!("{:?}", self.0)
  }
}

/// TLS transport configuration.
#[derive(Debug, Clone, derive_more::From)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[pyclass(name = "TlsTransportOptions")]
pub struct PythonTlsTransportOptions(ruraft_bindings_common::transport::TlsTransportOptions);

impl From<PythonTlsTransportOptions> for ruraft_bindings_common::transport::TlsTransportOptions {
  fn from(opts: PythonTlsTransportOptions) -> Self {
    opts.0
  }
}

impl Pyi for PythonTlsTransportOptions {
  fn pyi() -> std::borrow::Cow<'static, str> {
    r#"

class TlsTransportOptions:
  def __init__(self, domain: str, transport_opts: TcpTransportOptions, server_config: TlsServerConfig, client_config: TlsClientConfig) -> None: ...

  @property
  def domain(self) -> str:...
  
  @domain.setter
  def domain(self, value: str) -> None:...
  
  @property
  def header(self) -> Header:...
  
  @header.setter
  def header(self, value: Header) -> None:...
  
  @property
  def bind_addr(self) -> str:...
  
  @bind_addr.setter
  def bind_addr(self, value: str) -> None:...
  
  @property
  def resolv_conf(self) -> PathLike:...
  
  @resolv_conf.setter
  def resolv_conf(self, value: PathLike) -> None:...

  @property
  def max_pool(self) -> int:...
  
  @max_pool.setter
  def max_pool(self, value: int) -> None:...
  
  @property
  def max_inflight_requests(self) -> int:...
  
  @max_inflight_requests.setter
  def max_inflight_requests(self, value: int) -> None:...
  
  @property
  def timeout(self) -> timedelta:...
  
  @timeout.setter
  def timeout(self, value: timedelta) -> None:...

  @property
  def tls_server_config(self) -> TlsServerConfig:...
  
  @tls_server_config.setter
  def tls_server_config(self, value: TlsServerConfig) -> None:...
  
  @property
  def tls_client_config(self) -> TlsClientConfig:...
  
  @tls_client_config.setter
  def tls_client_config(self, value: TlsClientConfig) -> None:...
  
  def __str__(self) -> str:...
  
  def __repr__(self) -> str:...

"#.into()
  }
}

#[pymethods]
impl PythonTlsTransportOptions {
  #[new]
  pub fn new(
    domain: String,
    transport_opts: PythonTcpTransportOptions,
    server_config: PythonTlsServerConfig,
    client_config: PythonTlsClientConfig,
  ) -> Self {
    Self(ruraft_bindings_common::transport::TlsTransportOptions::new(
      domain,
      transport_opts.into(),
      server_config.into(),
      client_config.into(),
    ))
  }

  /// Sets the address to bind to.
  #[setter]
  pub fn set_bind_addr(&mut self, bind_addr: &str) -> PyResult<()> {
    let addr = bind_addr
      .parse::<SocketAddr>()
      .map_err(|e| PyTypeError::new_err(e.to_string()))?;
    self.0.set_bind_addr(addr);
    Ok(())
  }

  /// Returns the address to bind to.
  #[getter]
  pub fn bind_addr(&self) -> String {
    self.0.bind_addr().to_string()
  }

  /// Sets the header used to identify the node.
  #[setter]
  pub fn set_header(&mut self, header: Header) {
    self.0.set_header(header.into());
  }

  /// Returns the header used to identify the node.
  #[getter]
  pub fn header(&self) -> Header {
    self.0.header().clone().into()
  }

  /// Sets the domain name of the server.
  #[setter]
  pub fn set_domain(&mut self, domain: String) {
    self.0.set_domain(domain);
  }

  /// Returns the domain name of the server.
  #[getter]
  pub fn domain(&self) -> &String {
    self.0.domain()
  }

  /// Sets the path to the resolv.conf file, this is used for DNS address resolve.
  /// If you can make sure all addresses you used in the
  /// Raft cluster is a socket address, then you can ignore this option.
  #[setter]
  pub fn set_resolv_conf(&mut self, resolv_conf: Option<PathBuf>) {
    self.0.set_resolv_conf(resolv_conf);
  }

  /// Returns the path to the resolv.conf file.
  #[getter]
  pub fn resolv_conf(&self) -> Option<&PathBuf> {
    self.0.resolv_conf()
  }

  /// Sets the maximum number of connections to keep in the connection pool.
  #[setter]
  pub fn set_max_pool(&mut self, max_pool: usize) {
    self.0.set_max_pool(max_pool);
  }

  /// Returns the maximum number of connections to keep in the connection pool.
  #[getter]
  pub fn max_pool(&self) -> usize {
    self.0.max_pool()
  }

  /// Sets the maximum number of in-flight append entries requests.
  #[setter]
  pub fn set_max_inflight_requests(&mut self, max_idle: usize) {
    self.0.set_max_inflight_requests(max_idle);
  }

  /// Returns the maximum number of in-flight append entries requests.
  #[getter]
  pub fn max_inflight_requests(&self) -> usize {
    self.0.max_inflight_requests()
  }

  /// Set the timeout used to apply I/O deadlines.
  #[setter]
  pub fn set_timeout(&mut self, timeout: ::chrono::Duration) -> PyResult<()> {
    timeout
      .to_std()
      .map(|timeout| self.0.set_timeout(timeout))
      .map_err(|e| PyTypeError::new_err(e.to_string()))
  }

  /// Returns the timeout used to apply I/O deadlines.
  #[getter]
  pub fn timeout(&self) -> PyResult<::chrono::Duration> {
    ::chrono::Duration::from_std(self.0.timeout()).map_err(|e| PyTypeError::new_err(e.to_string()))
  }

  /// Sets the TLS server configuration.
  #[setter]
  pub fn set_tls_server_config(&mut self, server_config: PythonTlsServerConfig) {
    self.0.set_tls_server_config(server_config.into());
  }

  /// Returns the TLS server configuration.
  #[getter]
  pub fn tls_server_config(&self) -> PythonTlsServerConfig {
    self.0.tls_server_config().clone().into()
  }

  /// Sets the TLS client configuration.
  #[setter]
  pub fn set_tls_client_config(&mut self, client_config: PythonTlsClientConfig) {
    self.0.set_tls_client_config(client_config.into());
  }

  /// Returns the TLS client configuration.
  #[getter]
  pub fn tls_client_config(&self) -> PythonTlsClientConfig {
    self.0.tls_client_config().clone().into()
  }

  fn __str__(&self) -> PyResult<String> {
    if cfg!(feature = "serde") {
      serde_json::to_string(&self.0).map_err(|e| PyTypeError::new_err(e.to_string()))
    } else {
      Ok(format!("{:?}", self.0))
    }
  }

  fn __repr__(&self) -> String {
    format!("{:?}", self.0)
  }
}

pub fn register_tls_transport_options(module: &PyModule) -> PyResult<()> {
  module.add_class::<PythonPrivateKey>()?;
  module.add_class::<PythonCertChain>()?;
  module.add_class::<PythonCertChainAndPrivateKey>()?;
  module.add_class::<PythonTlsServerConfig>()?;
  module.add_class::<PythonTlsClientConfig>()?;
  module.add_class::<PythonTlsTransportOptions>()?;
  Ok(())
}

pub fn tls_transport_pyi() -> String {
  let mut pyi = String::new();

  pyi.push_str(&PythonPrivateKey::pyi());
  pyi.push_str(&PythonCertChain::pyi());
  pyi.push_str(&PythonCertChainAndPrivateKey::pyi());
  pyi.push_str(&PythonTlsServerConfig::pyi());
  pyi.push_str(&PythonTlsClientConfig::pyi());
  pyi.push_str(&PythonTlsTransportOptions::pyi());

  pyi
}
