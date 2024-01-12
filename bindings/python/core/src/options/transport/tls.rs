use std::{
  hash::{Hash, Hasher},
  path::PathBuf,
};

use ::smallvec::SmallVec;
use pyo3::{
  exceptions::{PyIndexError, PyTypeError},
  types::PyModule,
  *,
};
use ruraft_bindings_common::transport::Array;

use crate::options::PythonTcpTransportOptions;

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
#[pyclass(name = "CertAndPrivateKey")]
pub struct PythonCertAndPrivateKey(ruraft_bindings_common::transport::CertAndPrivateKey);

impl From<PythonCertAndPrivateKey> for ruraft_bindings_common::transport::CertAndPrivateKey {
  fn from(cert_and_pk: PythonCertAndPrivateKey) -> Self {
    cert_and_pk.0
  }
}

#[pymethods]
impl PythonCertAndPrivateKey {
  #[staticmethod]
  pub fn pkcs8(cert_chain: PythonCertChain, pk: Array) -> Self {
    Self(ruraft_bindings_common::transport::CertAndPrivateKey::new(
      cert_chain.0.to_vec(),
      PythonPrivateKey::pkcs8(pk).into(),
    ))
  }

  #[staticmethod]
  pub fn pkcs1(cert_chain: PythonCertChain, pk: Array) -> Self {
    Self(ruraft_bindings_common::transport::CertAndPrivateKey::new(
      cert_chain.0.to_vec(),
      PythonPrivateKey::pkcs1(pk).into(),
    ))
  }

  #[staticmethod]
  pub fn sec1(cert_chain: PythonCertChain, pk: Array) -> Self {
    Self(ruraft_bindings_common::transport::CertAndPrivateKey::new(
      cert_chain.0.to_vec(),
      PythonPrivateKey::sec1(pk).into(),
    ))
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

  pub fn cert_and_private_key(&self) -> PythonCertAndPrivateKey {
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

  pub fn cert_and_private_key(&self) -> Option<PythonCertAndPrivateKey> {
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

  pub fn set_domain(&mut self, domain: String) {
    self.0.set_domain(domain);
  }

  pub fn domain(&self) -> &String {
    self.0.domain()
  }

  pub fn set_resolv_conf(&mut self, resolv_conf: Option<PathBuf>) {
    self.0.set_resolv_conf(resolv_conf);
  }

  pub fn resolv_conf(&self) -> Option<&PathBuf> {
    self.0.resolv_conf()
  }

  pub fn set_max_pool(&mut self, max_pool: usize) {
    self.0.set_max_pool(max_pool);
  }

  pub fn max_pool(&self) -> usize {
    self.0.max_pool()
  }

  pub fn set_max_inflight_requests(&mut self, max_idle: usize) {
    self.0.set_max_inflight_requests(max_idle);
  }

  pub fn max_inflight_requests(&self) -> usize {
    self.0.max_inflight_requests()
  }

  pub fn set_tls_server_config(&mut self, server_config: PythonTlsServerConfig) {
    self.0.set_tls_server_config(server_config.into());
  }

  pub fn tls_server_config(&self) -> PythonTlsServerConfig {
    self.0.tls_server_config().clone().into()
  }

  pub fn set_tls_client_config(&mut self, client_config: PythonTlsClientConfig) {
    self.0.set_tls_client_config(client_config.into());
  }

  pub fn tls_client_config(&self) -> PythonTlsClientConfig {
    self.0.tls_client_config().clone().into()
  }

  /// Set the timeout used to apply I/O deadlines.
  pub fn set_timeout(&mut self, timeout: ::chrono::Duration) -> PyResult<()> {
    timeout
      .to_std()
      .map(|timeout| self.0.set_timeout(timeout))
      .map_err(|e| PyTypeError::new_err(e.to_string()))
  }

  /// Returns the timeout used to apply I/O deadlines.
  pub fn timeout(&self) -> PyResult<::chrono::Duration> {
    ::chrono::Duration::from_std(self.0.timeout()).map_err(|e| PyTypeError::new_err(e.to_string()))
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
  module.add_class::<PythonCertAndPrivateKey>()?;
  module.add_class::<PythonTlsServerConfig>()?;
  module.add_class::<PythonTlsClientConfig>()?;
  module.add_class::<PythonTlsTransportOptions>()?;
  Ok(())
}
