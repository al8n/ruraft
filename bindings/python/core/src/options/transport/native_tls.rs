use std::{path::PathBuf, hash::{Hash, Hasher}};

use pyo3::{*, exceptions::PyTypeError};

/// Identity used for TLS.
#[derive(Debug, Clone, Eq, PartialEq, Hash, derive_more::From)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[pyclass]
pub struct Identity(ruraft_bindings_common::transport::Identity);

impl From<Identity> for ruraft_bindings_common::transport::Identity {
  fn from(identity: Identity) -> Self {
    identity.0
  }
}

#[pymethods]
impl Identity {
  #[staticmethod]
  pub fn pkcs12(cert: Vec<u8>, password: String) -> Self {
    Self(ruraft_bindings_common::transport::Identity::pkcs12(cert, password))
  }

  #[staticmethod]
  pub fn pkcs8(cert: Vec<u8>, key: Vec<u8>) -> Self {
    Self(ruraft_bindings_common::transport::Identity::pkcs8(cert, key))
  }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, derive_more::From)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[pyclass]
pub struct NativeTlsTransportOptions(ruraft_bindings_common::transport::NativeTlsTransportOptions);

impl From<NativeTlsTransportOptions> for ruraft_bindings_common::transport::NativeTlsTransportOptions {
  fn from(opts: NativeTlsTransportOptions) -> Self {
    opts.0
  }
}


#[pymethods]
impl NativeTlsTransportOptions {
  /// Creates a new `NativeTlsTransportOptions` with the default configuration.
  /// 
  /// Arguments:
  ///   domain: The domain name of the server.
  ///   identity: The identity used for TLS.
  #[new]
  pub fn new(domain: String, identity: Identity) -> Self {
    Self(ruraft_bindings_common::transport::NativeTlsTransportOptions::new(domain, identity.into()))
  }

  /// Sets the domain name of the server.
  pub fn set_domain(&mut self, domain: String) {
    self.set_domain(domain);
  }

  /// Returns the domain name of the server.
  pub fn domain(&self) -> &String {
    self.0.domain()
  }

  /// Sets the identity used for TLS.
  pub fn set_identity(&mut self, identity: Identity) {
    self.0.set_identity(identity.into());
  }

  /// Returns the identity used for TLS.
  pub fn identity(&self) -> Identity {
    self.0.identity().clone().into()
  }

  /// Sets the path to the resolv.conf file, this is used for DNS address resolve.
  /// If you can make sure all addresses you used in the
  /// Raft cluster is a socket address, then you can ignore this option.
  pub fn set_resolv_conf(&mut self, resolv_conf: Option<PathBuf>) {
    self.0.set_resolv_conf(resolv_conf);
  }

  /// Returns the path to the resolv.conf file.
  pub fn resolv_conf(&self) -> Option<&PathBuf> {
    self.0.resolv_conf()
  }

  /// Sets the maximum number of connections to keep in the connection pool.
  pub fn set_max_pool(&mut self, max_pool: usize) {
    self.0.set_max_pool(max_pool);
  }

  /// Returns the maximum number of connections to keep in the connection pool.
  pub fn max_pool(&self) -> usize {
    self.0.max_pool()
  }

  /// Sets the maximum number of in-flight append entries requests.
  pub fn set_max_inflight_requests(&mut self, max_idle: usize) {
    self.0.set_max_inflight_requests(max_idle);
  }

  /// Returns the maximum number of in-flight append entries requests.
  pub fn max_inflight_requests(&self) -> usize {
    self.0.max_inflight_requests()
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

