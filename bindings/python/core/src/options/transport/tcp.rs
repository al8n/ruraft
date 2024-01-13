use std::{
  hash::{Hash, Hasher},
  net::SocketAddr,
  path::PathBuf,
};

use crate::{types::Header, Pyi};
use pyo3::{exceptions::PyTypeError, *};

/// Options for the TCP transport.
#[derive(Debug, Clone, Eq, PartialEq, Hash, derive_more::From)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[pyclass(name = "TcpTransportOptions")]
pub struct PythonTcpTransportOptions(ruraft_bindings_common::transport::TcpTransportOptions);

impl From<PythonTcpTransportOptions> for ruraft_bindings_common::transport::TcpTransportOptions {
  fn from(opts: PythonTcpTransportOptions) -> Self {
    opts.0
  }
}

impl Pyi for PythonTcpTransportOptions {
  fn pyi() -> std::borrow::Cow<'static, str> {
    r#"

class TcpTransportOptions:
  def __init__(self, header: Header, bind_addr: str) -> None: ...

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

  def __eq__(self, __value: TcpTransportOptions) -> bool: ...
  
  def __ne__(self, __value: TcpTransportOptions) -> bool: ...
  
  def __hash__(self) -> int: ...
  
  def __str__(self) -> str: ...
  
  def __repr__(self) -> str: ...

"#
    .into()
  }
}

#[pymethods]
impl PythonTcpTransportOptions {
  /// Creates a new `TcpTransportOptions` with the default configuration.
  #[new]
  pub fn new(header: Header, bind_addr: &str) -> PyResult<Self> {
    let addr = bind_addr
      .parse::<SocketAddr>()
      .map_err(|e| PyTypeError::new_err(e.to_string()))?;
    Ok(Self(
      ruraft_bindings_common::transport::TcpTransportOptions::new(header.into(), addr),
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
