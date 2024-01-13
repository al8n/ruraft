use pyo3::{types::PyModule, *, exceptions::PyTypeError};

mod tcp;
pub use tcp::*;

#[cfg(feature = "tls")]
mod tls;
#[cfg(feature = "tls")]
pub use tls::*;

#[cfg(feature = "native-tls")]
mod native_tls;
#[cfg(feature = "native-tls")]
pub use native_tls::*;

use crate::{Pyi, register_type};

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[pyclass]
pub struct PythonTransportOptions(ruraft_bindings_common::transport::SupportedTransportOptions);

impl From<PythonTransportOptions> for ruraft_bindings_common::transport::SupportedTransportOptions {
  fn from(opts: PythonTransportOptions) -> Self {
    opts.0
  }
}

impl Pyi for PythonTransportOptions {
  fn pyi() -> std::borrow::Cow<'static, str> {
    let mut content = String::new();

    #[cfg(feature = "native-tls")]
    content.push_str(r#"
  
  @staticmethod
  def native_tls(opts: NativeTlsTransportOptions) -> TransportOptions:...
  
    "#);

    #[cfg(feature = "tls")]
    content.push_str(r#"
  
  @staticmethod
  def tls(opts: TlsTransportOptions) -> TransportOptions:...

    "#);
    #[cfg(feature = "native-tls")]

    format!(
r#"
  
class TransportOptions:
  @staticmethod
  def tcp(opts: TcpTransportOptions) -> TransportOptions:...

  {content}

  def __str__(self) -> str:...

  def __repr__(self) -> str:...

"#
    ).into()
  }
}


#[pymethods]
impl PythonTransportOptions {
  #[staticmethod]
  pub fn tcp(opts: PythonTcpTransportOptions) -> Self {
    Self(ruraft_bindings_common::transport::SupportedTransportOptions::Tcp(opts.into()))
  }

  #[staticmethod]
  #[cfg(feature = "native-tls")]
  pub fn native_tls(opts: PythonNativeTlsTransportOptions) -> Self {
    Self(ruraft_bindings_common::transport::SupportedTransportOptions::NativeTls(opts.into()))
  }

  #[staticmethod]
  #[cfg(feature = "tls")]
  pub fn tls(opts: PythonTlsTransportOptions) -> Self {
    Self(ruraft_bindings_common::transport::SupportedTransportOptions::Tls(opts.into()))
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

pub fn register_transport_options(module: &PyModule) -> PyResult<String> {
  let mut pyi = String::new();
  register_type::<PythonTransportOptions>(&mut pyi, module)?;
  register_type::<PythonTcpTransportOptions>(&mut pyi, module)?;
  #[cfg(feature = "native-tls")]
  pyi.push_str(&register_native_tls_transport_options(module)?);
  #[cfg(feature = "tls")]
  pyi.push_str(&register_tls_transport_options(module)?);
  Ok(pyi)
}
