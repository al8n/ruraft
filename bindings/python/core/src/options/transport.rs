use pyo3::{exceptions::PyTypeError, types::PyModule, *};

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

use crate::Pyi;

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
    content.push_str(
      r#"
  
  @staticmethod
  def native_tls(opts: NativeTlsTransportOptions) -> TransportOptions:...
  
    "#,
    );

    #[cfg(feature = "tls")]
    content.push_str(
      r#"
  
  @staticmethod
  def tls(opts: TlsTransportOptions) -> TransportOptions:...

    "#,
    );
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
    )
    .into()
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

pub fn register_transport_options(module: &PyModule) -> PyResult<()> {
  module.add_class::<PythonTransportOptions>()?;
  module.add_class::<PythonTcpTransportOptions>()?;
  #[cfg(feature = "native-tls")]
  register_native_tls_transport_options(module)?;

  #[cfg(feature = "tls")]
  register_tls_transport_options(module)?;
  Ok(())
}

pub fn transport_pyi() -> String {
  let mut pyi = String::new();
  pyi.push_str(&PythonTransportOptions::pyi());
  pyi.push_str(&PythonTcpTransportOptions::pyi());
  #[cfg(feature = "native-tls")]
  pyi.push_str(&native_tls::native_tls_transport_pyi());
  #[cfg(feature = "tls")]
  pyi.push_str(&tls::tls_transport_pyi());
  pyi
}
