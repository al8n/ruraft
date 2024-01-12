use pyo3::{types::PyModule, *};

mod tcp;
pub use tcp::*;

#[cfg(feature = "tls")]
mod tls;
pub use tls::*;

#[cfg(feature = "native-tls")]
mod native_tls;
#[cfg(feature = "native-tls")]
pub use native_tls::*;

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
