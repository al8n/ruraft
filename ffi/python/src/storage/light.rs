use pyo3::prelude::*;

#[cfg(feature = "sled")]
mod sled;

#[cfg(feature = "redb")]
mod redb;

#[cfg(feature = "jammdb")]
mod jammdb;

/// Expose [`ruraft-lightwal`](https://crates.io/crates/ruraft-lightwal) to a Python module.
#[pymodule]
pub fn light(py: Python, m: &PyModule) -> PyResult<()> {
  #[cfg(feature = "sled")]
  m.add_submodule(sled::submodule(py)?)?;
  #[cfg(feature = "redb")]
  m.add_submodule(redb::submodule(py)?)?;
  #[cfg(feature = "jammdb")]
  m.add_submodule(jammdb::submodule(py)?)?;
  Ok(())
}

// This function creates and returns the sled submodule.
pub fn submodule(py: Python) -> PyResult<&PyModule> {
  let module = PyModule::new(py, "light")?;
  light(py, module)?;
  Ok(module)
}
