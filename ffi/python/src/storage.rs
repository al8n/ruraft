use pyo3::prelude::*;

mod light;
mod snapshot;

/// Expose [`ruraft-lightwal`](https://crates.io/crates/ruraft-lightwal) to a Python module.
#[pymodule]
pub fn storage(py: Python, m: &PyModule) -> PyResult<()> {
  m.add_submodule(light::submodule(py)?)?;
  m.add_submodule(snapshot::submodule(py)?)?;
  Ok(())
}

// This function creates and returns the sled submodule.
pub fn submodule(py: Python) -> PyResult<&PyModule> {
  let module = PyModule::new(py, "storage")?;
  storage(py, module)?;
  Ok(module)
}
