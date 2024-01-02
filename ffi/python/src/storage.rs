use pyo3::prelude::*;

pub mod light;
pub mod snapshot;

/// Expose [`ruraft-lightwal`](https://crates.io/crates/ruraft-lightwal) to a Python module.
#[pymodule]
pub fn storage(py: Python, m: &PyModule) -> PyResult<()> {
  m.add_submodule(light::submodule(py)?)?;
  m.add_class::<snapshot::SnapshotId>()?;

  #[cfg(feature = "tokio")]
  {
    let tokio = PyModule::new(py, "tokio")?;
    tokio.add_class::<snapshot::tokio::FileSnapshotSink>()?;
  }

  #[cfg(feature = "async-std")]
  {
    let astd = PyModule::new(py, "async_std")?;
    astd.add_class::<snapshot::async_std::FileSnapshotSink>()?;
  }
 
  Ok(())
}

// This function creates and returns the sled submodule.
pub fn submodule(py: Python) -> PyResult<&PyModule> {
  let module = PyModule::new(py, "storage")?;
  storage(py, module)?;
  Ok(module)
}
