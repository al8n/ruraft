use pyo3::prelude::*;
use ruraft_python::*;

#[pymodule]
pub fn _internal(py: Python, m: &PyModule) -> PyResult<()> {
  register::<TokioRuntime>(py, m)
}
