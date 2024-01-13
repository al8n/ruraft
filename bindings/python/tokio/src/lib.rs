use std::path::PathBuf;

use heck::ToSnakeCase;
use pyo3::prelude::*;
use ruraft_python::*;

pub fn rewrite_on_modified(path: &std::path::PathBuf, new_content: &str) -> std::io::Result<()> {
  use std::fs::{File, OpenOptions};
  use std::io::prelude::*;
  use std::io::BufReader;
  use std::io::SeekFrom;

  let mut file = OpenOptions::new().create(true).read(true).write(true).open(path)?;

  let mut contents = String::new();
  file.read_to_string(&mut contents)?;

  let modified = contents.eq(new_content);
  if modified {
    file.seek(SeekFrom::Start(0))?;
    file.write_all(new_content.as_bytes())?;
    file.flush()?;
  }

  Ok(())
}

/// Expose [`ruraft`](https://crates.io/crates/ruraft) Raft protocol implementation to a Python module.
#[pymodule]
pub fn _internal(py: Python, m: &PyModule) -> PyResult<()> {
  let manifest_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
  let manifest = cargo_manifest::Manifest::from_path(manifest_path.join("Cargo.toml")).unwrap();
  let name = manifest.package.expect("expect package field in Cargo.toml of the crate").name.to_snake_case();

  let lib_name = match manifest.lib {
    Some(p) => match p.name {
      Some(n) => n.to_snake_case(),
      None => name,
    },
    None => name,
  };

  let python_path = manifest_path.join("python").join(&lib_name);

  let typem = types::register(py)?;
  types::register_tokio(typem)?;
  m.add_submodule(typem)?;
  py.import("sys")?
    .getattr("modules")?
    .set_item("tokio_raft.types", typem)?;

  let (pyi, membershipm) = types::membership::register(py)?;
  m.add_submodule(membershipm)?;
  py.import("sys")?
    .getattr("modules")?
    .set_item("tokio_raft.membership", membershipm)?;
  rewrite_on_modified(&python_path.join("membership.pyi"), pyi.as_str())?;

  let (pyi, optionsm) = options::register(py)?;
  m.add_submodule(optionsm)?;
  py.import("sys")?
    .getattr("modules")?
    .set_item("tokio_raft.options", optionsm)?;
  rewrite_on_modified(&python_path.join("options.pyi"), pyi.as_str())?;

  let pyi = fsm::pyi();
  rewrite_on_modified(&python_path.join("fsm.pyi"), pyi)?;
  let py = fsm::py();
  rewrite_on_modified(&python_path.join("fsm.py"), py)?;

  m.add_class::<raft::TokioRaft>()?;


  Ok(())
}
