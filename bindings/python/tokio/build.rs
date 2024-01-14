use std::path::PathBuf;
use heck::ToSnakeCase;

fn main() {
  let manifest_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
  let manifest = cargo_manifest::Manifest::from_path(manifest_path.join("Cargo.toml")).unwrap();
  let name = manifest
    .package
    .expect("expect package field in Cargo.toml of the crate")
    .name
    .to_snake_case();

  let lib_name = match manifest.lib {
    Some(p) => match p.name {
      Some(n) => n.to_snake_case(),
      None => name,
    },
    None => name,
  };

  let python_path = manifest_path.join("python").join(&lib_name);

  ruraft_python::generate::<_, TokioRuntime>(&lib_name, python_path).unwrap();
}