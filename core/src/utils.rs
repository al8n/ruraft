use std::{fs::DirBuilder, io, path::Path};

pub mod checksumable;

/// Like [`std::fs::create_dir_all`] but with a mode
#[cfg(unix)]
pub fn make_dir_all<P: AsRef<Path>>(path: &P, mode: u32) -> io::Result<()> {
  use std::os::unix::fs::DirBuilderExt;

  DirBuilder::new().recursive(true).mode(mode).create(path)
}
