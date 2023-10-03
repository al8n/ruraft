pub mod file;

#[cfg(any(feature = "test", test))]
#[cfg_attr(docsrs, doc(cfg(feature = "test")))]
pub mod memory;

#[cfg(any(feature = "test", test))]
pub mod tests {
  pub use super::file::tests::*;
  pub use super::memory::tests::*;
}
