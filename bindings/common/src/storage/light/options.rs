#[cfg(feature = "sled")]
mod sled;
#[cfg(feature = "sled")]
pub use sled::*;

#[cfg(feature = "redb")]
mod redb;
#[cfg(feature = "redb")]
pub use redb::*;

#[cfg(feature = "jammdb")]
mod jammdb;
#[cfg(feature = "jammdb")]
pub use jammdb::*;

#[derive(Clone, Debug, derive_more::From, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(untagged, rename_all = "snake_case"))]
pub enum LightBackendOptions {
  #[cfg(feature = "sled")]
  Sled(SledOptions),
  #[cfg(feature = "redb")]
  Redb(RedbOptions),
  #[cfg(feature = "jammdb")]
  Jammdb(JammdbOptions),
}
