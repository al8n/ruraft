use super::*;

/// The response returned from an
/// [`InstallSnapshotRequest`].
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct InstallSnapshotResponse<I, A> {
  /// The header of the response
  #[viewit(getter(const))]
  header: Header<I, A>,

  /// The term
  term: u64,

  /// Successfully install the snapshot or not.
  success: bool,
}
