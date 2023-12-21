use super::*;

/// The response returned from an
/// [`HeartbeatRequest`].
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct HeartbeatResponse<I, A> {
  /// The header of the response
  #[viewit(getter(const))]
  header: Header<I, A>,

  /// We may not succeed if we have a conflicting entry
  success: bool,
}

impl<I, A> HeartbeatResponse<I, A> {
  /// Create a new HeartbeatResponse
  pub const fn new(header: Header<I, A>, success: bool) -> Self {
    Self { header, success }
  }
}
