use super::*;

/// The response returned from an
/// [`HeartbeatRequest`].
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct HeartbeatResponse<I, A> {
  /// The header of the response
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the header of the response"),),
    setter(attrs(doc = "Set the header of the response"),)
  )]
  header: Header<I, A>,

  /// We may not succeed if we have a conflicting entry
  #[viewit(
    getter(const, attrs(doc = "Get if the heartbeat success"),),
    setter(attrs(doc = "Set if the heartbeat success"),)
  )]
  success: bool,
}

impl<I, A> HeartbeatResponse<I, A> {
  /// Create a new [`HeartbeatResponse`]
  #[inline]
  pub const fn new(header: Header<I, A>, success: bool) -> Self {
    Self { header, success }
  }
}

impl<I: CheapClone, A: CheapClone> CheapClone for HeartbeatResponse<I, A> {
  fn cheap_clone(&self) -> Self {
    Self {
      header: self.header.cheap_clone(),
      success: self.success,
    }
  }
}
