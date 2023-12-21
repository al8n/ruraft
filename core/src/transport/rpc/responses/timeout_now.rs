use super::*;

/// The response to [`TimeoutNowRequest`].
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TimeoutNowResponse<I, A> {
  /// The header of the response
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the header of the response"),),
    setter(attrs(doc = "Set the header of the response"),)
  )]
  header: Header<I, A>,
}

impl<I, A> TimeoutNowResponse<I, A> { 
  /// Create a new [`TimeoutNowResponse`] with the given `id` and `addr` and `version`.
  #[inline]
  pub const fn new(version: ProtocolVersion, id: I, addr: A) -> Self {
    Self {
      header: Header::new(version, id, addr),
    }
  }

  /// Create a new [`TimeoutNowResponse`] with the given protocol version and node.
  #[inline]
  pub const fn from_node(version: ProtocolVersion, node: Node<I, A>) -> Self {
    Self {
      header: Header::from_node(version, node),
    }
  }

  /// Create a new [`TimeoutNowResponse`] with the given header.
  #[inline]
  pub const fn from_header(header: Header<I, A>) -> Self {
    Self {
      header,
    }
  }
}

impl<I: CheapClone, A: CheapClone> CheapClone for TimeoutNowResponse<I, A> {
  fn cheap_clone(&self) -> Self {
    Self {
      header: self.header.cheap_clone(),
    }
  }
}
