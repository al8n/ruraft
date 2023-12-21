use super::*;

/// The response returned from an
/// [`InstallSnapshotRequest`].
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct InstallSnapshotResponse<I, A> {
  /// The header of the response
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the header of the response."),),
    setter(attrs(doc = "Set the header of the response."),)
  )]
  header: Header<I, A>,

  /// The term
  #[viewit(
    getter(const, attrs(doc = "Get the newer term if leader is out of date."),),
    setter(attrs(doc = "Set the newer term if leader is out of date."),)
  )]
  term: u64,

  /// Successfully install the snapshot or not.
  #[viewit(
    getter(const, attrs(doc = "Get if successfully install the snapshot or not."),),
    setter(attrs(doc = "Set if successfully install the snapshot or not."),)
  )]
  success: bool,
}

impl<I, A> InstallSnapshotResponse<I, A> {
  /// Create a new [`InstallSnapshotResponse`] with the given `id` and `addr` and `version`. Other fields
  /// are set to their default values.
  #[inline]
  pub const fn new(version: ProtocolVersion, id: I, addr: A) -> Self {
    Self {
      header: Header::new(version, id, addr),
      term: 0,
      success: false,
    }
  }

  /// Create a new [`InstallSnapshotResponse`] with the given protocol version, node and default values.
  #[inline]
  pub const fn from_node(version: ProtocolVersion, node: Node<I, A>) -> Self {
    Self {
      header: Header::from_node(version, node),
      term: 0,
      success: false,
    }
  }

  /// Create a new [`InstallSnapshotResponse`] with the given header and default values.
  #[inline]
  pub const fn from_header(header: Header<I, A>) -> Self {
    Self {
      header,
      term: 0,
      success: false,
    }
  }
}

impl<I: CheapClone, A: CheapClone> CheapClone for InstallSnapshotResponse<I, A> {
  fn cheap_clone(&self) -> Self {
    Self {
      header: self.header.cheap_clone(),
      term: self.term,
      success: self.success,
    }
  }
}
