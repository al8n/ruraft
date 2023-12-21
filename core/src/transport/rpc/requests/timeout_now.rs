use super::*;

/// The command used by a leader to signal another server to
/// start an election.
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TimeoutNowRequest<I, A> {
  /// The header of the request
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the header of the request"),),
    setter(attrs(doc = "Set the header of the request"),)
  )]
  header: Header<I, A>,
}

impl<I, A> TimeoutNowRequest<I, A> {
  /// Create a new [`TimeoutNowRequest`] with the given protocol version, id, address.
  #[inline]
  pub const fn new(version: ProtocolVersion, id: I, addr: A) -> Self {
    Self {
      header: Header {
        protocol_version: version,
        from: Node::new(id, addr),
      },
    }
  }

  /// Create a new [`TimeoutNowRequest`] with the given protocol version and node.
  #[inline]
  pub const fn from_node(version: ProtocolVersion, node: Node<I, A>) -> Self {
    Self {
      header: Header {
        protocol_version: version,
        from: node,
      },
    }
  }

  /// Create a new [`TimeoutNowRequest`] with the given header.
  #[inline]
  pub const fn from_header(header: Header<I, A>) -> Self {
    Self { header }
  }
}

impl<I: CheapClone, A: CheapClone> CheapClone for TimeoutNowRequest<I, A> {}
