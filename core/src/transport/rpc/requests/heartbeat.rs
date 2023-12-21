use super::*;

/// The heartbeat command.
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct HeartbeatRequest<I, A> {
  /// The header of the request
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the header of the request"),),
    setter(attrs(doc = "Set the header of the request"),)
  )]
  header: Header<I, A>,

  /// Provide the current term and leader
  #[viewit(
    getter(const, attrs(doc = "Get the term of the heartbeat request"),),
    setter(attrs(doc = "Set the term of the heartbeat request"),)
  )]
  term: u64,
}

impl<I, A> HeartbeatRequest<I, A> {
  /// Create a new [`HeartbeatRequest`] with the given protocol version, id, address and default term.
  #[inline]
  pub const fn new(version: ProtocolVersion, id: I, addr: A) -> Self {
    Self {
      header: Header {
        protocol_version: version,
        from: Node::new(id, addr),
      },
      term: 0,
    }
  }

  /// Create a new [`HeartbeatRequest`] with the given protocol version, node and default term.
  #[inline]
  pub const fn from_node(version: ProtocolVersion, node: Node<I, A>) -> Self {
    Self {
      header: Header {
        protocol_version: version,
        from: node,
      },
      term: 0,
    }
  }

  /// Create a new [`HeartbeatRequest`] with the given header and default term.
  #[inline]
  pub const fn from_header(header: Header<I, A>) -> Self {
    Self { header, term: 0 }
  }
}

impl<I: CheapClone, A: CheapClone> CheapClone for HeartbeatRequest<I, A> {}
