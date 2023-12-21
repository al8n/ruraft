use super::*;

/// The command used by a candidate to ask a Raft peer
/// for a vote in an election.
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct VoteRequest<I, A> {
  /// The header of the request
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the header of the request"),),
    setter(attrs(doc = "Set the header of the request"),)
  )]
  header: Header<I, A>,

  /// The term of the candidate
  #[viewit(
    getter(const, attrs(doc = "Get the term of the candidate"),),
    setter(attrs(doc = "Set the term of the candidate"),)
  )]
  term: u64,

  /// The index of the candidate's last log entry
  #[viewit(
    getter(const, attrs(doc = "Get the index of the candidate's last log entry"),),
    setter(attrs(doc = "Set the index of the candidate's last log entry"),)
  )]
  last_log_index: u64,

  /// The term of the candidate's last log entry
  #[viewit(
    getter(const, attrs(doc = "Get the term of the candidate's last log entry"),),
    setter(attrs(doc = "Set the term of the candidate's last log entry"),)
  )]
  last_log_term: u64,

  /// Used to indicate to peers if this vote was triggered by a leadership
  /// transfer. It is required for leadership transfer to work, because servers
  /// wouldn't vote otherwise if they are aware of an existing leader.
  #[viewit(
    getter(
      const,
      attrs(doc = "Get if this vote was triggered by a leadership transfer"),
    ),
    setter(attrs(doc = "Set if this vote was triggered by a leadership transfer"),)
  )]
  leadership_transfer: bool,
}

impl<I, A> VoteRequest<I, A> {
  /// Create a new [`VoteRequest`] with the given `version`, `id`, and `addr`. Other fields
  /// are set to their default values.
  #[inline]
  pub const fn new(version: ProtocolVersion, id: I, addr: A) -> Self {
    Self {
      header: Header {
        protocol_version: version,
        from: Node::new(id, addr),
      },
      term: 0,
      last_log_index: 0,
      last_log_term: 0,
      leadership_transfer: false,
    }
  }

  /// Create a new [`VoteRequest`] with the given protocol version, node and default values.
  #[inline]
  pub const fn from_node(version: ProtocolVersion, node: Node<I, A>) -> Self {
    Self {
      header: Header {
        protocol_version: version,
        from: node,
      },
      term: 0,
      last_log_index: 0,
      last_log_term: 0,
      leadership_transfer: false,
    }
  }

  /// Create a new [`VoteRequest`] with the given header and default values.
  #[inline]
  pub const fn from_header(header: Header<I, A>) -> Self {
    Self {
      header,
      term: 0,
      last_log_index: 0,
      last_log_term: 0,
      leadership_transfer: false,
    }
  }
}

impl<I: CheapClone, A: CheapClone> CheapClone for VoteRequest<I, A> {}
