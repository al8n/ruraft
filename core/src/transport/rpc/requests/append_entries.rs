use super::*;

/// The command used to append entries to the
/// replicated log.
#[viewit::viewit]
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AppendEntriesRequest<I, A, D> {
  /// The header of the request
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the header of the request"),),
    setter(attrs(doc = "Set the header of the request"),)
  )]
  #[cfg_attr(
    feature = "serde",
    serde(
      bound = "I: Eq + ::core::hash::Hash + ::core::fmt::Display + ::serde::Serialize + for<'a> ::serde::Deserialize<'a>, A: Eq + ::core::fmt::Display + ::serde::Serialize + for<'a> ::serde::Deserialize<'a>, D: ::serde::Serialize + for<'a> ::serde::Deserialize<'a>"
    )
  )]
  header: Header<I, A>,

  /// Provide the current term and leader
  #[viewit(
    getter(const, attrs(doc = "Get the term the append request"),),
    setter(attrs(doc = "Set the term the append request"),)
  )]
  term: u64,

  /// Provide the previous entries for integrity checking
  #[viewit(
    getter(
      const,
      attrs(doc = "Get the previous log entry index of the append request"),
    ),
    setter(attrs(doc = "Set the previous log entry index of the append request"),)
  )]
  prev_log_entry: u64,
  /// Provide the previous term for integrity checking
  #[viewit(
    getter(
      const,
      attrs(doc = "Get the previous log entry term of the append request"),
    ),
    setter(attrs(doc = "Set the previous log entry term of the append request"),)
  )]
  prev_log_term: u64,

  /// New entries to commit
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Get the log entries of the append request"),
    ),
    setter(attrs(doc = "Set the log entries of the append request"),)
  )]
  #[cfg_attr(
    feature = "serde",
    serde(
      bound = "I: Eq + ::core::hash::Hash + ::core::fmt::Display + ::serde::Serialize + for<'a> ::serde::Deserialize<'a>, A: Eq + ::core::fmt::Display + ::serde::Serialize + for<'a> ::serde::Deserialize<'a>, D: ::serde::Serialize + for<'a> ::serde::Deserialize<'a>"
    )
  )]
  entries: Vec<Log<I, A, D>>,

  /// Commit index on the leader
  #[viewit(
    getter(
      const,
      attrs(doc = "Get the commit index of the leader of the append request"),
    ),
    setter(attrs(doc = "Set the commit index of the leader of the append request"),)
  )]
  leader_commit: u64,
}

impl<I, A, D> AppendEntriesRequest<I, A, D> {
  /// Create a new [`AppendEntriesRequest`] with the given `id` and `addr` and `version`. Other fields
  /// are set to their default values.
  #[inline]
  pub const fn new(version: ProtocolVersion, id: I, addr: A) -> Self {
    Self {
      header: Header {
        protocol_version: version,
        from: Node::new(id, addr),
      },
      term: 0,
      prev_log_entry: 0,
      prev_log_term: 0,
      entries: Vec::new(),
      leader_commit: 0,
    }
  }

  /// Create a new [`AppendEntriesRequest`] with the given protocol version, node and default values.
  #[inline]
  pub const fn from_node(version: ProtocolVersion, node: Node<I, A>) -> Self {
    Self {
      header: Header {
        protocol_version: version,
        from: node,
      },
      term: 0,
      prev_log_entry: 0,
      prev_log_term: 0,
      entries: Vec::new(),
      leader_commit: 0,
    }
  }

  /// Create a new [`AppendEntriesRequest`] with the given header and default values.
  #[inline]
  pub const fn from_header(header: Header<I, A>) -> Self {
    Self {
      header,
      term: 0,
      prev_log_entry: 0,
      prev_log_term: 0,
      entries: Vec::new(),
      leader_commit: 0,
    }
  }
}
