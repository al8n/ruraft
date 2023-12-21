use super::*;

/// The command used to append entries to the
/// replicated log.
#[viewit::viewit]
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AppendEntriesRequest<I, A, D> {
  /// The header of the request
  #[viewit(getter(const))]
  #[cfg_attr(
    feature = "serde",
    serde(
      bound = "I: Eq + ::core::hash::Hash + ::core::fmt::Display + ::serde::Serialize + for<'a> ::serde::Deserialize<'a>, A: Eq + ::core::fmt::Display + ::serde::Serialize + for<'a> ::serde::Deserialize<'a>, D: ::serde::Serialize + for<'a> ::serde::Deserialize<'a>"
    )
  )]
  header: Header<I, A>,

  /// Provide the current term and leader
  term: u64,

  /// Provide the previous entries for integrity checking
  prev_log_entry: u64,
  /// Provide the previous term for integrity checking
  prev_log_term: u64,

  /// New entries to commit
  #[cfg_attr(
    feature = "serde",
    serde(
      bound = "I: Eq + ::core::hash::Hash + ::core::fmt::Display + ::serde::Serialize + for<'a> ::serde::Deserialize<'a>, A: Eq + ::core::fmt::Display + ::serde::Serialize + for<'a> ::serde::Deserialize<'a>, D: ::serde::Serialize + for<'a> ::serde::Deserialize<'a>"
    )
  )]
  entries: Vec<Log<I, A, D>>,

  /// Commit index on the leader
  leader_commit: u64,
}

impl<I, A, D> AppendEntriesRequest<I, A, D> {
  /// Create a new [`AppendEntriesRequest`] with the given `id` and `addr` and `version`. Other fields
  /// are set to their default values.
  pub fn new(version: ProtocolVersion, id: I, addr: A) -> Self {
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
}
