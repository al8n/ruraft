use super::*;

/// The command sent to a Raft peer to bootstrap its
/// log (and state machine) from a snapshot on another peer.
#[viewit::viewit]
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct InstallSnapshotRequest<I, A> {
  /// The header of the request
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the header of the request"),),
    setter(attrs(doc = "Set the header of the request"),)
  )]
  #[cfg_attr(
    feature = "serde",
    serde(
      bound = "I: Eq + ::core::hash::Hash + ::core::fmt::Display + ::serde::Serialize + for<'a> ::serde::Deserialize<'a>, A: Eq + ::core::fmt::Display + ::serde::Serialize + for<'a> ::serde::Deserialize<'a>"
    )
  )]
  header: Header<I, A>,

  /// The snapshot version
  #[viewit(
    getter(const, attrs(doc = "Get the version of the install snapshot request"),),
    setter(attrs(doc = "Set the version of the install snapshot request"),)
  )]
  snapshot_version: SnapshotVersion,

  /// The term
  #[viewit(
    getter(const, attrs(doc = "Get the term of the install snapshot request"),),
    setter(attrs(doc = "Set the term of the install snapshot request"),)
  )]
  term: u64,

  /// The last index included in the snapshot
  #[viewit(
    getter(const, attrs(doc = "Get the last index included in the snapshot"),),
    setter(attrs(doc = "Set the last index included in the snapshot"),)
  )]
  last_log_index: u64,

  /// The last term included in the snapshot
  #[viewit(
    getter(const, attrs(doc = "Get the last term included in the snapshot"),),
    setter(attrs(doc = "Set the last term included in the snapshot"),)
  )]
  last_log_term: u64,

  /// Cluster membership.
  #[cfg_attr(
    feature = "serde",
    serde(
      bound = "I: Eq + ::core::hash::Hash + ::core::fmt::Display + ::serde::Serialize + for<'a> ::serde::Deserialize<'a>, A: Eq + ::core::fmt::Display + ::serde::Serialize + for<'a> ::serde::Deserialize<'a>"
    )
  )]
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Get the [`Membership`] of the install snapshot request"),
    ),
    setter(attrs(doc = "Set the [`Membership`] of the install snapshot request"),)
  )]
  membership: Membership<I, A>,

  /// Log index where [`Membership`] entry was originally written.
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Get log index where [`Membership`] entry was originally written of the install snapshot request"
      ),
    ),
    setter(attrs(
      doc = "Set log index where [`Membership`] entry was originally written of the install snapshot request"
    ),)
  )]
  membership_index: u64,

  /// Size of the snapshot
  #[viewit(
    getter(const, attrs(doc = "Get the size of the snapshot"),),
    setter(attrs(doc = "Set the size of the snapshot"),)
  )]
  size: u64,
}

impl<I, A> InstallSnapshotRequest<I, A> {
  /// Create a new [`InstallSnapshotRequest`] with the given `version`, `id`, `addr` and `membership`. Other fields
  /// are set to their default values.
  #[inline]
  pub const fn new(version: ProtocolVersion, id: I, addr: A, membership: Membership<I, A>) -> Self {
    Self {
      header: Header {
        protocol_version: version,
        from: Node::new(id, addr),
      },
      term: 0,
      snapshot_version: SnapshotVersion::V1,
      last_log_index: 0,
      last_log_term: 0,
      membership,
      membership_index: 0,
      size: 0,
    }
  }

  /// Create a new [`InstallSnapshotRequest`] with the given protocol version, node, membership and default values.
  #[inline]
  pub const fn from_node(
    version: ProtocolVersion,
    node: Node<I, A>,
    membership: Membership<I, A>,
  ) -> Self {
    Self {
      header: Header {
        protocol_version: version,
        from: node,
      },
      term: 0,
      snapshot_version: SnapshotVersion::V1,
      last_log_index: 0,
      last_log_term: 0,
      membership,
      membership_index: 0,
      size: 0,
    }
  }

  /// Create a new [`InstallSnapshotRequest`] with the given header, membership and default values.
  #[inline]
  pub const fn from_header(header: Header<I, A>, membership: Membership<I, A>) -> Self {
    Self {
      header,
      term: 0,
      snapshot_version: SnapshotVersion::V1,
      last_log_index: 0,
      last_log_term: 0,
      membership,
      membership_index: 0,
      size: 0,
    }
  }
}

impl<I: CheapClone, A: CheapClone> CheapClone for InstallSnapshotRequest<I, A> {}
