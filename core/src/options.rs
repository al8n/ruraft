#![allow(missing_docs)]

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

/// The version of the protocol (which includes RPC messages
/// as well as Raft-specific log entries) that this server can _understand_. Use
/// the ProtocolVersion member of the [`Options`] to control the version of
/// the protocol to use when _speaking_ to other servers. Note that depending on
/// the protocol version being spoken, some otherwise understood RPC messages
/// may be refused. See dispositionRPC for details of this logic.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize_repr, Deserialize_repr)]
#[repr(u8)]
pub enum ProtocolVersion {
  /// The current version of the protocol.
  V1 = 1,
}

/// The version of snapshots that this server can understand.
/// Currently, it is always assumed that the server generates the latest version,
/// though this may be changed in the future to include a configurable version.
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Hash, Serialize_repr, Deserialize_repr)]
#[repr(u8)]
pub enum SnapshotVersion {
  /// The current version of the protocol.
  #[default]
  V1 = 1,
}

impl SnapshotVersion {
  #[inline]
  pub const fn valid(&self) -> bool {
    match self {
      SnapshotVersion::V1 => true,
    }
  }
}

/// Provides any necessary configuration for the Raft server.
#[viewit::viewit]
#[derive(Serialize, Deserialize)]
pub struct Options {
  /// Allows a Raft server to inter-operate with older
  /// Raft servers running an older version of the code. This is used to
  /// version the wire protocol as well as Raft-specific log entries that
  /// the server uses when _speaking_ to other servers. There is currently
  /// no auto-negotiation of versions so all servers must be manually
  /// configured with compatible versions. See ProtocolVersionMin and
  /// ProtocolVersionMax for the versions of the protocol that this server
  // can _understand_.
  protocol_version: ProtocolVersion,

  #[serde(with = "humantime_serde")]
  heartbeat_timeout: Duration,

  /// Specifies the time in candidate state without contact
  /// from a leader before we attempt an election.
  #[serde(with = "humantime_serde")]
  election_timeout: Duration,

  /// Specifies the time without an Apply operation before the
  /// leader sends an AppendEntry RPC to followers, to ensure a timely commit of
  /// log entries.
  /// Due to random staggering, may be delayed as much as 2x this value.
  #[serde(with = "humantime_serde")]
  commit_timeout: Duration,

  /// Controls the maximum number of append entries
  /// to send at once. We want to strike a balance between efficiency
  /// and avoiding waste if the follower is going to reject because of
  /// an inconsistent log.
  max_append_entries: usize,

  /// Indicates whether we should buffer apply channel
  /// to size `max_append_entries`. This enables batch log commitment,
  /// but breaks the timeout guarantee on `apply`. Specifically,
  /// a log can be added to the apply channel buffer but not actually be
  /// processed until after the specified timeout.
  batch_apply: bool,

  /// If we are a member of a cluster, and `remove_peer` is invoked for the
  /// local node, then we forget all peers and transition into the follower state.
  /// If `shutdown_on_remove` is set, we additional shutdown Raft. Otherwise,
  /// we can become a leader of a cluster containing only this node.
  shutdown_on_remove: bool,

  /// Controls how many logs we leave after a snapshot. This is used
  /// so that we can quickly replay logs on a follower instead of being forced to
  /// send an entire snapshot. The value passed here is the initial setting used.
  /// This can be tuned during operation using `reload_config`.
  trailing_logs: u64,

  /// Controls how often we check if we should perform a
  /// snapshot. We randomly stagger between this value and 2x this value to avoid
  /// the entire cluster from performing a snapshot at once. The value passed
  /// here is the initial setting used. This can be tuned during operation using
  /// `reload_config`.
  #[serde(with = "humantime_serde")]
  snapshot_interval: Duration,

  /// Controls how many outstanding logs there must be before
  /// we perform a snapshot. This is to prevent excessive snapshotting by
  /// replaying a small set of logs instead. The value passed here is the initial
  /// setting used. This can be tuned during operation using `reload_config`.
  snapshot_threshold: u64,

  /// Used to control how long the "lease" lasts
  /// for being the leader without being able to contact a quorum
  /// of nodes. If we reach this interval without contact, we will
  /// step down as leader.
  #[serde(with = "humantime_serde")]
  leader_lease_timeout: Duration,

  /// Controls if raft will restore a snapshot to the
  /// FSM on start. This is useful if your FSM recovers from other mechanisms
  /// than raft snapshotting. Snapshot metadata will still be used to initialize
  /// raft's configuration and index values.
  no_snapshot_restore_on_start: bool,
}
