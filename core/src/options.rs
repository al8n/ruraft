use ruraft_utils::duration::PadDuration;
use std::time::Duration;

/// The version of the protocol (which includes RPC messages
/// as well as Raft-specific log entries) that this server can _understand_. Use
/// the ProtocolVersion member of the [`Options`] to control the version of
/// the protocol to use when _speaking_ to other servers. Note that depending on
/// the protocol version being spoken, some otherwise understood RPC messages
/// may be refused. See dispositionRPC for details of this logic.
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Hash)]
#[repr(u8)]
#[non_exhaustive]
#[cfg_attr(
  feature = "serde",
  derive(serde_repr::Serialize_repr, serde_repr::Deserialize_repr)
)]
pub enum ProtocolVersion {
  /// The current version of the protocol.
  #[default]
  V1 = 1,
}

/// UnknownProtocolVersion is returned when a protocol version is
/// requested that is not understood by the server.
#[derive(Debug)]
pub struct UnknownProtocolVersion(u8);

impl UnknownProtocolVersion {
  /// Returns the version that was requested in byte.
  #[inline]
  pub const fn version(&self) -> u8 {
    self.0
  }
}

impl core::fmt::Display for UnknownProtocolVersion {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "unknown protocol version {}", self.0)
  }
}

impl std::error::Error for UnknownProtocolVersion {}

impl TryFrom<u8> for ProtocolVersion {
  type Error = UnknownProtocolVersion;

  #[inline]
  fn try_from(value: u8) -> Result<Self, Self::Error> {
    match value {
      1 => Ok(ProtocolVersion::V1),
      val => Err(UnknownProtocolVersion(val)),
    }
  }
}

/// The version of snapshots that this server can understand.
/// Currently, it is always assumed that the server generates the latest version,
/// though this may be changed in the future to include a configurable version.
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(
  feature = "serde",
  derive(serde_repr::Serialize_repr, serde_repr::Deserialize_repr)
)]
#[repr(u8)]
#[non_exhaustive]
pub enum SnapshotVersion {
  /// The current version of the protocol.
  #[default]
  V1 = 1,
}

impl SnapshotVersion {
  /// Returns true if the snapshot version is valid.
  #[inline]
  pub const fn valid(&self) -> bool {
    match self {
      SnapshotVersion::V1 => true,
    }
  }
}

/// UnknownSnapshotVersion is returned when a snapshot version is
/// requested that is not understood by the server.
#[derive(Debug)]
pub struct UnknownSnapshotVersion(u8);

impl UnknownSnapshotVersion {
  /// Returns the version that was requested in byte.
  #[inline]
  pub const fn version(&self) -> u8 {
    self.0
  }
}

impl core::fmt::Display for UnknownSnapshotVersion {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "unknown snapshot version {}", self.0)
  }
}

impl std::error::Error for UnknownSnapshotVersion {}

impl TryFrom<u8> for SnapshotVersion {
  type Error = UnknownSnapshotVersion;

  #[inline]
  fn try_from(value: u8) -> Result<Self, Self::Error> {
    match value {
      1 => Ok(SnapshotVersion::V1),
      val => Err(UnknownSnapshotVersion(val)),
    }
  }
}

/// Provides any necessary configuration for the Raft server.
#[viewit::viewit(
  vis_all = "pub(crate)",
  getters(vis_all = "pub"),
  setters(vis_all = "pub", prefix = "with")
)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Options {
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Get the time in follower state without a leader before we attempt an election."
      ),
    ),
    setter(attrs(
      doc = "Set the time in follower state without a leader before we attempt an election. (Builder pattern)"
    ),)
  )]
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  heartbeat_timeout: Duration,

  /// Specifies the time in candidate state without contact
  /// from a leader before we attempt an election.
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Get the time in candidate state without a leader before we attempt an election."
      ),
    ),
    setter(attrs(
      doc = "Set the time in candidate state without a leader before we attempt an election. (Builder pattern)"
    ),)
  )]
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  election_timeout: Duration,

  /// Specifies the time without an Apply operation before the
  /// leader sends an AppendEntry RPC to followers, to ensure a timely commit of
  /// log entries.
  /// Due to random staggering, may be delayed as much as 2x this value.
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Get the time without an Apply operation before the leader sends an AppendEntry RPC to followers, to ensure a timely commit of log entries. Due to random staggering, may be delayed as much as 2x this value."
      ),
    ),
    setter(attrs(
      doc = "Set the time without an Apply operation before the leader sends an AppendEntry RPC to followers, to ensure a timely commit of log entries. Due to random staggering, may be delayed as much as 2x this value. (Builder pattern)"
    ),)
  )]
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  commit_timeout: Duration,

  /// Controls the maximum number of append entries
  /// to send at once. We want to strike a balance between efficiency
  /// and avoiding waste if the follower is going to reject because of
  /// an inconsistent log.
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Get the maximum number of append entries to send at once. We want to strike a balance between efficiency and avoiding waste if the follower is going to reject because of an inconsistent log."
      ),
    ),
    setter(attrs(
      doc = "Set the maximum number of append entries to send at once. We want to strike a balance between efficiency and avoiding waste if the follower is going to reject because of an inconsistent log. (Builder pattern)"
    ),)
  )]
  max_append_entries: usize,

  /// Indicates whether we should buffer apply channel
  /// to size `max_append_entries`. This enables batch log commitment,
  /// but breaks the timeout guarantee on `apply`. Specifically,
  /// a log can be added to the apply channel buffer but not actually be
  /// processed until after the specified timeout.
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Get whether we should buffer apply channel to size `max_append_entries`. This enables batch log commitment,but breaks the timeout guarantee on `apply`. Specifically, a log can be added to the apply channel buffer but not actually be processed until after the specified timeout."
      ),
    ),
    setter(attrs(
      doc = "Set whether we should buffer apply channel to size `max_append_entries`. This enables batch log commitment,but breaks the timeout guarantee on `apply`. Specifically, a log can be added to the apply channel buffer but not actually be processed until after the specified timeout. (Builder pattern)"
    ),)
  )]
  batch_apply: bool,

  /// If we are a member of a cluster, and `remove_peer` is invoked for the
  /// local node, then we forget all peers and transition into the follower state.
  /// If `shutdown_on_remove` is set, we additional shutdown Raft. Otherwise,
  /// we can become a leader of a cluster containing only this node.
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Get if we are a member of a cluster, and `remove_peer` is invoked for the local node, then we forget all peers and transition into the follower state. If `shutdown_on_remove` is set, we additional shutdown Raft. Otherwise, we can become a leader of a cluster containing only this node."
      ),
    ),
    setter(attrs(
      doc = "Set if we are a member of a cluster, and `remove_peer` is invoked for the local node, then we forget all peers and transition into the follower state. If `shutdown_on_remove` is set, we additional shutdown Raft. Otherwise, we can become a leader of a cluster containing only this node. (Builder pattern)"
    ),)
  )]
  shutdown_on_remove: bool,

  /// Controls how many logs we leave after a snapshot. This is used
  /// so that we can quickly replay logs on a follower instead of being forced to
  /// send an entire snapshot. The value passed here is the initial setting used.
  /// This can be tuned during operation using `reload_config`.
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Get how many logs we leave after a snapshot. This is used so that we can quickly replay logs on a follower instead of being forced to send an entire snapshot. The value passed here is the initial setting used. This can be tuned during operation using `reload_config`."
      ),
    ),
    setter(attrs(
      doc = "Set how many logs we leave after a snapshot. This is used so that we can quickly replay logs on a follower instead of being forced to send an entire snapshot. The value passed here is the initial setting used. This can be tuned during operation using `reload_config`. (Builder pattern)"
    ),)
  )]
  trailing_logs: u64,

  /// Controls how often we check if we should perform a
  /// snapshot. We randomly stagger between this value and 2x this value to avoid
  /// the entire cluster from performing a snapshot at once. The value passed
  /// here is the initial setting used. This can be tuned during operation using
  /// `reload_config`.
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Get how often we check if we should perform a snapshot. We randomly stagger between this value and 2x this value to avoid the entire cluster from performing a snapshot at once. The value passed here is the initial setting used. This can be tuned during operation using `reload_config`."
      ),
    ),
    setter(attrs(
      doc = "Set how often we check if we should perform a snapshot. We randomly stagger between this value and 2x this value to avoid the entire cluster from performing a snapshot at once. The value passed here is the initial setting used. This can be tuned during operation using `reload_config`. (Builder pattern)"
    ),)
  )]
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  snapshot_interval: Duration,

  /// Controls how many outstanding logs there must be before
  /// we perform a snapshot. This is to prevent excessive snapshotting by
  /// replaying a small set of logs instead. The value passed here is the initial
  /// setting used. This can be tuned during operation using `reload_config`.
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Get how many outstanding logs there must be before we perform a snapshot. This is to prevent excessive snapshotting by replaying a small set of logs instead. The value passed here is the initial setting used. This can be tuned during operation using `reload_config`."
      ),
    ),
    setter(attrs(
      doc = "Set how many outstanding logs there must be before we perform a snapshot. This is to prevent excessive snapshotting by replaying a small set of logs instead. The value passed here is the initial setting used. This can be tuned during operation using `reload_config`. (Builder pattern)"
    ),)
  )]
  snapshot_threshold: u64,

  /// Used to control how long the "lease" lasts
  /// for being the leader without being able to contact a quorum
  /// of nodes. If we reach this interval without contact, we will
  /// step down as leader.
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Get how long the \"lease\" lasts for being the leader without being able to contact a quorum of nodes. If we reach this interval without contact, we will step down as leader."
      ),
    ),
    setter(attrs(
      doc = "Set how long the \"lease\" lasts for being the leader without being able to contact a quorum of nodes. If we reach this interval without contact, we will step down as leader. (Builder pattern)"
    ),)
  )]
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  leader_lease_timeout: Duration,

  /// Controls if raft will restore a snapshot to the
  /// [`FinateStateMachine`](crate::fsm::FinateStateMachine) on start. This is useful if your [`FinateStateMachine`](crate::fsm::FinateStateMachine) recovers from other mechanisms
  /// than raft snapshotting. Snapshot metadata will still be used to initialize
  /// raft's configuration and index values.
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Get if raft will restore a snapshot to the [`FinateStateMachine`](crate::fsm::FinateStateMachine) on start. This is useful if your [`FinateStateMachine`](crate::fsm::FinateStateMachine) recovers from other mechanisms than raft snapshotting. Snapshot metadata will still be used to initialize raft's configuration and index values."
      ),
    ),
    setter(attrs(
      doc = "Set if raft will restore a snapshot to the [`FinateStateMachine`](crate::fsm::FinateStateMachine) on start. This is useful if your [`FinateStateMachine`](crate::fsm::FinateStateMachine) recovers from other mechanisms than raft snapshotting. Snapshot metadata will still be used to initialize raft's configuration and index values. (Builder pattern)"
    ),)
  )]
  no_snapshot_restore_on_start: bool,
}

impl Default for Options {
  fn default() -> Self {
    Self::new()
  }
}

const MILLISECONDS: u64 = Duration::from_millis(1).as_millis() as u64;

impl Options {
  /// Returns an [`Options`] with usable defaults.
  #[inline]
  pub const fn new() -> Self {
    Self {
      heartbeat_timeout: Duration::from_millis(1000),
      election_timeout: Duration::from_millis(1000),
      commit_timeout: Duration::from_millis(50),
      max_append_entries: 64,
      batch_apply: false,
      shutdown_on_remove: true,
      trailing_logs: 10240,
      snapshot_interval: Duration::from_secs(120),
      snapshot_threshold: 8192,
      leader_lease_timeout: Duration::from_millis(500),
      no_snapshot_restore_on_start: false,
    }
  }

  /// Used to validate a sane configuration
  pub const fn validate(&self) -> Result<(), OptionsError> {
    let commit_timout_millis = self.commit_timeout.as_millis() as u64;

    if commit_timout_millis < MILLISECONDS {
      return Err(OptionsError::CommitTimeoutTooShort(self.commit_timeout));
    }

    if self.max_append_entries == 0 || self.max_append_entries > 1024 {
      return Err(OptionsError::BadMaxAppendEntries(self.max_append_entries));
    }

    ReloadableOptions::from_options(self).validate(self.leader_lease_timeout)
  }

  /// Apply the given [`ReloadableOptions`] into this [`Options`].
  #[inline]
  pub const fn apply(&self, other: ReloadableOptions) -> Self {
    Self {
      trailing_logs: other.trailing_logs,
      snapshot_threshold: other.snapshot_threshold,
      snapshot_interval: other.snapshot_interval.to_std(),
      heartbeat_timeout: other.heartbeat_timeout.to_std(),
      election_timeout: other.election_timeout.to_std(),
      ..*self
    }
  }

  /// Set the time in follower state without
  /// a leader before we attempt an election.
  #[inline]
  pub fn set_heartbeat_timeout(&mut self, timeout: Duration) {
    self.heartbeat_timeout = timeout;
  }

  /// Set the time in candidate state without
  /// a leader before we attempt an election.
  #[inline]
  pub fn set_election_timeout(&mut self, timeout: Duration) {
    self.election_timeout = timeout;
  }

  /// Set the time without an Apply operation before the
  /// leader sends an AppendEntry RPC to followers, to ensure a timely commit of
  /// log entries.
  /// Due to random staggering, may be delayed as much as 2x this value.
  #[inline]
  pub fn set_commit_timeout(&mut self, timeout: Duration) {
    self.commit_timeout = timeout;
  }

  /// Sets the maximum number of append entries
  /// to send at once. We want to strike a balance between efficiency
  /// and avoiding waste if the follower is going to reject because of
  /// an inconsistent log.
  #[inline]
  pub fn set_max_append_entries(&mut self, val: usize) {
    self.max_append_entries = val;
  }

  /// Set whether we should buffer apply channel
  /// to size `max_append_entries`. This enables batch log commitment,
  /// but breaks the timeout guarantee on `apply`. Specifically,
  /// a log can be added to the apply channel buffer but not actually be
  /// processed until after the specified timeout.
  #[inline]
  pub fn set_batch_apply(&mut self, val: bool) {
    self.batch_apply = val;
  }

  /// Set if we are a member of a cluster, and `remove_peer` is invoked for the
  /// local node, then we forget all peers and transition into the follower state.
  /// If `shutdown_on_remove` is set, we additional shutdown Raft. Otherwise,
  /// we can become a leader of a cluster containing only this node
  #[inline]
  pub fn set_shutdown_on_remove(&mut self, val: bool) {
    self.shutdown_on_remove = val;
  }

  /// Sets how many logs we leave after a snapshot. This is used
  /// so that we can quickly replay logs on a follower instead of being forced to
  /// send an entire snapshot. The value passed here is the initial setting used.
  /// This can be tuned during operation using `reload_config`.
  #[inline]
  pub fn set_trailing_logs(&mut self, logs: u64) {
    self.trailing_logs = logs;
  }

  /// Set how often we check if we should perform a snapshot.
  #[inline]
  pub fn set_snapshot_interval(&mut self, interval: Duration) {
    self.snapshot_interval = interval;
  }

  /// Set how many outstanding logs there must be before
  /// we perform a snapshot. This is to prevent excessive snapshotting by
  /// replaying a small set of logs instead. The value passed here is the initial
  /// setting used. This can be tuned during operation using `reload_config`.
  #[inline]
  pub fn set_snapshot_threshold(&mut self, val: u64) {
    self.snapshot_threshold = val;
  }

  /// Set to control how long the "lease" lasts
  /// for being the leader without being able to contact a quorum
  /// of nodes. If we reach this interval without contact, we will
  /// step down as leader.
  #[inline]
  pub fn set_leader_lease_timeout(&mut self, timeout: Duration) {
    self.leader_lease_timeout = timeout;
  }

  /// Set if raft will restore a snapshot to the
  /// [`FinateStateMachine`](crate::fsm::FinateStateMachine) on start. This is useful if your [`FinateStateMachine`](crate::fsm::FinateStateMachine) recovers from other mechanisms
  /// than raft snapshotting. Snapshot metadata will still be used to initialize
  /// raft's configuration and index values.
  #[inline]
  pub fn set_no_snapshot_restore_on_start(&mut self, val: bool) {
    self.no_snapshot_restore_on_start = val;
  }
}

/// The subset of [`Options`] that may be reconfigured during
/// runtime using [`Raft::reload_options`]. We choose to duplicate fields over embedding
/// or accepting a [`Options`] but only using specific fields to keep the API clear.
/// Reconfiguring some fields is potentially dangerous so we should only
/// selectively enable it for fields where that is allowed.
#[derive(bytemuck::NoUninit, Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(C)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ReloadableOptions {
  /// Controls how many logs we leave after a snapshot. This is used
  /// so that we can quickly replay logs on a follower instead of being forced to
  /// send an entire snapshot. The value passed here updates the setting at runtime
  /// which will take effect as soon as the next snapshot completes and truncation
  /// occurs.
  trailing_logs: u64,

  /// Controls how many outstanding logs there must be before
  /// we perform a snapshot. This is to prevent excessive snapshots when we can
  /// just replay a small set of logs.
  snapshot_threshold: u64,

  /// Controls how often we check if we should perform a snapshot.
  /// We randomly stagger between this value and 2x this value to avoid the entire
  /// cluster from performing a snapshot at once.
  snapshot_interval: PadDuration,

  /// Specifies the time in follower state without
  /// a leader before we attempt an election.
  heartbeat_timeout: PadDuration,

  /// Specifies the time in candidate state without
  /// a leader before we attempt an election.
  election_timeout: PadDuration,
}

impl Default for ReloadableOptions {
  fn default() -> Self {
    Self::new()
  }
}

impl ReloadableOptions {
  /// Create a new `ReloadableOptions` with default values
  #[inline]
  pub const fn new() -> Self {
    Self::from_options(&Options::new())
  }

  /// Create a new [`ReloadableOptions`] according to the given [`Options`].
  #[inline]
  pub const fn from_options(options: &Options) -> Self {
    Self {
      trailing_logs: options.trailing_logs,
      snapshot_threshold: options.snapshot_threshold,
      snapshot_interval: PadDuration::from_std(options.snapshot_interval),
      heartbeat_timeout: PadDuration::from_std(options.heartbeat_timeout),
      election_timeout: PadDuration::from_std(options.election_timeout),
    }
  }

  /// Used to validate a sane configuration
  pub(crate) const fn validate(&self, leader_lease_timeout: Duration) -> Result<(), OptionsError> {
    let election_timeout = self.election_timeout();
    let election_timeout_millis = election_timeout.as_millis() as u64;
    let heartbeat_timeout = self.heartbeat_timeout();
    let heartbeat_timeout_millis = heartbeat_timeout.as_millis() as u64;
    let leader_lease_timeout_millis = leader_lease_timeout.as_millis() as u64;
    let snapshot_interval = self.snapshot_interval();
    let snapshot_interval_millis = snapshot_interval.as_millis() as u64;

    if snapshot_interval_millis < 5 * MILLISECONDS {
      return Err(OptionsError::SnapshotIntervalTooShort(snapshot_interval));
    }

    if heartbeat_timeout_millis < 5 * MILLISECONDS {
      return Err(OptionsError::HeartbeatTimeoutTooShort(heartbeat_timeout));
    }

    if election_timeout_millis < 5 * MILLISECONDS {
      return Err(OptionsError::ElectionTimeoutTooShort(election_timeout));
    }

    if leader_lease_timeout_millis < 5 * MILLISECONDS {
      return Err(OptionsError::LeaderLeaseTimeoutTooShort(
        leader_lease_timeout,
      ));
    }

    if leader_lease_timeout_millis > heartbeat_timeout_millis {
      return Err(OptionsError::BadLeaderLeaseTimeout {
        leader_lease_timeout,
        heartbeat_timeout,
      });
    }

    if election_timeout_millis < heartbeat_timeout_millis {
      return Err(OptionsError::BadElectionTimeout {
        election_timeout,
        heartbeat_timeout,
      });
    }

    Ok(())
  }

  /// Get how many logs we leave after a snapshot.
  #[inline]
  pub const fn trailing_logs(&self) -> u64 {
    self.trailing_logs
  }

  /// Set how many logs we leave after a snapshot.
  #[inline]
  pub fn set_trailing_logs(&mut self, val: u64) {
    self.trailing_logs = val;
  }

  /// Set how many logs we leave after a snapshot. (Builder pattern)
  #[inline]
  pub const fn with_trailing_logs(mut self, val: u64) -> Self {
    self.trailing_logs = val;
    self
  }

  /// Get how many how many outstanding logs there must be before
  /// we perform a snapshot.
  #[inline]
  pub const fn snapshot_threshold(&self) -> u64 {
    self.snapshot_threshold
  }

  /// Set how many how many outstanding logs there must be before
  /// we perform a snapshot.
  #[inline]
  pub fn set_snapshot_threshold(&mut self, val: u64) {
    self.snapshot_threshold = val;
  }

  /// Set how many how many outstanding logs there must be before
  /// we perform a snapshot. (Builder pattern)
  #[inline]
  pub const fn with_snapshot_threshold(mut self, val: u64) -> Self {
    self.snapshot_threshold = val;
    self
  }

  /// Get how often we check if we should perform a snapshot.
  #[inline]
  pub const fn snapshot_interval(&self) -> Duration {
    self.snapshot_interval.to_std()
  }

  /// Set how often we check if we should perform a snapshot.
  #[inline]
  pub fn set_snapshot_interval(&mut self, val: Duration) {
    self.snapshot_interval = PadDuration::from_std(val);
  }

  /// Set how often we check if we should perform a snapshot. (Builder pattern)
  #[inline]
  pub const fn with_snapshot_interval(mut self, val: Duration) -> Self {
    self.snapshot_interval = PadDuration::from_std(val);
    self
  }

  /// Get the time in follower state without
  /// a leader before we attempt an election.
  #[inline]
  pub const fn heartbeat_timeout(&self) -> Duration {
    self.heartbeat_timeout.to_std()
  }

  /// Set the time in follower state without
  /// a leader before we attempt an election. (Builder pattern)
  #[inline]
  pub const fn with_heartbeat_timeout(mut self, val: Duration) -> Self {
    self.heartbeat_timeout = PadDuration::from_std(val);
    self
  }

  /// Set the time in follower state without
  /// a leader before we attempt an election.
  #[inline]
  pub fn set_heartbeat_timeout(&mut self, val: Duration) {
    self.heartbeat_timeout = PadDuration::from_std(val);
  }

  /// Get the time in candidate state without
  /// a leader before we attempt an election.
  #[inline]
  pub const fn election_timeout(&self) -> Duration {
    self.election_timeout.to_std()
  }

  /// Set the time in candidate state without
  /// a leader before we attempt an election. (Builder pattern)
  #[inline]
  pub const fn with_election_timeout(mut self, val: Duration) -> Self {
    self.election_timeout = PadDuration::from_std(val);
    self
  }

  /// Set the time in candidate state without
  /// a leader before we attempt an election.
  #[inline]
  pub fn set_election_timeout(&mut self, val: Duration) {
    self.election_timeout = PadDuration::from_std(val);
  }
}

/// OptionsError is returned when an invalid configuration is
/// provided to the Raft server.
#[derive(Debug)]
pub enum OptionsError {
  /// Returned when max_append_entries is zero, or larger than 1024
  BadMaxAppendEntries(usize),
  /// Returned when leader_lease_timeout larger than heartbeat_timeout
  BadLeaderLeaseTimeout {
    /// The value of leader_lease_timeout
    leader_lease_timeout: Duration,
    /// The value of heartbeat_timeout
    heartbeat_timeout: Duration,
  },
  /// Returns when election timeout less than heartbeat_timeout
  BadElectionTimeout {
    /// The value of election_timeout
    election_timeout: Duration,
    /// The value of heartbeat_timeout
    heartbeat_timeout: Duration,
  },
  /// Returned when commit_timeout is less than 5ms
  CommitTimeoutTooShort(Duration),
  /// Returned when eleection_timeout is less than 5ms
  ElectionTimeoutTooShort(Duration),
  /// Returned when heartbeat_timeout is less than 5ms
  HeartbeatTimeoutTooShort(Duration),
  /// Returned when leader_lease_timeout less than 5ms
  LeaderLeaseTimeoutTooShort(Duration),
  /// Returned when snapshot_interval less than 5ms
  SnapshotIntervalTooShort(Duration),
}

impl core::fmt::Display for OptionsError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    use humantime::Duration as HumanDuration;

    match self {
      Self::BadMaxAppendEntries(val) => {
        if *val == 0 {
          write!(f, "max_append_entries must be larger than 0")
        } else {
          write!(
            f,
            "max_append_entries is too large, maximum 1024, but got {val}"
          )
        }
      }
      Self::BadLeaderLeaseTimeout {
        leader_lease_timeout,
        heartbeat_timeout,
      } => {
        write!(
          f,
          "leader_lease_timeout ({}) cannot be larger than heartbeat_timeout ({})",
          HumanDuration::from(*leader_lease_timeout),
          HumanDuration::from(*heartbeat_timeout)
        )
      }
      Self::BadElectionTimeout {
        election_timeout,
        heartbeat_timeout,
      } => {
        write!(
          f,
          "election_timeout ({}) must be equal or greater than heartbeat_timeout ({})",
          HumanDuration::from(*election_timeout),
          HumanDuration::from(*heartbeat_timeout)
        )
      }
      Self::CommitTimeoutTooShort(d) => {
        write!(
          f,
          "commit_timeout ({}) is too short, at least 1ms",
          HumanDuration::from(*d)
        )
      }
      Self::ElectionTimeoutTooShort(d) => {
        write!(
          f,
          "election_timeout ({}) is too short, at least 5ms",
          HumanDuration::from(*d)
        )
      }
      Self::HeartbeatTimeoutTooShort(d) => {
        write!(
          f,
          "heartbeat_timeout ({}) is too short, at least 5ms",
          HumanDuration::from(*d)
        )
      }
      Self::LeaderLeaseTimeoutTooShort(d) => {
        write!(
          f,
          "leader_lease_timeout ({}) is too short, at least 5ms",
          HumanDuration::from(*d)
        )
      }
      Self::SnapshotIntervalTooShort(d) => {
        write!(
          f,
          "snapshot_interval ({}) is too short, at least 5ms",
          HumanDuration::from(*d)
        )
      }
    }
  }
}

impl std::error::Error for OptionsError {}
