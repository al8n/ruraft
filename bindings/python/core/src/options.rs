use std::hash::{DefaultHasher, Hash, Hasher};

use pyo3::exceptions::PyTypeError;

use super::*;

mod transport;
pub use transport::*;
mod storage;
pub use storage::*;

/// Provides any necessary configuration for the Raft node.
#[derive(Debug, Copy, Clone)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[pyclass]
pub struct Options(ruraft_core::options::Options);

impl From<Options> for ruraft_core::options::Options {
  fn from(o: Options) -> Self {
    o.0
  }
}

impl From<ruraft_core::options::Options> for Options {
  fn from(o: ruraft_core::options::Options) -> Self {
    Self(o)
  }
}

impl Pyi for Options {
  fn pyi() -> std::borrow::Cow<'static, str> {
    r#"

class Options:
  @property
  def heartbeat_timeout(self) -> timedelta:...
  
  @heartbeat_timeout.setter
  def heartbeat_timeout(self, value: timedelta) -> None:...

  @property
  def election_timeout(self) -> timedelta:...
  
  @election_timeout.setter
  def election_timeout(self, value: timedelta) -> None:...

  @property
  def commit_timeout(self) -> timedelta:...
  
  @commit_timeout.setter
  def commit_timeout(self, value: timedelta) -> None:...
  
  @property
  def max_append_entries(self) -> int:...
  
  @max_append_entries.setter
  def max_append_entries(self, value: int) -> None:...
  
  @property
  def batch_apply(self) -> bool:...
  
  @batch_apply.setter
  def batch_apply(self, value: bool) -> None:...
  
  @property
  def shutdown_on_remove(self) -> bool:...
  
  @shutdown_on_remove.setter
  def shutdown_on_remove(self, value: bool) -> None:...
  
  @property
  def trailing_logs(self) -> int:...
  
  @trailing_logs.setter
  def trailing_logs(self, value: int) -> None:...
  
  @property
  def snapshot_interval(self) -> timedelta:...
  
  @snapshot_interval.setter
  def snapshot_interval(self, value: timedelta) -> None:...
  
  @property
  def snapshot_threshold(self) -> int:...
  
  @snapshot_threshold.setter
  def snapshot_threshold(self, value: int) -> None:...
  
  @property
  def leader_lease_timeout(self) -> timedelta:...
  
  @leader_lease_timeout.setter
  def leader_lease_timeout(self, value: timedelta) -> None:...
  
  @property
  def no_snapshot_restore_on_start(self) -> bool:...
  
  @no_snapshot_restore_on_start.setter
  def no_snapshot_restore_on_start(self, value: bool) -> None:...

  def __eq__(self, __value: Options) -> bool: ...
  
  def __ne__(self, __value: Options) -> bool: ...
  
  def __hash__(self) -> int: ...
  
  def __str__(self) -> str: ...
  
  def __repr__(self) -> str: ...

"#
    .into()
  }
}

#[pymethods]
impl Options {
  #[new]
  pub fn new() -> Self {
    Self(Default::default())
  }

  /// Get the time in follower state without
  /// a leader before we attempt an election.
  #[getter]
  pub fn heartbeat_timeout(&self) -> PyResult<::chrono::Duration> {
    ::chrono::Duration::from_std(self.0.heartbeat_timeout())
      .map_err(|e| PyErr::new::<PyTypeError, _>(e.to_string()))
  }

  /// Set the time in follower state without
  /// a leader before we attempt an election.
  #[setter]
  pub fn set_heartbeat_timeout(&mut self, timeout: ::chrono::Duration) -> PyResult<()> {
    timeout
      .to_std()
      .map(|d| self.0.set_heartbeat_timeout(d))
      .map_err(|e| PyErr::new::<PyTypeError, _>(e.to_string()))
  }

  /// Get the time in candidate state without
  /// a leader before we attempt an election.
  #[getter]
  pub fn election_timeout(&self) -> PyResult<::chrono::Duration> {
    ::chrono::Duration::from_std(self.0.election_timeout())
      .map_err(|e| PyErr::new::<PyTypeError, _>(e.to_string()))
  }

  /// Set the time in candidate state without
  /// a leader before we attempt an election.
  #[setter]
  pub fn set_election_timeout(&mut self, timeout: ::chrono::Duration) -> PyResult<()> {
    timeout
      .to_std()
      .map(|d| self.0.set_election_timeout(d))
      .map_err(|e| PyErr::new::<PyTypeError, _>(e.to_string()))
  }

  /// Get the time without an Apply operation before the
  /// leader sends an AppendEntry RPC to followers, to ensure a timely commit of
  /// log entries.
  /// Due to random staggering, may be delayed as much as 2x this value.
  #[getter]
  pub fn commit_timeout(&self) -> PyResult<::chrono::Duration> {
    ::chrono::Duration::from_std(self.0.commit_timeout())
      .map_err(|e| PyErr::new::<PyTypeError, _>(e.to_string()))
  }

  /// Set the time without an Apply operation before the
  /// leader sends an AppendEntry RPC to followers, to ensure a timely commit of
  /// log entries.
  /// Due to random staggering, may be delayed as much as 2x this value.
  #[setter]
  pub fn set_commit_timeout(&mut self, timeout: ::chrono::Duration) -> PyResult<()> {
    timeout
      .to_std()
      .map(|d| self.0.set_commit_timeout(d))
      .map_err(|e| PyErr::new::<PyTypeError, _>(e.to_string()))
  }

  /// Gets the maximum number of append entries
  /// to send at once. We want to strike a balance between efficiency
  /// and avoiding waste if the follower is going to reject because of
  /// an inconsistent log.
  #[getter]
  pub fn max_append_entries(&self) -> usize {
    self.0.max_append_entries()
  }

  /// Sets the maximum number of append entries
  /// to send at once. We want to strike a balance between efficiency
  /// and avoiding waste if the follower is going to reject because of
  /// an inconsistent log.
  #[setter]
  pub fn set_max_append_entries(&mut self, val: usize) {
    self.0.set_max_append_entries(val);
  }

  /// Gets whether we should buffer apply channel
  /// to size `max_append_entries`. This enables batch log commitment,
  /// but breaks the timeout guarantee on `apply`. Specifically,
  /// a log can be added to the apply channel buffer but not actually be
  /// processed until after the specified timeout.
  #[getter]
  pub fn batch_apply(&self) -> bool {
    self.0.batch_apply()
  }

  /// Sets whether we should buffer apply channel
  /// to size `max_append_entries`. This enables batch log commitment,
  /// but breaks the timeout guarantee on `apply`. Specifically,
  /// a log can be added to the apply channel buffer but not actually be
  /// processed until after the specified timeout.
  #[setter]
  pub fn set_batch_apply(&mut self, val: bool) {
    self.0.set_batch_apply(val);
  }

  /// Gets if we are a member of a cluster, and `remove_peer` is invoked for the
  /// local node, then we forget all peers and transition into the follower state.
  /// If `shutdown_on_remove` is set, we additional shutdown Raft. Otherwise,
  /// we can become a leader of a cluster containing only this node
  #[getter]
  pub fn shutdown_on_remove(&self) -> bool {
    self.0.shutdown_on_remove()
  }

  /// Sets if we are a member of a cluster, and `remove_peer` is invoked for the
  /// local node, then we forget all peers and transition into the follower state.
  /// If `shutdown_on_remove` is set, we additional shutdown Raft. Otherwise,
  /// we can become a leader of a cluster containing only this node
  #[setter]
  pub fn set_shutdown_on_remove(&mut self, val: bool) {
    self.0.set_shutdown_on_remove(val);
  }

  /// Gets how many logs we leave after a snapshot. This is used
  /// so that we can quickly replay logs on a follower instead of being forced to
  /// send an entire snapshot. The value passed here is the initial setting used.
  /// This can be tuned during operation using `reload_config`.
  #[getter]
  pub fn trailing_logs(&self) -> u64 {
    self.0.trailing_logs()
  }

  /// Sets how many logs we leave after a snapshot. This is used
  /// so that we can quickly replay logs on a follower instead of being forced to
  /// send an entire snapshot. The value passed here is the initial setting used.
  /// This can be tuned during operation using `reload_config`.
  #[setter]
  pub fn set_trailing_logs(&mut self, logs: u64) {
    self.0.set_trailing_logs(logs);
  }

  /// Gets how often we check if we should perform a snapshot.
  #[getter]
  pub fn snapshot_interval(&self) -> PyResult<::chrono::Duration> {
    ::chrono::Duration::from_std(self.0.snapshot_interval())
      .map_err(|e| PyErr::new::<PyTypeError, _>(e.to_string()))
  }

  /// Sets how often we check if we should perform a snapshot.
  #[setter]
  pub fn set_snapshot_interval(&mut self, interval: ::chrono::Duration) -> PyResult<()> {
    interval
      .to_std()
      .map(|d| self.0.set_snapshot_interval(d))
      .map_err(|e| PyErr::new::<PyTypeError, _>(e.to_string()))
  }

  /// Gets how many outstanding logs there must be before
  /// we perform a snapshot. This is to prevent excessive snapshotting by
  /// replaying a small set of logs instead. The value passed here is the initial
  /// setting used. This can be tuned during operation using `reload_config`.
  #[getter]
  pub fn snapshot_threshold(&self) -> u64 {
    self.0.snapshot_threshold()
  }

  /// Sets how many outstanding logs there must be before
  /// we perform a snapshot. This is to prevent excessive snapshotting by
  /// replaying a small set of logs instead. The value passed here is the initial
  /// setting used. This can be tuned during operation using `reload_config`.
  #[setter]
  pub fn set_snapshot_threshold(&mut self, threshold: u64) {
    self.0.set_snapshot_threshold(threshold);
  }

  /// Gets how long the "lease" lasts
  /// for being the leader without being able to contact a quorum
  /// of nodes. If we reach this interval without contact, we will
  /// step down as leader.
  #[getter]
  pub fn leader_lease_timeout(&self) -> PyResult<::chrono::Duration> {
    ::chrono::Duration::from_std(self.0.leader_lease_timeout())
      .map_err(|e| PyErr::new::<PyTypeError, _>(e.to_string()))
  }

  /// Sets to control how long the "lease" lasts
  /// for being the leader without being able to contact a quorum
  /// of nodes. If we reach this interval without contact, we will
  /// step down as leader.
  #[setter]
  pub fn set_leader_lease_timeout(&mut self, timeout: ::chrono::Duration) -> PyResult<()> {
    timeout
      .to_std()
      .map(|d| self.0.set_leader_lease_timeout(d))
      .map_err(|e| PyErr::new::<PyTypeError, _>(e.to_string()))
  }

  /// Gets if raft will restore a snapshot to the
  /// FSM on start. This is useful if your FSM recovers from other mechanisms
  /// than raft snapshotting. Snapshot metadata will still be used to initialize
  /// raft's configuration and index values.
  #[getter]
  pub fn no_snapshot_restore_on_start(&self) -> bool {
    self.0.no_snapshot_restore_on_start()
  }

  /// Sets if raft will restore a snapshot to the
  /// FSM on start. This is useful if your FSM recovers from other mechanisms
  /// than raft snapshotting. Snapshot metadata will still be used to initialize
  /// raft's configuration and index values.
  #[setter]
  pub fn set_no_snapshot_restore_on_start(&mut self, val: bool) {
    self.0.set_no_snapshot_restore_on_start(val);
  }

  pub fn __eq__(&self, other: &Self) -> bool {
    self.0.eq(&other.0)
  }

  pub fn __ne__(&self, other: &Self) -> bool {
    self.0.ne(&other.0)
  }

  pub fn __hash__(&self) -> u64 {
    let mut hasher = DefaultHasher::new();
    self.0.hash(&mut hasher);
    hasher.finish()
  }

  pub fn __str__(&self) -> PyResult<String> {
    if cfg!(feature = "serde") {
      serde_json::to_string(&self.0).map_err(|e| PyTypeError::new_err(e.to_string()))
    } else {
      Ok(format!("{:?}", self.0))
    }
  }

  pub fn __repr__(&self) -> String {
    format!("{:?}", self.0)
  }
}

/// The subset of `Options` that may be reconfigured during
/// runtime using `reload_options`. We choose to duplicate fields over embedding
/// or accepting a `Options` but only using specific fields to keep the API clear.
/// Reconfiguring some fields is potentially dangerous so we should only
/// selectively enable it for fields where that is allowed.
#[derive(Debug, Copy, Clone)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[pyclass]
pub struct ReloadableOptions(ruraft_core::options::ReloadableOptions);

impl From<ReloadableOptions> for ruraft_core::options::ReloadableOptions {
  fn from(o: ReloadableOptions) -> Self {
    o.0
  }
}

impl From<ruraft_core::options::ReloadableOptions> for ReloadableOptions {
  fn from(o: ruraft_core::options::ReloadableOptions) -> Self {
    Self(o)
  }
}

impl Pyi for ReloadableOptions {
  fn pyi() -> std::borrow::Cow<'static, str> {
    r#"

class ReloadableOptions:
  @property
  def heartbeat_timeout(self) -> timedelta:...

  @heartbeat_timeout.setter
  def heartbeat_timeout(self, value: timedelta) -> None:...

  @property
  def election_timeout(self) -> timedelta:...

  @election_timeout.setter
  def election_timeout(self, value: timedelta) -> None:...

  @property
  def trailing_logs(self) -> int:...

  @trailing_logs.setter
  def trailing_logs(self, value: int) -> None:...

  @property
  def snapshot_interval(self) -> timedelta:...

  @snapshot_interval.setter
  def snapshot_interval(self, value: timedelta) -> None:...

  @property
  def snapshot_threshold(self) -> int:...

  @snapshot_threshold.setter
  def snapshot_threshold(self, value: int) -> None:...

  def __eq__(self, __value: ReloadableOptions) -> bool: ...
  
  def __ne__(self, __value: ReloadableOptions) -> bool: ...
  
  def __hash__(self) -> int: ...
  
  def __str__(self) -> str: ...
  
  def __repr__(self) -> str: ...

"#
    .into()
  }
}

#[pymethods]
impl ReloadableOptions {
  #[new]
  pub fn new() -> Self {
    Self(Default::default())
  }

  /// Get the time in candidate state without
  /// a leader before we attempt an election.
  #[getter]
  pub fn election_timeout(&self) -> PyResult<::chrono::Duration> {
    ::chrono::Duration::from_std(self.0.election_timeout())
      .map_err(|e| PyErr::new::<PyTypeError, _>(e.to_string()))
  }

  /// Set the time in candidate state without
  /// a leader before we attempt an election.
  #[setter]
  pub fn set_election_timeout(&mut self, timeout: ::chrono::Duration) -> PyResult<()> {
    self.0.set_election_timeout(
      timeout
        .to_std()
        .map_err(|e| PyErr::new::<PyTypeError, _>(e.to_string()))?,
    );
    Ok(())
  }

  /// Get the time in follower state without
  /// a leader before we attempt an election.
  #[getter]
  pub fn heartbeat_timeout(&self) -> PyResult<::chrono::Duration> {
    ::chrono::Duration::from_std(self.0.heartbeat_timeout())
      .map_err(|e| PyErr::new::<PyTypeError, _>(e.to_string()))
  }

  /// Set the time in follower state without
  /// a leader before we attempt an election.
  #[setter]
  pub fn set_heartbeat_timeout(&mut self, timeout: ::chrono::Duration) -> PyResult<()> {
    self.0.set_heartbeat_timeout(
      timeout
        .to_std()
        .map_err(|e| PyErr::new::<PyTypeError, _>(e.to_string()))?,
    );
    Ok(())
  }

  /// Get how often we check if we should perform a snapshot.
  #[getter]
  pub fn snapshot_interval(&self) -> PyResult<::chrono::Duration> {
    ::chrono::Duration::from_std(self.0.snapshot_interval())
      .map_err(|e| PyErr::new::<PyTypeError, _>(e.to_string()))
  }

  /// Set how often we check if we should perform a snapshot.
  #[setter]
  pub fn set_snapshot_interval(&mut self, interval: ::chrono::Duration) -> PyResult<()> {
    self.0.set_snapshot_interval(
      interval
        .to_std()
        .map_err(|e| PyErr::new::<PyTypeError, _>(e.to_string()))?,
    );
    Ok(())
  }

  /// Get how many how many outstanding logs there must be before
  /// we perform a snapshot.
  #[getter]
  pub fn snapshot_threshold(&self) -> u64 {
    self.0.snapshot_threshold()
  }

  /// Set how many how many outstanding logs there must be before
  /// we perform a snapshot.
  #[setter]
  pub fn set_snapshot_threshold(&mut self, threshold: u64) {
    self.0.set_snapshot_threshold(threshold);
  }

  /// Get how many logs we leave after a snapshot.
  #[getter]
  pub fn trailing_logs(&self) -> u64 {
    self.0.trailing_logs()
  }

  /// Set how many logs we leave after a snapshot.
  #[setter]
  pub fn set_trailing_logs(&mut self, logs: u64) {
    self.0.set_trailing_logs(logs);
  }

  pub fn __eq__(&self, other: &Self) -> bool {
    self.0.eq(&other.0)
  }

  pub fn __ne__(&self, other: &Self) -> bool {
    self.0.ne(&other.0)
  }

  pub fn __hash__(&self) -> u64 {
    let mut hasher = DefaultHasher::new();
    self.0.hash(&mut hasher);
    hasher.finish()
  }

  pub fn __str__(&self) -> PyResult<String> {
    if cfg!(feature = "serde") {
      serde_json::to_string(&self.0).map_err(|e| PyTypeError::new_err(e.to_string()))
    } else {
      Ok(format!("{:?}", self.0))
    }
  }

  pub fn __repr__(&self) -> String {
    format!("{:?}", self.0)
  }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize))]
#[non_exhaustive]
#[repr(u8)]
#[pyclass(frozen)]
pub enum SnapshotVersion {
  V1 = 1,
}

impl From<ruraft_core::options::SnapshotVersion> for SnapshotVersion {
  fn from(v: ruraft_core::options::SnapshotVersion) -> Self {
    match v {
      ruraft_core::options::SnapshotVersion::V1 => Self::V1,
      _ => unreachable!(),
    }
  }
}

impl From<SnapshotVersion> for ruraft_core::options::SnapshotVersion {
  fn from(v: SnapshotVersion) -> Self {
    match v {
      SnapshotVersion::V1 => Self::V1,
    }
  }
}

impl Pyi for SnapshotVersion {
  fn pyi() -> std::borrow::Cow<'static, str> {
    r#"

class SnapshotVersion:
  def v1() -> SnapshotVersion:...

  def __eq__(self, __value: SnapshotVersion) -> bool: ...

  def __ne__(self, __value: SnapshotVersion) -> bool: ...

  def __hash__(self) -> int: ...

  def __str__(self) -> str: ...

  def __repr__(self) -> str: ...

  def __int__(self) -> int: ...

"#
    .into()
  }
}

#[pymethods]
impl SnapshotVersion {
  #[new]
  pub fn new() -> Self {
    Self::V1
  }

  #[inline]
  #[staticmethod]
  pub fn v1() -> Self {
    Self::V1
  }

  #[inline]
  pub fn __str__(&self) -> &'static str {
    match self {
      Self::V1 => "v1",
    }
  }

  #[inline]
  pub fn __repr__(&self) -> &'static str {
    match self {
      Self::V1 => "SnapshotVersion::V1",
    }
  }

  fn __int__(&self) -> u8 {
    *self as u8
  }

  fn __eq__(&self, other: &Self) -> bool {
    self.eq(other)
  }

  fn __ne__(&self, other: &Self) -> bool {
    self.ne(other)
  }

  fn __hash__(&self) -> u64 {
    let mut hasher = DefaultHasher::new();
    self.hash(&mut hasher);
    hasher.finish()
  }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize))]
#[non_exhaustive]
#[repr(u8)]
#[pyclass(frozen)]
pub enum ProtocolVersion {
  V1 = 1,
}

impl From<ruraft_core::options::ProtocolVersion> for ProtocolVersion {
  fn from(v: ruraft_core::options::ProtocolVersion) -> Self {
    match v {
      ruraft_core::options::ProtocolVersion::V1 => Self::V1,
      _ => unreachable!(),
    }
  }
}

impl From<ProtocolVersion> for ruraft_core::options::ProtocolVersion {
  fn from(v: ProtocolVersion) -> Self {
    match v {
      ProtocolVersion::V1 => Self::V1,
    }
  }
}

impl Pyi for ProtocolVersion {
  fn pyi() -> std::borrow::Cow<'static, str> {
    r#"

class ProtocolVersion:
  def v1() -> ProtocolVersion:...

  def __eq__(self, __value: ProtocolVersion) -> bool: ...

  def __ne__(self, __value: ProtocolVersion) -> bool: ...

  def __hash__(self) -> int: ...

  def __str__(self) -> str: ...

  def __repr__(self) -> str: ...

  def __int__(self) -> int: ...

"#
    .into()
  }
}

#[pymethods]
impl ProtocolVersion {
  #[new]
  pub fn new() -> Self {
    Self::V1
  }

  #[inline]
  #[staticmethod]
  pub fn v1() -> Self {
    Self::V1
  }

  #[inline]
  pub fn __str__(&self) -> &'static str {
    match self {
      Self::V1 => "v1",
    }
  }

  #[inline]
  pub fn __repr__(&self) -> &'static str {
    match self {
      Self::V1 => "ProtocolVersion::V1",
    }
  }

  fn __eq__(&self, other: &Self) -> bool {
    self.eq(other)
  }

  fn __ne__(&self, other: &Self) -> bool {
    self.ne(other)
  }

  fn __hash__(&self) -> u64 {
    let mut hasher = DefaultHasher::new();
    self.hash(&mut hasher);
    hasher.finish()
  }

  fn __int__(&self) -> u8 {
    *self as u8
  }
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum SupportedSnapshotStorageOptions {
  File(FileSnapshotStorageOptions),
  Memory,
}

pub fn register(py: Python<'_>) -> PyResult<&PyModule> {
  let submodule = PyModule::new(py, "options")?;
  submodule.add_class::<Options>()?;
  submodule.add_class::<ReloadableOptions>()?;
  submodule.add_class::<SnapshotVersion>()?;
  submodule.add_class::<ProtocolVersion>()?;
  register_storage_options(submodule)?;
  register_transport_options(submodule)?;
  Ok(submodule)
}

pub fn pyi() -> String {
  let mut pyi = r#"

from datetime import timedelta
from os import PathLike
from .types import Header
from typing import Optional

  "#
  .to_string();

  pyi.push_str(&Options::pyi());
  pyi.push_str(&ReloadableOptions::pyi());
  pyi.push_str(&SnapshotVersion::pyi());
  pyi.push_str(&ProtocolVersion::pyi());

  pyi
}
