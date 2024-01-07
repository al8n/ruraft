use super::*;

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
  V1,
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
  V1,
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
}

pub fn register(m: &PyModule) -> PyResult<()> {
  m.add_class::<ReloadableOptions>()?;
  m.add_class::<SnapshotVersion>()?;
  m.add_class::<ProtocolVersion>()?;
  Ok(())
}
