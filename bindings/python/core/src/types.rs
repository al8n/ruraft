use std::{
  hash::{DefaultHasher, Hash, Hasher},
  time::Duration,
};

use ::either::Either;
use nodecraft::{NodeAddress as RNodeAddress, NodeId as RNodeId, Transformable};
use pyo3::{exceptions::PyTypeError, pyclass::CompareOp, types::PyModule, *};
use ruraft_core::{
  membership::{
    Membership as RMembership, MembershipBuilder as RMembershipBuilder, Server as RServer,
    ServerSuffrage as RServerSuffrage,
  },
  storage::{CommittedLog as RCommittedLog, CommittedLogKind as RCommittedLogKind},
  transport::Header as RHeader,
  Node as RNode,
};

use crate::{options::*, Pyi, RaftData};

pub mod membership;
pub use membership::*;

/// A unique string identifying a server for all time. The maximum length of an id is 512 bytes.
#[pyclass(frozen)]
#[derive(Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Debug, derive_more::From)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct NodeId(RNodeId);

impl From<NodeId> for RNodeId {
  fn from(value: NodeId) -> Self {
    value.0
  }
}

impl Pyi for NodeId {
  fn pyi() -> std::borrow::Cow<'static, str> {
    r#"

class NodeId:
  def __init__(self, src: str) -> None: ...

  def to_bytes(self) -> bytes: ...
  
  def from_bytes(src: bytes) -> NodeId: ...

  def __str__(self) -> str: ...

  def __repr__(self) -> str: ...

  def __hash__(self) -> int: ...

  def __richcmp__(self, other: NodeId, op) -> bool: ...

"#
    .into()
  }
}

#[pymethods]
impl NodeId {
  /// Construct a new [`NodeId`] from a string.
  #[new]
  pub fn new(id: &str) -> PyResult<Self> {
    Ok(NodeId(RNodeId::new(id).map_err(|e| {
      PyErr::new::<PyTypeError, _>(format!("{}", e))
    })?))
  }

  /// Encode the [`NodeId`] into bytes.
  pub fn to_bytes(&self) -> PyResult<Vec<u8>> {
    self
      .0
      .encode_to_vec()
      .map_err(|e| PyTypeError::new_err(e.to_string()))
  }

  /// Decode the [`NodeId`] from bytes.
  ///
  /// Returns the number of bytes read and the [`NodeId`].
  #[staticmethod]
  pub fn from_bytes(bytes: &[u8]) -> PyResult<(usize, Self)> {
    RNodeId::decode(bytes)
      .map_err(|e| PyTypeError::new_err(e.to_string()))
      .map(|(readed, val)| (readed, Self(val)))
  }

  pub fn __str__(&self) -> &str {
    self.0.as_str()
  }

  pub fn __repr__(&self) -> String {
    format!("NodeId({})", self.0.as_str())
  }

  fn __hash__(&self) -> u64 {
    let mut hasher = DefaultHasher::new();
    self.0.hash(&mut hasher);
    hasher.finish()
  }

  fn __richcmp__(&self, other: &Self, op: CompareOp) -> bool {
    match op {
      CompareOp::Lt => self.0 < other.0,
      CompareOp::Le => self.0 <= other.0,
      CompareOp::Eq => self.0 == other.0,
      CompareOp::Ne => self.0 != other.0,
      CompareOp::Gt => self.0 > other.0,
      CompareOp::Ge => self.0 >= other.0,
    }
  }
}

/// A unique string identifying a server for all time. The maximum length of an id is 512 bytes.
#[pyclass(frozen)]
#[derive(Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Debug)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct NodeAddress(RNodeAddress);

impl From<NodeAddress> for RNodeAddress {
  fn from(value: NodeAddress) -> Self {
    value.0
  }
}

impl From<RNodeAddress> for NodeAddress {
  fn from(value: RNodeAddress) -> Self {
    Self(value)
  }
}

impl Pyi for NodeAddress {
  fn pyi() -> std::borrow::Cow<'static, str> {
    r#"

class NodeAddress:

  def __init__(self, src: str) -> None: ...

  def to_bytes(self) -> bytes: ...

  def from_bytes(src: bytes) -> NodeAddress: ...

  @property
  def port(self) -> int: ...

  def __str__(self) -> str: ...

  def __repr__(self) -> str: ...

  def __hash__(self) -> int: ...

  def __richcmp__(self, other: NodeAddress, op) -> bool: ...

"#
    .into()
  }
}

#[pymethods]
impl NodeAddress {
  /// Construct a new [`NodeId`] from a string.
  #[new]
  pub fn new(addr: &str) -> PyResult<Self> {
    Ok(NodeAddress(RNodeAddress::try_from(addr).map_err(|e| {
      PyErr::new::<PyTypeError, _>(format!("{}", e))
    })?))
  }

  #[getter]
  pub fn port(&self) -> u16 {
    self.0.port()
  }

  fn __hash__(&self) -> u64 {
    let mut hasher = DefaultHasher::new();
    self.0.hash(&mut hasher);
    hasher.finish()
  }

  /// Encode the [`NodeAddress`] into bytes.
  pub fn to_bytes(&self) -> PyResult<Vec<u8>> {
    self
      .0
      .encode_to_vec()
      .map_err(|e| PyTypeError::new_err(e.to_string()))
  }

  /// Decode the [`NodeAddress`] from bytes.
  ///
  /// Returns the number of bytes read and the [`NodeAddress`].
  #[staticmethod]
  pub fn from_bytes(bytes: &[u8]) -> PyResult<(usize, Self)> {
    RNodeAddress::decode(bytes)
      .map_err(|e| PyTypeError::new_err(e.to_string()))
      .map(|(readed, val)| (readed, Self(val)))
  }

  pub fn __str__(&self) -> String {
    self.0.to_string()
  }

  pub fn __repr__(&self) -> String {
    format!("NodeAddress({})", self.0)
  }

  fn __richcmp__(&self, other: &Self, op: CompareOp) -> bool {
    match op {
      CompareOp::Lt => self.0 < other.0,
      CompareOp::Le => self.0 <= other.0,
      CompareOp::Eq => self.0 == other.0,
      CompareOp::Ne => self.0 != other.0,
      CompareOp::Gt => self.0 > other.0,
      CompareOp::Ge => self.0 >= other.0,
    }
  }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[pyclass(frozen)]
#[repr(u8)]
pub enum Role {
  /// The initial state of a Raft node.
  Follower,
  /// One of the valid states of a Raft node.
  Candidate,
  /// One of the valid states of a Raft node.
  Leader,
  /// The terminal state of a Raft node.
  Shutdown,
}

impl From<Role> for ruraft_core::Role {
  fn from(r: Role) -> Self {
    match r {
      Role::Follower => Self::Follower,
      Role::Candidate => Self::Candidate,
      Role::Leader => Self::Leader,
      Role::Shutdown => Self::Shutdown,
    }
  }
}

impl From<ruraft_core::Role> for Role {
  fn from(r: ruraft_core::Role) -> Self {
    match r {
      ruraft_core::Role::Follower => Self::Follower,
      ruraft_core::Role::Candidate => Self::Candidate,
      ruraft_core::Role::Leader => Self::Leader,
      ruraft_core::Role::Shutdown => Self::Shutdown,
    }
  }
}

impl Pyi for Role {
  fn pyi() -> std::borrow::Cow<'static, str> {
    r#"

class Role:
  def __init__(self) -> None: ...

  def follower() -> Role: ...
  
  def candidate() -> Role: ...
  
  def leader() -> Role: ...
  
  def is_leader(self) -> bool: ...
  
  def is_follower(self) -> bool: ...
  
  def is_candidate(self) -> bool: ...

  def __str__(self) -> str: ...

  def __repr__(self) -> str: ...

  def __eq__(self, other: Role) -> bool: ...

  def __ne__(self, other: Role) -> bool: ...

  def __hash__(self) -> int: ...

  def __int__(self) -> int: ...

"#
    .into()
  }
}

#[pymethods]
impl Role {
  /// The initial state of a Raft node.
  #[inline]
  #[staticmethod]
  pub fn follower() -> Self {
    Self::Follower
  }

  /// One of the valid states of a Raft node.
  #[inline]
  #[staticmethod]
  pub fn candidate() -> Self {
    Self::Candidate
  }

  /// One of the valid states of a Raft node.
  #[inline]
  #[staticmethod]
  pub fn leader() -> Self {
    Self::Leader
  }

  pub fn is_leader(&self) -> bool {
    matches!(self, Self::Leader)
  }

  pub fn is_follower(&self) -> bool {
    matches!(self, Self::Follower)
  }

  pub fn is_candidate(&self) -> bool {
    matches!(self, Self::Candidate)
  }

  #[inline]
  pub fn __str__(&self) -> &'static str {
    match self {
      Self::Follower => "follower",
      Self::Candidate => "candidate",
      Self::Leader => "leader",
      Self::Shutdown => "shutdown",
    }
  }

  #[inline]
  pub fn __repr__(&self) -> &'static str {
    match self {
      Self::Follower => "Role::Follower",
      Self::Candidate => "Role::Candidate",
      Self::Leader => "Role::Leader",
      Self::Shutdown => "Role::Shutdown",
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
/// An identifier of Raft node in the cluster.
#[pyclass]
#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct Node(RNode<RNodeId, RNodeAddress>);

impl From<Node> for RNode<RNodeId, RNodeAddress> {
  fn from(n: Node) -> Self {
    n.0
  }
}

impl From<RNode<RNodeId, RNodeAddress>> for Node {
  fn from(n: RNode<RNodeId, RNodeAddress>) -> Self {
    Self(n)
  }
}

impl Pyi for Node {
  fn pyi() -> std::borrow::Cow<'static, str> {
    r#"

class Node:
  def __init__(self, id: NodeId, address: Node) -> None: ...

  @property
  def id(self) -> NodeId: ...
  
  @id.setter
  def id(self, value: NodeId) -> None : ...
  
  @property
  def address(self) -> NodeAddress: ...
  
  @address.setter
  def address(self, value: NodeAddress) -> None : ... 

  def __str__(self) -> str: ...

  def __repr__(self) -> str: ...

  def __hash__(self) -> int: ...

  def __richcmp__(self, other: Node, op) -> bool: ...

"#
    .into()
  }
}

#[pymethods]
impl Node {
  /// Construct a new [`NodeId`] from a string.
  #[new]
  pub fn new(id: NodeId, address: NodeAddress) -> Self {
    Node(RNode::new(id.0, address.0))
  }

  /// Get the id of the node.
  #[getter]
  pub fn id(&self) -> NodeId {
    NodeId(self.0.id().clone())
  }

  /// Set the id of the node.
  #[setter]
  pub fn set_id(&mut self, id: NodeId) {
    self.0.set_id(id.0);
  }

  /// Get the address of the node.
  #[getter]
  pub fn address(&self) -> NodeAddress {
    NodeAddress(self.0.addr().clone())
  }

  /// Set the address of the node.
  #[setter]
  pub fn set_address(&mut self, address: NodeAddress) {
    self.0.set_addr(address.0);
  }

  fn __hash__(&self) -> u64 {
    let mut hasher = DefaultHasher::new();
    self.0.hash(&mut hasher);
    hasher.finish()
  }

  pub fn __str__(&self) -> String {
    self.0.to_string()
  }

  pub fn __repr__(&self) -> String {
    format!("NodeAddress({})", self.0)
  }

  fn __richcmp__(&self, other: &Self, op: CompareOp) -> bool {
    match op {
      CompareOp::Lt => self.0 < other.0,
      CompareOp::Le => self.0 <= other.0,
      CompareOp::Eq => self.0 == other.0,
      CompareOp::Ne => self.0 != other.0,
      CompareOp::Gt => self.0 > other.0,
      CompareOp::Ge => self.0 >= other.0,
    }
  }
}

/// An identifier of Raft node in the cluster.
#[pyclass]
#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct Header(RHeader<RNodeId, RNodeAddress>);

impl From<Header> for RHeader<RNodeId, RNodeAddress> {
  fn from(n: Header) -> Self {
    n.0
  }
}

impl From<RHeader<RNodeId, RNodeAddress>> for Header {
  fn from(n: RHeader<RNodeId, RNodeAddress>) -> Self {
    Self(n)
  }
}

impl Pyi for Header {
  fn pyi() -> std::borrow::Cow<'static, str> {
    r#"

class Header:
  def __init__(self, protocol_version: ProtocolVersion, id: NodeId, addr: NodeAddress) -> None: ...

  @property
  def id(self) -> NodeId: ...
  
  @id.setter
  def id(self, value: NodeId) -> None : ...
  
  @property
  def address(self) -> NodeAddress: ...
  
  @address.setter
  def address(self, value: NodeAddress) -> None : ...
  
  @property
  def protocol_version(self) -> ProtocolVersion: ...
  
  @protocol_version.setter
  def protocol_version(self, value: ProtocolVersion) -> None : ...

  def __str__(self) -> str: ...

  def __repr__(self) -> str: ...

  def __hash__(self) -> int: ...

  def __eq__(self, other: Header) -> bool: ...

  def __ne__(self, other: Header) -> bool: ...

"#
    .into()
  }
}

#[pymethods]
impl Header {
  /// Construct a new [`NodeId`] from a string.
  #[new]
  pub fn new(protocol_version: ProtocolVersion, id: NodeId, address: NodeAddress) -> Self {
    Header(RHeader::new(
      protocol_version.into(),
      id.into(),
      address.into(),
    ))
  }

  /// Get the id of the node.
  #[getter]
  pub fn id(&self) -> NodeId {
    NodeId(self.0.id().clone())
  }

  /// Set the id of the node.
  #[setter]
  pub fn set_id(&mut self, id: NodeId) {
    self.0.set_id(id.0);
  }

  /// Get the address of the node.
  #[getter]
  pub fn address(&self) -> NodeAddress {
    NodeAddress(self.0.addr().clone())
  }

  /// Set the address of the node.
  #[setter]
  pub fn set_address(&mut self, address: NodeAddress) {
    self.0.set_addr(address.0);
  }

  /// Get the protocol version of the node.
  #[getter]
  pub fn protocol_version(&self) -> ProtocolVersion {
    self.0.protocol_version().into()
  }

  /// Set the protocol version of the node.
  #[setter]
  pub fn set_protocol_version(&mut self, protocol_version: ProtocolVersion) {
    self.0.set_protocol_version(protocol_version.into());
  }

  fn __hash__(&self) -> u64 {
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

  pub fn __eq__(&self, other: &Self) -> bool {
    self.0.eq(&other.0)
  }

  pub fn __ne__(&self, other: &Self) -> bool {
    self.0.ne(&other.0)
  }
}

/// A committed log, which may contains two kinds of data: a bytes array or [`Membership`].
#[pyclass(frozen)]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct CommittedLog(RCommittedLog<RNodeId, RNodeAddress, RaftData>);

impl From<RCommittedLog<RNodeId, RNodeAddress, RaftData>> for CommittedLog {
  fn from(value: RCommittedLog<RNodeId, RNodeAddress, RaftData>) -> Self {
    Self(value)
  }
}

impl Pyi for CommittedLog {
  fn pyi() -> std::borrow::Cow<'static, str> {
    r#"

class CommittedLog:
  @property
  def index(self) -> int: ...

  @property
  def term(self) -> int: ...

  def data(self) -> Union[bytes, Membership]: ...

  def __str__(self) -> str: ...

  def __repr__(self) -> str: ... 

  def __richcmp__(self, other: CommittedLog, op) -> bool: ...

"#
    .into()
  }
}

#[pymethods]
impl CommittedLog {
  #[getter]
  pub fn term(&self) -> u64 {
    self.0.term()
  }

  #[getter]
  pub fn index(&self) -> u64 {
    self.0.index()
  }

  pub fn data(&self) -> Either<RaftData, Membership> {
    match self.0.kind() {
      RCommittedLogKind::Log(data) => Either::Left(data.as_ref().clone()),
      RCommittedLogKind::Membership(membership) => Either::Right(membership.clone().into()),
    }
  }

  #[inline]
  pub fn __str__(&self) -> PyResult<String> {
    if cfg!(feature = "serde") {
      serde_json::to_string(&self.0).map_err(|e| PyTypeError::new_err(e.to_string()))
    } else {
      Ok(format!("{:?}", self.0))
    }
  }

  #[inline]
  pub fn __repr__(&self) -> String {
    format!("{:?}", self.0)
  }

  fn __richcmp__(&self, other: &Self, op: CompareOp) -> bool {
    match op {
      CompareOp::Lt => self.0 < other.0,
      CompareOp::Le => self.0 <= other.0,
      CompareOp::Eq => self.0 == other.0,
      CompareOp::Ne => self.0 != other.0,
      CompareOp::Gt => self.0 > other.0,
      CompareOp::Ge => self.0 >= other.0,
    }
  }
}

/// The information about the current stats of the Raft node.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize))]
#[pyclass(frozen)]
pub struct RaftStats {
  /// The role of the raft.
  #[pyo3(get)]
  role: Role,
  /// The term of the raft.
  #[pyo3(get)]
  term: u64,
  /// The last log index of the raft.
  #[pyo3(get)]
  last_log_index: u64,
  /// Returns the last log term of the raft.
  #[pyo3(get)]
  last_log_term: u64,
  /// Returns the committed index of the raft.
  #[pyo3(get)]
  commit_index: u64,
  /// Returns the applied index of the raft.
  #[pyo3(get)]
  applied_index: u64,
  /// Returns the number of pending fsm requests.
  #[pyo3(get)]
  fsm_pending: u64,
  /// Returns the last snapshot index of the raft.
  #[pyo3(get)]
  last_snapshot_index: u64,
  /// Returns the last snapshot term of the raft.
  #[pyo3(get)]
  last_snapshot_term: u64,
  /// Returns the protocol version of the raft.
  #[pyo3(get)]
  protocol_version: ProtocolVersion,
  /// Returns the version of the snapshot.
  #[pyo3(get)]
  snapshot_version: SnapshotVersion,
  /// Returns the last contact time of the raft.
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde::option"))]
  last_contact: Option<Duration>,
  /// Returns the latest membership in use by Raft.
  #[pyo3(get)]
  latest_membership: Membership,
  /// Returns the index of the latest membership in use by Raft.
  #[pyo3(get)]
  latest_membership_index: u64,
  /// Returns the number of peers in the cluster.
  #[pyo3(get)]
  num_peers: u64,
}

impl From<ruraft_core::RaftStats<RNodeId, RNodeAddress>> for RaftStats {
  fn from(s: ruraft_core::RaftStats<RNodeId, RNodeAddress>) -> Self {
    Self {
      role: s.role().into(),
      term: s.term(),
      last_log_index: s.last_log_index(),
      last_log_term: s.last_log_term(),
      commit_index: s.commit_index(),
      applied_index: s.applied_index(),
      fsm_pending: s.fsm_pending(),
      last_snapshot_index: s.last_snapshot_index(),
      last_snapshot_term: s.last_snapshot_term(),
      protocol_version: s.protocol_version().into(),
      snapshot_version: s.snapshot_version().into(),
      last_contact: s.last_contact(),
      latest_membership: s.latest_membership().clone().into(),
      latest_membership_index: s.latest_membership_index(),
      num_peers: s.num_peers(),
    }
  }
}

impl Pyi for RaftStats {
  fn pyi() -> std::borrow::Cow<'static, str> {
    r#"

class RaftStats:
  @property
  def role(self) -> Role: ...
  
  @property
  def term(self) -> int: ...
  
  @property
  def last_log_index(self) -> int: ...
  
  @property
  def last_log_term(self) -> int: ...
  
  @property
  def commit_index(self) -> int: ...
  
  @property
  def applied_index(self) -> int: ...
  
  @property
  def fsm_pending(self) -> int: ...
  
  @property
  def last_snapshot_index(self) -> int: ...
  
  @property
  def last_snapshot_term(self) -> int: ...
  
  @property
  def protocol_version(self) -> ProtocolVersion: ...
  
  @property
  def snapshot_version(self) -> SnapshotVersion: ...
  
  @property
  def last_contact(self) -> Optional[timedelta]: ...

  def membership(self) -> Membership: ...

  def __str__(self) -> str: ...

  def __repr__(self) -> str: ...

  def __eq__(self, other: RaftStats) -> bool: ...

  def __ne__(self, other: RaftStats) -> bool: ...

"#
    .into()
  }
}

#[pymethods]
impl RaftStats {
  #[getter]
  pub fn last_contact(&self) -> PyResult<Option<::chrono::Duration>> {
    self
      .last_contact
      .map(|d| {
        ::chrono::Duration::from_std(d).map_err(|e| PyErr::new::<PyTypeError, _>(e.to_string()))
      })
      .transpose()
  }

  pub fn __eq__(&self, other: &Self) -> bool {
    self.eq(other)
  }

  pub fn __ne__(&self, other: &Self) -> bool {
    self.ne(other)
  }

  pub fn __str__(&self) -> PyResult<String> {
    if cfg!(feature = "serde") {
      serde_json::to_string(&self).map_err(|e| PyTypeError::new_err(e.to_string()))
    } else {
      Ok(format!("{:?}", self))
    }
  }

  pub fn __repr__(&self) -> String {
    format!("{:?}", self)
  }
}

pub fn register(py: Python<'_>) -> PyResult<&PyModule> {
  let subm = PyModule::new(py, "types")?;
  subm.add_class::<NodeId>()?;
  subm.add_class::<NodeAddress>()?;
  subm.add_class::<Node>()?;
  subm.add_class::<Header>()?;
  subm.add_class::<CommittedLog>()?;
  subm.add_class::<RaftStats>()?;
  subm.add_class::<Role>()?;
  Ok(subm)
}

pub fn pyi() -> String {
  let mut pyi = r#"

from typing import Union, Optional
from datetime import timedelta
from .membership import Membership
from .options import ProtocolVersion, SnapshotVersion

  "#
  .to_string();

  pyi.push_str(&NodeId::pyi());
  pyi.push_str(&NodeAddress::pyi());
  pyi.push_str(&Node::pyi());
  pyi.push_str(&Header::pyi());
  pyi.push_str(&CommittedLog::pyi());
  pyi.push_str(&RaftStats::pyi());
  pyi.push_str(&Role::pyi());

  pyi
}
