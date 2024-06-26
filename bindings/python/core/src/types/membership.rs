use crate::{exceptions::MembershipError, Pyi};

use super::*;

/// The suffrage of a server.
#[pyclass(frozen)]
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
#[repr(u8)]
pub enum ServerSuffrage {
  /// The server can vote.
  #[default]
  Voter = 1,
  /// The server cannot vote.
  #[serde(rename = "nonvoter")]
  Nonvoter = 0,
}

impl From<RServerSuffrage> for ServerSuffrage {
  fn from(s: RServerSuffrage) -> Self {
    match s {
      RServerSuffrage::Voter => Self::Voter,
      RServerSuffrage::Nonvoter => Self::Nonvoter,
      _ => unreachable!(),
    }
  }
}

impl From<ServerSuffrage> for RServerSuffrage {
  fn from(s: ServerSuffrage) -> Self {
    match s {
      ServerSuffrage::Voter => Self::Voter,
      ServerSuffrage::Nonvoter => Self::Nonvoter,
    }
  }
}

impl Pyi for ServerSuffrage {
  fn pyi() -> std::borrow::Cow<'static, str> {
    r#"

class ServerSuffrage:
  def voter(self) -> ServerSuffrage: ...
  
  def nonvoter(self) -> ServerSuffrage: ...
  
  def is_voter(self) -> bool: ...
  
  def is_nonvoter(self) -> bool: ...

  def __str__(self) -> str: ...

  def __repr__(self) -> str: ...

  def __eq__(self, other: ServerSuffrage) -> bool: ...

  def __ne__(self, other: ServerSuffrage) -> bool: ...

  def __hash__(self) -> int: ...

  def __int__(self) -> int: ...

"#
    .into()
  }
}

#[pymethods]
impl ServerSuffrage {
  /// Construct the default suffrage [`ServerSuffrage::Voter`].
  #[inline]
  #[new]
  pub const fn new() -> Self {
    Self::Voter
  }

  /// The server can vote.
  #[inline]
  #[staticmethod]
  pub fn voter() -> Self {
    Self::Voter
  }

  /// The server cannot vote.
  #[inline]
  #[staticmethod]
  pub fn nonvoter() -> Self {
    Self::Nonvoter
  }

  pub fn is_nonvoter(&self) -> bool {
    matches!(self, Self::Nonvoter)
  }

  pub fn is_voter(&self) -> bool {
    matches!(self, Self::Voter)
  }

  #[inline]
  pub fn __str__(&self) -> &'static str {
    match self {
      Self::Voter => "voter",
      Self::Nonvoter => "nonvoter",
    }
  }

  #[inline]
  pub fn __repr__(&self) -> &'static str {
    match self {
      Self::Voter => "ServerSuffrage::Voter",
      Self::Nonvoter => "ServerSuffrage::Nonvoter",
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

/// A server in the cluster.
#[pyclass]
#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct Server(RServer<NodeId, NodeAddress>);

impl From<Server> for RServer<RNodeId, RNodeAddress> {
  fn from(s: Server) -> Self {
    Self::new(s.0.id().0.clone(), s.0.addr().0.clone(), *s.0.suffrage())
  }
}

impl Pyi for Server {
  fn pyi() -> std::borrow::Cow<'static, str> {
    r#"

class Server:
  def __init__(self, id: NodeId, address: NodeAddress, suffrage: ServerSuffrage) -> None: ...

  @property
  def id(self) -> NodeId: ...
  
  @id.setter
  def id(self, value: NodeId) -> None : ...
  
  @property
  def address(self) -> NodeAddress: ...
  
  @address.setter
  def address(self, value: NodeAddress) -> None : ...

  @property
  def suffrage(self) -> ServerSuffrage: ...

  @suffrage.setter
  def suffrage(self, value: ServerSuffrage) -> None : ...
  
  def __str__(self) -> str: ...

  def __repr__(self) -> str: ...

  def __eq__(self, other: Server) -> bool: ...

  def __ne__(self, other: Server) -> bool: ...

  def __hash__(self) -> int: ...

"#
    .into()
  }
}

#[pymethods]
impl Server {
  /// Construct a new [`Server`] from a node and suffrage.
  #[new]
  pub fn new(id: NodeId, addr: NodeAddress, suffrage: ServerSuffrage) -> Self {
    Server(RServer::new(id, addr, suffrage.into()))
  }

  /// Get the [`NodeId`] of the server.
  #[getter]
  pub fn id(&self) -> NodeId {
    self.0.id().clone()
  }

  #[setter]
  pub fn set_id(&mut self, id: NodeId) {
    self.0.set_id(id);
  }

  /// Get the [`NodeAddress`] of the server.
  #[getter]
  pub fn address(&self) -> NodeAddress {
    self.0.addr().clone()
  }

  /// Set the [`NodeAddress`] of the server.
  #[setter]
  pub fn set_address(&mut self, address: NodeAddress) {
    self.0.set_addr(address);
  }

  /// Get the suffrage of the server.
  #[getter]
  pub fn suffrage(&self) -> ServerSuffrage {
    (*self.0.suffrage()).into()
  }

  /// Set the suffrage of the server.
  #[setter]
  pub fn set_suffrage(&mut self, suffrage: ServerSuffrage) {
    self.0.set_suffrage(suffrage.into());
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
}

/// The builder for [`Membership`].
#[pyclass]
#[derive(Clone)]
pub struct MembershipBuilder(RMembershipBuilder<RNodeId, RNodeAddress>);

impl Pyi for MembershipBuilder {
  fn pyi() -> std::borrow::Cow<'static, str> {
    r#"

class MembershipBuilder:
  def __init__(self) -> None: ...

  def insert(self, server: Server) -> None : ...
  
  def insert_many(self, servers: List[Server]) -> None : ...

  def remove(self, id: NodeId) -> None : ...

  def contains_id(self, node: NodeId) -> bool: ...
  
  def contains_addr(self, address: NodeAddress) -> bool: ...
  
  def contains_voter(self) -> bool: ...
  
  def is_voter(self, id: NodeId) -> bool: ...
  
  def is_nonvoter(self, id: NodeId) -> bool: ...

  def is_empty(self) -> bool: ...
  
  def build(self) -> Membership: ...

"#
    .into()
  }
}

#[pymethods]
impl MembershipBuilder {
  /// Construct a new [`MembershipBuilder`].
  #[new]
  pub fn new() -> Self {
    Self(RMembershipBuilder::new())
  }

  /// Add a server to the membership.
  pub fn insert(&mut self, server: Server) -> PyResult<()> {
    self
      .0
      .insert(server.into())
      .map_err(|e| PyTypeError::new_err(e.to_string()))
  }

  /// Inserts a collection of servers into the membership.
  ///
  /// # Errors
  /// - If the one of the server address is already in the membership.
  /// - If the one of the server id is already in the membership.
  pub fn insert_many(&mut self, server: ::smallvec::SmallVec<[Server; 4]>) -> PyResult<()> {
    self
      .0
      .insert_many(server.into_iter().map(|s| s.into()))
      .map_err(|e| PyTypeError::new_err(e.to_string()))
  }

  /// Returns true if the server identified by 'id' is in in the
  /// provided [`Membership`].
  pub fn contains_id(&self, id: &NodeId) -> bool {
    self.0.contains_id(&id.0)
  }

  /// Returns true if the server identified by 'addr' is in in the
  /// provided [`Membership`].
  pub fn contains_addr(&self, addr: &NodeAddress) -> bool {
    self.0.contains_addr(&addr.0)
  }

  /// Returns `true` if the membership contains a server who is [`ServerSuffrage::Voter`].
  pub fn contains_voter(&self) -> bool {
    self.0.contains_voter()
  }

  /// Returns `true` if the server is a [`ServerSuffrage::Voter`].
  pub fn is_voter(&self, id: &NodeId) -> bool {
    self.0.is_voter(&id.0)
  }

  /// Returns `true` if the server is a [`ServerSuffrage::Nonvoter`].
  pub fn is_nonvoter(&self, id: &NodeId) -> bool {
    self.0.is_nonvoter(&id.0)
  }

  /// Returns `true` if the membership contains no elements.
  pub fn is_empty(&self) -> bool {
    self.0.is_empty()
  }

  /// Returns the number of servers in the membership.
  pub fn __len__(&self) -> usize {
    self.0.len()
  }

  /// Remove a server from the membership.
  pub fn remove(&mut self, id: &NodeId) {
    self.0.remove(&id.0);
  }

  /// Build the [`Membership`].
  pub fn build(&self) -> PyResult<Membership> {
    self
      .0
      .clone()
      .build()
      .map_err(|e| MembershipError::new_err(e.to_string()))
      .map(Membership)
  }
}

/// The membership of the cluster.
#[pyclass(frozen)]
#[derive(Clone, PartialEq, Eq, Debug)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct Membership(RMembership<RNodeId, RNodeAddress>);

impl From<Membership> for RMembership<RNodeId, RNodeAddress> {
  fn from(m: Membership) -> Self {
    m.0
  }
}

impl From<RMembership<RNodeId, RNodeAddress>> for Membership {
  fn from(m: RMembership<RNodeId, RNodeAddress>) -> Self {
    Self(m)
  }
}

impl Pyi for Membership {
  fn pyi() -> std::borrow::Cow<'static, str> {
    r#"

class Membership:
  def is_empty(self) -> bool: ...
  
  def contains_id(self, node: NodeId) -> bool: ...
  
  def contains_addr(self, address: NodeAddress) -> bool: ...
  
  def contains_voter(self) -> bool: ...
  
  def is_voter(self, id: NodeId) -> bool: ...
  
  def is_nonvoter(self, id: NodeId) -> bool: ...
  
  def num_voters(self) -> int: ...
  
  def num_nonvoters(self) -> int: ...
  
  def quorum_size(self) -> int: ...

  def __str__(self) -> str: ...

  def __repr__(self) -> str: ...

  def __eq__(self, other: Membership) -> bool: ...

  def __ne__(self, other: Membership) -> bool: ...

"#
    .into()
  }
}

#[pymethods]
impl Membership {
  /// Returns the number of servers in the membership.
  pub fn __len__(&self) -> usize {
    self.0.len()
  }

  /// Returns true if the membership contains no elements.
  pub fn is_empty(&self) -> bool {
    self.0.is_empty()
  }

  /// Returns true if the server identified by 'id' is in in the
  /// provided [`Membership`].
  pub fn contains_id(&self, id: &NodeId) -> bool {
    self.0.contains_id(&id.0)
  }

  /// Returns true if the server identified by 'addr' is in in the
  /// provided [`Membership`].
  pub fn contains_addr(&self, addr: &NodeAddress) -> bool {
    self.0.contains_addr(&addr.0)
  }

  /// Returns `true` if the membership contains a server who is [`ServerSuffrage::Voter`].
  pub fn contains_voter(&self) -> bool {
    self.0.contains_voter()
  }

  /// Returns `true` if the server is a [`ServerSuffrage::Voter`].
  pub fn is_voter(&self, id: &NodeId) -> bool {
    self.0.is_voter(&id.0)
  }

  /// Returns `true` if the server is a [`ServerSuffrage::Nonvoter`].
  pub fn is_nonvoter(&self, id: &NodeId) -> bool {
    self.0.is_nonvoter(&id.0)
  }

  /// Returns the number of voters in the membership.
  pub fn num_voters(&self) -> usize {
    self.0.voters()
  }

  /// Returns the number of non-voters in the membership.
  pub fn num_nonvoters(&self) -> usize {
    self.0.nonvoters()
  }

  /// Returns the quorum size of the membership.
  pub fn quorum_size(&self) -> usize {
    self.0.quorum_size()
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

  pub fn __eq__(&self, other: &Self) -> bool {
    self.0.eq(&other.0)
  }

  pub fn __ne__(&self, other: &Self) -> bool {
    self.0.ne(&other.0)
  }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[pyclass(frozen)]
pub struct LatestMembership {
  index: u64,
  membership: Membership,
}

impl PartialOrd for LatestMembership {
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    Some(self.index.cmp(&other.index))
  }
}

impl Ord for LatestMembership {
  fn cmp(&self, other: &Self) -> std::cmp::Ordering {
    self.index.cmp(&other.index)
  }
}

impl From<ruraft_core::LatestMembership<RNodeId, RNodeAddress>> for LatestMembership {
  fn from(m: ruraft_core::LatestMembership<RNodeId, RNodeAddress>) -> Self {
    let (index, membership) = m.into_components();
    Self {
      index,
      membership: membership.into(),
    }
  }
}

impl Pyi for LatestMembership {
  fn pyi() -> std::borrow::Cow<'static, str> {
    r#"

class LatestMembership:
  def index(self) -> int: ...
  
  def membership(self): Membership: ...

  def __richcmp__(self, other: LatestMembership, op):...

  def __str__(self) -> str: ...

  def __repr__(self) -> str: ...

"#
    .into()
  }
}

#[pymethods]
impl LatestMembership {
  /// Returns the index of the latest membership in use by Raft.
  #[getter]
  pub fn index(&self) -> u64 {
    self.index
  }

  /// Returns the latest membership in use by Raft.
  #[getter]
  pub fn membership(&self) -> Membership {
    self.membership.clone()
  }

  #[inline]
  pub fn __richcmp__(&self, other: &Self, op: CompareOp) -> bool {
    match op {
      CompareOp::Lt => self.index < other.index,
      CompareOp::Le => self.index <= other.index,
      CompareOp::Eq => self.index == other.index && self.membership == other.membership,
      CompareOp::Ne => self.index != other.index || self.membership != other.membership,
      CompareOp::Gt => self.index > other.index,
      CompareOp::Ge => self.index >= other.index,
    }
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
  let subm = PyModule::new(py, "membership")?;

  subm.add_class::<ServerSuffrage>()?;
  subm.add_class::<Server>()?;
  subm.add_class::<MembershipError>()?;
  subm.add_class::<MembershipBuilder>()?;
  subm.add_class::<Membership>()?;
  subm.add_class::<LatestMembership>()?;
  Ok(subm)
}

pub fn pyi() -> String {
  let mut pyi = r#"

from typing import List
from .types import NodeId, NodeAddress

  "#
  .to_string();

  pyi.push_str(&ServerSuffrage::pyi());
  pyi.push_str(&Server::pyi());
  pyi.push_str(&MembershipBuilder::pyi());
  pyi.push_str(&Membership::pyi());
  pyi.push_str(&LatestMembership::pyi());
  pyi
}
