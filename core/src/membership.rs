use std::{borrow::Borrow, sync::Arc};

use arc_swap::ArcSwapAny;
use indexmap::IndexMap;

use crate::transport::{NodeAddress, NodeId};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(
  feature = "serde",
  derive(serde_repr::Serialize_repr, serde_repr::Deserialize_repr)
)]
#[repr(u8)]
pub enum ServerSuffrage {
  Voter,
  Nonvoter,
}

impl ServerSuffrage {
  /// Returns a string representation of the suffrage.
  #[inline]
  pub const fn as_str(&self) -> &'static str {
    match self {
      Self::Voter => "voter",
      Self::Nonvoter => "nonvoter",
    }
  }

  /// Returns `true` is the suffrage is [`ServerSuffrage::Voter`].
  #[inline]
  pub const fn is_voter(&self) -> bool {
    matches!(self, Self::Voter)
  }

  /// Returns `true` is the suffrage is [`ServerSuffrage::Nonvoter`].
  #[inline]
  pub const fn is_nonvoter(&self) -> bool {
    matches!(self, Self::Nonvoter)
  }
}

#[viewit::viewit]
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Server<Id: NodeId, Address: NodeAddress> {
  /// A unique string identifying this server for all time.
  id: Id,
  /// The network address that a transport can contact.
  addr: Address,
  /// Determines whether the server gets a vote.
  suffrage: ServerSuffrage,
}

impl<Id: NodeId, Address: NodeAddress> PartialEq for Server<Id, Address> {
  fn eq(&self, other: &Self) -> bool {
    if self.id == other.id {
      return true;
    }

    if self.addr == other.addr {
      return true;
    }

    false
  }
}

impl<Id: NodeId, Address: NodeAddress> Eq for Server<Id, Address> {}

impl<Id: NodeId, Address: NodeAddress> Server<Id, Address> {
  /// Creates a new `Server`.
  #[inline]
  pub fn new(id: Id, addr: Address, suffrage: ServerSuffrage) -> Self {
    Self { id, addr, suffrage }
  }
}

/// The different ways to change the cluster
/// membership.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(untagged))]
pub enum MembershipChangeCommand<Id: NodeId, Address: NodeAddress> {
  /// Adds a server with [`ServerSuffrage`] of Voter.
  AddVoter {
    /// The server to execute the command on.
    id: Id,
    /// The server address.
    addr: Address,
    /// If nonzero, is the index of the only membership upon which
    /// this change may be applied; if another membership entry has been
    /// added in the meantime, this request will fail.
    prev_index: u64,
  },
  /// Makes a server [`ServerSuffrage::Nonvoter`] unless its [`ServerSuffrage::Staging`] or [`ServerSuffrage::Voter`].
  AddNonvoter {
    /// The server to execute the command on.
    id: Id,
    /// The server address.
    addr: Address,
    /// If nonzero, is the index of the only membership upon which
    /// this change may be applied; if another membership entry has been
    /// added in the meantime, this request will fail.
    prev_index: u64,
  },
  /// Makes a server [`ServerSuffrage::Nonvoter`] unless its absent.
  DemoteVoter {
    /// The server to execute the command on.
    id: Id,
    /// If nonzero, is the index of the only membership upon which
    /// this change may be applied; if another membership entry has been
    /// added in the meantime, this request will fail.
    prev_index: u64,
  },
  /// Removes a server entirely from the cluster membership.
  RemoveServer {
    /// The server to execute the command on.
    id: Id,
    /// If nonzero, is the index of the only membership upon which
    /// this change may be applied; if another membership entry has been
    /// added in the meantime, this request will fail.
    prev_index: u64,
  },
}

impl<Id: NodeId, Address: NodeAddress> MembershipChangeCommand<Id, Address> {
  /// Returns a string representation of the command.
  #[inline]
  pub const fn as_str(&self) -> &'static str {
    match self {
      Self::AddVoter { .. } => "AddVoter",
      Self::AddNonvoter { .. } => "AddNonvoter",
      Self::DemoteVoter { .. } => "DemoteVoter",
      Self::RemoveServer { .. } => "RemoveServer",
    }
  }

  /// Returns [`MembershipChangeCommand::AddVoter`].
  #[inline]
  pub const fn add_voter(id: Id, addr: Address, prev_index: u64) -> Self {
    Self::AddNonvoter {
      id,
      addr,
      prev_index,
    }
  }

  /// Returns [`MembershipChangeCommand::AddNonvoter`].
  #[inline]
  pub const fn add_nonvoter(id: Id, addr: Address, prev_index: u64) -> Self {
    Self::AddNonvoter {
      id,
      addr,
      prev_index,
    }
  }

  /// Returns [`MembershipChangeCommand::DemoteVoter`].
  #[inline]
  pub const fn demote_voter(id: Id, prev_index: u64) -> Self {
    Self::DemoteVoter { id, prev_index }
  }

  /// Returns [`MembershipChangeCommand::RemoveServer`].
  #[inline]
  pub const fn remove_server(id: Id, prev_index: u64) -> Self {
    Self::RemoveServer { id, prev_index }
  }
}

/// Tracks which servers are in the cluster, and whether they have
/// votes. This should include the local server, if it's a member of the cluster.
/// The servers are listed no particular order, but each should only appear once.
/// These entries are appended to the log during membership changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Membership<Id: NodeId, Address: NodeAddress> {
  servers: IndexMap<Id, (Address, ServerSuffrage)>,
}

impl<Id: NodeId, Address: NodeAddress> Default for Membership<Id, Address> {
  fn default() -> Self {
    Self::new()
  }
}

#[cfg(feature = "serde")]
impl<Id: NodeId + serde::Serialize, Address: NodeAddress + serde::Serialize> serde::Serialize
  for Membership<Id, Address>
{
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: serde::Serializer,
  {
    use serde::ser::SerializeSeq;

    #[derive(serde::Serialize)]
    struct ServerRef<'a, Id, Address> {
      id: &'a Id,
      addr: &'a Address,
      suffrage: ServerSuffrage,
    }

    let mut seq = serializer.serialize_seq(Some(self.servers.len()))?;
    for (id, (addr, suffrage)) in self.servers.iter() {
      let server = ServerRef {
        id: &id,
        addr: &addr,
        suffrage: *suffrage,
      };
      seq.serialize_element(&server)?;
    }
    seq.end()
  }
}

#[cfg(feature = "serde")]
impl<'de, Id: NodeId + serde::Deserialize<'de>, Address: NodeAddress + serde::Deserialize<'de>>
  serde::Deserialize<'de> for Membership<Id, Address>
{
  fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
  where
    D: serde::Deserializer<'de>,
  {
    let servers = Vec::<Server<Id, Address>>::deserialize(deserializer)?;
    let mut membership = Membership::new();
    membership
      .insert_many(servers.into_iter())
      .and_then(|_| membership.validate().map(|_| membership))
      .map_err(<D::Error as serde::de::Error>::custom)
  }
}

impl<Id: NodeId, Address: NodeAddress> FromIterator<Server<Id, Address>>
  for Result<Membership<Id, Address>, MembershipError<Id, Address>>
{
  fn from_iter<T: IntoIterator<Item = Server<Id, Address>>>(iter: T) -> Self {
    let mut membership = Membership::new();
    membership
      .insert_many(iter.into_iter())
      .and_then(|_| membership.validate().map(|_| membership))
  }
}

impl<Id: NodeId, Address: NodeAddress> Membership<Id, Address> {
  /// Create a new membership.
  pub fn new() -> Self {
    Self {
      servers: IndexMap::new(),
    }
  }

  /// Inserts a new server into the membership.
  ///
  /// # Errors
  /// - If the server address is already in the membership.
  /// - If the server id is already in the membership.
  pub fn insert(
    &mut self,
    server: Server<Id, Address>,
  ) -> Result<(), MembershipError<Id, Address>> {
    if self.servers.contains_key(&server.id) {
      return Err(MembershipError::DuplicateId(server.id));
    }

    if self.contains_addr(&server.addr) {
      return Err(MembershipError::DuplicateAddress(server.addr));
    }

    self
      .servers
      .insert(server.id, (server.addr, server.suffrage));
    Ok(())
  }

  /// Inserts a collection of servers into the membership.
  ///
  /// # Errors
  /// - If the one of the server address is already in the membership.
  /// - If the one of the server id is already in the membership.
  pub fn insert_many(
    &mut self,
    mut servers: impl Iterator<Item = Server<Id, Address>>,
  ) -> Result<(), MembershipError<Id, Address>> {
    servers.try_for_each(|server| self.insert(server))
  }

  /// Returns an iterator over the membership
  pub fn iter(&self) -> impl Iterator<Item = (&Id, &(Address, ServerSuffrage))> {
    self.servers.iter()
  }

  /// Returns an iterator that allows modifying each value.
  pub fn iter_mut(&mut self) -> impl Iterator<Item = (&Id, &mut (Address, ServerSuffrage))> {
    self.servers.iter_mut()
  }

  /// Returns the number of server in the membership, also referred to as its 'length'.
  pub fn len(&self) -> usize {
    self.servers.len()
  }

  /// Returns `true` if the membership contains no elements
  pub fn is_empty(&self) -> bool {
    self.servers.is_empty()
  }

  /// Validates a cluster membership configuration for common
  /// errors.
  pub fn validate(&self) -> Result<(), MembershipError<Id, Address>> {
    self
      .servers
      .values()
      .find(|s| s.1 == ServerSuffrage::Voter)
      .ok_or(MembershipError::EmptyVoter)
      .map(|_| {})
  }

  /// Returns `true` if the server is a [`ServerSuffrage::Voter`].
  pub fn is_voter<Q>(&self, id: &Q) -> bool
  where
    Id: Borrow<Q>,
    Q: core::hash::Hash + Eq + ?Sized,
  {
    self
      .servers
      .get(id)
      .map(|(_, s)| *s == ServerSuffrage::Voter)
      .unwrap_or_default()
  }

  /// Returns `true` if the membership contains a [`ServerSuffrage::Voter`].
  pub fn contains_voter(&self) -> bool {
    self.servers.values().any(|s| s.1 == ServerSuffrage::Voter)
  }

  /// Returns true if the server identified by 'id' is in in the
  /// provided [`Memberhsip`].
  pub fn contains_id<Q>(&self, id: &Q) -> bool
  where
    Id: Borrow<Q>,
    Q: core::hash::Hash + Eq + ?Sized,
  {
    self.servers.contains_key(id)
  }

  /// Returns true if the server address is in in the
  /// provided [`Memberhsip`].
  pub fn contains_addr<Q>(&self, addr: &Q) -> bool
  where
    Address: std::borrow::Borrow<Q>,
    Q: ?Sized + Eq,
  {
    self.servers.values().any(|s| s.0.borrow() == addr)
  }

  /// Remove a server from the membership and return its address and suffrage.
  pub fn remove_by_id<Q>(&mut self, id: &Q) -> Option<Server<Id, Address>>
  where
    Id: Borrow<Q>,
    Q: core::hash::Hash + Eq + ?Sized,
  {
    self
      .servers
      .remove_entry(id)
      .map(|(id, (addr, suffrage))| Server { id, addr, suffrage })
  }

  /// Generates a new [`Membership`] from the current one and a
  /// [`MembershipChangeCommand`]. It's split from append_membership_entry so
  /// that it can be unit tested easily.
  pub(crate) fn next(
    &self,
    current_index: u64,
    change: MembershipChangeCommand<Id, Address>,
  ) -> Result<Self, MembershipError<Id, Address>> {
    let check = |prev_index: u64| -> Result<(), MembershipError<Id, Address>> {
      if prev_index > 0 && prev_index != current_index {
        return Err(MembershipError::AlreadyChanged {
          since: prev_index,
          latest: current_index,
        });
      }

      Ok(())
    };

    match change {
      MembershipChangeCommand::AddVoter {
        id,
        addr,
        prev_index,
      } => check(prev_index).and_then(|_| {
        let mut new = self.clone();
        if let Some((address, suffrage)) = new.servers.get_mut(&id) {
          if *suffrage != ServerSuffrage::Voter {
            *suffrage = ServerSuffrage::Voter;
          }

          *address = addr;
        } else {
          new.servers.insert(id, (addr, ServerSuffrage::Voter));
        }
        new.validate().map(|_| new)
      }),
      MembershipChangeCommand::AddNonvoter {
        id,
        addr,
        prev_index,
      } => check(prev_index).and_then(|_| {
        let mut new = self.clone();
        if let Some((address, suffrage)) = new.servers.get_mut(&id) {
          if *suffrage != ServerSuffrage::Nonvoter {
            *suffrage = ServerSuffrage::Nonvoter;
          }

          *address = addr;
        } else {
          new.servers.insert(id, (addr, ServerSuffrage::Nonvoter));
        }
        new.validate().map(|_| new)
      }),
      MembershipChangeCommand::DemoteVoter { id, prev_index } => check(prev_index).and_then(|_| {
        let mut new = self.clone();
        if let Some((_, suffrage)) = new.servers.get_mut(&id) {
          *suffrage = ServerSuffrage::Nonvoter;
        }
        new.validate().map(|_| new)
      }),
      MembershipChangeCommand::RemoveServer { id, prev_index } => {
        check(prev_index).and_then(|_| {
          let mut new = self.clone();
          new.servers.remove(&id);
          new.validate().map(|_| new)
        })
      }
    }
  }
}

/// State tracked on every server about its Memberships.
/// Note that, per Diego's dissertation, there can be at most one uncommitted
/// membership at a time (the next membership may not be created until the
/// prior one has been committed).
///
/// One downside to storing just two memberships is that if you try to take a
/// snapshot when your state machine hasn't yet applied the `committed_index`, we
/// have no record of the membership that would logically fit into that
/// snapshot. We disallow snapshots in that case now. An alternative approach,
/// which LogCabin uses, is to track every membership change in the
/// log.
// TODO(al8n): Implement a WAL for membership changes.
#[viewit::viewit(setters(skip), getters(skip))]
pub(crate) struct Memberships<Id: NodeId, Address: NodeAddress> {
  /// committed is the latest membership in the log/snapshot that has been
  /// committed (the one with the largest index).
  committed: ArcSwapAny<Arc<(u64, Membership<Id, Address>)>>,
  /// latest is the latest membership in the log/snapshot (may be committed
  /// or uncommitted)
  latest: ArcSwapAny<Arc<(u64, Membership<Id, Address>)>>,
}

impl<Id: NodeId, Address: NodeAddress> Memberships<Id, Address> {
  pub(crate) fn set_latest(&self, membership: Membership<Id, Address>, index: u64) {
    self.latest.store(Arc::new((index, membership)))
  }

  pub(crate) fn set_committed(&self, membership: Membership<Id, Address>, index: u64) {
    self.committed.store(Arc::new((index, membership)))
  }

  pub(crate) fn latest(&self) -> arc_swap::Guard<Arc<(u64, Membership<Id, Address>)>> {
    self.latest.load()
  }

  pub(crate) fn committed(&self) -> arc_swap::Guard<Arc<(u64, Membership<Id, Address>)>> {
    self.committed.load()
  }
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum MembershipError<Id: NodeId, Address: NodeAddress> {
  #[error("found duplicate server address {0}")]
  DuplicateAddress(Address),
  #[error("found duplicate server id {0}")]
  DuplicateId(Id),
  #[error("server id cannot be empty")]
  EmptyServerId,
  #[error("no voter in the membership")]
  EmptyVoter,
  #[error("membership changed since {since} (latest is {latest})")]
  AlreadyChanged { since: u64, latest: u64 },
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::net::SocketAddr;

  fn sample_membership() -> Membership<String, SocketAddr> {
    let mut membership = Membership::new();
    membership
      .insert(Server {
        id: "id0".to_string(),
        addr: "127.0.0.1:8080".parse().unwrap(),
        suffrage: ServerSuffrage::Nonvoter,
      })
      .unwrap();

    membership
      .insert(Server {
        id: "id1".to_string(),
        addr: "127.0.0.1:8081".parse().unwrap(),
        suffrage: ServerSuffrage::Voter,
      })
      .unwrap();

    membership
      .insert(Server {
        id: "id2".to_string(),
        addr: "127.0.0.1:8082".parse().unwrap(),
        suffrage: ServerSuffrage::Nonvoter,
      })
      .unwrap();

    membership
  }

  fn single_server() -> Membership<String, SocketAddr> {
    let mut membership = Membership::new();
    membership
      .insert(Server {
        id: "id1".to_string(),
        addr: "127.0.0.1:8081".parse().unwrap(),
        suffrage: ServerSuffrage::Voter,
      })
      .unwrap();
    membership
  }

  #[test]
  fn test_membership_is_voter() {
    let membership = sample_membership();
    assert!(!membership.is_voter("id0"));
    assert!(membership.is_voter("id1"));
    assert!(!membership.is_voter("id2"));
  }

  #[test]
  fn test_membership_validate() {
    let Err(MembershipError::EmptyVoter) = Membership::<String, SocketAddr>::new().validate() else {
      panic!("should have failed for non voter")
    };

    let mut members = Membership::new();

    members.servers.insert(
      "id0".to_string(),
      ("127.0.0.1:8080".parse().unwrap(), ServerSuffrage::Nonvoter),
    );
    let Err(MembershipError::EmptyVoter) = members.validate() else {
      panic!("should have failed for non voter")
    };

    members.servers.insert(
      "id1".to_string(),
      ("127.0.0.1:8081".parse().unwrap(), ServerSuffrage::Voter),
    );
    members.validate().expect("should be ok");

    let id = "id0".to_string();
    let Err(MembershipError::DuplicateId(did)) = members.insert(Server::new(
      id.clone(),
      "127.0.0.1:8083".parse().unwrap(),
      ServerSuffrage::Voter,
    )) else {
      panic!("should have failed for duplicate id")
    };
    assert_eq!(did, id);

    let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
    let Err(MembershipError::DuplicateAddress(daddr)) = members.insert(Server::new(
      "id3".to_string(),
      addr,
      ServerSuffrage::Voter,
    )) else {
      panic!("should have failed for duplicate addr")
    };
    assert_eq!(daddr, addr);

    let command = MembershipChangeCommand::remove_server(id, 0);
    members.next(1, command).unwrap();

    let command = MembershipChangeCommand::demote_voter("id1".to_string(), 1);
    let Err(MembershipError::EmptyVoter) = members.next(1, command) else {
      panic!("should have failed for non voter")
    };
  }

  #[test]
  fn test_membership_next_prev_index() {
    let command =
      MembershipChangeCommand::add_voter("id2".to_string(), "127.0.0.1:8082".parse().unwrap(), 1);

    let err = single_server().next(2, command).unwrap_err();
    assert_eq!(
      err,
      MembershipError::AlreadyChanged {
        since: 1,
        latest: 2
      }
    );

    // current prev_index
    let command =
      MembershipChangeCommand::add_voter("id3".to_string(), "127.0.0.1:8083".parse().unwrap(), 2);
    single_server()
      .next(2, command)
      .expect("should have succeeded");

    // zero prev_index
    let command =
      MembershipChangeCommand::add_voter("id4".to_string(), "127.0.0.1:8084".parse().unwrap(), 2);
    single_server()
      .next(2, command)
      .expect("should have succeeded");
  }

  #[test]
  fn test_membership_next_and_validate() {
    let membership = Membership::<String, SocketAddr>::default();
    let command = MembershipChangeCommand::add_nonvoter(
      "id1".to_string(),
      "127.0.0.1:8080".parse().unwrap(),
      0,
    );
    let err = membership.next(1, command).unwrap_err();
    assert_eq!(err, MembershipError::EmptyVoter);
  }
}
