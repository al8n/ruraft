use std::{
  borrow::Borrow,
  fmt::{Debug, Display},
  hash::Hash,
  mem,
  sync::Arc,
};

use arc_swap::ArcSwapAny;
use indexmap::IndexMap;
use nodecraft::{Transformable, CheapClone};

use crate::{
  transport::{Address, Id},
  utils::invalid_data,
};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(
  feature = "serde",
  derive(serde_repr::Serialize_repr, serde_repr::Deserialize_repr)
)]
#[repr(u8)]
#[non_exhaustive]
pub enum ServerSuffrage {
  Voter,
  Nonvoter,
}

/// Returend when the fail to parse [`ServerSuffrage`].
#[derive(Debug)]
pub struct UnknownServerSuffrage(u8);

impl core::fmt::Display for UnknownServerSuffrage {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{} is not a valid server suffrage", self.0)
  }
}

impl std::error::Error for UnknownServerSuffrage {}

impl TryFrom<u8> for ServerSuffrage {
  type Error = UnknownServerSuffrage;

  fn try_from(value: u8) -> Result<Self, Self::Error> {
    match value {
      0 => Ok(Self::Voter),
      1 => Ok(Self::Nonvoter),
      val => Err(UnknownServerSuffrage(val)),
    }
  }
}

impl ServerSuffrage {
  const SIZE: usize = mem::size_of::<Self>();

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
pub struct Server<I, A> {
  /// A unique string identifying this server for all time.
  id: I,
  /// The network address that a transport can contact.
  addr: A,
  /// Determines whether the server gets a vote.
  suffrage: ServerSuffrage,
}

impl<I: PartialEq, A: PartialEq> PartialEq for Server<I, A> {
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

impl<I: Eq, A: Eq> Eq for Server<I, A> {}

impl<I, A> Server<I, A> {
  /// Creates a new `Server`.
  #[inline]
  pub fn new(id: I, addr: A, suffrage: ServerSuffrage) -> Self {
    Self { id, addr, suffrage }
  }
}

/// The different ways to change the cluster
/// membership.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(untagged))]
pub(crate) enum MembershipChangeCommand<I, A> {
  /// Adds a server with [`ServerSuffrage`] of Voter.
  AddVoter {
    /// The server to execute the command on.
    id: I,
    /// The server address.
    addr: A,
    /// If nonzero, is the index of the only membership upon which
    /// this change may be applied; if another membership entry has been
    /// added in the meantime, this request will fail.
    prev_index: u64,
  },
  /// Makes a server [`ServerSuffrage::Nonvoter`] unless its [`ServerSuffrage::Staging`] or [`ServerSuffrage::Voter`].
  AddNonvoter {
    /// The server to execute the command on.
    id: I,
    /// The server address.
    addr: A,
    /// If nonzero, is the index of the only membership upon which
    /// this change may be applied; if another membership entry has been
    /// added in the meantime, this request will fail.
    prev_index: u64,
  },
  /// Makes a server [`ServerSuffrage::Nonvoter`] unless its absent.
  DemoteVoter {
    /// The server to execute the command on.
    id: I,
    /// If nonzero, is the index of the only membership upon which
    /// this change may be applied; if another membership entry has been
    /// added in the meantime, this request will fail.
    prev_index: u64,
  },
  /// Removes a server entirely from the cluster membership.
  RemoveServer {
    /// The server to execute the command on.
    id: I,
    /// If nonzero, is the index of the only membership upon which
    /// this change may be applied; if another membership entry has been
    /// added in the meantime, this request will fail.
    prev_index: u64,
  },
}

impl<I, A> MembershipChangeCommand<I, A> {
  /// Returns [`MembershipChangeCommand::AddVoter`].
  #[inline]
  pub const fn add_voter(id: I, addr: A, prev_index: u64) -> Self {
    Self::AddVoter {
      id,
      addr,
      prev_index,
    }
  }

  /// Returns [`MembershipChangeCommand::AddNonvoter`].
  #[inline]
  pub const fn add_nonvoter(id: I, addr: A, prev_index: u64) -> Self {
    Self::AddNonvoter {
      id,
      addr,
      prev_index,
    }
  }

  /// Returns [`MembershipChangeCommand::DemoteVoter`].
  #[inline]
  pub const fn demote_voter(id: I, prev_index: u64) -> Self {
    Self::DemoteVoter { id, prev_index }
  }

  /// Returns [`MembershipChangeCommand::RemoveServer`].
  #[inline]
  pub const fn remove_server(id: I, prev_index: u64) -> Self {
    Self::RemoveServer { id, prev_index }
  }
}

/// The error type returned when encoding [`Membership`] to bytes or decoding a [`Membership`] from bytes.
#[derive(Debug)]
pub enum MembershipTransformableError<I: Transformable, A: Transformable> {
  /// Returned when the encode or decode id fails.
  Id(I::Error),
  /// Returned when the encode or decode address fails.
  Address(A::Error),
  IdTooLarge(I),
  AddressTooLarge(A),
  /// Returned when the number of nodes is too large.
  TooLarge(usize),
  /// Returned when the encode buffer is too small.
  EncodeBufferTooSmall,
  /// Returned when decode buffer has less data than expected.
  Corrupted,
  /// Returned when the suffrage is unknown.
  UnknownServerSuffrage(UnknownServerSuffrage),
  /// Returned when the membership is invalid.
  Membership(MembershipError<I, A>),
}

impl<I: Transformable, A: Transformable> From<MembershipError<I, A>>
  for MembershipTransformableError<I, A>
{
  fn from(e: MembershipError<I, A>) -> Self {
    Self::Membership(e)
  }
}

impl<I: Transformable, A: Transformable> From<UnknownServerSuffrage>
  for MembershipTransformableError<I, A>
{
  fn from(e: UnknownServerSuffrage) -> Self {
    Self::UnknownServerSuffrage(e)
  }
}

impl<I: Display + Transformable, A: Display + Transformable> Display
  for MembershipTransformableError<I, A>
{
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      MembershipTransformableError::Id(e) => write!(f, "id error: {}", e),
      MembershipTransformableError::Address(e) => write!(f, "address error: {}", e),
      MembershipTransformableError::IdTooLarge(id) => {
        write!(f, "the encoded size of id({}) is too large", id)
      }
      MembershipTransformableError::AddressTooLarge(addr) => {
        write!(f, "the encoded size of address({}) is too large", addr)
      }
      MembershipTransformableError::TooLarge(size) => {
        write!(f, "membership too large, too many servers({})", size)
      }
      MembershipTransformableError::EncodeBufferTooSmall => write!(f, "encode buffer too small"),
      MembershipTransformableError::Corrupted => write!(f, "corrupted"),
      MembershipTransformableError::UnknownServerSuffrage(e) => write!(f, "{}", e),
      MembershipTransformableError::Membership(e) => write!(f, "{}", e),
    }
  }
}

impl<I: Display + Debug + Transformable, A: Display + Debug + Transformable> std::error::Error
  for MembershipTransformableError<I, A>
{
}

#[derive(Debug, Clone)]
pub struct MembershipBuilder<I, A> {
  pub(crate) voters: usize,
  pub(crate) servers: IndexMap<I, (A, ServerSuffrage)>,
}

impl<I, A> Default for MembershipBuilder<I, A> {
  fn default() -> Self {
    Self::new()
  }
}

impl<I, A> MembershipBuilder<I, A> {
  /// Create a new membership.
  pub fn new() -> Self {
    Self {
      voters: 0,
      servers: IndexMap::new(),
    }
  }

  /// Create a new map with capacity for n key-value pairs. (Does not allocate if n is zero.)
  /// Computes in O(n) time.
  pub fn with_capacity(cap: usize) -> Self {
    Self {
      voters: 0,
      servers: IndexMap::with_capacity(cap),
    }
  }

  /// Returns the quorum size based on the current membership.
  pub const fn quorum_size(&self) -> usize {
    (self.voters / 2) + 1
  }

  /// Returns an iterator over the membership
  pub fn iter(&self) -> impl Iterator<Item = (&I, &(A, ServerSuffrage))> {
    self.servers.iter()
  }

  /// Returns an iterator that allows modifying each value.
  pub fn iter_mut(&mut self) -> impl Iterator<Item = (&I, &mut (A, ServerSuffrage))> {
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

  /// Returns `true` if the membership contains a [`ServerSuffrage::Voter`].
  pub fn contains_voter(&self) -> bool {
    self.servers.values().any(|s| s.1 == ServerSuffrage::Voter)
  }
}

impl<I: Eq + Hash, A> MembershipBuilder<I, A> {
  /// Finish building the membership.
  pub fn build(self) -> Result<Membership<I, A>, MembershipError<I, A>> {
    self.validate().map(|_| Membership {
      quorum_size: self.quorum_size(),
      voters: self.voters,
      servers: Arc::new(self.servers),
    })
  }

  /// Validates a cluster membership configuration for common
  /// errors.
  fn validate(&self) -> Result<(), MembershipError<I, A>> {
    self
      .servers
      .values()
      .find(|s| s.1 == ServerSuffrage::Voter)
      .ok_or(MembershipError::EmptyVoter)
      .map(|_| {})
  }
}

impl<I: Eq + Hash, A: Eq> MembershipBuilder<I, A> {
  /// Inserts a new server into the membership.
  ///
  /// # Errors
  /// - If the server address is already in the membership.
  /// - If the server id is already in the membership.
  pub fn insert(&mut self, server: Server<I, A>) -> Result<(), MembershipError<I, A>> {
    if self.servers.contains_key(&server.id) {
      return Err(MembershipError::DuplicateId(server.id));
    }

    if self.contains_addr(&server.addr) {
      return Err(MembershipError::DuplicateAddress(server.addr));
    }

    if server.suffrage == ServerSuffrage::Voter {
      self.voters += 1;
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
    mut servers: impl Iterator<Item = Server<I, A>>,
  ) -> Result<(), MembershipError<I, A>> {
    servers.try_for_each(|server| self.insert(server))
  }

  /// Returns `true` if the server is a [`ServerSuffrage::Voter`].
  pub fn is_voter<Q>(&self, id: &Q) -> bool
  where
    I: Borrow<Q>,
    Q: core::hash::Hash + Eq + ?Sized,
  {
    self
      .servers
      .get(id)
      .map(|(_, s)| *s == ServerSuffrage::Voter)
      .unwrap_or_default()
  }

  /// Returns true if the server identified by 'id' is in in the
  /// provided [`Membership`].
  pub fn contains_id<Q>(&self, id: &Q) -> bool
  where
    I: Borrow<Q>,
    Q: core::hash::Hash + Eq + ?Sized,
  {
    self.servers.contains_key(id)
  }

  /// Remove a server from the membership and return its address and suffrage.
  pub fn remove<Q>(&mut self, id: &Q) -> Option<Server<I, A>>
  where
    I: Borrow<Q>,
    Q: core::hash::Hash + Eq + ?Sized,
  {
    self
      .servers
      .remove_entry(id)
      .map(|(id, (addr, suffrage))| Server { id, addr, suffrage })
  }
}

impl<I, A: Eq> MembershipBuilder<I, A> {
  /// Returns true if the server address is in in the
  /// provided [`Membership`].
  pub fn contains_addr<Q>(&self, addr: &Q) -> bool
  where
    A: std::borrow::Borrow<Q>,
    Q: ?Sized + Eq,
  {
    self.servers.values().any(|s| s.0.borrow() == addr)
  }
}

/// Tracks which servers are in the cluster, and whether they have
/// votes. This should include the local server, if it's a member of the cluster.
/// The servers are listed no particular order, but each should only appear once.
/// These entries are appended to the log during membership changes.
#[derive(Debug)]
pub struct Membership<I, A> {
  pub(crate) quorum_size: usize,
  pub(crate) voters: usize,
  pub(crate) servers: Arc<IndexMap<I, (A, ServerSuffrage)>>,
}

impl<I, A> Clone for Membership<I, A> {
  fn clone(&self) -> Self {
    Self {
      quorum_size: self.quorum_size,
      voters: self.voters,
      servers: self.servers.clone(),
    }
  }
}

impl<I, A> CheapClone for Membership<I, A> {
  fn cheap_clone(&self) -> Self {
    Self {
      quorum_size: self.quorum_size,
      voters: self.voters,
      servers: self.servers.cheap_clone(),
    }
  }
}

impl<I: Display, A: Display> Display for Membership<I, A> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let len = self.servers.len();
    write!(f, "[")?;
    for (idx, (id, (addr, s))) in self.servers.iter().enumerate() {
      if idx == 0 {
        write!(f, "[{}({}):{},", id, addr, s.as_str())?;
        continue;
      }

      if idx == len - 1 {
        write!(f, "{}({}):{}]", id, addr, s.as_str())?;
        break;
      }

      write!(f, "{}({}):{},", id, addr, s.as_str())?;
    }
    Ok(())
  }
}

const U32_SIZE: usize = mem::size_of::<u32>();

impl<I, A> Transformable for Membership<I, A>
where
  I: Id + Send + Sync + 'static,
  <I as Transformable>::Error: Send + Sync + 'static,
  A: Address + Send + Sync + 'static,
  <A as Transformable>::Error: Send + Sync + 'static,
{
  type Error = MembershipTransformableError<I, A>;

  fn encode(&self, dst: &mut [u8]) -> Result<(), Self::Error> {
    let dst_len = dst.len();
    let encoded_len = self.encoded_len();
    if encoded_len > u32::MAX as usize {
      return Err(Self::Error::TooLarge(encoded_len));
    }

    if dst_len < encoded_len {
      return Err(Self::Error::EncodeBufferTooSmall);
    }

    let total_servers = self.servers.len();
    if total_servers > u32::MAX as usize {
      return Err(Self::Error::TooLarge(total_servers));
    }

    let mut cur = 0;
    dst[cur..cur + U32_SIZE].copy_from_slice(&(encoded_len as u32).to_be_bytes());
    cur += U32_SIZE;

    let total_servers = total_servers as u32;
    dst[cur..cur + U32_SIZE].copy_from_slice(&total_servers.to_be_bytes());
    cur += U32_SIZE;

    for (id, (addr, suffrage)) in self.servers.iter() {
      let id_len = id.encoded_len();
      let addr_len = addr.encoded_len();
      if id_len > u32::MAX as usize {
        return Err(Self::Error::IdTooLarge(id.clone()));
      }

      if addr_len > u32::MAX as usize {
        return Err(Self::Error::AddressTooLarge(addr.clone()));
      }
      id.encode(dst[cur..cur + id_len].as_mut())
        .map_err(Self::Error::Id)?;
      cur += id_len;
      addr
        .encode(dst[cur..cur + addr_len].as_mut())
        .map_err(Self::Error::Address)?;
      cur += addr_len;
      dst[cur] = *suffrage as u8;
      cur += ServerSuffrage::SIZE;
    }
    Ok(())
  }

  /// Encodes the value into the given writer.
  ///
  /// # Note
  /// The implementation of this method is not optimized, which means
  /// if your writer is expensive (e.g. [`TcpStream`](std::net::TcpStream), [`File`](std::fs::File)),
  /// it is better to use a [`BufWriter`](std::io::BufWriter)
  /// to wrap your orginal writer to cut down the number of I/O times.
  fn encode_to_writer<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
    let encoded_len = self.encoded_len();
    if encoded_len > u32::MAX as usize {
      return Err(invalid_data(Self::Error::TooLarge(encoded_len)));
    }

    let total_servers = self.servers.len();
    if total_servers > u32::MAX as usize {
      return Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        Self::Error::TooLarge(total_servers),
      ));
    }

    let mut inlined = [0; U32_SIZE * 2];
    inlined[..U32_SIZE].copy_from_slice(&(encoded_len as u32).to_be_bytes());
    inlined[U32_SIZE..].copy_from_slice(&(total_servers as u32).to_be_bytes());
    writer.write_all(&inlined)?;

    for (id, (addr, suffrage)) in self.servers.iter() {
      let id_len = id.encoded_len();
      let addr_len = addr.encoded_len();
      if id_len > u32::MAX as usize {
        return Err(invalid_data(Self::Error::IdTooLarge(id.clone())));
      }

      if addr_len > u32::MAX as usize {
        return Err(invalid_data(Self::Error::AddressTooLarge(addr.clone())));
      }
      id.encode_to_writer(writer)?;
      addr.encode_to_writer(writer)?;
      writer.write_all(&[*suffrage as u8])?;
    }
    Ok(())
  }

  /// Encodes the value into the given async writer.
  ///
  /// # Note
  /// The implementation of this method is not optimized, which means
  /// if your writer is expensive (e.g. `TcpStream`, `File`),
  /// it is better to use a [`BufWriter`](futures::io::BufWriter)
  /// to wrap your orginal writer to cut down the number of I/O times.
  async fn encode_to_async_writer<W: futures::io::AsyncWrite + Send + Unpin>(
    &self,
    writer: &mut W,
  ) -> std::io::Result<()>
  where
    Self::Error: Send + Sync + 'static,
  {
    use futures::io::AsyncWriteExt;
    let encoded_len = self.encoded_len();
    if encoded_len > u32::MAX as usize {
      return Err(invalid_data(Self::Error::TooLarge(encoded_len)));
    }

    let total_servers = self.servers.len();
    if total_servers > u32::MAX as usize {
      return Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        Self::Error::TooLarge(total_servers),
      ));
    }

    let mut inlined = [0; U32_SIZE * 2];
    inlined[..U32_SIZE].copy_from_slice(&(encoded_len as u32).to_be_bytes());
    inlined[U32_SIZE..].copy_from_slice(&(total_servers as u32).to_be_bytes());
    writer.write_all(&inlined).await?;

    for (id, (addr, suffrage)) in self.servers.iter() {
      let id_len = id.encoded_len();
      let addr_len = addr.encoded_len();
      if id_len > u32::MAX as usize {
        return Err(invalid_data(Self::Error::IdTooLarge(id.clone())));
      }

      if addr_len > u32::MAX as usize {
        return Err(invalid_data(Self::Error::AddressTooLarge(addr.clone())));
      }
      id.encode_to_async_writer(writer).await?;
      addr.encode_to_async_writer(writer).await?;
      writer.write_all(&[*suffrage as u8]).await?;
    }
    Ok(())
  }

  fn encoded_len(&self) -> usize {
    U32_SIZE // length of encoded bytes
    + U32_SIZE // total servers
    + self.servers.iter().map(|(id, (addr, _))| {
      let id_len = id.encoded_len();
      let addr_len = addr.encoded_len();
      id_len + addr_len + ServerSuffrage::SIZE
    }).sum::<usize>()
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    let mut cur = 0;
    if src.len() < U32_SIZE * 2 {
      return Err(Self::Error::Corrupted);
    }
    let len =
      u32::from_be_bytes(src[cur..cur + mem::size_of::<u32>()].try_into().unwrap()) as usize;
    cur += U32_SIZE;
    if src.len() < len {
      return Err(Self::Error::Corrupted);
    }
    let total_servers =
      u32::from_be_bytes(src[cur..cur + mem::size_of::<u32>()].try_into().unwrap()) as usize;
    cur += U32_SIZE;

    let mut voters = 0;
    let mut servers = IndexMap::with_capacity(total_servers);
    while servers.len() < total_servers {
      let (readed, id) = I::decode(&src[cur..]).map_err(Self::Error::Id)?;
      cur += readed;
      let (readed, addr) = A::decode(&src[cur..]).map_err(Self::Error::Address)?;
      cur += readed;
      let suffrage: ServerSuffrage = src[cur].try_into()?;
      cur += ServerSuffrage::SIZE;
      if suffrage.is_voter() {
        voters += 1;
      }
      servers.insert(id, (addr, suffrage));
    }
    MembershipBuilder { voters, servers }
      .build()
      .map(|m| (len, m))
      .map_err(Into::into)
  }

  /// Decodes the value from the given reader.
  ///
  /// # Note
  /// The implementation of this method is not optimized, which means
  /// if your reader is expensive (e.g. [`TcpStream`](std::net::TcpStream), [`File`](std::fs::File)),
  /// it is better to use a [`BufReader`](std::io::BufReader)
  /// to wrap your orginal reader to cut down the number of I/O times.
  fn decode_from_reader<R: std::io::Read>(reader: &mut R) -> std::io::Result<(usize, Self)>
  where
    Self: Sized,
  {
    let mut inlined = [0; U32_SIZE * 2];
    reader.read_exact(&mut inlined)?;
    let len = u32::from_be_bytes(inlined[..U32_SIZE].try_into().unwrap()) as usize;
    let total_servers = u32::from_be_bytes(inlined[U32_SIZE..].try_into().unwrap()) as usize;
    let remaining: usize = len - (U32_SIZE * 2);
    if remaining == 0 {
      return Err(invalid_data(MembershipError::<I, A>::Empty));
    }
    let mut src = vec![0; remaining];
    let mut cur = 0;
    reader.read_exact(&mut src)?;
    let mut servers = IndexMap::with_capacity(total_servers);
    let mut voters = 0;
    while servers.len() < total_servers {
      let (readed, id) = I::decode(&src[cur..]).map_err(|e| invalid_data(Self::Error::Id(e)))?;
      cur += readed;
      let (readed, addr) =
        A::decode(&src[cur..]).map_err(|e| invalid_data(Self::Error::Address(e)))?;
      cur += readed;
      let suffrage: ServerSuffrage = src[cur]
        .try_into()
        .map_err(|e| invalid_data(Self::Error::UnknownServerSuffrage(e)))?;
      cur += ServerSuffrage::SIZE;
      if suffrage.is_voter() {
        voters += 1;
      }

      servers.insert(id, (addr, suffrage));
    }
    MembershipBuilder { voters, servers }
      .build()
      .map(|m| (len, m))
      .map_err(invalid_data)
  }

  /// Decodes the value from the given async reader.
  ///
  /// # Note
  /// The implementation of this method is not optimized, which means
  /// if your reader is expensive (e.g. `TcpStream`, `File`),
  /// it is better to use a [`BufReader`](futures::io::BufReader)
  /// to wrap your orginal reader to cut down the number of I/O times.
  async fn decode_from_async_reader<R: futures::io::AsyncRead + Send + Unpin>(
    reader: &mut R,
  ) -> std::io::Result<(usize, Self)>
  where
    Self: Sized,
    Self::Error: Send + Sync + 'static,
  {
    use futures::AsyncReadExt;
    let mut inlined = [0; U32_SIZE * 2];
    reader.read_exact(&mut inlined).await?;
    let len = u32::from_be_bytes(inlined[..U32_SIZE].try_into().unwrap()) as usize;
    let total_servers = u32::from_be_bytes(inlined[U32_SIZE..].try_into().unwrap()) as usize;
    let remaining: usize = len - (U32_SIZE * 2);
    if remaining == 0 {
      return Err(invalid_data(MembershipError::<I, A>::Empty));
    }
    let mut src = vec![0; remaining];
    let mut cur = 0;
    reader.read_exact(&mut src).await?;
    let mut servers = IndexMap::with_capacity(total_servers);
    let mut voters = 0;
    while servers.len() < total_servers {
      let (readed, id) = I::decode(&src[cur..]).map_err(|e| invalid_data(Self::Error::Id(e)))?;
      cur += readed;
      let (readed, addr) =
        A::decode(&src[cur..]).map_err(|e| invalid_data(Self::Error::Address(e)))?;
      cur += readed;
      let suffrage: ServerSuffrage = src[cur]
        .try_into()
        .map_err(|e| invalid_data(Self::Error::UnknownServerSuffrage(e)))?;
      cur += ServerSuffrage::SIZE;

      if suffrage.is_voter() {
        voters += 1;
      }

      servers.insert(id, (addr, suffrage));
    }
    MembershipBuilder { voters, servers }
      .build()
      .map(|m| (len, m))
      .map_err(invalid_data)
  }
}

#[cfg(feature = "serde")]
impl<I: Eq + Hash + serde::Serialize, A: serde::Serialize> serde::Serialize for Membership<I, A> {
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
impl<
    'de,
    I: Display + Eq + Hash + serde::Deserialize<'de>,
    A: Display + Eq + serde::Deserialize<'de>,
  > serde::Deserialize<'de> for Membership<I, A>
{
  fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
  where
    D: serde::Deserializer<'de>,
  {
    let servers = Vec::<Server<I, A>>::deserialize(deserializer)?;
    let mut membership = MembershipBuilder::with_capacity(servers.len());
    membership
      .insert_many(servers.into_iter())
      .and_then(|_| membership.build())
      .map_err(<D::Error as serde::de::Error>::custom)
  }
}

impl<I: Id, A: Address> FromIterator<Server<I, A>>
  for Result<Membership<I, A>, MembershipError<I, A>>
{
  fn from_iter<T: IntoIterator<Item = Server<I, A>>>(iter: T) -> Self {
    let mut membership = MembershipBuilder::new();
    membership
      .insert_many(iter.into_iter())
      .and_then(|_| membership.build())
  }
}

impl<I, A> Membership<I, A> {
  /// Returns the quorum size based on the current membership.
  #[inline]
  pub const fn quorum_size(&self) -> usize {
    self.quorum_size
  }

  /// Returns an iterator over the membership
  pub fn iter(&self) -> impl Iterator<Item = (&I, &(A, ServerSuffrage))> {
    self.servers.iter()
  }

  /// Returns the number of server in the membership, also referred to as its 'length'.
  #[inline]
  pub fn len(&self) -> usize {
    self.servers.len()
  }

  /// Returns `true` if the membership contains no elements
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.servers.is_empty()
  }

  /// Returns `true` if the membership contains a [`ServerSuffrage::Voter`].
  pub fn contains_voter(&self) -> bool {
    self.servers.values().any(|s| s.1 == ServerSuffrage::Voter)
  }
}

impl<I: Eq + Hash, A> Membership<I, A> {
  /// Returns `true` if the server is a [`ServerSuffrage::Voter`].
  pub fn is_voter<Q>(&self, id: &Q) -> bool
  where
    I: Borrow<Q>,
    Q: core::hash::Hash + Eq + ?Sized,
  {
    self
      .servers
      .get(id)
      .map(|(_, s)| *s == ServerSuffrage::Voter)
      .unwrap_or_default()
  }

  /// Returns true if the server identified by 'id' is in in the
  /// provided [`Membership`].
  pub fn contains_id<Q>(&self, id: &Q) -> bool
  where
    I: Borrow<Q>,
    Q: core::hash::Hash + Eq + ?Sized,
  {
    self.servers.contains_key(id)
  }
}

impl<I, A: Eq> Membership<I, A> {
  /// Returns true if the server address is in in the
  /// provided [`Membership`].
  pub fn contains_addr<Q>(&self, addr: &Q) -> bool
  where
    A: std::borrow::Borrow<Q>,
    Q: ?Sized + Eq,
  {
    self.servers.values().any(|s| s.0.borrow() == addr)
  }
}

impl<I: Id, A: Address> Membership<I, A> {
  /// Generates a new [`Membership`] from the current one and a
  /// [`MembershipChangeCommand`]. It's split from append_membership_entry so
  /// that it can be unit tested easily.
  pub(crate) fn next(
    &self,
    current_index: u64,
    change: MembershipChangeCommand<I, A>,
  ) -> Result<Self, MembershipError<I, A>> {
    let check = |prev_index: u64| -> Result<(), MembershipError<I, A>> {
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
        let mut new = MembershipBuilder {
          voters: self.voters,
          servers: self.servers.as_ref().clone(),
        };

        if let Some((address, suffrage)) = new.servers.get_mut(&id) {
          if *suffrage != ServerSuffrage::Voter {
            *suffrage = ServerSuffrage::Voter;
          }

          *address = addr;
        } else {
          new.insert(Server::new(id, addr, ServerSuffrage::Voter))?;
        }
        new.build()
      }),
      MembershipChangeCommand::AddNonvoter {
        id,
        addr,
        prev_index,
      } => check(prev_index).and_then(|_| {
        let mut new = MembershipBuilder {
          voters: self.voters,
          servers: self.servers.as_ref().clone(),
        };
        if let Some((address, suffrage)) = new.servers.get_mut(&id) {
          if *suffrage != ServerSuffrage::Nonvoter {
            *suffrage = ServerSuffrage::Nonvoter;
          }

          *address = addr;
        } else {
          new.insert(Server::new(id, addr, ServerSuffrage::Nonvoter))?;
        }
        new.build()
      }),
      MembershipChangeCommand::DemoteVoter { id, prev_index } => check(prev_index).and_then(|_| {
        let mut new = MembershipBuilder {
          voters: self.voters,
          servers: self.servers.as_ref().clone(),
        };
        if let Some((_, suffrage)) = new.servers.get_mut(&id) {
          *suffrage = ServerSuffrage::Nonvoter;
        }
        new.build()
      }),
      MembershipChangeCommand::RemoveServer { id, prev_index } => {
        check(prev_index).and_then(|_| {
          let mut new = MembershipBuilder {
            voters: self.voters,
            servers: self.servers.as_ref().clone(),
          };
          new.servers.remove(&id);
          new.build()
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
pub(crate) struct Memberships<I, A> {
  /// committed is the latest membership in the log/snapshot that has been
  /// committed (the one with the largest index).
  committed: ArcSwapAny<Arc<(u64, Membership<I, A>)>>,
  /// latest is the latest membership in the log/snapshot (may be committed
  /// or uncommitted)
  latest: ArcSwapAny<Arc<(u64, Membership<I, A>)>>,
}

impl<I, A> Memberships<I, A> {
  pub(crate) fn set_latest(&self, membership: Membership<I, A>, index: u64) {
    self.latest.store(Arc::new((index, membership)))
  }

  pub(crate) fn set_committed(&self, membership: Membership<I, A>, index: u64) {
    self.committed.store(Arc::new((index, membership)))
  }

  pub(crate) fn latest(&self) -> arc_swap::Guard<Arc<(u64, Membership<I, A>)>> {
    self.latest.load()
  }

  pub(crate) fn committed(&self) -> arc_swap::Guard<Arc<(u64, Membership<I, A>)>> {
    self.committed.load()
  }
}

#[derive(Debug, PartialEq, Eq)]
pub enum MembershipError<I, A> {
  DuplicateAddress(A),
  DuplicateId(I),
  EmptyServerId,
  EmptyVoter,
  AlreadyChanged { since: u64, latest: u64 },
  Empty,
}

impl<I: Display, A: Display> core::fmt::Display for MembershipError<I, A> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      MembershipError::DuplicateAddress(addr) => {
        write!(f, "found duplicate server address {}", addr)
      }
      MembershipError::DuplicateId(id) => write!(f, "found duplicate server id {}", id),
      MembershipError::EmptyServerId => write!(f, "server id cannot be empty"),
      MembershipError::EmptyVoter => write!(f, "no voter in the membership"),
      MembershipError::AlreadyChanged { since, latest } => write!(
        f,
        "membership changed since {} (latest is {})",
        since, latest
      ),
      MembershipError::Empty => write!(f, "membership is empty"),
    }
  }
}

impl<I: Display + Debug, A: Display + Debug> std::error::Error for MembershipError<I, A> {}

#[cfg(test)]
mod tests {
  use super::*;
  use smol_str::SmolStr;
  use std::net::SocketAddr;

  fn sample_membership() -> Membership<SmolStr, SocketAddr> {
    let mut membership = MembershipBuilder::new();
    membership
      .insert(Server {
        id: "id0".into(),
        addr: "127.0.0.1:8080".parse().unwrap(),
        suffrage: ServerSuffrage::Nonvoter,
      })
      .unwrap();

    membership
      .insert(Server {
        id: "id1".into(),
        addr: "127.0.0.1:8081".parse().unwrap(),
        suffrage: ServerSuffrage::Voter,
      })
      .unwrap();

    membership
      .insert(Server {
        id: "id2".into(),
        addr: "127.0.0.1:8082".parse().unwrap(),
        suffrage: ServerSuffrage::Nonvoter,
      })
      .unwrap();

    membership.build().unwrap()
  }

  fn single_server() -> Membership<SmolStr, SocketAddr> {
    let mut membership = MembershipBuilder::new();
    membership
      .insert(Server {
        id: "id1".into(),
        addr: "127.0.0.1:8081".parse().unwrap(),
        suffrage: ServerSuffrage::Voter,
      })
      .unwrap();
    membership.build().unwrap()
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
    let Err(MembershipError::EmptyVoter) =
      MembershipBuilder::<SmolStr, SocketAddr>::new().validate()
    else {
      panic!("should have failed for non voter")
    };

    let mut members = MembershipBuilder::new();

    members.servers.insert(
      "id0".into(),
      ("127.0.0.1:8080".parse().unwrap(), ServerSuffrage::Nonvoter),
    );
    let Err(MembershipError::EmptyVoter) = members.validate() else {
      panic!("should have failed for non voter")
    };

    members.servers.insert(
      "id1".into(),
      ("127.0.0.1:8081".parse().unwrap(), ServerSuffrage::Voter),
    );
    members.validate().expect("should be ok");

    let id: SmolStr = "id0".into();
    let Err(MembershipError::DuplicateId(did)) = members.insert(Server::new(
      id.clone(),
      "127.0.0.1:8083".parse().unwrap(),
      ServerSuffrage::Voter,
    )) else {
      panic!("should have failed for duplicate id")
    };
    assert_eq!(did, id);

    let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
    let Err(MembershipError::DuplicateAddress(daddr)) =
      members.insert(Server::new("id3".into(), addr, ServerSuffrage::Voter))
    else {
      panic!("should have failed for duplicate addr")
    };
    assert_eq!(daddr, addr);

    let members = members.build().unwrap();
    let command = MembershipChangeCommand::remove_server(id, 0);
    members.next(1, command).unwrap();

    let command = MembershipChangeCommand::demote_voter("id1".into(), 1);
    let Err(MembershipError::EmptyVoter) = members.next(1, command) else {
      panic!("should have failed for non voter")
    };
  }

  #[test]
  fn test_membership_next_prev_index() {
    let command =
      MembershipChangeCommand::add_voter("id2".into(), "127.0.0.1:8082".parse().unwrap(), 1);

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
      MembershipChangeCommand::add_voter("id3".into(), "127.0.0.1:8083".parse().unwrap(), 2);
    single_server()
      .next(2, command)
      .expect("should have succeeded");

    // zero prev_index
    let command =
      MembershipChangeCommand::add_voter("id4".into(), "127.0.0.1:8084".parse().unwrap(), 2);
    single_server()
      .next(2, command)
      .expect("should have succeeded");
  }

  #[test]
  fn test_membership_next_and_validate() {
    let membership = MembershipBuilder::<SmolStr, SocketAddr>::default();
    let command =
      MembershipChangeCommand::add_nonvoter("id1".into(), "127.0.0.1:8080".parse().unwrap(), 0);
    let err = membership.build().unwrap().next(1, command).unwrap_err();
    assert_eq!(err, MembershipError::EmptyVoter);
  }
}
