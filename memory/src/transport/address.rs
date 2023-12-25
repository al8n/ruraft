use std::{fmt::Display, io};

use ruraft_core::{
  transport::{Address, Transformable},
  CheapClone,
};
use uuid::Uuid;

/// An [`Address`] implementor for [`MemoryTransport`](super::MemoryTransport).
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MemoryAddress(Uuid);

impl CheapClone for MemoryAddress {}

impl Default for MemoryAddress {
  fn default() -> Self {
    Self::new()
  }
}

impl Display for MemoryAddress {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", self.0)
  }
}

impl MemoryAddress {
  /// Create a new randome [`MemoryAddress`].
  #[inline]
  pub fn new() -> Self {
    Self(Uuid::new_v4())
  }
}

impl From<Uuid> for MemoryAddress {
  #[inline]
  fn from(uuid: Uuid) -> Self {
    Self(uuid)
  }
}

impl From<[u8; 16]> for MemoryAddress {
  #[inline]
  fn from(bytes: [u8; 16]) -> Self {
    Self(Uuid::from_bytes(bytes))
  }
}

impl Address for MemoryAddress {}

impl Transformable for MemoryAddress {
  type Error = <[u8; 16] as Transformable>::Error;

  fn encode(&self, dst: &mut [u8]) -> Result<usize, Self::Error> {
    self.0.into_bytes().encode(dst)
  }

  fn encode_to_writer<W: io::Write>(&self, writer: &mut W) -> io::Result<usize> {
    self.0.into_bytes().encode_to_writer(writer)
  }

  async fn encode_to_async_writer<W: futures::io::AsyncWrite + Send + Unpin>(
    &self,
    writer: &mut W,
  ) -> io::Result<usize>
  where
    Self::Error: Send + Sync + 'static,
  {
    self.0.into_bytes().encode_to_async_writer(writer).await
  }

  fn encoded_len(&self) -> usize {
    self.0.into_bytes().encoded_len()
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    <[u8; 16] as Transformable>::decode(src).map(|(size, val)| (size, Self(Uuid::from_bytes(val))))
  }

  fn decode_from_reader<R: io::Read>(reader: &mut R) -> io::Result<(usize, Self)>
  where
    Self: Sized,
  {
    <[u8; 16] as Transformable>::decode_from_reader(reader)
      .map(|(size, val)| (size, Self(Uuid::from_bytes(val))))
  }

  async fn decode_from_async_reader<R: futures::io::AsyncRead + Send + Unpin>(
    reader: &mut R,
  ) -> io::Result<(usize, Self)>
  where
    Self: Sized,
    Self::Error: Send + Sync + 'static,
  {
    <[u8; 16] as Transformable>::decode_from_async_reader(reader)
      .await
      .map(|(size, val)| (size, Self(Uuid::from_bytes(val))))
  }
}
