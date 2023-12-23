use std::io;

use futures::{AsyncRead, AsyncWrite};

use super::*;

/// The response to [`TimeoutNowRequest`].
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TimeoutNowResponse<I, A> {
  /// The header of the response
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the header of the response"),),
    setter(attrs(doc = "Set the header of the response"),)
  )]
  header: Header<I, A>,
}

impl<I, A> TimeoutNowResponse<I, A> {
  /// Create a new [`TimeoutNowResponse`] with the given `id` and `addr` and `version`.
  #[inline]
  pub const fn new(version: ProtocolVersion, id: I, addr: A) -> Self {
    Self {
      header: Header::new(version, id, addr),
    }
  }

  /// Create a new [`TimeoutNowResponse`] with the given protocol version and node.
  #[inline]
  pub const fn from_node(version: ProtocolVersion, node: Node<I, A>) -> Self {
    Self {
      header: Header::from_node(version, node),
    }
  }

  /// Create a new [`TimeoutNowResponse`] with the given header.
  #[inline]
  pub const fn from_header(header: Header<I, A>) -> Self {
    Self { header }
  }
}

impl<I: CheapClone, A: CheapClone> CheapClone for TimeoutNowResponse<I, A> {
  fn cheap_clone(&self) -> Self {
    Self {
      header: self.header.cheap_clone(),
    }
  }
}

impl<I, A> Transformable for TimeoutNowResponse<I, A>
where
  I: Transformable + Send + Sync + 'static,
  I::Error: Send + Sync + 'static,
  A: Transformable + Send + Sync + 'static,
  A::Error: Send + Sync + 'static,
{
  type Error = TransformError;

  fn encode(&self, dst: &mut [u8]) -> Result<(), Self::Error> {
    <Header<I, A> as Transformable>::encode(&self.header, dst)
  }

  fn encode_to_writer<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
    <Header<I, A> as Transformable>::encode_to_writer(&self.header, writer)
  }

  async fn encode_to_async_writer<W: AsyncWrite + Send + Unpin>(
    &self,
    writer: &mut W,
  ) -> io::Result<()>
  where
    Self::Error: Send + Sync + 'static,
  {
    <Header<I, A> as Transformable>::encode_to_async_writer(&self.header, writer).await
  }

  fn encoded_len(&self) -> usize {
    self.header.encoded_len()
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    <Header<I, A> as Transformable>::decode(src).map(|(size, h)| (size, Self::from_header(h)))
  }

  fn decode_from_reader<R: std::io::Read>(reader: &mut R) -> std::io::Result<(usize, Self)>
  where
    Self: Sized,
  {
    <Header<I, A> as Transformable>::decode_from_reader(reader)
      .map(|(size, h)| (size, Self::from_header(h)))
  }

  async fn decode_from_async_reader<R: AsyncRead + Send + Unpin>(
    reader: &mut R,
  ) -> io::Result<(usize, Self)>
  where
    Self: Sized,
    Self::Error: Send + Sync + 'static,
  {
    <Header<I, A> as Transformable>::decode_from_async_reader(reader)
      .await
      .map(|(size, h)| (size, Self::from_header(h)))
  }
}
