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
  I: Transformable,
  A: Transformable,
{
  type Error = TransformError;

  fn encode(&self, dst: &mut [u8]) -> Result<usize, Self::Error> {
    <Header<I, A> as Transformable>::encode(&self.header, dst)
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
}

#[cfg(any(feature = "test", test))]
impl TimeoutNowResponse<smol_str::SmolStr, std::net::SocketAddr> {
  #[doc(hidden)]
  pub fn __large() -> Self {
    Self {
      header: Header::__large(),
    }
  }

  #[doc(hidden)]
  pub fn __small() -> Self {
    Self {
      header: Header::__small(),
    }
  }
}

#[cfg(test)]
unit_test_transformable_roundtrip!(TimeoutNowResponse <smol_str::SmolStr, std::net::SocketAddr> => heart_response);
