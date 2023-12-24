use super::*;

/// The command used by a leader to signal another server to
/// start an election.
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TimeoutNowRequest<I, A> {
  /// The header of the request
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the header of the request"),),
    setter(attrs(doc = "Set the header of the request"),)
  )]
  header: Header<I, A>,
}

impl<I, A> TimeoutNowRequest<I, A> {
  /// Create a new [`TimeoutNowRequest`] with the given protocol version, id, address.
  #[inline]
  pub const fn new(version: ProtocolVersion, id: I, addr: A) -> Self {
    Self {
      header: Header {
        protocol_version: version,
        from: Node::new(id, addr),
      },
    }
  }

  /// Create a new [`TimeoutNowRequest`] with the given protocol version and node.
  #[inline]
  pub const fn from_node(version: ProtocolVersion, node: Node<I, A>) -> Self {
    Self {
      header: Header {
        protocol_version: version,
        from: node,
      },
    }
  }

  /// Create a new [`TimeoutNowRequest`] with the given header.
  #[inline]
  pub const fn from_header(header: Header<I, A>) -> Self {
    Self { header }
  }
}

impl<I: CheapClone, A: CheapClone> CheapClone for TimeoutNowRequest<I, A> {
  fn cheap_clone(&self) -> Self {
    Self {
      header: self.header.cheap_clone(),
    }
  }
}

impl<I, A> Transformable for TimeoutNowRequest<I, A>
where
  I: Id + Send + Sync + 'static,
  I::Error: Send + Sync + 'static,
  A: Address + Send + Sync + 'static,
  A::Error: Send + Sync + 'static,
{
  type Error = TransformError;

  fn encode(&self, dst: &mut [u8]) -> Result<(), Self::Error> {
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
impl TimeoutNowRequest<smol_str::SmolStr, std::net::SocketAddr> {
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
unit_test_transformable_roundtrip!(TimeoutNowRequest <smol_str::SmolStr, std::net::SocketAddr> => timeout_now_request);
