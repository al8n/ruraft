use std::{fmt::Display, net::SocketAddr};

/// Node address
pub trait NodeAddress: Clone + Eq + Display + Send + Sync + 'static {
  /// The error type returned when encoding or decoding fails.
  type Error: std::error::Error + Send + Sync + 'static;

  /// Encodes the value into the given buffer for transmission.
  fn encode(&self, dst: &mut [u8]) -> Result<(), Self::Error>;

  /// Returns the encoded length of the value.
  /// This is used to pre-allocate a buffer for encoding.
  fn encoded_len(&self) -> usize;

  /// Decodes the value from the given buffer received over the wire.
  fn decode(src: &[u8]) -> Result<Self, Self::Error>
  where
    Self: Sized;
}

/// The wire error type for [`SocketAddr`].
#[derive(Debug, thiserror::Error)]
pub enum SocketAddrWireError {
  /// Returned when the buffer is too small to encode the [`SocketAddr`].
  #[error(
    "buffer is too small, use `SocketAddr::encoded_len` to pre-allocate a buffer with enough space"
  )]
  EncodeBufferTooSmall,
  /// Returned when the address family is unknown.
  #[error("invalid address family: {0}, only IPv4 and IPv6 are supported")]
  UnknownAddressFamily(u8),
  /// Returned when the address is corrupted.
  #[error("{0}")]
  Corrupted(&'static str),
}

impl NodeAddress for SocketAddr {
  type Error = SocketAddrWireError;

  fn encode(&self, dst: &mut [u8]) -> Result<(), Self::Error> {
    let encoded_len = self.encoded_len();
    if dst.len() < encoded_len {
      return Err(Self::Error::EncodeBufferTooSmall);
    }
    dst[0] = match self {
      SocketAddr::V4(_) => 4,
      SocketAddr::V6(_) => 6,
    };
    match self {
      SocketAddr::V4(addr) => {
        dst[1..5].copy_from_slice(&addr.ip().octets());
        dst[5..7].copy_from_slice(&addr.port().to_be_bytes());
      }
      SocketAddr::V6(addr) => {
        dst[1..17].copy_from_slice(&addr.ip().octets());
        dst[17..19].copy_from_slice(&addr.port().to_be_bytes());
      }
    }

    Ok(())
  }

  fn encoded_len(&self) -> usize {
    1 + match self {
      SocketAddr::V4(_) => 4,
      SocketAddr::V6(_) => 16,
    } + core::mem::size_of::<u16>()
  }

  fn decode(src: &[u8]) -> Result<Self, Self::Error>
  where
    Self: Sized,
  {
    match src[0] {
      4 => {
        if src.len() < 7 {
          return Err(SocketAddrWireError::Corrupted(
            "corrupted socket v4 address",
          ));
        }

        let ip = std::net::Ipv4Addr::new(src[1], src[2], src[3], src[4]);
        let port = u16::from_be_bytes([src[5], src[6]]);
        Ok(SocketAddr::from((ip, port)))
      }
      6 => {
        if src.len() < 19 {
          return Err(SocketAddrWireError::Corrupted(
            "corrupted socket v6 address",
          ));
        }

        let mut buf = [0u8; 16];
        buf.copy_from_slice(&src[1..17]);
        let ip = std::net::Ipv6Addr::from(buf);
        let port = u16::from_be_bytes([src[17], src[18]]);
        Ok(SocketAddr::from((ip, port)))
      }
      val => Err(SocketAddrWireError::UnknownAddressFamily(val)),
    }
  }
}
