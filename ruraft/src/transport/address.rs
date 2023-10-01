use std::{
  mem,
  net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
  str::FromStr,
};

use ruraft_core::transport::NodeAddress;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

#[derive(PartialEq, Eq, Hash, Clone)]
pub(crate) enum Kind {
  Ip(IpAddr),
  Domain { safe: SmolStr, original: SmolStr },
}

/// An error which can be returned when parsing a [`Address`].
#[derive(Debug, thiserror::Error)]
pub enum ParseAddressError {
  #[error("address is missing port")]
  MissingPort,
  #[error("invalid domain")]
  InvalidDomain,
  #[error("invalid port: {0}")]
  InvalidPort(#[from] std::num::ParseIntError),
}

/// An error which can be returned when encoding/decoding a [`Address`].
#[derive(Debug, thiserror::Error)]
pub enum AddressError {
  #[error(
    "buffer is too small, use `Address::encoded_len` to pre-allocate a buffer with enough space"
  )]
  EncodeBufferTooSmall,
  #[error("{0}")]
  ParseAddressError(#[from] ParseAddressError),
  #[error("{0}")]
  Corrupted(&'static str),
  #[error("unknown address tag: {0}")]
  UnknownAddressTag(u8),
  #[error("{0}")]
  Utf8Error(#[from] core::str::Utf8Error),
}

/// A node address which supports both `domain:port` and socket address.
///
/// e.g. Valid format
/// 1. `www.example.com:8080`
/// 2. `[::1]:8080`
/// 3. `127.0.0.1:8080`
#[derive(PartialEq, Eq, Hash, Clone)]
pub struct Address {
  pub(crate) kind: Kind,
  pub(crate) port: u16,
}

impl Serialize for Address {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: serde::Serializer,
  {
    match &self.kind {
      Kind::Ip(ip) => SocketAddr::new(*ip, self.port)
        .to_string()
        .serialize(serializer),
      Kind::Domain { original, .. } => serializer.serialize_str(original.as_str()),
    }
  }
}

impl<'de> Deserialize<'de> for Address {
  fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
  where
    D: serde::Deserializer<'de>,
  {
    <&str as Deserialize>::deserialize(deserializer)
      .and_then(|s| Address::from_str(s).map_err(<D::Error as serde::de::Error>::custom))
  }
}

impl From<SocketAddr> for Address {
  fn from(addr: SocketAddr) -> Self {
    Self {
      kind: Kind::Ip(addr.ip()),
      port: addr.port(),
    }
  }
}

impl From<(IpAddr, u16)> for Address {
  fn from(addr: (IpAddr, u16)) -> Self {
    Self {
      kind: Kind::Ip(addr.0),
      port: addr.1,
    }
  }
}

impl TryFrom<String> for Address {
  type Error = ParseAddressError;

  fn try_from(s: String) -> Result<Self, Self::Error> {
    Address::from_str(s.as_str())
  }
}

impl TryFrom<&str> for Address {
  type Error = ParseAddressError;

  fn try_from(value: &str) -> Result<Self, Self::Error> {
    Address::from_str(value)
  }
}

impl FromStr for Address {
  type Err = ParseAddressError;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    let res: Result<SocketAddr, _> = s.parse();
    match res {
      Ok(addr) => Ok(addr.into()),
      Err(_) => {
        let res: Result<IpAddr, _> = s.parse();
        match res {
          Ok(_) => Err(ParseAddressError::MissingPort),
          Err(_) => {
            let Some((domain, port)) = s.rsplit_once(':') else {
              return Err(ParseAddressError::MissingPort);
            };

            let port = port.parse().map_err(ParseAddressError::InvalidPort)?;
            idna::domain_to_ascii_strict(domain)
              .map(|mut domain| {
                // make sure we will only issue one query
                if !domain.ends_with('.') {
                  domain.push('.');
                }

                Self {
                  kind: Kind::Domain {
                    safe: SmolStr::from(domain),
                    original: SmolStr::from(s),
                  },
                  port,
                }
              })
              .map_err(|_| ParseAddressError::InvalidDomain)
          }
        }
      }
    }
  }
}

impl core::fmt::Display for Address {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match &self.kind {
      Kind::Ip(addr) => write!(f, "{}", SocketAddr::new(*addr, self.port)),
      Kind::Domain { original, .. } => write!(f, "{original}:{}", self.port),
    }
  }
}

const PORT_SIZE: usize = mem::size_of::<u16>();
const TAG_SIZE: usize = 1;
/// A domain is less than 255 bytes, so u8 is enough to represent the length of a domain.
const DOMAIN_LEN_SIZE: usize = 1;
const V6_SIZE: usize = 16;
const V4_SIZE: usize = 4;

impl NodeAddress for Address {
  type Error = AddressError;

  fn encode(&self, dst: &mut [u8]) -> Result<(), Self::Error> {
    if dst.len() < self.encoded_len() {
      return Err(AddressError::EncodeBufferTooSmall);
    }

    match &self.kind {
      Kind::Ip(addr) => match addr {
        IpAddr::V4(addr) => {
          dst[0] = 4;
          dst[1..5].copy_from_slice(&addr.octets());
          dst[5..7].copy_from_slice(&self.port.to_be_bytes());
        }
        IpAddr::V6(addr) => {
          dst[0] = 6;
          dst[1..17].copy_from_slice(&addr.octets());
          dst[17..19].copy_from_slice(&self.port.to_be_bytes());
        }
      },
      Kind::Domain { safe, .. } => {
        let mut cur = 0;
        dst[cur] = 0;
        cur += TAG_SIZE;
        dst[cur] = safe.len() as u8;
        cur += DOMAIN_LEN_SIZE;
        dst[cur..cur + safe.len()].copy_from_slice(safe.as_bytes());
        cur += safe.len();
        dst[cur..cur + PORT_SIZE].copy_from_slice(&self.port.to_be_bytes());
      }
    }
    Ok(())
  }

  fn encoded_len(&self) -> usize {
    match &self.kind {
      Kind::Ip(addr) => match addr {
        IpAddr::V4(_) => TAG_SIZE + V4_SIZE + PORT_SIZE,
        IpAddr::V6(_) => TAG_SIZE + V6_SIZE + PORT_SIZE,
      },
      Kind::Domain { safe, .. } => TAG_SIZE + DOMAIN_LEN_SIZE + safe.len() + PORT_SIZE,
    }
  }

  fn decode(src: &[u8]) -> Result<Self, Self::Error>
  where
    Self: Sized,
  {
    if src.len() < TAG_SIZE + DOMAIN_LEN_SIZE {
      return Err(AddressError::Corrupted("corrupted address"));
    }

    let mut cur = 0;
    let tag = src[0];
    cur += TAG_SIZE;

    match tag {
      0 => {
        let len = src[cur] as usize;
        cur += DOMAIN_LEN_SIZE;
        if src.len() < cur + len + PORT_SIZE {
          return Err(AddressError::Corrupted("corrupted address"));
        }

        let s = core::str::from_utf8(&src[cur..cur + len])?;
        cur += len;
        let port = u16::from_be_bytes([src[cur], src[cur + 1]]);
        let original = format!("{s}:{port}");
        Address::from_str(original.as_str()).map_err(Into::into)
      }
      4 => {
        if src.len() < cur + V4_SIZE + PORT_SIZE {
          return Err(AddressError::Corrupted("corrupted address"));
        }

        let ip = Ipv4Addr::new(src[cur], src[cur + 1], src[cur + 2], src[cur + 3]);
        let port = u16::from_be_bytes([src[cur + V4_SIZE], src[cur + V4_SIZE + 1]]);
        Ok(SocketAddr::from((ip, port)).into())
      }
      6 => {
        if src.len() < cur + V6_SIZE + PORT_SIZE {
          return Err(AddressError::Corrupted("corrupted address"));
        }

        let mut buf = [0u8; V6_SIZE];
        buf.copy_from_slice(&src[cur..cur + V6_SIZE]);
        let ip = Ipv6Addr::from(buf);
        let port = u16::from_be_bytes([src[cur + V6_SIZE], src[cur + V6_SIZE + 1]]);
        Ok(SocketAddr::from((ip, port)).into())
      }
      val => Err(AddressError::UnknownAddressTag(val)),
    }
  }
}

impl Address {
  /// Returns the domain of the address if this address can only be represented by domain name
  pub fn domain(&self) -> Option<&str> {
    match &self.kind {
      Kind::Ip(_) => None,
      Kind::Domain { original, .. } => Some(original.rsplit_once(':').unwrap().1),
    }
  }

  /// Returns the ip of the address if this address can be represented by [`IpAddr`]
  pub const fn ip(&self) -> Option<IpAddr> {
    match &self.kind {
      Kind::Ip(addr) => Some(*addr),
      Kind::Domain { .. } => None,
    }
  }

  /// Returns the port
  pub const fn port(&self) -> u16 {
    self.port
  }

  /// Set the port
  pub fn set_port(&mut self, port: u16) {
    self.port = port;
  }
}

#[test]
fn test_() {}
