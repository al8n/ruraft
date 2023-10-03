use std::borrow::{Borrow, Cow};

use ruraft_core::transport::NodeId;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

#[derive(Debug, thiserror::Error)]
pub enum Error {
  #[error("id cannot be empty")]
  Empty,
  #[error("id is too large, maximum size is 512 bytes, but got {0} bytes")]
  TooLarge(usize),
  #[error("buffer is too small, use Id::encoded_size to pre-allocate a buffer with enough space")]
  EncodeBufferTooSmall,
  #[error("corrupted {0}")]
  Corrupted(Cow<'static, str>),
  #[error("{0}")]
  Utf8Error(#[from] core::str::Utf8Error),
}

/// A unique string identifying a server for all time.
/// The maximum length of an id is 512 bytes.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Id(SmolStr);

impl Id {
  /// Creates a new `Id` from the source.
  pub fn new<T: AsRef<str>>(src: T) -> Result<Self, Error> {
    let src = src.as_ref();
    if src.is_empty() {
      return Err(Error::Empty);
    }

    if src.len() > 512 {
      return Err(Error::TooLarge(src.len()));
    }

    Ok(Self(SmolStr::new(src)))
  }

  /// converts the `Id` into a `&str`.
  pub fn as_str(&self) -> &str {
    self.0.as_ref()
  }

  /// Returns a byte slice.
  /// To convert the byte slice back into a string slice, use the [`core::str::from_utf8`] function.
  pub fn as_bytes(&self) -> &[u8] {
    self.0.as_bytes()
  }

  /// Returns random `Id`, this function should only be used in `#[cfg(feature = "test")]` or `#[cfg(test)]`
  #[cfg(any(test, feature = "test"))]
  pub fn random() -> Self {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let id: String = (0..10).map(|_| rng.gen::<char>()).collect();
    Self::new(id).unwrap()
  }
}

impl NodeId for Id {
  type Error = Error;

  fn encode(&self, dst: &mut [u8]) -> Result<(), Self::Error> {
    const LENGTH: usize = core::mem::size_of::<u16>();

    let encoded_len = self.encoded_len();
    if dst.len() < encoded_len {
      return Err(Error::EncodeBufferTooSmall);
    }

    let mut cur = 0;
    dst[cur..cur + LENGTH].copy_from_slice(&(self.0.len() as u16).to_be_bytes());
    cur += LENGTH;
    dst[cur..cur + self.0.len()].copy_from_slice(self.0.as_bytes());
    Ok(())
  }

  fn encoded_len(&self) -> usize {
    core::mem::size_of::<u16>() + self.0.len()
  }

  fn decode(src: &[u8]) -> Result<Self, Self::Error>
  where
    Self: Sized,
  {
    const LENGTH: usize = core::mem::size_of::<u16>();

    if src.len() < LENGTH {
      return Err(Error::Corrupted(Cow::Borrowed("corrupted id")));
    }

    let len = u16::from_be_bytes([src[0], src[1]]) as usize;
    if src.len() < LENGTH + len {
      return Err(Error::Corrupted(Cow::Owned(format!(
        "corrupted id, expected length is {len}, but the src length is {}",
        src.len()
      ))));
    }

    let id = Self::new(core::str::from_utf8(&src[LENGTH..LENGTH + len])?)?;
    Ok(id)
  }
}

impl std::str::FromStr for Id {
  type Err = Error;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    Self::new(s)
  }
}

impl Borrow<str> for Id {
  fn borrow(&self) -> &str {
    self.as_str()
  }
}

impl AsRef<str> for Id {
  fn as_ref(&self) -> &str {
    self.0.as_ref()
  }
}

impl core::fmt::Display for Id {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    self.0.fmt(f)
  }
}

impl core::fmt::Debug for Id {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    self.0.fmt(f)
  }
}
