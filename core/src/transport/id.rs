use std::{fmt::Display, hash::Hash};

use smol_str::SmolStr;

/// Node id
pub trait NodeId: Clone + Eq + Hash + Display + Send + Sync + 'static {
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

/// Error type for string based node id.
#[derive(Debug, thiserror::Error)]
pub enum StringIdError {
  /// Returned when the buffer is too small to encode.
  #[error("buffer is too small, use `<Self as NodeId>::encoded_len` to pre-allocate a buffer with enough space")]
  EncodeBufferTooSmall,
  /// Returned when the id is corrupted.
  #[error("corrupted id")]
  Corrupted,
  /// Returned when the id is not a utf8 encoded.
  #[error("{0}")]
  Utf8Error(#[from] core::str::Utf8Error),
}

macro_rules! impl_string_based_id {
  ($($ty: ty), + $(,)?) => {
    $(
      impl NodeId for $ty
      {
        type Error = StringIdError;

        fn encode(&self, dst: &mut [u8]) -> Result<(), Self::Error> {
          let encoded_len = self.encoded_len();
          if dst.len() < encoded_len {
            return Err(Self::Error::EncodeBufferTooSmall);
          }

          let s: &str = self.as_ref();
          let len = s.len();
          dst[0..4].copy_from_slice(&(len as u32).to_be_bytes());
          dst[4..4+len].copy_from_slice(s.as_bytes());
          Ok(())
        }

        fn encoded_len(&self) -> usize {
          let s: &str = self.as_ref();
          core::mem::size_of::<u32>() + s.len()
        }

        fn decode(src: &[u8]) -> Result<Self, Self::Error> where Self: Sized {
          const LENGTH: usize = core::mem::size_of::<u32>();

          if src.len() < LENGTH {
            return Err(Self::Error::Corrupted);
          }

          let len = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
          if src.len() < LENGTH + len {
            return Err(Self::Error::Corrupted);
          }

          let id = Self::from(core::str::from_utf8(&src[LENGTH..LENGTH + len])?);
          Ok(id)
        }
      }
    )+
  };
}

impl_string_based_id!(String, SmolStr,);

/// Error type for string based node id.
#[derive(Debug, thiserror::Error)]
pub enum NumberIdError {
  /// Returned when the buffer is too small to encode.
  #[error("buffer is too small, use `<Self as NodeId>::encoded_len` to pre-allocate a buffer with enough space")]
  EncodeBufferTooSmall,
  /// Returned when the id is corrupted.
  #[error("corrupted id")]
  Corrupted,
}

macro_rules! impl_number_based_id {
  ($($ty: ty), + $(,)?) => {
    $(
      impl NodeId for $ty {
        type Error = NumberIdError;

        fn encode(&self, dst: &mut [u8]) -> Result<(), Self::Error> {
          const SIZE: usize = core::mem::size_of::<$ty>();

          let encoded_len = self.encoded_len();
          if dst.len() < encoded_len {
            return Err(Self::Error::EncodeBufferTooSmall);
          }

          dst[..SIZE].copy_from_slice(&self.to_be_bytes());

          Ok(())
        }

        fn encoded_len(&self) -> usize {
          core::mem::size_of::<$ty>()
        }

        fn decode(src: &[u8]) -> Result<Self, Self::Error> where Self: Sized {
          const SIZE: usize = core::mem::size_of::<$ty>();

          if src.len() < SIZE {
            return Err(Self::Error::Corrupted);
          }

          let id = <$ty>::from_be_bytes((&src[..SIZE]).try_into().unwrap());
          Ok(id)
        }
      }
    )+
  };
}

impl_number_based_id!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128,);
