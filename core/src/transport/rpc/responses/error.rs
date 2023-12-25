use crate::MESSAGE_SIZE_LEN;

use super::*;

/// The response returned from remote when an error occurs.
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ErrorResponse<I, A> {
  /// The header of the response
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the header of the response"),),
    setter(attrs(doc = "Set the header of the response"),)
  )]
  header: Header<I, A>,
  /// The error message
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the error message"),),
    setter(attrs(doc = "Set the error message"),)
  )]
  error: String,
}

impl<I, A> From<ErrorResponse<I, A>> for String {
  fn from(value: ErrorResponse<I, A>) -> Self {
    value.error
  }
}

impl<I, A> ErrorResponse<I, A> {
  /// Create a new [`ErrorResponse`] with the given header and error message.
  pub const fn new(header: Header<I, A>, error: String) -> Self {
    Self { header, error }
  }
}

// Encode
//
// --------------------------------------------------------
// | len (4 bytes) | header (variable) | error (variable) |
// --------------------------------------------------------
impl<I, A> Transformable for ErrorResponse<I, A>
where
  I: Transformable + Send + Sync + 'static,
  I::Error: Send + Sync + 'static,
  A: Transformable + Send + Sync + 'static,
  A::Error: Send + Sync + 'static,
{
  type Error = TransformError;

  fn encode(&self, dst: &mut [u8]) -> Result<usize, Self::Error> {
    let encoded_len = self.encoded_len();

    if dst.len() < encoded_len {
      return Err(TransformError::EncodeBufferTooSmall);
    }
    let mut offset = 0;
    NetworkEndian::write_u32(&mut dst[..MESSAGE_SIZE_LEN], encoded_len as u32);
    offset += MESSAGE_SIZE_LEN;

    offset += self.header.encode(&mut dst[offset..])?;

    offset += self
      .error
      .encode(&mut dst[offset..])
      .map_err(TransformError::encode)?;

    debug_assert_eq!(
      offset, encoded_len,
      "expected bytes wrote ({}) not match actual bytes wrote ({})",
      encoded_len, offset
    );
    Ok(offset)
  }

  fn encoded_len(&self) -> usize {
    MESSAGE_SIZE_LEN + self.header.encoded_len() + self.error.encoded_len()
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    let src_len = src.len();
    if src_len < MESSAGE_SIZE_LEN + 1 {
      return Err(TransformError::DecodeBufferTooSmall);
    }

    let mut offset = 0;
    let encoded_len = NetworkEndian::read_u32(&src[offset..]) as usize;
    if encoded_len > src_len {
      return Err(TransformError::DecodeBufferTooSmall);
    }
    offset += MESSAGE_SIZE_LEN;

    let (header_len, header) = Header::<I, A>::decode(&src[offset..])?;
    offset += header_len;

    let (error_encoded_len, error) =
      String::decode(&src[offset..]).map_err(TransformError::decode)?;
    offset += error_encoded_len;

    debug_assert_eq!(
      offset, encoded_len,
      "expected bytes read ({}) not match actual bytes read ({})",
      encoded_len, offset
    );
    Ok((offset, Self { header, error }))
  }
}

#[cfg(any(feature = "test", test))]
impl ErrorResponse<smol_str::SmolStr, std::net::SocketAddr> {
  #[doc(hidden)]
  pub fn __large() -> Self {
    Self {
      header: Header::__large(),
      error: "err".into(),
    }
  }

  #[doc(hidden)]
  pub fn __small() -> Self {
    Self {
      header: Header::__small(),
      error: String::new(),
    }
  }
}

#[cfg(test)]
unit_test_transformable_roundtrip!(ErrorResponse <smol_str::SmolStr, std::net::SocketAddr> => error_response);
