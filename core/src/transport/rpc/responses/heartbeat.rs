use crate::MESSAGE_SIZE_LEN;

use super::*;

/// The response returned from an
/// [`HeartbeatRequest`].
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct HeartbeatResponse<I, A> {
  /// The header of the response
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the header of the response"),),
    setter(attrs(doc = "Set the header of the response"),)
  )]
  header: Header<I, A>,

  /// We may not succeed if we have a conflicting entry
  #[viewit(
    getter(const, attrs(doc = "Get if the heartbeat success"),),
    setter(attrs(doc = "Set if the heartbeat success"),)
  )]
  success: bool,
}

impl<I, A> HeartbeatResponse<I, A> {
  /// Create a new [`HeartbeatResponse`]
  #[inline]
  pub const fn new(header: Header<I, A>, success: bool) -> Self {
    Self { header, success }
  }
}

impl<I: CheapClone, A: CheapClone> CheapClone for HeartbeatResponse<I, A> {
  fn cheap_clone(&self) -> Self {
    Self {
      header: self.header.cheap_clone(),
      success: self.success,
    }
  }
}

// Encode
//
// --------------------------------------------------------
// | len (4 bytes) | header (variable) | success (1 byte) |
// --------------------------------------------------------
impl<I, A> Transformable for HeartbeatResponse<I, A>
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
    dst[offset..offset + MESSAGE_SIZE_LEN].copy_from_slice(&(encoded_len as u32).to_be_bytes());
    offset += MESSAGE_SIZE_LEN;

    offset += self.header.encode(&mut dst[offset..])?;

    dst[offset] = self.success as u8;
    offset += 1;

    debug_assert_eq!(
      offset, encoded_len,
      "expected bytes wrote ({}) not match actual bytes wrote ({})",
      encoded_len, offset
    );

    Ok(offset)
  }

  fn encoded_len(&self) -> usize {
    MESSAGE_SIZE_LEN + 1 + self.header.encoded_len()
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
    let encoded_len =
      u32::from_be_bytes(src[offset..offset + MESSAGE_SIZE_LEN].try_into().unwrap()) as usize;
    if encoded_len > src_len {
      return Err(TransformError::DecodeBufferTooSmall);
    }

    offset += MESSAGE_SIZE_LEN;

    let (header_len, header) = Header::<I, A>::decode(&src[offset..])?;
    offset += header_len;

    let success = src[offset] != 0;
    offset += 1;

    Ok((offset, Self { header, success }))
  }
}

#[cfg(any(feature = "test", test))]
impl HeartbeatResponse<smol_str::SmolStr, std::net::SocketAddr> {
  #[doc(hidden)]
  pub fn __large() -> Self {
    Self {
      header: Header::__large(),
      success: false,
    }
  }

  #[doc(hidden)]
  pub fn __small() -> Self {
    Self {
      header: Header::__small(),
      success: true,
    }
  }
}

#[cfg(test)]
unit_test_transformable_roundtrip!(HeartbeatResponse <smol_str::SmolStr, std::net::SocketAddr> => heartbeat_response);
