use std::time::SystemTime;

use ruraft_utils::{decode_varint, encode_varint, encoded_len_varint};

use crate::MESSAGE_SIZE_LEN;

use super::*;

/// The response returned from an
/// [`AppendEntriesRequest`].
#[viewit::viewit(setters(prefix = "with"))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AppendEntriesResponse<I, A> {
  /// The header of the response
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the header of the response"),),
    setter(attrs(doc = "Set the header of the response"),)
  )]
  header: Header<I, A>,

  /// Newer term if leader is out of date
  #[viewit(
    getter(const, attrs(doc = "Get the newer term if leader is out of date"),),
    setter(attrs(doc = "Set the newer term if leader is out of date"),)
  )]
  term: u64,

  /// A hint to help accelerate rebuilding slow nodes
  #[viewit(
    getter(
      const,
      attrs(doc = "Get a hint to help accelerate rebuilding slow nodes"),
    ),
    setter(attrs(doc = "Set a hint to help accelerate rebuilding slow nodes"),)
  )]
  last_log: u64,

  /// We may not succeed if we have a conflicting entry
  #[viewit(
    getter(const, attrs(doc = "Get if we have a conflicting entry"),),
    setter(attrs(doc = "Set if we have a conflicting entry"),)
  )]
  success: bool,

  /// There are scenarios where this request didn't succeed
  /// but there's no need to wait/back-off the next attempt.
  #[viewit(
    getter(
      const,
      attrs(doc = "Get if there's no need to wait/back-off the next attempt"),
    ),
    setter(attrs(doc = "Set if there's no need to wait/back-off the next attempt"),)
  )]
  no_retry_backoff: bool,
}

impl<I, A> AppendEntriesResponse<I, A> {
  /// Create a new [`AppendEntriesResponse`] with the given `id` and `addr` and `version`. Other fields
  /// are set to their default values.
  #[inline]
  pub const fn new(version: ProtocolVersion, id: I, addr: A) -> Self {
    Self {
      header: Header::new(version, id, addr),
      term: 0,
      last_log: 0,
      success: false,
      no_retry_backoff: false,
    }
  }

  /// Create a new [`AppendEntriesResponse`] with the given protocol version, node and default values.
  #[inline]
  pub const fn from_node(version: ProtocolVersion, node: Node<I, A>) -> Self {
    Self {
      header: Header::from_node(version, node),
      term: 0,
      last_log: 0,
      success: false,
      no_retry_backoff: false,
    }
  }

  /// Create a new [`AppendEntriesResponse`] with the given header and default values.
  #[inline]
  pub const fn from_header(header: Header<I, A>) -> Self {
    Self {
      header,
      term: 0,
      last_log: 0,
      success: false,
      no_retry_backoff: false,
    }
  }
}

impl<I: CheapClone, A: CheapClone> CheapClone for AppendEntriesResponse<I, A> {}

// Encode
//
// -----------------------------------------------------------------------------------------------------------------
// | len (4 bytes) | header (variable) | success & no_retry_backoff (1 byte) | term (uvarint) | last_log (uvarint) |
// -----------------------------------------------------------------------------------------------------------------
impl<I, A> Transformable for AppendEntriesResponse<I, A>
where
  I: Transformable,
  A: Transformable,
{
  type Error = TransformError;

  fn encode(&self, dst: &mut [u8]) -> Result<usize, Self::Error> {
    let encoded_len = self.encoded_len();
    if dst.len() < encoded_len {
      return Err(TransformError::EncodeBufferTooSmall);
    }

    let mut offset = 0;
    NetworkEndian::write_u32(&mut dst[offset..], encoded_len as u32);
    offset += MESSAGE_SIZE_LEN;

    offset += self.header.encode(&mut dst[offset..])?;

    let mut result = 0u8;
    // Set the first bit for `success`
    if self.success {
      result |= 1 << 0; // 1 << 0 is 1
    }

    // Set the second bit for `no_retry_backoff`
    if self.no_retry_backoff {
      result |= 1 << 1; // 1 << 1 is 2
    }

    dst[offset] = result;
    offset += 1;

    offset += encode_varint(self.term, &mut dst[offset..])?;
    offset += encode_varint(self.last_log, &mut dst[offset..])?;

    debug_assert_eq!(
      offset, encoded_len,
      "expected bytes wrote ({}) not match actual bytes wrote ({})",
      encoded_len, offset
    );
    Ok(offset)
  }

  fn encoded_len(&self) -> usize {
    MESSAGE_SIZE_LEN
      + self.header.encoded_len()
      + 1
      + encoded_len_varint(self.term)
      + encoded_len_varint(self.last_log)
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    let src_len = src.len();
    if src_len < MESSAGE_SIZE_LEN {
      return Err(TransformError::DecodeBufferTooSmall);
    }

    let mut offset = 0;
    let encoded_len = NetworkEndian::read_u32(&src[offset..]) as usize;
    if encoded_len > src_len {
      return Err(TransformError::DecodeBufferTooSmall);
    }
    offset += MESSAGE_SIZE_LEN;

    let (header_size, header) = <Header<I, A> as Transformable>::decode(&src[offset..])?;
    offset += header_size;

    let result = src[offset];
    let success = result & (1 << 0) != 0;
    let no_retry_backoff = result & (1 << 1) != 0;
    offset += 1;

    let (readed, term) = decode_varint(&src[offset..])?;
    offset += readed;

    let (readed, last_log) = decode_varint(&src[offset..])?;
    offset += readed;

    debug_assert_eq!(
      offset, encoded_len,
      "expected bytes read ({}) not match actual bytes read ({})",
      encoded_len, offset
    );

    Ok((
      offset,
      Self {
        header,
        term,
        last_log,
        success,
        no_retry_backoff,
      },
    ))
  }
}

/// The response returned by a pipeline.
///
/// The difference between this and [`AppendEntriesResponse`] is that this
/// keeps some extra information:
///
/// 1. the time that the append request was started
/// 2. the original request's `term`
/// 3. the number of entries the original request has
/// 4. highest log index of the original request's entries
#[viewit::viewit(getters(vis_all = "pub"), setters(prefix = "with"))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PipelineAppendEntriesResponse<I, A> {
  /// The term of the request
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Get the term of the original [`AppendEntriesRequest`](crate::transport::AppendEntriesRequest)."
      ),
    ),
    setter(attrs(
      doc = "Set the term which comes from the original [`AppendEntriesRequest`](crate::transport::AppendEntriesRequest)."
    ),)
  )]
  term: u64,

  /// The highest log index of the [`AppendEntriesRequest`]'s entries
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Get the highest log index of the original [`AppendEntriesRequest`](crate::transport::AppendEntriesRequest)."
      ),
    ),
    setter(attrs(
      doc = "Set the highest log index which comes from the original [`AppendEntriesRequest`](crate::transport::AppendEntriesRequest)."
    ),)
  )]
  highest_log_index: Option<u64>,

  /// The number of entries in the [`AppendEntriesRequest`]'s
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Get the number of entries of the original [`AppendEntriesRequest`](crate::transport::AppendEntriesRequest)."
      ),
    ),
    setter(attrs(
      doc = "Set the number of entries which comes from the original [`AppendEntriesRequest`](crate::transport::AppendEntriesRequest)."
    ),)
  )]
  num_entries: usize,

  /// The time that the original request was started
  #[cfg_attr(feature = "serde", serde(with = "crate::utils::serde_system_time"))]
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Get the time that the original [`AppendEntriesRequest`](crate::transport::AppendEntriesRequest) was started."
      ),
    ),
    setter(attrs(
      doc = "Set the time that the original [`AppendEntriesRequest`](crate::transport::AppendEntriesRequest) was started."
    ),)
  )]
  start: SystemTime,

  /// The response of the [`AppendEntriesRequest`]
  #[viewit(
    getter(
      const,
      style = "ref",
      rename = "response",
      attrs(doc = "Get the [`AppendEntriesResponse`].")
    ),
    setter(attrs(doc = "Set the [`AppendEntriesResponse`]."),)
  )]
  resp: AppendEntriesResponse<I, A>,
}

impl<I, A> PipelineAppendEntriesResponse<I, A> {
  /// Create a new [`PipelineAppendEntriesResponse`] with the given [`AppendEntriesResponse`]. Other fields
  /// are set to their default values.
  #[inline]
  pub fn new(resp: AppendEntriesResponse<I, A>) -> Self {
    Self {
      term: 0,
      highest_log_index: None,
      num_entries: 0,
      start: SystemTime::now(),
      resp,
    }
  }

  /// Returns the mutable reference to the response.
  #[inline]
  pub fn response_mut(&mut self) -> &mut AppendEntriesResponse<I, A> {
    &mut self.resp
  }
}

impl<I, A> From<PipelineAppendEntriesResponse<I, A>> for AppendEntriesResponse<I, A> {
  fn from(value: PipelineAppendEntriesResponse<I, A>) -> Self {
    value.resp
  }
}

impl<I: CheapClone, A: CheapClone> CheapClone for PipelineAppendEntriesResponse<I, A> {
  fn cheap_clone(&self) -> Self {
    Self {
      term: self.term,
      highest_log_index: self.highest_log_index,
      num_entries: self.num_entries,
      start: self.start,
      resp: self.resp.cheap_clone(),
    }
  }
}

#[cfg(any(feature = "test", test))]
impl AppendEntriesResponse<smol_str::SmolStr, std::net::SocketAddr> {
  #[doc(hidden)]
  pub fn __large() -> Self {
    Self {
      header: Header::__large(),
      term: 1,
      last_log: 1,
      success: true,
      no_retry_backoff: false,
    }
  }

  #[doc(hidden)]
  pub fn __small() -> Self {
    Self {
      header: Header::__small(),
      term: 1,
      last_log: 1,
      success: true,
      no_retry_backoff: true,
    }
  }
}

#[cfg(test)]
unit_test_transformable_roundtrip!(AppendEntriesResponse <smol_str::SmolStr, std::net::SocketAddr> => append_entries_response);
