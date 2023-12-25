//! Utilities for [`ruraft`](https://crates.io/crates/ruraft).
#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
#![deny(missing_docs, warnings)]
#![forbid(unsafe_code)]

/// Pod duration
pub mod duration;

/// Useful I/O utilities
#[cfg(feature = "io")]
#[cfg_attr(docsrs, doc(cfg(feature = "io")))]
pub mod io;

use core::time::Duration;
#[cfg(feature = "std")]
use std::cmp;

/// Like [`std::fs::create_dir_all`] but with a mode
#[cfg(all(unix, feature = "std"))]
#[cfg_attr(docsrs, doc(cfg(all(unix, feature = "std"))))]
pub fn make_dir_all<P: AsRef<std::path::Path>>(path: &P, mode: u32) -> std::io::Result<()> {
  use std::os::unix::fs::DirBuilderExt;
  std::fs::DirBuilder::new()
    .recursive(true)
    .mode(mode)
    .create(path)
}

/// Like [`std::fs::create_dir_all`] but with a mode
///
/// On Windows, this function does nothing with mode.
// TODO(al8n): I am not fimilar with Windows, so I am not sure how to create dir with mode.
#[cfg(all(windows, feature = "std"))]
#[cfg_attr(docsrs, doc(cfg(all(windows, feature = "std"))))]
pub fn make_dir_all<P: AsRef<std::path::Path>>(path: &P, _mode: u32) -> std::io::Result<()> {
  std::fs::DirBuilder::new().recursive(true).create(path)
}

/// Returns a value that is between the min_val and 2x min_val.
pub fn random_timeout(min_val: Duration) -> Option<Duration> {
  if min_val == Duration::from_secs(0) {
    return None;
  }

  let min_val_millis = min_val.as_millis() as u64;
  let extra = rand::random::<u64>() % min_val_millis;
  Some(Duration::from_millis(min_val_millis + extra))
}

/// Used to compute an exponential backoff
/// duration. Base time is scaled by the current round,
/// up to some maximum scale factor.
pub fn backoff(base: Duration, round: u64, limit: u64) -> Duration {
  let mut power = cmp::min(round, limit);
  let mut result = base;
  while power > 2 {
    result *= 2;
    power -= 1;
  }
  result
}

/// Computes the exponential backoff with an adjustable
/// cap on the max timeout.
pub fn capped_exponential_backoff(
  base: Duration,
  round: u64,
  limit: u64,
  cap: Duration,
) -> Duration {
  let mut power = cmp::min(round, limit);
  let mut result = base;
  while power > 2 {
    if result > cap {
      return cap;
    }
    result *= 2;
    power -= 1;
  }
  cmp::min(result, cap)
}

/// Returns a unix timestamp in milliseconds.
#[inline]
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub fn now_timestamp() -> u64 {
  std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)
    .expect("SystemTime before UNIX EPOCH!")
    .as_millis() as u64
}

/// Returns the encoded length of the value in LEB128 variable length format.
/// The returned value will be between 1 and 10, inclusive.
#[inline]
pub const fn encoded_len_varint(value: u64) -> usize {
  // Based on [VarintSize64][1].
  // [1]: https://github.com/google/protobuf/blob/3.3.x/src/google/protobuf/io/coded_stream.h#L1301-L1309
  ((((value | 1).leading_zeros() ^ 63) * 9 + 73) / 64) as usize
}

/// Encoding varint error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EncodeVarintError {
  /// The buffer did not have enough space to encode the value.
  BufferTooSmall,
}

impl core::fmt::Display for EncodeVarintError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::BufferTooSmall => write!(
        f,
        "the buffer did not have enough space to encode the value"
      ),
    }
  }
}

#[cfg(feature = "std")]
impl std::error::Error for EncodeVarintError {}

/// Encodes an integer value into LEB128 variable length format, and writes it to the buffer.
#[inline]
pub fn encode_varint(mut x: u64, buf: &mut [u8]) -> Result<usize, EncodeVarintError> {
  let mut i = 0;

  while x >= 0x80 {
    if i >= buf.len() {
      return Err(EncodeVarintError::BufferTooSmall);
    }

    buf[i] = (x as u8) | 0x80;
    x >>= 7;
    i += 1;
  }
  buf[i] = x as u8;
  Ok(i + 1)
}

/// Decoding varint error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DecodeVarintError {
  /// The buffer did not contain a valid LEB128 encoding.
  InvalidEncoding,
  /// The buffer did not contain enough bytes to decode a value.
  BufferTooSmall,
}

impl core::fmt::Display for DecodeVarintError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::InvalidEncoding => write!(f, "the buffer did not contain a valid LEB128 encoding"),
      Self::BufferTooSmall => write!(
        f,
        "the buffer did not contain enough bytes to decode a value"
      ),
    }
  }
}

#[cfg(feature = "std")]
impl std::error::Error for DecodeVarintError {}

/// Decodes a value from LEB128 variable length format.
///
/// # Arguments
///
/// * `buf` - A byte slice containing the LEB128 encoded value.
///
/// # Returns
///
/// * Returns the bytes readed and the decoded value as `u64` if successful.
///
/// * Returns [`DecodeVarintError`] if the buffer did not contain a valid LEB128 encoding
/// or the decode buffer did not contain enough bytes to decode a value.
pub const fn decode_varint(buf: &[u8]) -> Result<(usize, u64), DecodeVarintError> {
  let (mut x, mut s) = (0, 0);
  let mut i = 0usize;
  loop {
    if i == 10 {
      // It's not a valid LEB128 encoding if it exceeds 10 bytes for u64.
      return Err(DecodeVarintError::InvalidEncoding);
    }

    if i >= buf.len() {
      return Err(DecodeVarintError::BufferTooSmall);
    }

    let b = buf[i];
    if b < 0x80 {
      if i == 10 - 1 && b > 1 {
        return Err(DecodeVarintError::InvalidEncoding);
      }
      return Ok((i + 1, x | (b as u64) << s));
    }
    x |= ((b & 0x7f) as u64) << s;
    s += 7;
    i += 1;
  }
}
