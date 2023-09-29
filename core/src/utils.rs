use std::{cmp, fs::DirBuilder, io, path::Path, time::Duration};

pub mod checksumable;

/// Like [`std::fs::create_dir_all`] but with a mode
#[cfg(unix)]
pub fn make_dir_all<P: AsRef<Path>>(path: &P, mode: u32) -> io::Result<()> {
  use std::os::unix::fs::DirBuilderExt;

  DirBuilder::new().recursive(true).mode(mode).create(path)
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

/// Encodes an integer value into LEB128 variable length format, and writes it to the buffer.
/// The buffer must have enough remaining space (maximum 10 bytes).
#[inline]
pub fn encode_varint(mut value: u64, buf: &mut [u8]) {
  let mut i = 0usize;
  loop {
    if value < 0x80 {
      buf[i] = value as u8;
      break;
    } else {
      buf[i] = ((value & 0x7F) | 0x80) as u8;
      value >>= 7;
    }
    i += 1;
  }
}

/// Decodes a value from LEB128 variable length format.
///
/// # Arguments
///
/// * `buf` - A byte slice containing the LEB128 encoded value.
///
/// # Returns
///
/// * `Option<u64>` - Returns the decoded value as `u64` if successful.
///   Returns `None` if the buffer did not contain a valid LEB128 encoding.
pub const fn decode_varint(buf: &[u8]) -> Option<u64> {
  let mut result: u64 = 0;
  let mut shift = 0;

  let mut i = 0usize;
  loop {
    if i == 10 {
      // It's not a valid LEB128 encoding if it exceeds 10 bytes for u64.
      return None;
    }

    let value = (buf[i] & 0x7F) as u64;
    result |= value << shift;

    // If the high-order bit is not set, this byte is the end of the encoding.
    if buf[i] & 0x80 == 0 {
      return Some(result);
    }

    shift += 7;
    i += 1;
  }
}
