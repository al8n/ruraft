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
