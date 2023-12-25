pub(crate) async fn override_notify_bool(
  tx: &async_channel::Sender<bool>,
  rx: &async_channel::Receiver<bool>,
  v: bool,
) {
  use futures::FutureExt;

  futures::select! {
    _ = tx.send(v).fuse() => {
      // value sent, all done
    }
    _ = rx.recv().fuse() => {
      // channel had an old value
      futures::select! {
        _ = tx.send(v).fuse() => {
          // value sent, all done
        }
        default => {
          panic!("race: channel was sent concurrently");
        }
      }
    }
  }
}

#[cfg(feature = "serde")]
pub(crate) mod serde_system_time {
  use serde::{Deserialize, Deserializer, Serializer};
  use std::time::{Duration, SystemTime};

  const fn encode_duration(duration: Duration) -> u128 {
    let seconds = duration.as_secs() as u128;
    let nanos = duration.subsec_nanos() as u128;
    (seconds << 32) + nanos
  }

  const fn decode_duration(encoded: u128) -> Duration {
    let seconds = (encoded >> 32) as u64;
    let nanos = (encoded & 0xFFFFFFFF) as u32;
    Duration::new(seconds, nanos)
  }

  pub fn serialize<S>(time: &SystemTime, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    serializer.serialize_u128(encode_duration(
      time
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(<S::Error as serde::ser::Error>::custom)?,
    ))
  }

  pub fn deserialize<'de, D>(deserializer: D) -> Result<SystemTime, D::Error>
  where
    D: Deserializer<'de>,
  {
    Ok(SystemTime::UNIX_EPOCH + decode_duration(u128::deserialize(deserializer)?))
  }

  pub mod option {
    use super::*;

    const fn encode_option_duration(option_duration: Option<Duration>) -> u128 {
      match option_duration {
        Some(duration) => {
          let seconds = duration.as_secs() as u128;
          let nanos = duration.subsec_nanos() as u128;
          (1 << 127) | (seconds << 32) | nanos
        }
        None => 0,
      }
    }
    const fn decode_option_duration(encoded: u128) -> Option<Duration> {
      if encoded >> 127 == 0 {
        None
      } else {
        let seconds = ((encoded << 1) >> 33) as u64;
        let nanos = (encoded & 0xFFFFFFFF) as u32;
        Some(Duration::new(seconds, nanos))
      }
    }

    pub fn serialize<S>(time: &Option<SystemTime>, serializer: S) -> Result<S::Ok, S::Error>
    where
      S: Serializer,
    {
      match time {
        Some(t) => serializer.serialize_u128(encode_option_duration(Some(
          t.duration_since(SystemTime::UNIX_EPOCH)
            .map_err(<S::Error as serde::ser::Error>::custom)?,
        ))),
        None => serializer.serialize_u128(encode_option_duration(None)),
      }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<SystemTime>, D::Error>
    where
      D: Deserializer<'de>,
    {
      Ok(
        decode_option_duration(u128::deserialize(deserializer)?)
          .map(|d| SystemTime::UNIX_EPOCH + d),
      )
    }
  }
}
