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
  use atomic_time::utils::{decode_duration, encode_duration};
  use serde::{Deserialize, Deserializer, Serializer};
  use std::time::SystemTime;

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
    use atomic_time::utils::{decode_option_duration, encode_option_duration};

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
