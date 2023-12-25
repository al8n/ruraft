use core::time::Duration;

/// A duration type that does not contain any padding bytes
#[derive(Clone, Copy, bytemuck::NoUninit)]
#[repr(transparent)]
pub struct PadDuration(u64);

impl PadDuration {
  /// Creates a duration from the [`Duration`] type from the standard library.
  pub const fn from_std(d: Duration) -> Self {
    Self(d.as_millis() as u64)
  }

  /// Consumes the pod duration and returns the [`Duration`] type from the standard library.
  pub const fn to_std(self) -> Duration {
    Duration::from_millis(self.0)
  }
}

#[cfg(feature = "serde")]
impl serde::Serialize for PadDuration {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: serde::Serializer,
  {
    humantime_serde::serialize(&self.to_std(), serializer)
  }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for PadDuration {
  fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
  where
    D: serde::Deserializer<'de>,
  {
    humantime_serde::deserialize(deserializer).map(PadDuration::from_std)
  }
}
