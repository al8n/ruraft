use core::{sync::atomic::Ordering, time::Duration as StdDuration};

use atomic::Atomic;

#[derive(Debug)]
#[repr(transparent)]
pub struct AtomicDuration(Atomic<Duration>);

impl AtomicDuration {
  pub const fn new(duration: StdDuration) -> Self {
    Self(Atomic::new(Duration::from_std(duration)))
  }

  pub fn load(&self, ordering: Ordering) -> StdDuration {
    self.0.load(ordering).to_std()
  }

  pub fn store(&self, val: StdDuration, ordering: Ordering) {
    self.0.store(Duration::from_std(val), ordering)
  }

  pub fn swap(&self, val: StdDuration, ordering: Ordering) -> StdDuration {
    self.0.swap(Duration::from_std(val), ordering).to_std()
  }

  pub fn compare_exchange_weak(
    &self,
    current: StdDuration,
    new: StdDuration,
    success: Ordering,
    failure: Ordering,
  ) -> Result<StdDuration, StdDuration> {
    self
      .0
      .compare_exchange_weak(
        Duration::from_std(current),
        Duration::from_std(new),
        success,
        failure,
      )
      .map(|d| d.to_std())
      .map_err(|d| d.to_std())
  }

  pub fn compare_exchange(
    &self,
    current: StdDuration,
    new: StdDuration,
    success: Ordering,
    failure: Ordering,
  ) -> Result<StdDuration, StdDuration> {
    self
      .0
      .compare_exchange(
        Duration::from_std(current),
        Duration::from_std(new),
        success,
        failure,
      )
      .map(|d| d.to_std())
      .map_err(|d| d.to_std())
  }

  pub fn fetch_update<F>(
    &self,
    set_order: Ordering,
    fetch_order: Ordering,
    mut f: F,
  ) -> Result<StdDuration, StdDuration>
  where
    F: FnMut(StdDuration) -> Option<StdDuration>,
  {
    self
      .0
      .fetch_update(set_order, fetch_order, |d| {
        f(d.to_std()).map(Duration::from_std)
      })
      .map(|d| d.to_std())
      .map_err(|d| d.to_std())
  }

  pub fn into_inner(self) -> StdDuration {
    self.0.into_inner().to_std()
  }
}

#[derive(bytemuck::NoUninit, PartialEq, Eq, PartialOrd, Ord, Debug, Clone, Copy)]
#[repr(transparent)]
pub struct Duration(u64);

impl Duration {
  pub const fn from_std(d: StdDuration) -> Self {
    Self(d.as_millis() as u64)
  }

  pub const fn to_std(self) -> StdDuration {
    StdDuration::from_millis(self.0)
  }
}

#[cfg(feature = "serde")]
impl serde::Serialize for Duration {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: serde::Serializer,
  {
    humantime_serde::serialize(&self.to_std(), serializer)
  }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for Duration {
  fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
  where
    D: serde::Deserializer<'de>,
  {
    humantime_serde::deserialize(deserializer).map(Duration::from_std)
  }
}
