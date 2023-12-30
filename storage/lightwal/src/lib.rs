//! Lightwal is a lightweight write-ahead log for Ruraft.
#![forbid(unsafe_code)]
#![deny(warnings, missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

use ruraft_core::storage::{LogStorage, SnapshotStorage, StableStorage, Storage, StorageError};

#[cfg(not(any(feature = "redb", feature = "sled", feature = "jammdb")))]
compile_error!("At least one of the following features must be enabled: redb, sled, jammdb");

/// [`jammdb`](::jammdb) backed [`StableStorage`](ruraft_core::storage::StableStorage) and [`LogStorage`](ruraft_core::storage::LogStorage) implementor.
#[cfg(feature = "jammdb")]
#[cfg_attr(docsrs, doc(cfg(feature = "jammdb")))]
pub mod jammdb;

/// [`redb`](::redb) backed [`StableStorage`](ruraft_core::storage::StableStorage) and [`LogStorage`](ruraft_core::storage::LogStorage) implementor.
#[cfg(feature = "redb")]
#[cfg_attr(docsrs, doc(cfg(feature = "redb")))]
pub mod redb;

/// [`sled`](::sled) backed [`StableStorage`](ruraft_core::storage::StableStorage) and [`LogStorage`](ruraft_core::storage::LogStorage) implementor.
#[cfg(feature = "sled")]
#[cfg_attr(docsrs, doc(cfg(feature = "sled")))]
pub mod sled;

const LAST_CANIDATE_ID: &str = "__ruraft_last_candidate_id__";
const LAST_CANIDATE_ADDR: &str = "__ruraft_last_candidate_addr__";
const LAST_VOTE_TERM: &str = "__ruraft_last_vote_term__";
const CURRENT_TERM: &str = "__ruraft_current_term__";

#[cfg(feature = "metrics")]
fn report_store_many(logs: usize, start: std::time::Instant) {
  let duration = start.elapsed();
  let nanos = duration.as_nanos(); // Get the elapsed time in nanoseconds
  let val = if nanos == 0 {
    0.0
  } else {
    (1_000_000_000.0 / nanos as f64) * logs as f64
  };

  metrics::histogram!("ruraft.lightwal.write_capacity", val);
  metrics::histogram!("ruraft.lightwal.store_logs", start.elapsed().as_secs_f64());
}

#[doc(hidden)]
pub trait Backend: sealed::Sealed {}

// Implementation of the private, sealed trait
mod sealed {
  use agnostic::Runtime;
  use ruraft_core::transport::{Address, Id, Transformable};

  use super::*;

  pub trait Sealed:
    LogStorage
    + StableStorage<
      Id = <Self as LogStorage>::Id,
      Address = <Self as LogStorage>::Address,
      Runtime = <Self as LogStorage>::Runtime,
    >
  {
  }

  #[cfg(feature = "jammdb")]
  impl<I, A, D, R> Sealed for crate::jammdb::Db<I, A, D, R>
  where
    I: Id,
    A: Address,
    D: Transformable,
    R: Runtime,
  {
  }

  #[cfg(feature = "redb")]
  impl<I, A, D, R> Sealed for crate::redb::Db<I, A, D, R>
  where
    I: Id,
    A: Address,
    D: Transformable,
    R: Runtime,
  {
  }

  #[cfg(feature = "sled")]
  impl<I, A, D, R> Sealed for crate::sled::Db<I, A, D, R>
  where
    I: Id,
    A: Address,
    D: Transformable,
    R: Runtime,
  {
  }

  impl<S: Sealed> Backend for S {}
}

/// Error kind for [`Error`].
pub enum ErrorKind<S: SnapshotStorage, B: Backend> {
  /// Snapshot storage error.
  Snapshot(<S as SnapshotStorage>::Error),
  /// Stable storage error.
  Stable(<B as StableStorage>::Error),
  /// Log storage error.
  Log(<B as LogStorage>::Error),
  /// IO error.
  IO(std::io::Error),
  /// Custom error.
  Custom(String),
}

impl<S: SnapshotStorage, B: Backend> std::fmt::Display for ErrorKind<S, B> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::Snapshot(e) => write!(f, "snapshot storage: {e}"),
      Self::Stable(e) => write!(f, "stable storage: {e}"),
      Self::Log(e) => write!(f, "log storage: {e}"),
      Self::IO(e) => write!(f, "io: {e}"),
      Self::Custom(e) => write!(f, "custom: {e}"),
    }
  }
}

impl<S: SnapshotStorage, B: Backend> std::fmt::Debug for ErrorKind<S, B> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::Snapshot(e) => write!(f, "snapshot storage: {e:?}"),
      Self::Stable(e) => write!(f, "stable storage: {e:?}"),
      Self::Log(e) => write!(f, "log storage: {e:?}"),
      Self::IO(e) => write!(f, "io: {e:?}"),
      Self::Custom(e) => write!(f, "custom: {e:?}"),
    }
  }
}

/// [`StorageError`](ruraft_core::storage::StorageError) implementation for [`LightStorage`].
pub struct Error<S: SnapshotStorage, B: Backend> {
  kind: ErrorKind<S, B>,
  messages: Vec<std::borrow::Cow<'static, str>>,
}

impl<S: SnapshotStorage, B: Backend> Error<S, B> {
  /// Creates a new [`Error`].
  pub fn new(kind: ErrorKind<S, B>) -> Self {
    Self {
      kind,
      messages: Vec::new(),
    }
  }

  /// Returns the error kind.
  pub fn kind(&self) -> &ErrorKind<S, B> {
    &self.kind
  }

  /// Returns the error messages.
  pub fn messages(&self) -> &[std::borrow::Cow<'static, str>] {
    &self.messages
  }

  /// Consumes the error and returns the error kind.
  pub fn into_kind(self) -> ErrorKind<S, B> {
    self.kind
  }

  /// Consumes the error and returns the error messages.
  pub fn into_messages(self) -> Vec<std::borrow::Cow<'static, str>> {
    self.messages
  }

  /// Consumes the error and returns the error kind and messages.
  pub fn into_components(self) -> (ErrorKind<S, B>, Vec<std::borrow::Cow<'static, str>>) {
    (self.kind, self.messages)
  }
}

impl<S: SnapshotStorage, B: Backend> std::fmt::Debug for Error<S, B> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct(std::any::type_name::<Self>())
      .field("kind", &self.kind)
      .field("messages", &self.messages)
      .finish()
  }
}

impl<S: SnapshotStorage, B: Backend> std::fmt::Display for Error<S, B> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    writeln!(f, "{}", self.kind)?;

    for (idx, msg) in self.messages.iter().enumerate() {
      writeln!(f, "\t{idx}: {msg}")?;
    }
    Ok(())
  }
}

impl<S: SnapshotStorage, B: Backend> std::error::Error for Error<S, B> {}

impl<S: SnapshotStorage, B: Backend> StorageError for Error<S, B> {
  type Stable = B;

  type Snapshot = S;

  type Log = B;

  fn stable(err: <Self::Stable as StableStorage>::Error) -> Self {
    Self::new(ErrorKind::Stable(err))
  }

  fn snapshot(err: <Self::Snapshot as SnapshotStorage>::Error) -> Self {
    Self::new(ErrorKind::Snapshot(err))
  }

  fn log(err: <Self::Log as LogStorage>::Error) -> Self {
    Self::new(ErrorKind::Log(err))
  }

  fn with_message(mut self, msg: std::borrow::Cow<'static, str>) -> Self {
    self.messages.push(msg);
    self
  }

  fn io(err: std::io::Error) -> Self {
    Self::new(ErrorKind::IO(err))
  }

  fn custom<T>(msg: T) -> Self
  where
    Self: Sized,
    T: core::fmt::Display,
  {
    Self::new(ErrorKind::Custom(msg.to_string()))
  }
}

/// [`SnapshotStorage`](ruraft_core::storage::SnapshotStorage) implementation which consists of a [`SnapshotStorage`] and a backend storage which implements [`LogStorage`] and [`StableStorage`].
pub struct LightStorage<S, B> {
  snapshot: S,
  backend: B,
}

impl<S, B> LightStorage<S, B> {
  /// Create a new [`LightStorage`] with the given [`SnapshotStorage`] and backend storage which implements [`LogStorage`] and [`StableStorage`].
  pub fn new(snapshot: S, backend: B) -> Self {
    Self { snapshot, backend }
  }
}

impl<
    S: SnapshotStorage<
      Id = <B as LogStorage>::Id,
      Address = <B as LogStorage>::Address,
      Runtime = <B as LogStorage>::Runtime,
    >,
    B: Backend,
  > Storage for LightStorage<S, B>
{
  type Error = Error<S, B>;

  type Id = <B as StableStorage>::Id;

  type Address = <B as StableStorage>::Address;

  type Data = <B as LogStorage>::Data;

  type Stable = B;

  type Snapshot = S;

  type Log = B;

  type Runtime = <B as LogStorage>::Runtime;

  fn stable_store(&self) -> &Self::Stable {
    &self.backend
  }

  fn log_store(&self) -> &Self::Log {
    &self.backend
  }

  fn snapshot_store(&self) -> &Self::Snapshot {
    &self.snapshot
  }
}

#[cfg(any(feature = "test", test))]
mod test {
  pub use ruraft_core::tests::storage::*;
}
