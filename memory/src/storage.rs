use std::io;

use agnostic::Runtime;
use ruraft_core::{
  storage::{Storage, StorageError},
  transport::{Address, Id},
  Data,
};

use self::{
  log::{MemoryLogStorage, MemoryLogStorageError},
  snapshot::MemorySnapshotStorage,
  stable::MemoryStableStorage,
};

/// Memory based [`SnapshotStorage`](ruraft_core::storage::SnapshotStorage) implementation.
pub mod snapshot;

/// Memory based [`StableStorage`](ruraft_core::storage::StableStorage) implementation.
pub mod stable;

/// Memory based [`LogStorage`](ruraft_core::storage::LogStorage) implementation.
pub mod log;

/// Error kind for [`Error`].
#[derive(Debug)]
pub enum ErrorKind {
  /// Snapshot storage error.
  Snapshot(io::Error),
  /// Log storage error.
  Log(MemoryLogStorageError),
  /// IO error.
  IO(io::Error),
  /// Custom error.
  Custom(String),
}

impl std::fmt::Display for ErrorKind {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::Snapshot(e) => write!(f, "snapshot storage: {e}"),
      Self::Log(e) => write!(f, "log storage: {e}"),
      Self::IO(e) => write!(f, "io: {e}"),
      Self::Custom(e) => write!(f, "custom: {e}"),
    }
  }
}

/// [`StorageError`](ruraft_core::storage::StorageError) implementation for [`MemoryStorage`].
pub struct Error<I, A, D, R> {
  kind: ErrorKind,
  messages: Vec<std::borrow::Cow<'static, str>>,
  _runtime: std::marker::PhantomData<(I, A, D, R)>,
}

impl<I, A, D, R> Error<I, A, D, R> {
  /// Creates a new [`Error`].
  pub fn new(kind: ErrorKind) -> Self {
    Self {
      kind,
      messages: Vec::new(),
      _runtime: std::marker::PhantomData,
    }
  }

  /// Returns the error kind.
  pub fn kind(&self) -> &ErrorKind {
    &self.kind
  }

  /// Returns the error messages.
  pub fn messages(&self) -> &[std::borrow::Cow<'static, str>] {
    &self.messages
  }

  /// Consumes the error and returns the error messages and error kind.
  pub fn into_components(self) -> (ErrorKind, Vec<std::borrow::Cow<'static, str>>) {
    (self.kind, self.messages)
  }
}

impl<I, A, D, R> std::fmt::Debug for Error<I, A, D, R> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct(std::any::type_name::<Self>())
      .field("kind", &self.kind)
      .field("messages", &self.messages)
      .finish()
  }
}

impl<I, A, D, R> std::fmt::Display for Error<I, A, D, R> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    writeln!(f, "{}", self.kind)?;

    for (idx, msg) in self.messages.iter().enumerate() {
      writeln!(f, "\t{idx}: {msg}")?;
    }
    Ok(())
  }
}

impl<I: Id, A: Address, D: Data, R: Runtime> std::error::Error for Error<I, A, D, R> {}

impl<I: Id, A: Address, D: Data, R: Runtime> StorageError for Error<I, A, D, R> {
  type Stable = MemoryStableStorage<I, A, R>;

  type Snapshot = MemorySnapshotStorage<I, A, R>;

  type Log = MemoryLogStorage<I, A, D, R>;

  fn stable(
    _err: <MemoryStableStorage<I, A, R> as ruraft_core::storage::StableStorage>::Error,
  ) -> Self {
    unreachable!()
  }

  fn snapshot(
    err: <MemorySnapshotStorage<I, A, R> as ruraft_core::storage::SnapshotStorage>::Error,
  ) -> Self {
    Self {
      kind: ErrorKind::Snapshot(err),
      messages: Vec::new(),
      _runtime: std::marker::PhantomData,
    }
  }

  fn log(err: <MemoryLogStorage<I, A, D, R> as ruraft_core::storage::LogStorage>::Error) -> Self {
    Self {
      kind: ErrorKind::Log(err),
      messages: Vec::new(),
      _runtime: std::marker::PhantomData,
    }
  }

  fn with_message(mut self, msg: std::borrow::Cow<'static, str>) -> Self {
    self.messages.push(msg);
    self
  }

  fn io(err: std::io::Error) -> Self {
    Self {
      kind: ErrorKind::IO(err),
      messages: Vec::new(),
      _runtime: std::marker::PhantomData,
    }
  }

  fn custom<T>(msg: T) -> Self
  where
    Self: Sized,
    T: core::fmt::Display,
  {
    Self {
      kind: ErrorKind::Custom(msg.to_string()),
      messages: Vec::new(),
      _runtime: std::marker::PhantomData,
    }
  }
}

/// Memory based [`Storage`](ruraft_core::storage::Storage) implementation.
pub struct MemoryStorage<I, A, D, R> {
  stable: MemoryStableStorage<I, A, R>,
  snapshot: MemorySnapshotStorage<I, A, R>,
  log: MemoryLogStorage<I, A, D, R>,
}

impl<I, A, D, R> Storage for MemoryStorage<I, A, D, R>
where
  I: Id + Unpin,
  A: Address + Unpin,
  D: Data,
  R: Runtime,
{
  type Error = Error<I, A, D, R>;
  type Runtime = R;
  type Id = I;
  type Address = A;
  type Data = D;
  type Snapshot = MemorySnapshotStorage<I, A, R>;
  type Stable = MemoryStableStorage<I, A, R>;
  type Log = MemoryLogStorage<I, A, D, R>;

  fn stable_store(&self) -> &Self::Stable {
    &self.stable
  }

  fn log_store(&self) -> &Self::Log {
    &self.log
  }

  fn snapshot_store(&self) -> &Self::Snapshot {
    &self.snapshot
  }
}
