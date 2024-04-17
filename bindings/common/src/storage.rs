use std::{
  convert::Infallible,
  error::Error,
  fmt::{self, Debug},
  ops::RangeBounds,
  sync::Arc,
};

use agnostic::Runtime;
use nodecraft::{NodeAddress, NodeId, Transformable};
use ruraft_core::{
  storage::{Log, LogStorage, RaftStorage, RaftStorageError, StableStorage, Storage, StorageError},
  Data,
};
use ruraft_memory::storage::{
  log::{MemoryLogStorage, MemoryLogStorageError},
  snapshot::MemorySnapshotStorage,
  stable::MemoryStableStorage,
};

mod snapshot;
pub use snapshot::*;

#[cfg(any(feature = "jammdb", feature = "redb", feature = "sled"))]
mod light;
#[cfg(any(feature = "jammdb", feature = "redb", feature = "sled"))]
pub use light::*;

#[derive(Clone, Debug, derive_more::From, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum BackendOptions {
  Memory,
  Light(LightBackendOptions),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
pub struct SupportedStorageOptions {
  snapshot: SnapshotStorageOptions,
  #[cfg_attr(feature = "serde", serde(flatten))]
  backend: BackendOptions,
}

impl SupportedStorageOptions {
  pub fn new(snapshot: SnapshotStorageOptions, backend: BackendOptions) -> Self {
    Self { snapshot, backend }
  }
}

#[derive(derive_more::From, derive_more::Display)]
pub enum SupportedLogStorageError<D: Data> {
  #[cfg(any(feature = "jammdb", feature = "redb", feature = "sled"))]
  Light(SupportedLightBackendError<D>),
  Memory(MemoryLogStorageError),
}

impl<D: Data> Clone for SupportedLogStorageError<D> {
  fn clone(&self) -> Self {
    match self {
      #[cfg(any(feature = "jammdb", feature = "redb", feature = "sled"))]
      Self::Light(db) => Self::Light(db.clone()),
      Self::Memory(db) => Self::Memory(db.clone()),
    }
  }
}

impl<D: Data> Debug for SupportedLogStorageError<D> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      #[cfg(any(feature = "jammdb", feature = "redb", feature = "sled"))]
      Self::Light(e) => write!(f, "{e:?}"),
      Self::Memory(e) => write!(f, "{e:?}"),
    }
  }
}

impl<D: Data> Error for SupportedLogStorageError<D> {}

#[derive(derive_more::From)]
pub enum SupportedLogStorage<D: Data, R: Runtime> {
  #[cfg(any(feature = "jammdb", feature = "redb", feature = "sled"))]
  Light(SupportedLightBackend<D, R>),
  Memory(MemoryLogStorage<NodeId, NodeAddress, D, R>),
}

impl<D: Data, R: Runtime> LogStorage for SupportedLogStorage<D, R> {
  type Error = SupportedLogStorageError<D>;
  type Runtime = R;

  type Id = NodeId;
  type Address = NodeAddress;
  type Data = D;

  async fn first_index(&self) -> Result<Option<u64>, Self::Error> {
    match self {
      #[cfg(any(feature = "jammdb", feature = "redb", feature = "sled"))]
      Self::Light(db) => db.first_index().await.map_err(From::from),
      Self::Memory(db) => db.first_index().await.map_err(From::from),
    }
  }

  async fn last_index(&self) -> Result<Option<u64>, Self::Error> {
    match self {
      #[cfg(any(feature = "jammdb", feature = "redb", feature = "sled"))]
      Self::Light(db) => db.last_index().await.map_err(From::from),
      Self::Memory(db) => db.last_index().await.map_err(From::from),
    }
  }

  async fn get_log(&self, index: u64) -> Result<Option<Log<Self::Id, Self::Address>>, Self::Error> {
    match self {
      #[cfg(any(feature = "jammdb", feature = "redb", feature = "sled"))]
      Self::Light(db) => db.get_log(index).await.map_err(From::from),
      Self::Memory(db) => db.get_log(index).await.map_err(From::from),
    }
  }

  async fn store_log(&self, log: &Log<Self::Id, Self::Address>) -> Result<(), Self::Error> {
    match self {
      #[cfg(any(feature = "jammdb", feature = "redb", feature = "sled"))]
      Self::Light(db) => db.store_log(log).await.map_err(From::from),
      Self::Memory(db) => db.store_log(log).await.map_err(From::from),
    }
  }

  async fn store_logs(&self, logs: &[Log<Self::Id, Self::Address>]) -> Result<(), Self::Error> {
    match self {
      #[cfg(any(feature = "jammdb", feature = "redb", feature = "sled"))]
      Self::Light(db) => db.store_logs(logs).await.map_err(From::from),
      Self::Memory(db) => db.store_logs(logs).await.map_err(From::from),
    }
  }

  async fn remove_range(&self, range: impl RangeBounds<u64> + Send) -> Result<(), Self::Error> {
    match self {
      #[cfg(any(feature = "jammdb", feature = "redb", feature = "sled"))]
      Self::Light(db) => db.remove_range(range).await.map_err(From::from),
      Self::Memory(db) => db.remove_range(range).await.map_err(From::from),
    }
  }

  fn is_monotonic() -> bool {
    false
  }
}

#[derive(derive_more::From, derive_more::Display)]
pub enum SupportedStableStorageError<D: Data> {
  #[cfg(any(feature = "jammdb", feature = "redb", feature = "sled"))]
  Light(SupportedLightBackendError<D>),
  Memory(Infallible),
}

impl<D: Data> Debug for SupportedStableStorageError<D> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      #[cfg(any(feature = "jammdb", feature = "redb", feature = "sled"))]
      Self::Light(e) => write!(f, "{e:?}"),
      Self::Memory(e) => write!(f, "{e:?}"),
    }
  }
}

impl<D: Data> Error for SupportedStableStorageError<D> {}

#[derive(derive_more::From)]
pub enum SupportedStableStorage<D: Data, R: Runtime> {
  #[cfg(any(feature = "jammdb", feature = "redb", feature = "sled"))]
  Light(SupportedLightBackend<D, R>),
  Memory(MemoryStableStorage<NodeId, NodeAddress, R>),
}

impl<D: Data, R: Runtime> StableStorage for SupportedStableStorage<D, R> {
  type Error = SupportedStableStorageError<D>;

  type Runtime = R;

  type Id = NodeId;

  type Address = NodeAddress;

  async fn current_term(&self) -> Result<Option<u64>, Self::Error> {
    match self {
      #[cfg(any(feature = "jammdb", feature = "redb", feature = "sled"))]
      Self::Light(db) => db.current_term().await.map_err(From::from),
      Self::Memory(db) => db.current_term().await.map_err(From::from),
    }
  }

  async fn store_current_term(&self, term: u64) -> Result<(), Self::Error> {
    match self {
      #[cfg(any(feature = "jammdb", feature = "redb", feature = "sled"))]
      Self::Light(db) => db.store_current_term(term).await.map_err(From::from),
      Self::Memory(db) => db.store_current_term(term).await.map_err(From::from),
    }
  }

  async fn last_vote_term(&self) -> Result<Option<u64>, Self::Error> {
    match self {
      #[cfg(any(feature = "jammdb", feature = "redb", feature = "sled"))]
      Self::Light(db) => db.last_vote_term().await.map_err(From::from),
      Self::Memory(db) => db.last_vote_term().await.map_err(From::from),
    }
  }

  async fn store_last_vote_term(&self, term: u64) -> Result<(), Self::Error> {
    match self {
      #[cfg(any(feature = "jammdb", feature = "redb", feature = "sled"))]
      Self::Light(db) => db.store_last_vote_term(term).await.map_err(From::from),
      Self::Memory(db) => db.store_last_vote_term(term).await.map_err(From::from),
    }
  }

  async fn last_vote_candidate(
    &self,
  ) -> Result<Option<ruraft_core::Node<Self::Id, Self::Address>>, Self::Error> {
    match self {
      #[cfg(any(feature = "jammdb", feature = "redb", feature = "sled"))]
      Self::Light(db) => db.last_vote_candidate().await.map_err(From::from),
      Self::Memory(db) => db.last_vote_candidate().await.map_err(From::from),
    }
  }

  async fn store_last_vote_candidate(
    &self,
    candidate: ruraft_core::Node<Self::Id, Self::Address>,
  ) -> Result<(), Self::Error> {
    match self {
      #[cfg(any(feature = "jammdb", feature = "redb", feature = "sled"))]
      Self::Light(db) => db
        .store_last_vote_candidate(candidate)
        .await
        .map_err(From::from),
      Self::Memory(db) => db
        .store_last_vote_candidate(candidate)
        .await
        .map_err(From::from),
    }
  }
}

pub struct SupportedStorage<D: Data, R: Runtime>(
  RaftStorage<SupportedLogStorage<D, R>, SupportedStableStorage<D, R>, SupportedSnapshotStorage<R>>,
);

impl<D: Data, R: Runtime> SupportedStorage<D, R> {
  pub async fn new(opts: SupportedStorageOptions) -> Result<Self, <Self as Storage>::Error> {
    let snapshot = match opts.snapshot {
      SnapshotStorageOptions::Memory => {
        SupportedSnapshotStorage::Memory(MemorySnapshotStorage::default())
      }
      SnapshotStorageOptions::File(opts) => SupportedSnapshotStorage::File(
        ruraft_snapshot::sync::FileSnapshotStorage::new(opts).map_err(|e| {
          <<Self as Storage>::Error as StorageError>::snapshot(SupportedSnapshotStorageError::from(
            e,
          ))
        })?,
      ),
    };

    #[cfg(any(feature = "jammdb", feature = "redb", feature = "sled"))]
    macro_rules! light {
      ($opts:ident -> $ty:ty) => {{
        <$ty>::new($opts.into())
          .map(|db| {
            let db = Arc::new(db);
            let log = SupportedLogStorage::Light(SupportedLightBackend::from(db.clone()));
            let stable = SupportedStableStorage::Light(SupportedLightBackend::from(db));
            Self(RaftStorage::new(log, stable, snapshot))
          })
          .map_err(|e| {
            <<Self as Storage>::Error as StorageError>::log(
              SupportedLightBackendError::from(e).into(),
            )
          })
      }};
    }

    match opts.backend {
      BackendOptions::Memory => Ok(Self(RaftStorage::new(
        SupportedLogStorage::Memory(MemoryLogStorage::default()),
        SupportedStableStorage::Memory(MemoryStableStorage::default()),
        snapshot,
      ))),
      #[cfg(any(feature = "jammdb", feature = "redb", feature = "sled"))]
      BackendOptions::Light(opts) => match opts {
        #[cfg(feature = "sled")]
        LightBackendOptions::Sled(opts) => {
          light!(opts -> ruraft_lightwal::sled::Db::<_, _, _, _>)
        }
        #[cfg(feature = "redb")]
        LightBackendOptions::Redb(opts) => {
          light!(opts -> ruraft_lightwal::redb::Db::<_, _, _, _>)
        }
        #[cfg(feature = "jammdb")]
        LightBackendOptions::Jammdb(opts) => {
          light!(opts -> ruraft_lightwal::jammdb::Db::<_, _, _, _>)
        }
      },
    }
  }
}

impl<D: Data, R: Runtime> Storage for SupportedStorage<D, R> {
  type Error = RaftStorageError<
    SupportedLogStorage<D, R>,
    SupportedStableStorage<D, R>,
    SupportedSnapshotStorage<R>,
  >;
  type Id = NodeId;
  type Address = NodeAddress;
  type Data = D;
  type Stable = SupportedStableStorage<D, R>;
  type Snapshot = SupportedSnapshotStorage<R>;
  type Log = SupportedLogStorage<D, R>;
  type Runtime = R;

  fn stable_store(&self) -> &Self::Stable {
    self.0.stable_store()
  }

  fn log_store(&self) -> &Self::Log {
    self.0.log_store()
  }

  fn snapshot_store(&self) -> &Self::Snapshot {
    self.0.snapshot_store()
  }
}
