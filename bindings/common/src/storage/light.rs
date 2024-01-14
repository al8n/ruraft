use std::ops::RangeBounds;

use ruraft_core::storage::{Log, LogStorage};

use super::*;

mod options;
pub use options::*;

#[derive(derive_more::From, derive_more::Display)]
pub enum SupportedLightBackendError<D: Transformable> {
  #[cfg(feature = "sled")]
  Sled(ruraft_lightwal::sled::Error<NodeId, NodeAddress, D>),
  #[cfg(feature = "redb")]
  Redb(ruraft_lightwal::redb::Error<NodeId, NodeAddress, D>),
  #[cfg(feature = "jammdb")]
  Jammdb(ruraft_lightwal::jammdb::Error<NodeId, NodeAddress, D>),
}

impl<D: Transformable> Clone for SupportedLightBackendError<D> {
  fn clone(&self) -> Self {
    match self {
      #[cfg(feature = "sled")]
      Self::Sled(db) => Self::Sled(db.clone()),
      #[cfg(feature = "redb")]
      Self::Redb(db) => Self::Redb(db.clone()),
      #[cfg(feature = "jammdb")]
      Self::Jammdb(db) => Self::Jammdb(db.clone()),
    }
  }
}

impl<D: Transformable> fmt::Debug for SupportedLightBackendError<D> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      #[cfg(feature = "sled")]
      Self::Sled(e) => write!(f, "{e:?}"),
      #[cfg(feature = "redb")]
      Self::Redb(e) => write!(f, "{e:?}"),
      #[cfg(feature = "jammdb")]
      Self::Jammdb(e) => write!(f, "{e:?}"),
    }
  }
}

impl<D: Transformable> std::error::Error for SupportedLightBackendError<D> {}

#[derive(derive_more::From)]
pub enum SupportedLightBackend<D, R> {
  #[cfg(feature = "sled")]
  Sled(Arc<ruraft_lightwal::sled::Db<NodeId, NodeAddress, D, R>>),
  #[cfg(feature = "redb")]
  Redb(Arc<ruraft_lightwal::redb::Db<NodeId, NodeAddress, D, R>>),
  #[cfg(feature = "jammdb")]
  Jammdb(Arc<ruraft_lightwal::jammdb::Db<NodeId, NodeAddress, D, R>>),
}

impl<D, R> Clone for SupportedLightBackend<D, R> {
  fn clone(&self) -> Self {
    match self {
      #[cfg(feature = "sled")]
      Self::Sled(db) => Self::Sled(db.clone()),
      #[cfg(feature = "redb")]
      Self::Redb(db) => Self::Redb(db.clone()),
      #[cfg(feature = "jammdb")]
      Self::Jammdb(db) => Self::Jammdb(db.clone()),
    }
  }
}

impl<D: Data, R: Runtime> StableStorage for SupportedLightBackend<D, R> {
  type Error = SupportedLightBackendError<D>;

  type Runtime = R;

  type Id = NodeId;

  type Address = NodeAddress;

  async fn current_term(&self) -> Result<Option<u64>, Self::Error> {
    match self {
      #[cfg(feature = "sled")]
      Self::Sled(db) => db.current_term().await.map_err(From::from),
      #[cfg(feature = "redb")]
      Self::Redb(db) => db.current_term().await.map_err(From::from),
      #[cfg(feature = "jammdb")]
      Self::Jammdb(db) => db.current_term().await.map_err(From::from),
    }
  }

  async fn store_current_term(&self, term: u64) -> Result<(), Self::Error> {
    match self {
      #[cfg(feature = "sled")]
      Self::Sled(db) => db.store_current_term(term).await.map_err(From::from),
      #[cfg(feature = "redb")]
      Self::Redb(db) => db.store_current_term(term).await.map_err(From::from),
      #[cfg(feature = "jammdb")]
      Self::Jammdb(db) => db.store_current_term(term).await.map_err(From::from),
    }
  }

  async fn last_vote_term(&self) -> Result<Option<u64>, Self::Error> {
    match self {
      #[cfg(feature = "sled")]
      Self::Sled(db) => db.last_vote_term().await.map_err(From::from),
      #[cfg(feature = "redb")]
      Self::Redb(db) => db.last_vote_term().await.map_err(From::from),
      #[cfg(feature = "jammdb")]
      Self::Jammdb(db) => db.last_vote_term().await.map_err(From::from),
    }
  }

  async fn store_last_vote_term(&self, term: u64) -> Result<(), Self::Error> {
    match self {
      #[cfg(feature = "sled")]
      Self::Sled(db) => db.store_last_vote_term(term).await.map_err(From::from),
      #[cfg(feature = "redb")]
      Self::Redb(db) => db.store_last_vote_term(term).await.map_err(From::from),
      #[cfg(feature = "jammdb")]
      Self::Jammdb(db) => db.store_last_vote_term(term).await.map_err(From::from),
    }
  }

  async fn last_vote_candidate(
    &self,
  ) -> Result<Option<ruraft_core::Node<Self::Id, Self::Address>>, Self::Error> {
    match self {
      #[cfg(feature = "sled")]
      Self::Sled(db) => db.last_vote_candidate().await.map_err(From::from),
      #[cfg(feature = "redb")]
      Self::Redb(db) => db.last_vote_candidate().await.map_err(From::from),
      #[cfg(feature = "jammdb")]
      Self::Jammdb(db) => db.last_vote_candidate().await.map_err(From::from),
    }
  }

  async fn store_last_vote_candidate(
    &self,
    candidate: ruraft_core::Node<Self::Id, Self::Address>,
  ) -> Result<(), Self::Error> {
    match self {
      #[cfg(feature = "sled")]
      Self::Sled(db) => db
        .store_last_vote_candidate(candidate)
        .await
        .map_err(From::from),
      #[cfg(feature = "redb")]
      Self::Redb(db) => db
        .store_last_vote_candidate(candidate)
        .await
        .map_err(From::from),
      #[cfg(feature = "jammdb")]
      Self::Jammdb(db) => db
        .store_last_vote_candidate(candidate)
        .await
        .map_err(From::from),
    }
  }
}

impl<D: Data, R: Runtime> LogStorage for SupportedLightBackend<D, R> {
  type Error = SupportedLightBackendError<D>;
  type Runtime = R;
  type Id = NodeId;
  type Address = NodeAddress;
  type Data = D;

  async fn first_index(&self) -> Result<Option<u64>, Self::Error> {
    match self {
      #[cfg(feature = "sled")]
      Self::Sled(db) => db.first_index().await.map_err(From::from),
      #[cfg(feature = "redb")]
      Self::Redb(db) => db.first_index().await.map_err(From::from),
      #[cfg(feature = "jammdb")]
      Self::Jammdb(db) => db.first_index().await.map_err(From::from),
    }
  }

  async fn last_index(&self) -> Result<Option<u64>, Self::Error> {
    match self {
      #[cfg(feature = "sled")]
      Self::Sled(db) => db.last_index().await.map_err(From::from),
      #[cfg(feature = "redb")]
      Self::Redb(db) => db.last_index().await.map_err(From::from),
      #[cfg(feature = "jammdb")]
      Self::Jammdb(db) => db.last_index().await.map_err(From::from),
    }
  }

  async fn get_log(
    &self,
    index: u64,
  ) -> Result<Option<Log<Self::Id, Self::Address, Self::Data>>, Self::Error> {
    match self {
      #[cfg(feature = "sled")]
      Self::Sled(db) => db.get_log(index).await.map_err(From::from),
      #[cfg(feature = "redb")]
      Self::Redb(db) => db.get_log(index).await.map_err(From::from),
      #[cfg(feature = "jammdb")]
      Self::Jammdb(db) => db.get_log(index).await.map_err(From::from),
    }
  }

  async fn store_log(
    &self,
    log: &Log<Self::Id, Self::Address, Self::Data>,
  ) -> Result<(), Self::Error> {
    match self {
      #[cfg(feature = "sled")]
      Self::Sled(db) => db.store_log(log).await.map_err(From::from),
      #[cfg(feature = "redb")]
      Self::Redb(db) => db.store_log(log).await.map_err(From::from),
      #[cfg(feature = "jammdb")]
      Self::Jammdb(db) => db.store_log(log).await.map_err(From::from),
    }
  }

  async fn store_logs(
    &self,
    logs: &[Log<Self::Id, Self::Address, Self::Data>],
  ) -> Result<(), Self::Error> {
    match self {
      #[cfg(feature = "sled")]
      Self::Sled(db) => db.store_logs(logs).await.map_err(From::from),
      #[cfg(feature = "redb")]
      Self::Redb(db) => db.store_logs(logs).await.map_err(From::from),
      #[cfg(feature = "jammdb")]
      Self::Jammdb(db) => db.store_logs(logs).await.map_err(From::from),
    }
  }

  async fn remove_range(&self, range: impl RangeBounds<u64> + Send) -> Result<(), Self::Error> {
    match self {
      #[cfg(feature = "sled")]
      Self::Sled(db) => db.remove_range(range).await.map_err(From::from),
      #[cfg(feature = "redb")]
      Self::Redb(db) => db.remove_range(range).await.map_err(From::from),
      #[cfg(feature = "jammdb")]
      Self::Jammdb(db) => db.remove_range(range).await.map_err(From::from),
    }
  }
}
