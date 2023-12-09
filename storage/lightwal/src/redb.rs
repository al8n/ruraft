use std::{
  borrow::Cow,
  io,
  path::{Path, PathBuf},
  sync::Arc,
};

use ::redb::{
  Builder, CommitError, Database, DatabaseError, Error as DbError, ReadableTable, RepairSession,
  StorageError, TableDefinition, TableError, TransactionError,
};
use agnostic::Runtime;
use ruraft_core::{
  storage::{Log, LogStorage, LogTransformError, StableStorage},
  transport::{Address, Id, Transformable},
  Node,
};

use super::*;

const CANDIDATE_TABLE_DEFINITION: TableDefinition<'static, &'static str, Vec<u8>> =
  TableDefinition::new("__ruraft_redb_candidate__");
const TERM_TABLE_DEFINITION: TableDefinition<'static, &'static str, u64> =
  TableDefinition::new("__ruraft_redb_term__");
const LOG_TABLE_DEFINITION: TableDefinition<'static, u64, Vec<u8>> =
  TableDefinition::new("__ruraft_redb_log__");

/// Error kind.
pub enum ErrorKind<I: Transformable, A: Transformable, D: Transformable> {
  /// [redb](::redb)'s [`Error`](::redb::Error).
  Db(DbError),
  /// [redb](::redb)'s [`DatabaseError`]
  Database(DatabaseError),
  /// [redb](::redb)'s [`TransactionError`]
  Transaction(TransactionError),
  /// [redb](::redb)'s [`TableError`]
  Table(TableError),
  /// [redb](::redb)'s [`StorageError`](::redb::StorageError)
  Storage(StorageError),
  /// [redb](::redb)'s [`CommitError`]
  Commit(CommitError),
  /// Id transform error.
  Id(I::Error),
  /// Address transform error.
  Address(A::Error),
  /// Log transform error.
  Log(LogTransformError<I, A, D>),
  /// Corrupted database.
  Corrupted(Cow<'static, str>),
}

impl<I, A, D> From<DatabaseError> for ErrorKind<I, A, D>
where
  I: Transformable,
  A: Transformable,
  D: Transformable,
{
  fn from(value: DatabaseError) -> Self {
    Self::Database(value)
  }
}

impl<I, A, D> From<TransactionError> for ErrorKind<I, A, D>
where
  I: Transformable,
  A: Transformable,
  D: Transformable,
{
  fn from(value: TransactionError) -> Self {
    Self::Transaction(value)
  }
}

impl<I, A, D> From<DbError> for ErrorKind<I, A, D>
where
  I: Transformable,
  A: Transformable,
  D: Transformable,
{
  fn from(value: DbError) -> Self {
    Self::Db(value)
  }
}

impl<I, A, D> From<TableError> for ErrorKind<I, A, D>
where
  I: Transformable,
  A: Transformable,
  D: Transformable,
{
  fn from(value: TableError) -> Self {
    Self::Table(value)
  }
}

impl<I, A, D> From<StorageError> for ErrorKind<I, A, D>
where
  I: Transformable,
  A: Transformable,
  D: Transformable,
{
  fn from(value: StorageError) -> Self {
    Self::Storage(value)
  }
}

impl<I, A, D> From<CommitError> for ErrorKind<I, A, D>
where
  I: Transformable,
  A: Transformable,
  D: Transformable,
{
  fn from(value: CommitError) -> Self {
    Self::Commit(value)
  }
}

/// Error type for [`Db`].
pub struct Error<I, A, D>(Arc<ErrorKind<I, A, D>>)
where
  I: Transformable,
  A: Transformable,
  D: Transformable;

impl<I, A, D> Clone for Error<I, A, D>
where
  I: Transformable,
  A: Transformable,
  D: Transformable,
{
  fn clone(&self) -> Self {
    Self(self.0.clone())
  }
}

impl<I, A, D> From<ErrorKind<I, A, D>> for Error<I, A, D>
where
  I: Transformable,
  A: Transformable,
  D: Transformable,
{
  fn from(value: ErrorKind<I, A, D>) -> Self {
    Self(Arc::new(value))
  }
}

impl<I, A, D> From<io::Error> for Error<I, A, D>
where
  I: Transformable,
  A: Transformable,
  D: Transformable,
{
  fn from(value: io::Error) -> Self {
    Self(Arc::new(ErrorKind::Db(DbError::Io(value))))
  }
}

impl<I, A, D> core::fmt::Debug for Error<I, A, D>
where
  I: Transformable,
  A: Transformable,
  D: Transformable,
{
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self.0.as_ref() {
      ErrorKind::Db(e) => write!(f, "{:?}", e),
      ErrorKind::Transaction(e) => write!(f, "{:?}", e),
      ErrorKind::Table(e) => write!(f, "{:?}", e),
      ErrorKind::Storage(e) => write!(f, "{:?}", e),
      ErrorKind::Commit(e) => write!(f, "{:?}", e),
      ErrorKind::Id(e) => write!(f, "{:?}", e),
      ErrorKind::Address(e) => write!(f, "{:?}", e),
      ErrorKind::Log(e) => write!(f, "{:?}", e),
      ErrorKind::Corrupted(e) => write!(f, "{:?}", e),
      ErrorKind::Database(e) => write!(f, "{:?}", e),
    }
  }
}

impl<I, A, D> core::fmt::Display for Error<I, A, D>
where
  I: Transformable,
  A: Transformable,
  D: Transformable,
{
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self.0.as_ref() {
      ErrorKind::Db(e) => write!(f, "{e}"),
      ErrorKind::Transaction(e) => write!(f, "{e}"),
      ErrorKind::Table(e) => write!(f, "{e}"),
      ErrorKind::Storage(e) => write!(f, "{e}"),
      ErrorKind::Commit(e) => write!(f, "{e}"),
      ErrorKind::Id(e) => write!(f, "{e}"),
      ErrorKind::Address(e) => write!(f, "{e}"),
      ErrorKind::Log(e) => write!(f, "{e}"),
      ErrorKind::Corrupted(e) => write!(f, "{e}"),
      ErrorKind::Database(e) => write!(f, "{e}"),
    }
  }
}

impl<I, A, D> std::error::Error for Error<I, A, D>
where
  I: Transformable,
  A: Transformable,
  D: Transformable,
{
}

impl<I, A, D> Error<I, A, D>
where
  I: Transformable,
  A: Transformable,
  D: Transformable,
{
  /// Returns the error kind.
  pub fn kind(&self) -> &ErrorKind<I, A, D> {
    self.0.as_ref()
  }
}

/// Options used to create [`Db`].
pub struct DbOptions {
  cache_size: usize,
  path: PathBuf,
  repair_callback: Box<dyn Fn(&mut RepairSession)>,
}

impl DbOptions {
  /// Creates a new options.
  pub fn new<P: AsRef<Path>>(path: P) -> Self {
    Self {
      cache_size: 1024 * 1024 * 1024,
      path: path.as_ref().to_path_buf(),
      repair_callback: Box::new(|_| {}),
    }
  }

  /// Sets the cache size.
  pub fn set_cache_size(mut self, cache_size: usize) -> Self {
    self.cache_size = cache_size;
    self
  }

  /// Sets the repair callback.
  pub fn set_repair_callback(
    mut self,
    repair_callback: impl Fn(&mut RepairSession) + 'static,
  ) -> Self {
    self.repair_callback = Box::new(repair_callback);
    self
  }
}

/// [`StableStorage`] and [`LogStorage`] implementor backed by [`redb`](::redb).
pub struct Db<I, A, D, R> {
  db: Database,
  _marker: std::marker::PhantomData<(I, A, D, R)>,
}

impl<I, A, D, R> Db<I, A, D, R>
where
  I: Transformable,
  A: Transformable,
  D: Transformable,
{
  /// Creates a new [`Db`].
  pub fn new(opts: DbOptions) -> Result<Self, Error<I, A, D>> {
    Builder::new()
      .set_cache_size(opts.cache_size)
      .set_repair_callback(opts.repair_callback)
      .create(opts.path)
      .map(|db| Self {
        db,
        _marker: std::marker::PhantomData,
      })
      .map_err(|e| ErrorKind::from(e).into())
  }
}

impl<I, A, D, R> Db<I, A, D, R>
where
  I: Id + Send + Sync + 'static,
  A: Address + Send + Sync + 'static,
  D: Transformable + Send + Sync + 'static,
  R: Runtime,
{
  fn store_many(
    &self,
    _num_logs: usize,
    mut logs: impl Iterator<Item = (u64, Vec<u8>)>,
  ) -> Result<(), Error<I, A, D>> {
    #[cfg(feature = "metrics")]
    let start = std::time::Instant::now();

    self
      .db
      .begin_write()
      .map_err(ErrorKind::from)
      .and_then(|w| {
        w.open_table(LOG_TABLE_DEFINITION)
          .map_err(ErrorKind::from)
          .and_then(|mut t| {
            #[cfg(feature = "metrics")]
            let mut batch_size = 0;

            logs
              .try_for_each(|(idx, blob)| {
                #[cfg(feature = "metrics")]
                let blob_size = blob.len();

                #[cfg(feature = "metrics")]
                {
                  batch_size += blob_size;
                }

                t.insert(idx, blob).map(|_| {
                  #[cfg(feature = "metrics")]
                  {
                    metrics::histogram!("ruraft.lightwal.log_size", blob_size as f64);
                  }
                })
              })
              .map_err(ErrorKind::from)?;
            #[cfg(feature = "metrics")]
            {
              metrics::histogram!("ruraft.lightwal.log_batch_size", batch_size as f64);
              metrics::histogram!("ruraft.lightwal.logs_per_batch", _num_logs as f64);
            }
            Ok(())
          })
          .and_then(|_| {
            #[cfg(feature = "metrics")]
            scopeguard::defer!(super::report_store_many(_num_logs, start));
            w.commit().map_err(ErrorKind::from)
          })
      })
      .map_err(Into::into)
  }
}

impl<I, A, D, R> StableStorage for Db<I, A, D, R>
where
  I: Id + Send + Sync + 'static,
  I::Error: Send + Sync + 'static,
  A: Address + Send + Sync + 'static,
  A::Error: Send + Sync + 'static,
  D: Transformable + Send + Sync + 'static,
  D::Error: Send + Sync + 'static,
  R: Runtime,
{
  type Error = Error<I, A, D>;

  type Runtime = R;

  type Id = I;

  type Address = A;

  async fn current_term(&self) -> Result<Option<u64>, Self::Error> {
    let r = self.db.begin_read().map_err(ErrorKind::from)?;
    let t = r
      .open_table(TERM_TABLE_DEFINITION)
      .map_err(ErrorKind::from)?;
    let term = t.get(CURRENT_TERM).map_err(ErrorKind::from)?;
    Ok(term.map(|t| t.value()))
  }

  async fn store_current_term(&self, term: u64) -> Result<(), Self::Error> {
    let w = self.db.begin_write().map_err(ErrorKind::from)?;
    {
      let mut t = w
        .open_table(TERM_TABLE_DEFINITION)
        .map_err(ErrorKind::from)?;
      t.insert(CURRENT_TERM, term).map_err(ErrorKind::from)?;
    }
    Ok(())
  }

  async fn last_vote_term(&self) -> Result<Option<u64>, Self::Error> {
    let r = self.db.begin_read().map_err(ErrorKind::from)?;
    let t = r
      .open_table(TERM_TABLE_DEFINITION)
      .map_err(ErrorKind::from)?;
    let term = t.get(LAST_VOTE_TERM).map_err(ErrorKind::from)?;
    Ok(term.map(|t| t.value()))
  }

  async fn store_last_vote_term(&self, term: u64) -> Result<(), Self::Error> {
    let w = self.db.begin_write().map_err(ErrorKind::from)?;
    {
      let mut t = w
        .open_table(TERM_TABLE_DEFINITION)
        .map_err(ErrorKind::from)?;
      t.insert(LAST_VOTE_TERM, term).map_err(ErrorKind::from)?;
    }
    Ok(())
  }

  async fn last_vote_candidate(
    &self,
  ) -> Result<Option<Node<Self::Id, Self::Address>>, Self::Error> {
    let r = self.db.begin_read().map_err(ErrorKind::from)?;
    {
      let t = r
        .open_table(CANDIDATE_TABLE_DEFINITION)
        .map_err(ErrorKind::from)?;
      let id = t.get(LAST_CANIDATE_ID).map_err(ErrorKind::from)?;
      let addr = t.get(LAST_CANIDATE_ADDR).map_err(ErrorKind::from)?;

      // Rust does not smart enough to infer the lifetime if we
      // remove the return keyword.
      #[allow(clippy::needless_return)]
      return match (id, addr) {
        (None, None) => Ok(None),
        (None, Some(_)) => Err(ErrorKind::Corrupted(
          "last vote candidate corrupted: missing node id".into(),
        ))?,
        (Some(_), None) => Err(ErrorKind::Corrupted(
          "last vote candidate corrupted: missing node addr".into(),
        ))?,
        (Some(id), Some(addr)) => {
          let (_, id) = I::decode(id.value().as_slice()).map_err(ErrorKind::Id)?;
          let (_, addr) = A::decode(addr.value().as_slice()).map_err(ErrorKind::Address)?;
          Ok(Some(Node::new(id, addr)))
        }
      };
    }
  }

  async fn store_last_vote_candidate(
    &self,
    candidate: Node<Self::Id, Self::Address>,
  ) -> Result<(), Self::Error> {
    let id = candidate.id();
    let mut id_buf = vec![0; id.encoded_len()];
    id.encode(&mut id_buf).map_err(ErrorKind::Id)?;
    let addr = candidate.addr();
    let mut addr_buf = vec![0; addr.encoded_len()];
    addr.encode(&mut addr_buf).map_err(ErrorKind::Address)?;

    let w = self.db.begin_write().map_err(ErrorKind::from)?;
    {
      let mut t = w
        .open_table(CANDIDATE_TABLE_DEFINITION)
        .map_err(ErrorKind::from)?;
      t.insert(LAST_CANIDATE_ID, id_buf)
        .map_err(ErrorKind::from)?;
      t.insert(LAST_CANIDATE_ADDR, addr_buf)
        .map_err(ErrorKind::from)?;
    }

    w.commit().map_err(ErrorKind::from)?;
    Ok(())
  }
}

impl<I, A, D, R> LogStorage for Db<I, A, D, R>
where
  I: Id + Send + Sync + 'static,
  I::Error: Send + Sync + 'static,
  A: Address + Send + Sync + 'static,
  A::Error: Send + Sync + 'static,
  D: Transformable + Send + Sync + 'static,
  D::Error: Send + Sync + 'static,
  R: Runtime,
{
  type Error = Error<I, A, D>;

  type Runtime = R;

  type Id = I;

  type Address = A;

  type Data = D;

  async fn first_index(&self) -> Result<Option<u64>, Self::Error> {
    let r = self.db.begin_read().map_err(ErrorKind::from)?;
    let t = r
      .open_table(LOG_TABLE_DEFINITION)
      .map_err(ErrorKind::from)?;
    t.first()
      .map(|k| k.map(|(k, _)| k.value()))
      .map_err(|e| ErrorKind::from(e).into())
  }

  async fn last_index(&self) -> Result<Option<u64>, Self::Error> {
    let r = self.db.begin_read().map_err(ErrorKind::from)?;
    let t = r
      .open_table(LOG_TABLE_DEFINITION)
      .map_err(ErrorKind::from)?;
    t.last()
      .map(|k| k.map(|(k, _)| k.value()))
      .map_err(|e| ErrorKind::from(e).into())
  }

  async fn get_log(
    &self,
    index: u64,
  ) -> Result<Option<Log<Self::Id, Self::Address, Self::Data>>, Self::Error> {
    let r = self.db.begin_read().map_err(ErrorKind::from)?;
    let t = r
      .open_table(LOG_TABLE_DEFINITION)
      .map_err(ErrorKind::from)?;
    let log = t.get(index).map_err(ErrorKind::from)?;
    log
      .map(|l| {
        let l = l.value();
        Log::decode(l.as_slice())
          .map(|(_, l)| l)
          .map_err(|e| ErrorKind::Log(e).into())
      })
      .transpose()
  }

  async fn store_log(
    &self,
    log: &Log<Self::Id, Self::Address, Self::Data>,
  ) -> Result<(), Self::Error> {
    let idx = log.index();
    let mut buf = vec![0; log.encoded_len()];
    log.encode(&mut buf).map_err(ErrorKind::Log)?;
    self.store_many(1, std::iter::once((idx, buf)))
  }

  async fn store_logs(
    &self,
    logs: &[Log<Self::Id, Self::Address, Self::Data>],
  ) -> Result<(), Self::Error> {
    let num_logs = logs.len();
    logs
      .iter()
      .map(|l| {
        let idx = l.index();
        let mut buf = vec![0; l.encoded_len()];
        l.encode(&mut buf).map(|_| (idx, buf))
      })
      .collect::<Result<Vec<_>, _>>()
      .map_err(|e| ErrorKind::Log(e).into())
      .and_then(|logs| self.store_many(num_logs, logs.into_iter()))
  }

  async fn remove_range(
    &self,
    range: impl std::ops::RangeBounds<u64> + Send,
  ) -> Result<(), Self::Error> {
    self
      .db
      .begin_write()
      .map_err(ErrorKind::from)
      .and_then(|w| {
        w.open_table(LOG_TABLE_DEFINITION)
          .map_err(ErrorKind::from)
          .and_then(|mut t| t.drain(range).map(|_| ()).map_err(ErrorKind::from))
          .and_then(|_| w.commit().map_err(ErrorKind::from))
      })
      .map_err(Into::into)
  }
}
