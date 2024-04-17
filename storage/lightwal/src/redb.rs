use std::{
  borrow::Cow,
  io,
  path::{Path, PathBuf},
};

use ::redb::{
  Builder, CommitError, Database, DatabaseError, Error as DbError, ReadableTable, RepairSession,
  StorageError, TableDefinition, TableError, TransactionError,
};
use agnostic::Runtime;
use ruraft_core::{
  storage::{Log, LogTransformError},
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
pub enum ErrorKind<I: Transformable, A: Transformable> {
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
  Log(LogTransformError<I, A>),
  /// Corrupted database.
  Corrupted(Cow<'static, str>),
}

impl<I, A> From<DatabaseError> for ErrorKind<I, A>
where
  I: Transformable,
  A: Transformable,
{
  fn from(value: DatabaseError) -> Self {
    Self::Database(value)
  }
}

impl<I, A> From<TransactionError> for ErrorKind<I, A>
where
  I: Transformable,
  A: Transformable,
{
  fn from(value: TransactionError) -> Self {
    Self::Transaction(value)
  }
}

impl<I, A> From<DbError> for ErrorKind<I, A>
where
  I: Transformable,
  A: Transformable,
{
  fn from(value: DbError) -> Self {
    Self::Db(value)
  }
}

impl<I, A> From<TableError> for ErrorKind<I, A>
where
  I: Transformable,
  A: Transformable,
{
  fn from(value: TableError) -> Self {
    Self::Table(value)
  }
}

impl<I, A> From<StorageError> for ErrorKind<I, A>
where
  I: Transformable,
  A: Transformable,
{
  fn from(value: StorageError) -> Self {
    Self::Storage(value)
  }
}

impl<I, A> From<CommitError> for ErrorKind<I, A>
where
  I: Transformable,
  A: Transformable,
{
  fn from(value: CommitError) -> Self {
    Self::Commit(value)
  }
}

/// Error type for [`Db`].
pub struct Error<I, A>(Arc<ErrorKind<I, A>>)
where
  I: Transformable,
  A: Transformable;

impl<I, A> Clone for Error<I, A>
where
  I: Transformable,
  A: Transformable,
{
  fn clone(&self) -> Self {
    Self(self.0.clone())
  }
}

impl<I, A> From<ErrorKind<I, A>> for Error<I, A>
where
  I: Transformable,
  A: Transformable,
{
  fn from(value: ErrorKind<I, A>) -> Self {
    Self(Arc::new(value))
  }
}

impl<I, A> From<io::Error> for Error<I, A>
where
  I: Transformable,
  A: Transformable,
{
  fn from(value: io::Error) -> Self {
    Self(Arc::new(ErrorKind::Db(DbError::Io(value))))
  }
}

impl<I, A> core::fmt::Debug for Error<I, A>
where
  I: Id,
  A: Address,
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

impl<I, A> core::fmt::Display for Error<I, A>
where
  I: Id,
  A: Address,
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

impl<I, A> std::error::Error for Error<I, A>
where
  I: Id,
  A: Address,
{
}

impl<I, A> Error<I, A>
where
  I: Transformable,
  A: Transformable,
{
  /// Returns the error kind.
  pub fn kind(&self) -> &ErrorKind<I, A> {
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

  /// Returns the path of the database.
  pub fn path(&self) -> &Path {
    self.path.as_path()
  }

  /// Sets the path of the database in builder pattern.
  pub fn with_path(mut self, path: PathBuf) -> Self {
    self.path = path;
    self
  }

  /// Sets the path of the database.
  pub fn set_path(&mut self, path: PathBuf) {
    self.path = path;
  }

  /// Sets the cache size in builder pattern.
  pub fn with_cache_size(mut self, cache_size: usize) -> Self {
    self.cache_size = cache_size;
    self
  }

  /// Sets the cache size.
  pub fn set_cache_size(&mut self, cache_size: usize) {
    self.cache_size = cache_size;
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
pub struct Db<I, A, R> {
  db: Database,
  _marker: std::marker::PhantomData<(I, A, R)>,
}

impl<I, A, R> Db<I, A, R>
where
  I: Transformable,
  A: Transformable,
{
  /// Creates a new [`Db`].
  pub fn new(opts: DbOptions) -> Result<Self, Error<I, A>> {
    let this = Builder::new()
      .set_cache_size(opts.cache_size)
      .set_repair_callback(opts.repair_callback)
      .create(opts.path)
      .map(|db| Self {
        db,
        _marker: std::marker::PhantomData,
      })
      .map_err(ErrorKind::from)?;
    let t = this.db.begin_write().map_err(ErrorKind::from)?;
    t.open_table(TERM_TABLE_DEFINITION)
      .map_err(ErrorKind::from)?;
    t.open_table(CANDIDATE_TABLE_DEFINITION)
      .map_err(ErrorKind::from)?;
    t.open_table(LOG_TABLE_DEFINITION)
      .map_err(ErrorKind::from)?;
    t.commit().map_err(ErrorKind::from)?;
    Ok(this)
  }
}

impl<I, A, R> Db<I, A, R>
where
  I: Id,
  A: Address,
  R: Runtime,
{
  fn store_many(
    &self,
    _num_logs: usize,
    mut logs: impl Iterator<Item = (u64, Vec<u8>)>,
  ) -> Result<(), Error<I, A>> {
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
                    metrics::histogram!("ruraft.lightwal.log_size").record(blob_size as f64);
                  }
                })
              })
              .map_err(ErrorKind::from)?;
            #[cfg(feature = "metrics")]
            {
              metrics::histogram!("ruraft.lightwal.log_batch_size").record(batch_size as f64);
              metrics::histogram!("ruraft.lightwal.logs_per_batch").record(_num_logs as f64);
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

impl<I, A, R> StableStorage for Db<I, A, R>
where
  I: Id,
  A: Address,
  R: Runtime,
{
  type Error = Error<I, A>;

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
    w.commit().map_err(ErrorKind::from)?;
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
    w.commit().map_err(ErrorKind::from)?;
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

impl<I, A, R> LogStorage for Db<I, A, R>
where
  I: Id,
  A: Address,
  R: Runtime,
{
  type Error = Error<I, A>;

  type Runtime = R;

  type Id = I;

  type Address = A;

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

  async fn get_log(&self, index: u64) -> Result<Option<Log<Self::Id, Self::Address>>, Self::Error> {
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

  async fn store_log(&self, log: &Log<Self::Id, Self::Address>) -> Result<(), Self::Error> {
    let idx = log.index();
    let mut buf = vec![0; log.encoded_len()];
    log.encode(&mut buf).map_err(ErrorKind::Log)?;
    self.store_many(1, std::iter::once((idx, buf)))
  }

  async fn store_logs(&self, logs: &[Log<Self::Id, Self::Address>]) -> Result<(), Self::Error> {
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

/// Exports unit tests to let users test [`redb::Db`] implementation if they want to
/// use their own [`agnostic::Runtime`] implementation.
#[cfg(any(feature = "test", test))]
#[cfg_attr(docsrs, doc(cfg(any(test, feature = "test"))))]
pub mod test {
  use smol_str::SmolStr;
  use std::{net::SocketAddr, ops::Deref};

  use super::*;
  use crate::test;

  struct TestDb<R: Runtime> {
    _dir: tempfile::TempDir,
    db: Db<SmolStr, SocketAddr, R>,
  }

  impl<R: Runtime> Deref for TestDb<R> {
    type Target = Db<SmolStr, SocketAddr, R>;

    fn deref(&self) -> &Self::Target {
      &self.db
    }
  }

  fn test_db<R: Runtime>() -> TestDb<R> {
    use tempfile::tempdir;
    let dir = tempdir().unwrap();
    TestDb {
      db: Db::new(DbOptions::new(dir.path().join("test"))).unwrap(),
      _dir: dir,
    }
  }

  /// [`redb::Db`] test
  ///
  /// Description:
  ///
  /// Test get first index
  pub async fn first_index<R: Runtime>() {
    test::first_index(test_db::<R>().deref()).await;
  }

  /// [`redb::Db`] test
  ///
  /// Description:
  ///
  /// Test get last index
  pub async fn last_index<R: Runtime>() {
    test::last_index(test_db::<R>().deref()).await;
  }

  /// [`redb::Db`] test
  ///
  /// Description:
  ///
  /// Test get log
  pub async fn get_log<R: Runtime>() {
    test::get_log(test_db::<R>().deref()).await;
  }

  /// [`redb::Db`] test
  ///
  /// Description:
  ///
  /// Test store log
  pub async fn store_log<R: Runtime>() {
    test::store_log(test_db::<R>().deref()).await;
  }

  /// [`redb::Db`] test
  ///
  /// Description:
  ///
  /// Test store logs
  pub async fn store_logs<R: Runtime>() {
    test::store_logs(test_db::<R>().deref()).await;
  }

  /// [`redb::Db`] test
  ///
  /// Description:
  ///
  /// Test remove logs by range
  pub async fn remove_range<R: Runtime>() {
    test::remove_range(test_db::<R>().deref()).await;
  }

  /// [`redb::Db`] test
  ///
  /// Description:
  ///
  /// Test current term
  pub async fn current_term<R: Runtime>() {
    test::current_term(test_db::<R>().deref()).await;
  }

  /// [`redb::Db`] test
  ///
  /// Description:
  ///
  /// Test last vote term
  pub async fn last_vote_term<R: Runtime>() {
    test::last_vote_term(test_db::<R>().deref()).await;
  }

  /// [`redb::Db`] test
  ///
  /// Description:
  ///
  /// Test last vote candidate
  pub async fn last_vote_candidate<R: Runtime>() {
    test::last_vote_candidate(test_db::<R>().deref()).await;
  }

  /// [`redb::Db`] test
  ///
  /// Description:
  ///
  /// Test oldest log
  #[cfg(all(feature = "test", feature = "metrics"))]
  pub async fn oldest_log<R: Runtime>() {
    test::oldest_log(test_db::<R>().deref()).await;
  }
}
