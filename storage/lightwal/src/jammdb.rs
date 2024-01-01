use std::{borrow::Cow, io, ops::Bound, path::Path, sync::Arc};

use ::jammdb::{Error as DbError, DB};
use agnostic::Runtime;
use ruraft_core::{
  storage::{Log, LogStorage, LogTransformError, StableStorage},
  transport::{Address, Id, Transformable},
  Node,
};

use super::*;

pub use ::jammdb::OpenOptions as DbOptions;

const STABLE_BUCKET_NAME: &str = "__ruraft_stable__";
const LOG_BUCKET_NAME: &str = "__ruraft_log__";

/// Error kind.
pub enum ErrorKind<I: Transformable, A: Transformable, D: Transformable> {
  /// [`jammdb`](::jammdb)'s [`Error`](::jammdb::Error).
  Db(DbError),
  /// Id transform error.
  Id(I::Error),
  /// Address transform error.
  Address(A::Error),
  /// Log transform error.
  Log(LogTransformError<I, A, D>),
  /// `u64` transform error.
  U64(<u64 as Transformable>::Error),
  /// Corrupted database.
  Corrupted(Cow<'static, str>),
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
  I: Id,
  A: Address,
  D: Transformable,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self.0.as_ref() {
      ErrorKind::Db(e) => write!(f, "Db({:?})", e),
      ErrorKind::Id(e) => write!(f, "Id({:?})", e),
      ErrorKind::Address(e) => write!(f, "Address({:?})", e),
      ErrorKind::Log(e) => write!(f, "Log({:?})", e),
      ErrorKind::U64(e) => write!(f, "U64({:?})", e),
      ErrorKind::Corrupted(e) => write!(f, "Corrupted({:?})", e),
    }
  }
}

impl<I, A, D> core::fmt::Display for Error<I, A, D>
where
  I: Id,
  A: Address,
  D: Transformable,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self.0.as_ref() {
      ErrorKind::Db(e) => write!(f, "{e}"),
      ErrorKind::Id(e) => write!(f, "{e}"),
      ErrorKind::Address(e) => write!(f, "{e}"),
      ErrorKind::Log(e) => write!(f, "{e}"),
      ErrorKind::U64(e) => write!(f, "{e}"),
      ErrorKind::Corrupted(e) => write!(f, "{e}"),
    }
  }
}

impl<I, A, D> std::error::Error for Error<I, A, D>
where
  I: Id,
  A: Address,
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

/// [`StableStorage`] and [`LogStorage`] implementor backed by [`jammdb`](::jammdb).
pub struct Db<I, A, D, R> {
  db: DB,
  _marker: std::marker::PhantomData<(I, A, D, R)>,
}

impl<I, A, D, R> Db<I, A, D, R>
where
  I: Transformable,
  A: Transformable,
  D: Transformable,
{
  /// Creates a new [`Db`].
  pub fn new<P: AsRef<Path>>(path: P, opts: DbOptions) -> Result<Self, Error<I, A, D>> {
    let db = opts.open(path).map_err(ErrorKind::from)?;
    let tx = db.tx(true).map_err(ErrorKind::from)?;

    tx.get_or_create_bucket(STABLE_BUCKET_NAME)
      .map_err(ErrorKind::from)?;
    tx.get_or_create_bucket(LOG_BUCKET_NAME)
      .map_err(ErrorKind::from)?;
    tx.commit()
      .map(|_| Self {
        db,
        _marker: std::marker::PhantomData,
      })
      .map_err(|e| ErrorKind::from(e).into())
  }
}

impl<I, A, D, R> Db<I, A, D, R>
where
  I: Id,
  A: Address,
  D: Transformable,
  R: Runtime,
{
  fn store_many(
    &self,
    _num_logs: usize,
    mut logs: impl Iterator<Item = (u64, Vec<u8>)>,
  ) -> Result<(), Error<I, A, D>> {
    #[cfg(feature = "metrics")]
    let start = std::time::Instant::now();

    let txn = self.db.tx(true).map_err(ErrorKind::from)?;
    let b = txn.get_bucket(LOG_BUCKET_NAME).map_err(ErrorKind::from)?;

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

        b.put(idx.to_be_bytes(), blob).map(|_| {
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

    #[cfg(feature = "metrics")]
    scopeguard::defer!(super::report_store_many(_num_logs, start));

    txn.commit().map_err(|e| ErrorKind::from(e).into())
  }
}

impl<I, A, D, R> StableStorage for Db<I, A, D, R>
where
  I: Id,
  A: Address,
  D: Transformable,
  R: Runtime,
{
  type Error = Error<I, A, D>;

  type Runtime = R;

  type Id = I;

  type Address = A;

  async fn current_term(&self) -> Result<Option<u64>, Self::Error> {
    let txn = self.db.tx(false).map_err(ErrorKind::from)?;
    let b = txn
      .get_bucket(STABLE_BUCKET_NAME)
      .map_err(ErrorKind::from)?;

    match b.get(CURRENT_TERM) {
      Some(d) => {
        let buf = d.kv().value();
        u64::decode(buf)
          .map(|(_, term)| Some(term))
          .map_err(|e| ErrorKind::U64(e).into())
      }
      None => Ok(None),
    }
  }

  async fn store_current_term(&self, term: u64) -> Result<(), Self::Error> {
    let txn = self.db.tx(true).map_err(ErrorKind::from)?;
    let b = txn
      .get_bucket(STABLE_BUCKET_NAME)
      .map_err(ErrorKind::from)?;
    b.put(CURRENT_TERM, term.to_be_bytes())
      .map_err(ErrorKind::from)?;
    txn.commit().map_err(|e| ErrorKind::from(e).into())
  }

  async fn last_vote_term(&self) -> Result<Option<u64>, Self::Error> {
    let txn = self.db.tx(false).map_err(ErrorKind::from)?;
    let b = txn
      .get_bucket(STABLE_BUCKET_NAME)
      .map_err(ErrorKind::from)?;

    match b.get(LAST_VOTE_TERM) {
      Some(d) => {
        let buf = d.kv().value();
        u64::decode(buf)
          .map(|(_, term)| Some(term))
          .map_err(|e| ErrorKind::U64(e).into())
      }
      None => Ok(None),
    }
  }

  async fn store_last_vote_term(&self, term: u64) -> Result<(), Self::Error> {
    let txn = self.db.tx(true).map_err(ErrorKind::from)?;
    let b = txn
      .get_bucket(STABLE_BUCKET_NAME)
      .map_err(ErrorKind::from)?;
    b.put(LAST_VOTE_TERM, term.to_be_bytes())
      .map_err(ErrorKind::from)?;
    txn.commit().map_err(|e| ErrorKind::from(e).into())
  }

  async fn last_vote_candidate(
    &self,
  ) -> Result<Option<Node<Self::Id, Self::Address>>, Self::Error> {
    let txn = self.db.tx(false).map_err(ErrorKind::from)?;
    let b = txn
      .get_bucket(STABLE_BUCKET_NAME)
      .map_err(ErrorKind::from)?;
    {
      let id = b.get(LAST_CANIDATE_ID);
      let addr = b.get(LAST_CANIDATE_ADDR);

      match (id, addr) {
        (None, None) => Ok(None),
        (None, Some(_)) => Err(ErrorKind::Corrupted(
          "last vote candidate corrupted: missing node id".into(),
        ))?,
        (Some(_), None) => Err(ErrorKind::Corrupted(
          "last vote candidate corrupted: missing node addr".into(),
        ))?,
        (Some(id), Some(addr)) => {
          let (_, id) = I::decode(id.kv().value()).map_err(ErrorKind::Id)?;
          let (_, addr) = A::decode(addr.kv().value()).map_err(ErrorKind::Address)?;
          Ok(Some(Node::new(id, addr)))
        }
      }
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

    let w = self.db.tx(true).map_err(ErrorKind::from)?;
    let b = w.get_bucket(STABLE_BUCKET_NAME).map_err(ErrorKind::from)?;
    b.put(LAST_CANIDATE_ID, id_buf).map_err(ErrorKind::from)?;
    b.put(LAST_CANIDATE_ADDR, addr_buf)
      .map_err(ErrorKind::from)?;

    w.commit().map_err(|e| ErrorKind::from(e).into())
  }
}

impl<I, A, D, R> LogStorage for Db<I, A, D, R>
where
  I: Id,
  A: Address,
  D: Transformable,
  R: Runtime,
{
  type Error = Error<I, A, D>;

  type Runtime = R;

  type Id = I;

  type Address = A;

  type Data = D;

  async fn first_index(&self) -> Result<Option<u64>, Self::Error> {
    let txn = self.db.tx(false).map_err(ErrorKind::from)?;
    let b = txn.get_bucket(LOG_BUCKET_NAME).map_err(ErrorKind::from)?;
    b.cursor()
      .next()
      .map(|d| {
        u64::decode(d.key())
          .map(|(_, k)| k)
          .map_err(|e| ErrorKind::U64(e).into())
      })
      .transpose()
  }

  async fn last_index(&self) -> Result<Option<u64>, Self::Error> {
    let txn = self.db.tx(false).map_err(ErrorKind::from)?;
    let b = txn.get_bucket(LOG_BUCKET_NAME).map_err(ErrorKind::from)?;
    let mut cur = b.cursor();
    cur.seek(u64::MAX.to_be_bytes().as_slice());
    cur
      .last()
      .map(|d| {
        u64::decode(d.key())
          .map(|(_, k)| k)
          .map_err(|e| ErrorKind::U64(e).into())
      })
      .transpose()
  }

  async fn get_log(
    &self,
    index: u64,
  ) -> Result<Option<Log<Self::Id, Self::Address, Self::Data>>, Self::Error> {
    let r = self.db.tx(false).map_err(ErrorKind::from)?;
    let b = r.get_bucket(LOG_BUCKET_NAME).map_err(ErrorKind::from)?;
    let log = b.get(index.to_be_bytes().as_slice());
    log
      .map(|l| {
        let l = l.kv().value();
        Log::decode(l)
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
    let txn = self.db.tx(true).map_err(ErrorKind::from)?;
    let b = txn.get_bucket(LOG_BUCKET_NAME).map_err(ErrorKind::from)?;
    let start = match range.start_bound() {
      Bound::Included(v) => *v,
      Bound::Excluded(v) => *v + 1,
      Bound::Unbounded => 0u64,
    };

    let end = match range.end_bound() {
      Bound::Included(v) => *v,
      Bound::Excluded(v) => {
        if *v == 0 {
          0u64
        } else {
          *v - 1
        }
      }
      Bound::Unbounded => u64::MAX,
    };

    for idx in start..=end {
      if let Err(e) = b.delete(idx.to_be_bytes().as_slice()) {
        if let DbError::KeyValueMissing = e {
          continue;
        } else {
          return Err(ErrorKind::from(e).into());
        }
      }
    }

    txn.commit().map_err(|e| ErrorKind::from(e).into())
  }
}

/// Exports unit tests to let users test [`jammdb::Db`] implementation if they want to
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
    db: Db<SmolStr, SocketAddr, Vec<u8>, R>,
  }

  impl<R: Runtime> Deref for TestDb<R> {
    type Target = Db<SmolStr, SocketAddr, Vec<u8>, R>;

    fn deref(&self) -> &Self::Target {
      &self.db
    }
  }

  fn test_db<R: Runtime>() -> TestDb<R> {
    use tempfile::tempdir;
    let dir = tempdir().unwrap();
    TestDb {
      db: Db::new(dir.path().join("test"), DbOptions::new()).unwrap(),
      _dir: dir,
    }
  }

  /// [`jammdb::Db`] test
  ///
  /// Description:
  ///
  /// Test get first index
  pub async fn first_index<R: Runtime>() {
    test::first_index(test_db::<R>().deref()).await;
  }

  /// [`jammdb::Db`] test
  ///
  /// Description:
  ///
  /// Test get last index
  pub async fn last_index<R: Runtime>() {
    test::last_index(test_db::<R>().deref()).await;
  }

  /// [`jammdb::Db`] test
  ///
  /// Description:
  ///
  /// Test get log
  pub async fn get_log<R: Runtime>() {
    test::get_log(test_db::<R>().deref()).await;
  }

  /// [`jammdb::Db`] test
  ///
  /// Description:
  ///
  /// Test store log
  pub async fn store_log<R: Runtime>() {
    test::store_log(test_db::<R>().deref()).await;
  }

  /// [`jammdb::Db`] test
  ///
  /// Description:
  ///
  /// Test store logs
  pub async fn store_logs<R: Runtime>() {
    test::store_logs(test_db::<R>().deref()).await;
  }

  /// [`jammdb::Db`] test
  ///
  /// Description:
  ///
  /// Test remove logs by range
  pub async fn remove_range<R: Runtime>() {
    test::remove_range(test_db::<R>().deref()).await;
  }

  /// [`jammdb::Db`] test
  ///
  /// Description:
  ///
  /// Test current term
  pub async fn current_term<R: Runtime>() {
    test::current_term(test_db::<R>().deref()).await;
  }

  /// [`jammdb::Db`] test
  ///
  /// Description:
  ///
  /// Test last vote term
  pub async fn last_vote_term<R: Runtime>() {
    test::last_vote_term(test_db::<R>().deref()).await;
  }

  /// [`jammdb::Db`] test
  ///
  /// Description:
  ///
  /// Test last vote candidate
  pub async fn last_vote_candidate<R: Runtime>() {
    test::last_vote_candidate(test_db::<R>().deref()).await;
  }

  /// [`jammdb::Db`] test
  ///
  /// Description:
  ///
  /// Test oldest log
  #[cfg(all(feature = "test", feature = "metrics"))]
  pub async fn oldest_log<R: Runtime>() {
    test::oldest_log(test_db::<R>().deref()).await;
  }
}
