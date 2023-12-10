use std::{borrow::Cow, io, ops::Bound, sync::Arc};

pub use ::sled::Config as DbOptions;
use ::sled::{transaction::TransactionError, Batch, Error as DbError, Tree};
use agnostic::Runtime;
use ruraft_core::{
  storage::{Log, LogStorage, LogTransformError, StableStorage},
  transport::{Address, Id, Transformable},
  Node,
};

use super::*;

const TERM_TREE_NAME: &str = "__ruraft_stable__";
const LOG_TREE_NAME: &str = "__ruraft_log__";

/// Error kind.
pub enum ErrorKind<I: Transformable, A: Transformable, D: Transformable> {
  /// [`sled`](::sled)'s [`Error`](::sled::Error).
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
  /// Abort transaction.
  Abort,
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

impl<I, A, D> core::fmt::Debug for ErrorKind<I, A, D>
where
  I: Id,
  A: Address,
  D: Transformable,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::Db(e) => write!(f, "Db({:?})", e),
      Self::Id(e) => write!(f, "Id({:?})", e),
      Self::Address(e) => write!(f, "Address({:?})", e),
      Self::Log(e) => write!(f, "Log({:?})", e),
      Self::U64(e) => write!(f, "U64({:?})", e),
      Self::Corrupted(e) => write!(f, "Corrupted({:?})", e),
      Self::Abort => write!(f, "Abort"),
    }
  }
}

impl<I, A, D> core::fmt::Display for ErrorKind<I, A, D>
where
  I: Id,
  A: Address,
  D: Transformable,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::Db(e) => write!(f, "{e}"),
      Self::Id(e) => write!(f, "{e}"),
      Self::Address(e) => write!(f, "{e}"),
      Self::Log(e) => write!(f, "{e}"),
      Self::U64(e) => write!(f, "{e}"),
      Self::Corrupted(e) => write!(f, "{e}"),
      Self::Abort => write!(f, "abort"),
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
    self.0.as_ref().fmt(f)
  }
}

impl<I, A, D> core::fmt::Debug for Error<I, A, D>
where
  I: Id,
  A: Address,
  D: Transformable,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    self.0.as_ref().fmt(f)
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

/// [`StableStorage`] and [`LogStorage`] implementor backed by [`redb`](::redb).
pub struct Db<I, A, D, R> {
  stable_tree: Tree,
  log_tree: Tree,
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
    let db = opts.open().map_err(ErrorKind::from)?;
    let stable_tree = db.open_tree(TERM_TREE_NAME).map_err(ErrorKind::from)?;
    let log_tree = db.open_tree(LOG_TREE_NAME).map_err(ErrorKind::from)?;
    Ok(Self {
      stable_tree,
      log_tree,
      _marker: std::marker::PhantomData,
    })
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
    logs: impl Iterator<Item = (u64, Vec<u8>)>,
  ) -> Result<(), Error<I, A, D>> {
    #[cfg(feature = "metrics")]
    let start = std::time::Instant::now();

    #[cfg(feature = "metrics")]
    let mut batch_size = 0;
    let mut b = Batch::default();
    for (idx, blob) in logs {
      #[cfg(feature = "metrics")]
      let blob_size = blob.len();

      #[cfg(feature = "metrics")]
      {
        batch_size += blob_size;
      }

      #[cfg(feature = "metrics")]
      {
        metrics::histogram!("ruraft.lightwal.log_size", blob_size as f64);
      }
      b.insert(&idx.to_be_bytes(), blob)
    }

    let rst = self.log_tree.transaction(move |t| {
      #[cfg(feature = "metrics")]
      {
        metrics::histogram!("ruraft.lightwal.log_batch_size", batch_size as f64);
        metrics::histogram!("ruraft.lightwal.logs_per_batch", _num_logs as f64);
      }
      t.apply_batch(&b).map_err(Into::into)
    });

    #[cfg(feature = "metrics")]
    scopeguard::defer!(super::report_store_many(_num_logs, start));

    rst.map_err(|e: TransactionError<()>| match e {
      TransactionError::Abort(_) => ErrorKind::Abort.into(),
      TransactionError::Storage(e) => ErrorKind::from(e).into(),
    })
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
    match self
      .stable_tree
      .get(CURRENT_TERM)
      .map_err(ErrorKind::from)?
    {
      Some(d) => u64::decode(d.as_ref())
        .map(|(_, term)| Some(term))
        .map_err(|e| ErrorKind::U64(e).into()),
      None => Ok(None),
    }
  }

  async fn store_current_term(&self, term: u64) -> Result<(), Self::Error> {
    self
      .stable_tree
      .insert(CURRENT_TERM, &term.to_be_bytes())
      .map(|_| {})
      .map_err(|e| ErrorKind::from(e).into())
  }

  async fn last_vote_term(&self) -> Result<Option<u64>, Self::Error> {
    match self
      .stable_tree
      .get(LAST_VOTE_TERM)
      .map_err(ErrorKind::from)?
    {
      Some(d) => u64::decode(d.as_ref())
        .map(|(_, term)| Some(term))
        .map_err(|e| ErrorKind::U64(e).into()),
      None => Ok(None),
    }
  }

  async fn store_last_vote_term(&self, term: u64) -> Result<(), Self::Error> {
    self
      .stable_tree
      .insert(LAST_VOTE_TERM, &term.to_be_bytes())
      .map(|_| {})
      .map_err(|e| ErrorKind::from(e).into())
  }

  async fn last_vote_candidate(
    &self,
  ) -> Result<Option<Node<Self::Id, Self::Address>>, Self::Error> {
    let id = self
      .stable_tree
      .get(LAST_CANIDATE_ID)
      .map_err(ErrorKind::from)?;
    let addr = self
      .stable_tree
      .get(LAST_CANIDATE_ADDR)
      .map_err(ErrorKind::from)?;

    match (id, addr) {
      (None, None) => Ok(None),
      (None, Some(_)) => Err(ErrorKind::Corrupted(
        "last vote candidate corrupted: missing node id".into(),
      ))?,
      (Some(_), None) => Err(ErrorKind::Corrupted(
        "last vote candidate corrupted: missing node addr".into(),
      ))?,
      (Some(id), Some(addr)) => {
        let (_, id) = I::decode(id.as_ref()).map_err(ErrorKind::Id)?;
        let (_, addr) = A::decode(addr.as_ref()).map_err(ErrorKind::Address)?;
        Ok(Some(Node::new(id, addr)))
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

    let mut b = Batch::default();
    b.insert(LAST_CANIDATE_ID, id_buf);
    b.insert(LAST_CANIDATE_ADDR, addr_buf);
    self
      .stable_tree
      .apply_batch(b)
      .map_err(|e| ErrorKind::from(e).into())
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
    self
      .log_tree
      .first()
      .map_err(ErrorKind::from)
      .and_then(|d| {
        d.map(|(k, _)| {
          u64::decode(k.as_ref())
            .map(|(_, k)| k)
            .map_err(|e| ErrorKind::U64(e))
        })
        .transpose()
      })
      .map_err(Into::into)
  }

  async fn last_index(&self) -> Result<Option<u64>, Self::Error> {
    self
      .log_tree
      .last()
      .map_err(ErrorKind::from)
      .and_then(|d| {
        d.map(|(k, _)| {
          u64::decode(k.as_ref())
            .map(|(_, k)| k)
            .map_err(|e| ErrorKind::U64(e))
        })
        .transpose()
      })
      .map_err(Into::into)
  }

  async fn get_log(
    &self,
    index: u64,
  ) -> Result<Option<Log<Self::Id, Self::Address, Self::Data>>, Self::Error> {
    self
      .log_tree
      .get(index.to_be_bytes())
      .map_err(ErrorKind::from)?
      .map(|l| {
        Log::decode(l.as_ref())
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
      .log_tree
      .transaction(|t| {
        let start = match range.start_bound() {
          Bound::Included(s) => *s,
          Bound::Excluded(s) => s + 1,
          Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
          Bound::Included(e) => e + 1,
          Bound::Excluded(e) => *e,
          Bound::Unbounded => u64::MAX,
        };
        let mut b = Batch::default();
        for i in start..end {
          b.remove(&i.to_be_bytes());
        }

        t.apply_batch(&b).map_err(Into::into)
      })
      .map_err(|e: TransactionError<()>| match e {
        TransactionError::Abort(_) => ErrorKind::Abort.into(),
        TransactionError::Storage(e) => ErrorKind::from(e).into(),
      })
  }
}

#[cfg(test)]
mod tests {
  use std::net::SocketAddr;

  use agnostic::tokio::TokioRuntime;
  use smol_str::SmolStr;

  use super::*;
  use crate::test;

  fn test_db() -> Db<SmolStr, SocketAddr, Vec<u8>, TokioRuntime> {
    Db::new(DbOptions::new().temporary(true)).unwrap()
  }

  #[tokio::test]
  async fn test_first_index() {
    test::test_first_index(&test_db()).await;
  }

  #[tokio::test]
  async fn test_last_index() {
    test::test_last_index(&test_db()).await;
  }

  #[tokio::test]
  async fn test_get_log() {
    test::test_get_log(&test_db()).await;
  }

  #[tokio::test]
  async fn test_store_log() {
    test::test_store_log(&test_db()).await;
  }

  #[tokio::test]
  async fn test_store_logs() {
    test::test_store_logs(&test_db()).await;
  }

  #[tokio::test]
  async fn test_remove_range() {
    test::test_remove_range(&test_db()).await;
  }

  #[tokio::test]
  async fn test_current_term() {
    test::test_current_term(&test_db()).await;
  }

  #[tokio::test]
  async fn test_last_vote_term() {
    test::test_last_vote_term(&test_db()).await;
  }

  #[tokio::test]
  async fn test_last_vote_candidate() {
    test::test_last_vote_candidate(&test_db()).await;
  }
}
