use std::io;

use agnostic::Runtime;
use bytes::Bytes;
use redb::{Builder, Database, Error as DbError, TableDefinition};
use ruraft_core::{
  storage::{Log, LogStorage, StableStorage},
  transport::{Address, Id, Transformable},
  Node,
};

use super::*;

#[derive(Debug)]
pub struct Error(DbError);

impl From<io::Error> for Error {
  fn from(value: io::Error) -> Self {
    Self(DbError::Io(value))
  }
}

impl core::fmt::Display for Error {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", self.0)
  }
}

impl std::error::Error for Error {}


pub struct Db<I: Id, A: Address, D, R> {
  db: Database,
  candidate_table_definition: TableDefinition<'static, &'static str, Vec<u8>>,
  u64_table_definition: TableDefinition<'static, u64, u64>,
  log_table_definition: TableDefinition<'static, u64, Vec<u8>>,
  _marker: std::marker::PhantomData<(I, A, D, R)>,
}

impl<I, A, D, R> StableStorage for Db<I, A, D, R>
where
  I: Id + Send + Sync + 'static,
  A: Address + Send + Sync + 'static,
  D: Send + Sync + 'static,
  R: Runtime,
{
  type Error = Error;

  type Runtime = R;

  type Id = I;

  type Address = A;

  async fn insert(&self, key: Bytes, val: Bytes) -> Result<(), Self::Error> {
    todo!()
  }

  async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, Self::Error> {
    todo!()
  }

  async fn insert_u64(&self, key: Bytes, val: u64) -> Result<(), Self::Error> {
    todo!()
  }

  async fn get_u64(&self, key: &[u8]) -> Result<Option<u64>, Self::Error> {
    todo!()
  }

  async fn current_term(&self) -> Result<Option<u64>, Self::Error> {
    todo!()
  }

  async fn store_current_term(&self, term: u64) -> Result<(), Self::Error> {
    todo!()
  }

  async fn last_vote_term(&self) -> Result<Option<u64>, Self::Error> {
    todo!()
  }

  async fn store_last_vote_term(&self, term: u64) -> Result<(), Self::Error> {
    todo!()
  }

  async fn last_vote_candidate(
    &self,
  ) -> Result<Option<Node<Self::Id, Self::Address>>, Self::Error> {
    todo!()
  }

  async fn store_last_vote_candidate(
    &self,
    candidate: Node<Self::Id, Self::Address>,
  ) -> Result<(), Self::Error> {
    let id = candidate.id();
    let mut id_buf = vec![0; id.encoded_len()];
    id.encode(&mut id_buf).unwrap();
    let addr = candidate.addr();
    let mut addr_buf = vec![0; addr.encoded_len()];
    addr.encode(&mut addr_buf).unwrap();

    let w = self.db.begin_write().unwrap();
    {
      let mut t = w.open_table(self.candidate_table_definition).unwrap();
      t.insert(LAST_CANIDATE_ID, id_buf).unwrap();
      t.insert(LAST_CANIDATE_ADDR, addr_buf).unwrap();
    }

    w.commit().unwrap();
    Ok(())
  }
}

impl<I, A, D, R> LogStorage for Db<I, A, D, R>
where
  I: Id + Send + Sync + 'static,
  A: Address + Send + Sync + 'static,
  D: Transformable + Send + Sync + 'static,
  R: Runtime,
{
  type Error = Error;

  type Runtime = R;

  type Id = I;

  type Address = A;

  type Data = D;

  async fn first_index(&self) -> Result<Option<u64>, Self::Error> {
    todo!()
  }

  async fn last_index(&self) -> Result<Option<u64>, Self::Error> {
    todo!()
  }

  async fn get_log(
    &self,
    index: u64,
  ) -> Result<Option<Log<Self::Id, Self::Address, Self::Data>>, Self::Error> {
    todo!()
  }

  async fn store_log(
    &self,
    log: &Log<Self::Id, Self::Address, Self::Data>,
  ) -> Result<(), Self::Error> {
    todo!()
  }

  async fn store_logs(
    &self,
    logs: &[Log<Self::Id, Self::Address, Self::Data>],
  ) -> Result<(), Self::Error> {
    todo!()
  }

  async fn remove_range(
    &self,
    range: impl std::ops::RangeBounds<u64> + Send,
  ) -> Result<(), Self::Error> {
    todo!()
  }
}
