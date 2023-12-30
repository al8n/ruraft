#![allow(clippy::type_complexity)]

use std::{
  borrow::Cow, marker::PhantomData, net::SocketAddr, path::PathBuf, sync::Arc, time::{Duration, Instant}, io,
};

use agnostic::Runtime;
use async_lock::Mutex;
use futures::{AsyncWriteExt, FutureExt};
use ruraft_core::{
  membership::Membership,
  options::Options,
  transport::{Address, Id, Transformable, Wire},
  FinateStateMachine, FinateStateMachineError, FinateStateMachineLog, FinateStateMachineLogKind,
  FinateStateMachineResponse, FinateStateMachineSnapshot, FinateStateMachineLogTransformError, RaftCore, Observer, sidecar::NoopSidecar
};
use ruraft_memory::{storage::{snapshot::MemorySnapshotSink, MemoryStorage}, transport::{MemoryTransport, MemoryAddressResolver}};

use ruraft_snapshot::sync::FileSnapshotStorage;
use smol_str::SmolStr;

type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// NOTE: This is exposed for middleware testing purposes and is not a stable API
pub enum MockFSMErrorKind {
  IO(io::Error),
  Transform(FinateStateMachineLogTransformError<SmolStr, SocketAddr, Vec<u8>>),
}

/// NOTE: This is exposed for middleware testing purposes and is not a stable API
pub struct MockFSMError<R> {
  kind: Arc<MockFSMErrorKind>,
  messages: Vec<Cow<'static, str>>,
  _runtime: PhantomData<R>,
}

impl<R> Clone for MockFSMError<R> {
  fn clone(&self) -> Self {
    Self {
      kind: self.kind.clone(),
      messages: self.messages.clone(),
      _runtime: PhantomData,
    }
  }
}

impl<R: Runtime> From<io::Error> for MockFSMError<R> {
  fn from(err: io::Error) -> Self {
    Self {
      kind: Arc::new(MockFSMErrorKind::IO(err)),
      messages: Vec::new(),
      _runtime: PhantomData,
    }
  }
}

impl<R: Runtime> From<FinateStateMachineLogTransformError<SmolStr, SocketAddr, Vec<u8>>>
  for MockFSMError<R>
{
  fn from(err: FinateStateMachineLogTransformError<SmolStr, SocketAddr, Vec<u8>>) -> Self {
    Self {
      kind: Arc::new(MockFSMErrorKind::Transform(err)),
      messages: Vec::new(),
      _runtime: PhantomData,
    }
  }
}

impl<R: Runtime> std::fmt::Debug for MockFSMError<R> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct(std::any::type_name::<Self>())
      .field("messages", &self.messages)
      .finish()
  }
}

impl<R: Runtime> std::fmt::Display for MockFSMError<R> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    for (idx, msg) in self.messages.iter().enumerate() {
      writeln!(f, "\t{idx}: {}", msg)?;
    }
    Ok(())
  }
}

impl<R: Runtime> std::error::Error for MockFSMError<R> {}

impl<R: Runtime> FinateStateMachineError for MockFSMError<R> {
  type Snapshot = MockFSMSnapshot<R>;

  fn snapshot(err: <Self::Snapshot as FinateStateMachineSnapshot>::Error) -> Self {
    err
  }

  fn with_message(mut self, msg: std::borrow::Cow<'static, str>) -> Self {
    self.messages.push(msg);
    self
  }
}

/// NOTE: This is exposed for middleware testing purposes and is not a stable API
#[derive(Clone, Copy)]
pub struct MockFSMResponse(u64);

impl FinateStateMachineResponse for MockFSMResponse {
  fn index(&self) -> u64 {
    self.0
  }
}

struct MockFSMInner {
  logs: Vec<FinateStateMachineLog<SmolStr, SocketAddr, Vec<u8>>>,
  memberships: Vec<Membership<SmolStr, SocketAddr>>,
}

/// MockFSM is an implementation of the [`FinateStateMachine`] trait, and just stores
/// the logs sequentially.
///
/// NOTE: This is exposed for middleware testing purposes and is not a stable API
pub struct MockFSM<R> {
  inner: Arc<Mutex<MockFSMInner>>,
  _runtime: PhantomData<R>,
}

impl<R> Clone for MockFSM<R> {
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
      _runtime: PhantomData,
    }
  }
}

impl<R: Runtime> FinateStateMachine for MockFSM<R> {
  type Error = MockFSMError<R>;

  type Snapshot = MockFSMSnapshot<R>;

  type SnapshotSink = MemorySnapshotSink<SmolStr, SocketAddr, R>;

  type Response = MockFSMResponse;

  type Id = SmolStr;

  type Address = SocketAddr;

  type Data = Vec<u8>;

  type Runtime = R;

  /// NOTE: This is exposed for middleware testing purposes and is not a stable API
  async fn apply(
    &self,
    log: FinateStateMachineLog<Self::Id, Self::Address, Self::Data>,
  ) -> Result<Self::Response, Self::Error> {
    let mut inner = self.inner.lock().await;
    inner.logs.push(log.clone());
    Ok(MockFSMResponse(inner.logs.len() as u64))
  }

  async fn apply_batch(
    &self,
    logs: impl IntoIterator<Item = FinateStateMachineLog<Self::Id, Self::Address, Self::Data>>,
  ) -> Result<Vec<Self::Response>, Self::Error> {
    let mut inner = self.inner.lock().await;
    let mut responses = Vec::new();
    for log in logs {
      inner.logs.push(log.clone());
      responses.push(MockFSMResponse(inner.logs.len() as u64));
    }
    Ok(responses)
  }

  /// NOTE: This is exposed for middleware testing purposes and is not a stable API
  async fn snapshot(&self) -> Result<Self::Snapshot, Self::Error> {
    let inner = self.inner.lock().await;
    let logs = inner.logs.clone();
    let max_index = logs.len() as u64;
    Ok(MockFSMSnapshot {
      logs,
      max_index,
      _runtime: PhantomData,
    })
  }

  /// NOTE: This is exposed for middleware testing purposes and is not a stable API
  async fn restore(
    &self,
    _snapshot: impl futures::prelude::AsyncRead + Unpin,
  ) -> Result<(), Self::Error> {
    Ok(())
  }
}

/// NOTE: This is exposed for middleware testing purposes and is not a stable API
pub struct MockFSMMembershipStore<R> {
  fsm: MockFSM<R>,
}

/// NOTE: This is exposed for middleware testing purposes and is not a stable API
pub struct MockFSMSnapshot<R> {
  logs: Vec<FinateStateMachineLog<SmolStr, SocketAddr, Vec<u8>>>,
  max_index: u64,
  _runtime: PhantomData<R>,
}

impl<R: Runtime> FinateStateMachineSnapshot for MockFSMSnapshot<R> {
  type Error = MockFSMError<R>;

  type Sink = MemorySnapshotSink<SmolStr, SocketAddr, R>;

  type Runtime = R;

  async fn persist(&self, mut sink: Self::Sink) -> Result<(), Self::Error> {
    let encode_size = self.logs[..self.max_index as usize].iter().map(|l| l.encoded_len()).sum::<usize>();
    let mut buf = vec![0; encode_size + 4];
    let mut offset = 0;
    buf[..4].copy_from_slice(&(encode_size as u32).to_be_bytes());
    for log in &self.logs[..self.max_index as usize] {
      offset += log.encode(&mut buf[offset..])?;
    }
    sink.write_all(&buf[..offset]).await.map_err(Into::into)
  }
}

struct MakeClusterOptions {
  peers: usize,
  bootstrap: bool,
  options: Option<Options>,
  config_store_fsm: bool,
  longstop_timeout: Duration,
  monotonic_logs: bool,
}

struct Cluster<W, R>
where
  W: Wire<Id = SmolStr, Address = SocketAddr, Data = Vec<u8>>,
  R: Runtime,
  <R::Sleep as futures::Future>::Output: Send + 'static,
{
  dirs: Vec<PathBuf>,
  stores: Vec<Arc<MemoryStorage<SmolStr, SocketAddr, Vec<u8>, R>>>,
  fsms: Vec<MockFSM<R>>,
  snaps: Vec<Arc<FileSnapshotStorage<SmolStr, SocketAddr, R>>>,
  rafts: Vec<
    RaftCore<
      MockFSM<R>,
      MemoryStorage<SmolStr, SocketAddr, Vec<u8>, R>,
      MemoryTransport<SmolStr, MemoryAddressResolver<SocketAddr, R>, Vec<u8>, W>,
      NoopSidecar<R>,
      R
    >,
  >,

  trans: Vec<Arc<MemoryTransport<SmolStr, MemoryAddressResolver<SocketAddr, R>, Vec<u8>, W>>>,

  observation_tx: async_channel::Sender<Observer<SmolStr, SocketAddr>>,
  observation_rx: async_channel::Receiver<Observer<SmolStr, SocketAddr>>,

  opts: Options,
  propagate_timeout: Duration,
  long_stop_timeout: Duration,
  start_time: Instant,

  failed: async_lock::Mutex<bool>,
  failed_tx: async_channel::Sender<()>,
}

impl<W, R> Cluster<W, R>
where
  W: Wire<Id = SmolStr, Address = SocketAddr, Data = Vec<u8>>,
  R: Runtime,
  <R::Sleep as futures::Future>::Output: Send + 'static,
{
  // async fn make_cluster(n: usize, opts: MakeClusterOptions) -> Result<Self, DynError> {
  //   opts.options.get_or_insert(inmem_config());


  // }

  pub fn remove_server(&mut self, id: &str) {
    self.rafts.retain(|raft| raft.local_id().as_str() != id);
  }

  fn merge(&mut self, other: &Self) {
    self.dirs.extend(other.dirs.iter().cloned());
    self.stores.extend(other.stores.iter().cloned());
    self.fsms.extend(other.fsms.iter().cloned());
    self.snaps.extend(other.snaps.iter().cloned());
    self.rafts.extend(other.rafts.iter().cloned());
    self.trans.extend(other.trans.iter().cloned());
  }

  /// notifyFailed will close the failed channel which can signal the task
  /// running the test that another task has detected a failure in order to
  /// terminate the test.
  async fn notify_failed(&self) {
    let mut failed = self.failed.lock().await;
    if !*failed {
      *failed = true;
      self.failed_tx.close();
    }
  }

  fn close(&self)
}

fn inmem_config() -> Options {
  Options::default()
    .with_heartbeat_timeout(Duration::from_millis(50))
    .with_election_timeout(Duration::from_millis(50))
    .with_leader_lease_timeout(Duration::from_millis(50))
    .with_commit_timeout(Duration::from_millis(5))
}

#[test]
fn test_() {
}