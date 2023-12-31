#![allow(clippy::type_complexity)]

use core::panic;
use std::{
  borrow::Cow,
  collections::{HashMap, HashSet},
  io,
  marker::PhantomData,
  net::SocketAddr,
  path::PathBuf,
  sync::Arc,
  time::{Duration, Instant},
};

use agnostic::{Delay, Runtime, Sleep};
use async_lock::Mutex;
use futures::{AsyncWriteExt, Future, FutureExt};
use ruraft_core::{
  membership::Membership,
  options::Options,
  sidecar::NoopSidecar,
  transport::{Address, Id, Transformable, Transport, Wire},
  FinateStateMachine, FinateStateMachineError, FinateStateMachineLog, FinateStateMachineLogKind,
  FinateStateMachineLogTransformError, FinateStateMachineResponse, FinateStateMachineSnapshot,
  ObservationFilter, Observed, Observer, RaftCore, Role,
};
use ruraft_memory::{
  storage::{
    log::MemoryLogStorage, snapshot::MemorySnapshotSink, stable::MemoryStableStorage, MemoryStorage,
  },
  transport::{MemoryAddressResolver, MemoryTransport},
};

use ruraft_snapshot::sync::{FileSnapshotStorage, FileSnapshotStorageOptions};
use smol_str::SmolStr;
use tempfile::TempDir;

type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;
type Raft<W, R> = RaftCore<
  MockFSM<R>,
  MemoryStorage<SmolStr, SocketAddr, Vec<u8>, R>,
  MemoryTransport<SmolStr, MemoryAddressResolver<SocketAddr, R>, Vec<u8>, W>,
  NoopSidecar<R>,
  R,
>;

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

impl<R> Default for MockFSM<R> {
  fn default() -> Self {
    Self {
      inner: Arc::new(Mutex::new(MockFSMInner {
        logs: Vec::new(),
        memberships: Vec::new(),
      })),
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
    let encode_size = self.logs[..self.max_index as usize]
      .iter()
      .map(|l| l.encoded_len())
      .sum::<usize>();
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
  root_dir: TempDir,
  dirs: Vec<PathBuf>,
  stores: Vec<Arc<MemoryStorage<SmolStr, SocketAddr, Vec<u8>, R>>>,
  fsms: Vec<MockFSM<R>>,
  snaps: Vec<Arc<FileSnapshotStorage<SmolStr, SocketAddr, R>>>,
  rafts: Vec<Raft<W, R>>,

  trans: Vec<Arc<MemoryTransport<SmolStr, MemoryAddressResolver<SocketAddr, R>, Vec<u8>, W>>>,

  observation_tx: async_channel::Sender<Observed<SmolStr, SocketAddr>>,
  observation_rx: async_channel::Receiver<Observed<SmolStr, SocketAddr>>,

  opts: Options,
  propagate_timeout: Duration,
  long_stop_timeout: Duration,
  start_time: Instant,

  failed: async_lock::Mutex<bool>,
  failed_tx: async_channel::Sender<()>,
  failed_rx: async_channel::Receiver<()>,
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

  /// Shuts down the cluster and cleans up.
  async fn close(&self) {
    let mut d = R::delay(self.long_stop_timeout, async {
      panic!("timed out waiting for shutdown");
    });

    futures::future::join_all(self.rafts.iter().map(|r| r.shutdown())).await;

    for d in &self.dirs {
      let _ = std::fs::remove_dir_all(d);
    }

    d.cancel().await;
  }

  /// Returns a channel which will signal if an observation is made
  /// or a timeout occurs. It is possible to set a filter to look for specific
  /// observations. Setting timeout to 0 means that it will wait forever until a
  /// non-filtered observation is made.
  fn wait_event_rx(
    &self,
    ctx: async_channel::Receiver<()>,
    f: Option<impl ObservationFilter<SmolStr, SocketAddr>>,
  ) -> async_channel::Receiver<()> {
    let (tx, rx) = async_channel::bounded(1);
    let observation_rx = self.observation_rx.clone();
    R::spawn_detach(async move {
      scopeguard::defer!(tx.close(););
      loop {
        futures::select! {
          _ = ctx.recv().fuse() => return,
          o = observation_rx.recv().fuse() => {
            if o.is_err() || f.is_none() {
              return;
            }

            if f.as_ref().unwrap()(&o.unwrap()) {
              return;
            }
          }
        }
      }
    });
    rx
  }

  // WaitEvent waits until an observation is made, a timeout occurs, or a test
  // failure is signaled. It is possible to set a filter to look for specific
  // observations. Setting timeout to 0 means that it will wait forever until a
  // non-filtered observation is made or a test failure is signaled.
  async fn wait_event(
    &self,
    f: Option<impl ObservationFilter<SmolStr, SocketAddr>>,
    timeout: Duration,
  ) {
    let (ctx_tx, ctx_rx) = async_channel::bounded(1);

    let mut delay = R::delay(timeout, async move {
      ctx_tx.close();
    });

    let event_rx = self.wait_event_rx(ctx_rx, f);
    futures::select! {
      _ = event_rx.recv().fuse() => {}
      _ = self.failed_rx.recv().fuse() => {
        panic!("test failed");
      }
    }

    delay.cancel().await;
  }

  /// blocks until every FSM in the cluster has the given
  /// length, or the long sanity check timeout expires.
  async fn wait_for_replication(&self, fsm_length: usize) {
    let limit = After::new(self.long_stop_timeout);

    'outer: loop {
      let (ctx_tx, ctx_rx) = async_channel::bounded(1);
      let mut delay = R::delay(self.long_stop_timeout, async move {
        ctx_tx.close();
      });
      let f = |_observed: &Observed<SmolStr, SocketAddr>| true;
      let rx = self.wait_event_rx(ctx_rx, Some(f));
      futures::select! {
        _ = limit.fuse() => {
          panic!("timed out waiting for replication");
        }
        _ = self.failed_rx.recv().fuse() => {
          panic!("test failed");
        }
        _ = rx.recv().fuse() => {
          for fsm_raw in self.fsms.iter() {
            let fsm = fsm_raw.inner.lock().await;
            if fsm.logs.len() != fsm_length {
              continue 'outer;
            }
          }
          delay.cancel().await;
          return;
        }
      }
    }
  }

  /// Takes a snapshot of the state of the cluster. This might not be
  /// stable, so use GetInState() to apply some additional checks when waiting
  /// for the cluster to achieve a particular state.
  fn poll_state(&self, role: Role) -> (Vec<Raft<W, R>>, u64) {
    let mut highest_term = 0;
    let mut in_ = Vec::new();
    for r in self.rafts.iter() {
      if r.role() == role {
        in_.push(r.clone());
      }
      let term = r.current_term();
      if term > highest_term {
        highest_term = term;
      }
    }

    (in_, highest_term)
  }

  /// Polls the state of the cluster and attempts to identify when it has
  /// settled into the given state.
  async fn get_in_state(&self, role: Role) -> Vec<Raft<W, R>> {
    tracing::info!(raft_state = %role, "starting stability test");
    let limit = After::new(self.long_stop_timeout);

    // An election should complete after 2 * max(HeartbeatTimeout, ElectionTimeout)
    // because of the randomised timer expiring in 1 x interval ... 2 x interval.
    // We add a bit for propagation delay. If the election fails (e.g. because
    // two elections start at once), we will have got something through our
    // observer channel indicating a different state (i.e. one of the nodes
    // will have moved to candidate state) which will reset the timer.
    //
    // Because of an implementation peculiarity, it can actually be 3 x timeout.
    let mut timeout = self.opts.heartbeat_timeout();
    if timeout < self.opts.election_timeout() {
      timeout = self.opts.election_timeout();
    }
    timeout = 2 * timeout + self.opts.commit_timeout();
    let timer = R::sleep(timeout);
    futures::pin_mut!(timer);
    // Wait until we have a stable instate slice. Each time we see an
    // observation a state has changed, recheck it and if it has changed,
    // restart the timer.
    let poll_start_time = Instant::now();
    loop {
      let (_, highest_term) = self.poll_state(role);
      let instate_time = Instant::now();

      // Sometimes this routine is called very early on before the
      // rafts have started up. We then timeout even though no one has
      // even started an election. So if the highest term in use is
      // zero, we know there are no raft processes that have yet issued
      // a RequestVote, and we set a long time out. This is fixed when
      // we hear the first RequestVote, at which point we reset the
      // timer.
      if highest_term == 0 {
        timer
          .as_mut()
          .reset(Instant::now() + self.long_stop_timeout);
      } else {
        timer.as_mut().reset(Instant::now() + timeout);
      }

      // Filter will wake up whenever we observe a RequestVote.
      let filter = |ob: &Observed<SmolStr, SocketAddr>| -> bool {
        matches!(ob, Observed::Role(_) | Observed::RequestVote(_))
      };
      let (ctx_tx, ctx_rx) = async_channel::bounded(1);
      scopeguard::defer!(ctx_tx.close(););
      let event_rx = self.wait_event_rx(ctx_rx, Some(filter));
      futures::select! {
        _ = limit.fuse() => {
          panic!("timeout waiting for stable {role} state");
        }
        _ = self.failed_rx.recv().fuse() => {
          panic!("test failed");
        }
        _ = event_rx.recv().fuse() => {
          tracing::debug!("resetting stability timeout");
        }
        _ = timer.as_mut().fuse() => {
          let (in_state, highest_term) = self.poll_state(role);
          tracing::info!("stable state for {} reached at {} ({} nodes), highestTerm is {}, {} from start of poll, {} from cluster start.", role, instate_time.elapsed().readable(), in_state.len(), highest_term, instate_time.duration_since(poll_start_time).readable(), instate_time.duration_since(self.start_time).readable());
          return in_state;
        }
      }
    }
  }

  /// Waits for the cluster to elect a leader and stay in a stable state.
  async fn leader(&self) -> Raft<W, R> {
    let leaders = self.get_in_state(Role::Leader).await;
    assert_eq!(leaders.len(), 1, "expected 1 leader, got {}", leaders.len());
    leaders[0].clone()
  }

  /// Waits for the cluster to have N-1 followers and stay in a stable
  /// state.
  async fn followers(&self) -> Vec<Raft<W, R>> {
    let exp_followers = self.rafts.len() - 1;
    let followers = self.get_in_state(Role::Follower).await;
    assert_eq!(
      followers.len(),
      exp_followers,
      "timeout waiting for {} followers (followers are {:?})",
      exp_followers,
      followers
    );
    followers
  }

  /// Connects all the transports together.
  async fn fully_connect(&self) {
    tracing::debug!("fully connecting");
    for t1 in self.trans.iter() {
      for t2 in self.trans.iter() {
        if t1.header() != t2.header() {
          t1.connect(*t2.local_addr(), t2.as_ref().clone()).await;
          t2.connect(*t1.local_addr(), t1.as_ref().clone()).await;
        }
      }
    }
  }

  /// Disconnects all transports from the given address.
  async fn disconnect(&self, a: &SocketAddr) {
    tracing::debug!(address = %a, "disconnecting");
    for t in self.trans.iter() {
      if t.local_addr().eq(a) {
        t.disconnect_all().await;
      } else {
        t.disconnect(a).await;
      }
    }
  }

  /// Keeps the given list of addresses connected but isolates them
  /// from the other members of the cluster.
  async fn partition(&self, far: &[SocketAddr]) {
    tracing::debug!(addresses = ?far, "partitioning");

    // Gather the set of nodes on the "near" side of the partition (we
    // will call the supplied list of nodes the "far" side).
    let mut near = HashSet::new();

    'outer: loop {
      for t in self.trans.iter() {
        let l = t.local_addr();
        for a in far {
          if l.eq(a) {
            continue 'outer;
          }
          near.insert(*l);
        }
      }

      // Now fixup all the connections. The near side will be separated from
      // the far side, and vice-versa.
      for t in self.trans.iter() {
        let l = t.local_addr();
        if near.contains(l) {
          for a in far {
            t.disconnect(a).await;
          }
        } else {
          for a in near.iter() {
            t.disconnect(a).await;
          }
        }
      }
      break;
    }
  }

  /// Returns the index of the given raft instance.
  fn index_of(&self, raft: &Raft<W, R>) -> Option<usize> {
    for (i, r) in self.rafts.iter().enumerate() {
      if r.local_id().eq(raft.local_id()) {
        return Some(i);
      }
    }
    None
  }

  /// Checks that ALL the nodes think the leader is the given expected
  /// leader.
  fn ensure_leader(&self, expect: Option<SocketAddr>) {
    // We assume c.Leader() has been called already; now check all the rafts
    // think the leader is correct
    let mut fail = false;
    for r in self.rafts.iter() {
      let leader = r.leader().map(|l| *l.addr());

      if leader != expect {
        let leader_addr = leader
          .map(|l| l.to_string())
          .unwrap_or_else(|| "<none>".to_string());

        let expect_addr = expect
          .map(|e| e.to_string())
          .unwrap_or_else(|| "<none>".to_string());

        tracing::error!(peer = ?r, leader=leader_addr, expected_leader = expect_addr, "peer sees incorrect leader");
        fail = true;
      }
    }

    if fail {
      panic!("at least one peer has the wrong notion of leader");
    }
  }

  /// makes sure all the FSMs have the same contents.
  async fn ensure_same(&self) {
    let limit = Instant::now() + self.long_stop_timeout;
    let first = &self.fsms[0];

    let f = |_: &Observed<SmolStr, SocketAddr>| true;
    'outer: loop {
      let first = first.inner.lock().await;

      for (idx, fsm) in self.fsms.iter().enumerate() {
        if idx == 0 {
          continue;
        }

        let fsm = fsm.inner.lock().await;

        let len1 = first.logs.len();
        let len2 = fsm.logs.len();
        if len1 != len2 {
          drop(fsm);
          if Instant::now() > limit {
            panic!("FSM log length mismatch: {len1} {len2}");
          } else {
            drop(first);
            self.wait_event(Some(f), self.opts.commit_timeout()).await;
            continue 'outer;
          }
        }

        for i in 0..len1 {
          if first.logs[i] != fsm.logs[i] {
            drop(fsm);
            if Instant::now() > limit {
              panic!("FSM log mismatch at index {i}");
            } else {
              drop(first);
              self.wait_event(Some(f), self.opts.commit_timeout()).await;
              continue 'outer;
            }
          }
        }

        let num_memberships1 = first.memberships.len();
        let num_memberships2 = fsm.memberships.len();
        if len1 != len2 {
          drop(fsm);
          if Instant::now() > limit {
            panic!("FSM membership length mismatch: {num_memberships1} {num_memberships2}");
          } else {
            drop(first);
            self.wait_event(Some(f), self.opts.commit_timeout()).await;
            continue 'outer;
          }
        }

        for i in 0..num_memberships1 {
          if first.memberships[i] != fsm.memberships[i] {
            drop(fsm);
            if Instant::now() > limit {
              panic!("FSM membership mismatch at index {i}");
            } else {
              drop(first);
              self.wait_event(Some(f), self.opts.commit_timeout()).await;
              continue 'outer;
            }
          }
        }
      }

      return;
    }
  }

  /// Returns the configuration of the given Raft instance, or
  /// fails the test if there's an error
  fn get_membership(&self, r: &Raft<W, R>) -> Membership<SmolStr, SocketAddr> {
    r.latest_membership().membership().clone()
  }

  async fn ensure_same_peers(&self) {
    let limit = Instant::now() + self.long_stop_timeout;
    let peer_set = self.get_membership(&self.rafts[0]);

    'outer: loop {
      for (idx, r) in self.rafts.iter().enumerate() {
        if idx == 0 {
          continue;
        }

        let other_set = self.get_membership(r);

        if peer_set != other_set {
          if Instant::now() > limit {
            panic!("timed out waiting for peers to converge");
          } else {
            let f = |_: &Observed<SmolStr, SocketAddr>| true;
            self.wait_event(Some(f), self.opts.commit_timeout()).await;
            continue 'outer;
          }
        }
      }

      return;
    }
  }

  /// Return a cluster with the given config and number of peers.
  /// If bootstrap is true, the servers will know about each other before starting,
  /// otherwise their transports will be wired up but they won't yet have configured
  /// each other.
  async fn make_cluster(mut opts: MakeClusterOptions) -> Result<Self, DynError> {
    let raft_opts = opts.options.get_or_insert(inmem_config());

    let (observation_tx, observation_rx) = async_channel::bounded(1024);
    let propagate_timeout = raft_opts.heartbeat_timeout() * 2 + raft_opts.commit_timeout();
    let long_stop_timeout = if opts.longstop_timeout > Duration::ZERO {
      opts.longstop_timeout
    } else {
      Duration::from_secs(5)
    };

    let (fail_tx, fail_rx) = async_channel::bounded(1);

    let mut dirs = Vec::new();
    let mut stores = Vec::new();
    let mut fsms = Vec::new();
    // Setup the stores and transports
    for i in 0..opts.peers {
      let dir = tempfile::Builder::new().prefix("ruraft").tempdir().unwrap();
      dirs.push(dir);
      let stable_store = MemoryStableStorage::new();
      let log_store = MemoryLogStorage::new();
      let (dir2, snapshot_store) = file_snapshot().await;
      dirs.push(dir2);
      fsms.push(MockFSM::default());
    }
  }
}

fn inmem_config() -> Options {
  Options::default()
    .with_heartbeat_timeout(Duration::from_millis(50))
    .with_election_timeout(Duration::from_millis(50))
    .with_leader_lease_timeout(Duration::from_millis(50))
    .with_commit_timeout(Duration::from_millis(5))
}

#[derive(Clone, Copy)]
struct After {
  instant: Instant,
  timeout: Duration,
}

impl After {
  fn new(duration: Duration) -> Self {
    Self {
      instant: Instant::now(),
      timeout: duration,
    }
  }
}

impl Future for After {
  type Output = ();

  fn poll(
    self: std::pin::Pin<&mut Self>,
    _cx: &mut std::task::Context<'_>,
  ) -> std::task::Poll<Self::Output> {
    if self.instant.elapsed() >= self.timeout {
      std::task::Poll::Ready(())
    } else {
      std::task::Poll::Pending
    }
  }
}

trait DurationExt {
  fn readable(&self) -> humantime::Duration;
}

impl DurationExt for Duration {
  fn readable(&self) -> humantime::Duration {
    humantime::Duration::from(*self)
  }
}

async fn file_snapshot<R: Runtime>() -> (TempDir, FileSnapshotStorage<SmolStr, SocketAddr, R>) {
  // Create a test dir
  let dir = tempfile::Builder::new().prefix("ruraft").tempdir().unwrap();

  let opts = FileSnapshotStorageOptions::new(dir.path(), 3, true);
  let snap = FileSnapshotStorage::<SmolStr, SocketAddr, R>::new(opts)
    .await
    .unwrap();
  (dir, snap)
}
