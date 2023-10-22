use std::{
  collections::HashMap,
  future::Future,
  net::SocketAddr,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
  time::{Duration, Instant},
};

use agnostic::Runtime;
use arc_swap::{ArcSwap, ArcSwapOption};
use async_lock::Mutex;
use atomic::Atomic;
use futures::{channel::oneshot, FutureExt};
use metrics::atomics::AtomicU64;

use crate::{
  error::{Error, RaftError},
  fsm::FinateStateMachine,
  membership::{Membership, Memberships, Server, ServerSuffrage},
  options::{Options, ReloadableOptions},
  raft::{fsm::FSMRunner, snapshot::SnapshotRunner},
  sidecar::{NoopSidecar, Sidecar},
  storage::{Log, LogKind, LogStorage, SnapshotMeta, SnapshotStorage, StableStorage, Storage},
  transport::{Address, AddressResolver, Id, RpcConsumer, Transport},
};

mod candidate;
mod follower;
mod fsm;
mod leader;
mod snapshot;
mod state;
pub use state::*;

const MIN_CHECK_INTERVAL: Duration = Duration::from_millis(10);
const OLDEST_LOG_GAUGE_INTERVAL: Duration = Duration::from_secs(10);

const KEY_CURRENT_TERM: &[u8] = b"__ruraft_current_term__";
const KEY_LAST_VOTE_TERM: &[u8] = b"__ruraft_last_vote_term__";
const KEY_LAST_VOTE_FOR: &[u8] = b"__ruraft_last_vote_cand__";

pub struct Node<I: Id, A: Address> {
  id: I,
  addr: A,
}

impl<I, A> core::fmt::Display for Node<I, A>
where
  I: Id,
  A: Address,
{
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}({})", self.id, self.addr)
  }
}

impl<I: Id, A: Address> Node<I, A> {
  /// Returns the id of the leader.
  #[inline]
  pub const fn id(&self) -> &I {
    &self.id
  }

  /// Returns the address of the leader.
  #[inline]
  pub const fn addr(&self) -> &A {
    &self.addr
  }

  #[inline]
  pub const fn new(id: I, addr: A) -> Self {
    Self { id, addr }
  }
}

struct RaftInner<F, S, T, SC, R>
where
  F: FinateStateMachine<
    Id = T::Id,
    Address = <T::Resolver as AddressResolver>::Address,
    Runtime = R,
  >,
  S: Storage<Id = T::Id, Address = <T::Resolver as AddressResolver>::Address, Runtime = R>,
  T: Transport<Runtime = R>,
  <T::Resolver as AddressResolver>::Address: Send + Sync + 'static,
  SC: Sidecar<Runtime = R>,
  R: Runtime,
{
  state: State,
  storage: Arc<S>,
  transport: Arc<T>,
  leader: ArcSwapOption<Node<T::Id, <T::Resolver as AddressResolver>::Address>>,
  local: Node<T::Id, <T::Resolver as AddressResolver>::Address>,
  advertise_addr: SocketAddr,
  candidate_from_leadership_transfer: AtomicBool,

  memberships: Arc<Memberships<T::Id, <T::Resolver as AddressResolver>::Address>>,
  /// Used to request the leader to make membership changes.
  membership_change_tx:
    async_channel::Sender<Membership<T::Id, <T::Resolver as AddressResolver>::Address>>,

  membership_change_rx:
    async_channel::Receiver<Membership<T::Id, <T::Resolver as AddressResolver>::Address>>,

  /// Used to get the membership data safely from
  /// outside of the main thread.
  committed_membership_rx: async_channel::Receiver<
    oneshot::Sender<
      Result<
        Arc<(
          u64,
          Arc<Membership<T::Id, <T::Resolver as AddressResolver>::Address>>,
        )>,
        Error<F, S, T>,
      >,
    >,
  >,
  /// Stores the initial options to use. This is the most recent one
  /// provided. All reads of config values should use the options() helper method
  /// to read this safely.
  options: Options,
  /// Stores the current reloadable options. This is the most recent one provided.
  reloadable_options: Arc<Atomic<ReloadableOptions>>,
  /// Ensures that only one thread can reload options at once since
  /// we need to read-modify-write the atomic. It is NOT necessary to hold this
  /// for any other operation e.g. reading config using options().
  reload_options_lock: Mutex<()>,

  apply_tx: async_channel::Sender<()>,

  /// Used to send state-changing updates to the FSM. This
  /// receives pointers to commitTuple structures when applying logs or
  /// pointers to restoreFuture structures when restoring a snapshot. We
  /// need control over the order of these operations when doing user
  /// restores so that we finish applying any old log applies before we
  /// take a user snapshot on the leader, otherwise we might restore the
  /// snapshot and apply old logs to it that were in the pipe.
  fsm_mutate_tx: async_channel::Sender<fsm::FSMRequest<F, S, T>>,

  user_snapshot_tx: async_channel::Sender<
    oneshot::Sender<
      Result<
        Box<
          dyn Future<
              Output = Result<
                <S::Snapshot as SnapshotStorage>::Source,
                <S::Snapshot as SnapshotStorage>::Error,
              >,
            > + Send,
        >,
        Error<F, S, T>,
      >,
    >,
  >,

  /// Used to tell leader that `reloadbale_options` has changed
  leader_notify_tx: async_channel::Sender<()>,
  leader_notify_rx: async_channel::Receiver<()>,

  /// Used to tell followers that `reloadbale_options` has changed
  follower_notify_tx: async_channel::Sender<()>,
  follower_notify_rx: async_channel::Receiver<()>,
  /// last_contact is the last time we had contact from the
  /// leader node. This can be used to gauge staleness.
  last_contact: ArcSwapOption<Instant>,
  /// The sidecar to run alongside the Raft.
  sidecar: Option<Arc<SC>>,
  shutdown_tx: async_channel::Sender<()>,
}

impl<F, S, T, SC, R> core::ops::Deref for RaftInner<F, S, T, SC, R>
where
  F: FinateStateMachine<
    Id = T::Id,
    Address = <T::Resolver as AddressResolver>::Address,
    Runtime = R,
  >,
  S: Storage<Id = T::Id, Address = <T::Resolver as AddressResolver>::Address, Runtime = R>,
  T: Transport<Runtime = R>,
  <T::Resolver as AddressResolver>::Address: Send + Sync + 'static,
  SC: Sidecar<Runtime = R>,
  R: Runtime,
{
  type Target = State;

  fn deref(&self) -> &Self::Target {
    &self.state
  }
}

impl<F, S, T, SC, R> RaftInner<F, S, T, SC, R>
where
  F: FinateStateMachine<
    Id = T::Id,
    Address = <T::Resolver as AddressResolver>::Address,
    Runtime = R,
  >,
  S: Storage<Id = T::Id, Address = <T::Resolver as AddressResolver>::Address, Runtime = R>,
  T: Transport<Runtime = R>,
  <T::Resolver as AddressResolver>::Address: Send + Sync + 'static,
  SC: Sidecar<Runtime = R>,
  R: Runtime,
{
  #[inline]
  fn role(&self) -> Role {
    self.state.role()
  }

  #[inline]
  fn set_last_contact(&self, instant: Instant) {
    self.last_contact.store(Some(Arc::new(instant)));
  }

  #[inline]
  fn set_leader(&self, leader: Option<Node<T::Id, <T::Resolver as AddressResolver>::Address>>) {
    let new = leader.map(Arc::new);
    let old = self.leader.swap(new.clone());
    match (new, old) {
      (None, None) => {}
      (None, Some(old)) => {
        // TODO: self.observe(LeaderObservation::none())
      }
      (Some(new), None) => {
        // TODO: self.observe(LeaderObservation::new(new))
      }
      (Some(new), Some(old)) => {
        if old.addr != new.addr || old.id != new.id {
          // TODO: self.observe(LeaderObservation::new(new))
        }
      }
    }
  }

  /// Takes a log entry and updates the latest
  /// membership if the entry results in a new membership. This must only be
  /// called from the main thread, or from constructors before any threads have begun.
  fn process_membership_log(
    &self,
    log: crate::storage::Log<T::Id, <T::Resolver as AddressResolver>::Address>,
  ) {
    if let crate::storage::LogKind::Membership(m) = log.kind {
      self
        .memberships
        .committed
        .store(self.memberships.latest().clone());
      self.memberships.set_latest(m, log.index);
    }
  }

  /// Used to apply all the committed entries that haven't been
  /// applied up to the given index limit.
  /// This can be called from both leaders and followers.
  /// Followers call this from `append_entries`, for `n` entries at a time, and always
  /// pass futures = `None`.
  /// Leaders call this when entries are committed. They pass the futures from any
  /// inflight logs.
  async fn process_logs(&self, index: u64, futures: Option<HashMap<u64, ()>>) {
    todo!()
  }
}

pub struct RaftCore<F, S, T, SC, R>
where
  F: FinateStateMachine<
    Id = T::Id,
    Address = <T::Resolver as AddressResolver>::Address,
    Runtime = R,
  >,
  S: Storage<Id = T::Id, Address = <T::Resolver as AddressResolver>::Address, Runtime = R>,
  T: Transport<Runtime = R>,
  <T::Resolver as AddressResolver>::Address: Send + Sync + 'static,
  SC: Sidecar<Runtime = R>,
  R: Runtime,
{
  inner: Arc<RaftInner<F, S, T, SC, R>>,
}

impl<F, S, T, SC, R> Clone for RaftCore<F, S, T, SC, R>
where
  F: FinateStateMachine<
    Id = T::Id,
    Address = <T::Resolver as AddressResolver>::Address,
    Runtime = R,
  >,
  S: Storage<Id = T::Id, Address = <T::Resolver as AddressResolver>::Address, Runtime = R>,
  T: Transport<Runtime = R>,
  <T::Resolver as AddressResolver>::Address: Send + Sync + 'static,
  SC: Sidecar<Runtime = R>,
  R: Runtime,
{
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
  }
}

impl<F, S, T, R> RaftCore<F, S, T, NoopSidecar<R>, R>
where
  F: FinateStateMachine<
    Id = T::Id,
    Address = <T::Resolver as AddressResolver>::Address,
    SnapshotSink = <S::Snapshot as SnapshotStorage>::Sink,
    Runtime = R,
  >,
  S: Storage<Id = T::Id, Address = <T::Resolver as AddressResolver>::Address, Runtime = R>,
  T: Transport<Runtime = R>,
  <T::Resolver as AddressResolver>::Address: Send + Sync + 'static,
  R: Runtime,
  <R::Sleep as std::future::Future>::Output: Send,
  R: Runtime,
{
  /// Used to construct a new Raft node. It takes a options, as well
  /// as implementations of various traits that are required.
  ///
  /// **N.B.**
  /// - If there is no old state, then will initialize a new Raft cluster which contains only one voter node(self), users can then invoke `add_voter` on it to add other servers to the cluster.
  /// - If there are any
  /// old state, such as snapshots, logs, peers, etc, all those will be restored
  /// when creating the Raft node.
  pub async fn new(
    fsm: F,
    storage: S,
    transport: T,
    opts: Options,
  ) -> Result<Self, Error<F, S, T>> {
    // Validate the options
    opts.validate()?;

    let storage = Arc::new(storage);

    // Make sure we have a valid server address and ID.
    let advertise_addr = transport.advertise_addr();
    let local_addr = transport.local_addr().clone();
    let local_id = transport.local_id().clone();

    let mut initial_state = Self::fetch_initial_state(&storage).await?;

    let ls = storage.log_store();
    if initial_state.is_clean_state() {
      initial_state.current_term = Some(1);
      initial_state.last_log_index = Some(1);
      // If we have no state, then we are a new cluster. We need to
      // initialize the stable storage with the current term.
      storage
        .stable_store()
        .store_current_term(1)
        .await
        .map_err(|e| {
          tracing::error!(target = "ruraft", err=%e, "failed to initialize stable storage");
          Error::stable(e)
        })?;

      ls.store_log(&Log::crate_new(
        1,
        1,
        LogKind::Membership(
          std::iter::once(Server::new(
            local_id.clone(),
            local_addr.clone(),
            ServerSuffrage::Voter,
          ))
          .collect::<Result<_, _>>()
          .map(Arc::new)
          .map_err(Error::membership)?,
        ),
      ))
      .await
      .map_err(|e| {
        tracing::error!(target = "ruraft", err=%e, "failed to append membership entry to log");
        Error::log(e)
      })?;
    }

    // Try to restore the current term.
    let Some(current_term) = initial_state.current_term else {
      tracing::error!(target = "ruraft", err = "failed to load current term");
      return Err(Error::Raft(RaftError::FailedLoadCurrentTerm));
    };

    // Try to restore the last log index.
    let Some(last_log_index) = initial_state.last_log_index else {
      tracing::error!(target = "ruraft", err = "failed to load last log index");
      return Err(Error::Raft(RaftError::FailedLoadLastLogIndex));
    };

    // Get the last log entry.
    let Some(last_log) = (if last_log_index > 0 {
      ls.get_log(last_log_index).await.map_err(|e| {
        tracing::error!(target = "ruraft", err=%e, "failed to get last log at index {}", last_log_index);
        Error::log(e)
      })?
    } else {
      None
    }) else {
      tracing::error!(
        target = "ruraft",
        err = "failed to get last log at index {}",
        last_log_index
      );
      return Err(Error::Raft(RaftError::FailedLoadLastLog));
    };

    // Restore snapshot
    let snp = storage.snapshot_store();

    let RestoredState {
      last_applied,
      last_snapshot,
      mut membership_index,
      mut membership,
    } = Self::restore_snapshot(&fsm, snp, opts.no_snapshot_restore_on_start).await?;

    // Scan through the log for any membership change entries.
    for index in (last_snapshot.index + 1)..=last_log_index {
      match ls.get_log(index).await {
        Ok(Some(entry)) => {
          if let LogKind::Membership(m) = entry.kind {
            membership_index = entry.index;
            membership = m;
          }
        }
        Ok(None) => {}
        Err(e) => {
          tracing::error!(target = "ruraft", index=%index, err=%e, "failed to get log");
          panic!("{e}");
        }
      }
    }
    tracing::info!(target = "ruraft", index = %membership_index, members = %membership, "initial membership");

    // Buffer applyCh to MaxAppendEntries if the option is enabled
    let (apply_tx, apply_rx) = if opts.batch_apply {
      async_channel::bounded(opts.max_append_entries)
    } else {
      async_channel::unbounded()
    };

    let (shutdown_tx, shutdown_rx) = async_channel::bounded(1);

    let (fsm_mutate_tx, fsm_mutate_rx) = async_channel::bounded(128);
    let (fsm_snapshot_tx, fsm_snapshot_rx) = async_channel::unbounded();

    let (membership_change_tx, membership_change_rx) = async_channel::unbounded();

    let membership = Arc::new((membership_index, membership));
    let (committed_membership_tx, committed_membership_rx) = async_channel::bounded(8);

    let (leader_notify_tx, leader_notify_rx) = async_channel::bounded(1);
    let (follower_notify_tx, follower_notify_rx) = async_channel::bounded(1);

    FSMRunner::<F, S, T, R> {
      fsm,
      storage: storage.clone(),
      mutate_rx: fsm_mutate_rx,
      snapshot_rx: fsm_snapshot_rx,
      batching_apply: opts.batch_apply,
      shutdown_rx: shutdown_rx.clone(),
    }
    .spawn();

    let last = Arc::new(parking_lot::Mutex::new(Last {
      log: LastLog::new(last_log.index(), last_log.term()),
      snapshot: last_snapshot,
    }));
    let reloadable_options = Arc::new(Atomic::new(ReloadableOptions::from_options(&opts)));

    let (user_snapshot_tx, user_snapshot_rx) = async_channel::unbounded();

    SnapshotRunner::<F, S, T, R> {
      store: storage.clone(),
      last: last.clone(),
      fsm_snapshot_tx,
      committed_membership_tx,
      user_snapshot_rx,
      opts: reloadable_options.clone(),
      shutdown_rx: shutdown_rx.clone(),
    }
    .spawn();

    let this = Self {
      inner: Arc::new(RaftInner {
        state: State {
          current_term: AtomicU64::new(current_term),
          commit_index: AtomicU64::new(0),
          last_applied: AtomicU64::new(last_applied),
          last,
          role: Atomic::new(Role::Follower),
        },
        storage,
        transport: Arc::new(transport),
        leader: ArcSwapOption::from_pointee(None),
        local: Node::new(local_id, local_addr),
        advertise_addr,
        candidate_from_leadership_transfer: AtomicBool::new(false),
        memberships: Arc::new(Memberships {
          committed: ArcSwap::from(membership.clone()),
          latest: ArcSwap::from(membership),
        }),
        committed_membership_rx,
        membership_change_tx,
        membership_change_rx,
        options: opts,
        reloadable_options,
        reload_options_lock: async_lock::Mutex::new(()),
        apply_tx,
        fsm_mutate_tx,
        user_snapshot_tx,
        leader_notify_tx,
        leader_notify_rx,
        follower_notify_tx,
        follower_notify_rx,
        last_contact: ArcSwapOption::from_pointee(None),
        sidecar: None,
        shutdown_tx,
      }),
    };

    Ok(this)
  }

  async fn fetch_initial_state(s: &S) -> Result<InitialState<T>, Error<F, S, T>> {
    let current_term = s.stable_store().current_term().await.map_err(|e| {
      tracing::error!(target = "ruraft", err=%e, "failed to load current term");
      Error::stable(e)
    })?;

    let last_log_index = s.log_store().last_index().await.map_err(|e| {
      tracing::error!(target = "ruraft", err=%e, "failed to find last log index");
      Error::log(e)
    })?;

    let snapshots = s.snapshot_store().list().await.map_err(Error::snapshot)?;

    Ok(InitialState {
      current_term,
      last_log_index,
      snapshots,
    })
  }

  /// Attempts to restore the latest snapshots, and fails if none
  /// of them can be restored. This is called at initialization time, and is
  /// completely unsafe to call at any other time.
  async fn restore_snapshot(
    fsm: &F,
    snp: &S::Snapshot,
    skip_restore_on_start: bool,
  ) -> Result<RestoredState<T>, Error<F, S, T>> {
    let snapshots = snp.list().await.map_err(|e| {
      tracing::error!(target = "ruraft", err=%e, "failed to list snapshots");
      Error::snapshot(e)
    })?;

    let has_snapshots = !snapshots.is_empty();
    for snapshot in snapshots {
      if Self::try_restore_single_snapshot(&fsm, snp, &snapshot, skip_restore_on_start).await {
        continue;
      }

      return Ok(RestoredState {
        last_applied: snapshot.index(),
        last_snapshot: LastSnapshot::new(snapshot.term(), snapshot.index()),
        membership: snapshot.membership,
        membership_index: snapshot.membership_index,
      });
    }

    // If we had snapshots and failed to load them, its an error
    if has_snapshots {
      return Err(Error::Raft(RaftError::FailedLoadSnapshot));
    }

    Ok(RestoredState {
      last_applied: 0,
      last_snapshot: Default::default(),
      membership_index: 0,
      membership: Default::default(),
    })
  }

  async fn try_restore_single_snapshot(
    fsm: &F,
    s: &S::Snapshot,
    meta: &SnapshotMeta<T::Id, <T::Resolver as AddressResolver>::Address>,
    skip: bool,
  ) -> bool {
    if skip {
      return true;
    }

    let id = meta.id();
    tracing::info!(target = "ruraft.snapshot", id = %id, last_index = %meta.index(), last_term = %meta.term(), size_in_bytes = %meta.size(), "starting restore from snapshot");

    match s.open(&id).await {
      Ok(source) => {
        if let Err(e) =
          FSMRunner::<F, S, T, R>::fsm_restore_and_measure(fsm, source, meta.size()).await
        {
          tracing::error!(target = "ruraft.snapshot", id = %id, last_index = %meta.index(), last_term = %meta.term(), size_in_bytes = %meta.size(), err=%e, "failed to restore snapshot");
          return false;
        }
        tracing::info!(target = "ruraft.snapshot", id = %id, last_index = %meta.index(), last_term = %meta.term(), size_in_bytes = %meta.size(), "restored snapshot");
        true
      }
      Err(e) => {
        tracing::error!(target = "ruraft.snapshot", id = %id, last_index = %meta.index(), last_term = %meta.term(), size_in_bytes = %meta.size(), err=%e, "failed to open snapshot");
        false
      }
    }
  }
}

impl<F, S, T, SC, R> RaftCore<F, S, T, SC, R>
where
  F: FinateStateMachine<
    Id = T::Id,
    Address = <T::Resolver as AddressResolver>::Address,
    Runtime = R,
  >,
  S: Storage<Id = T::Id, Address = <T::Resolver as AddressResolver>::Address, Runtime = R>,
  T: Transport<Runtime = R>,
  <T::Resolver as AddressResolver>::Address: Send + Sync + 'static,
  SC: Sidecar<Runtime = R>,
  R: Runtime,
{
  /// Returns the current state of the reloadable fields in Raft's
  /// options. This is useful for programs to discover the current state for
  /// reporting to users or tests. It is safe to call concurrently. It is
  /// intended for reporting and testing purposes primarily; external
  /// synchronization would be required to safely use this in a read-modify-write
  /// pattern for reloadable options.
  pub fn reloadable_options(&self) -> ReloadableOptions {
    self.inner.reloadable_options.load(Ordering::Acquire)
  }

  /// Returns the current options in use by the Raft instance.
  pub fn options(&self) -> Options {
    self.inner.options.apply(self.reloadable_options())
  }

  /// Updates the options of a running raft node. If the new
  /// options is invalid an error is returned and no changes made to the
  /// instance. All fields will be copied from rc into the new options, even
  /// if they are zero valued.
  pub async fn reload_options(&self, rc: ReloadableOptions) -> Result<(), Error<F, S, T>> {
    rc.validate(self.inner.options.leader_lease_timeout)?;
    let _mu = self.inner.reload_options_lock.lock().await;
    let old = self.inner.reloadable_options.swap(rc, Ordering::Release);

    if rc.heartbeat_timeout() < old.heartbeat_timeout() {
      // On leader, ensure replication loops running with a longer
      // timeout than what we want now discover the change.
      // On follower, update current timer to use the shorter new value.
      let (lres, fres) = futures::future::join(
        self.inner.leader_notify_tx.send(()),
        self.inner.follower_notify_tx.send(()),
      )
      .await;
      if let Err(e) = lres {
        tracing::error!(target = "ruraft", err=%e, "failed to notify leader the options has been changed");
      }

      if let Err(e) = fres {
        tracing::error!(target = "ruraft", err=%e, "failed to notify followers the options has been changed");
      }
    }
    Ok(())
  }
}

// -------------------------------- Private Methods --------------------------------

struct RaftRunner<F, S, T, SC, R>
where
  F: FinateStateMachine<
    Id = T::Id,
    Address = <T::Resolver as AddressResolver>::Address,
    Runtime = R,
  >,
  S: Storage<Id = T::Id, Address = <T::Resolver as AddressResolver>::Address, Runtime = R>,
  T: Transport<Runtime = R>,
  <T::Resolver as AddressResolver>::Address: Send + Sync + 'static,
  SC: Sidecar<Runtime = R>,
  R: Runtime,
{
  inner: Arc<RaftInner<F, S, T, SC, R>>,
  shutdown_rx: async_channel::Receiver<()>,
}

impl<F, S, T, SC, R> core::ops::Deref for RaftRunner<F, S, T, SC, R>
where
  F: FinateStateMachine<
    Id = T::Id,
    Address = <T::Resolver as AddressResolver>::Address,
    Runtime = R,
  >,
  S: Storage<Id = T::Id, Address = <T::Resolver as AddressResolver>::Address, Runtime = R>,
  T: Transport<Runtime = R>,
  <T::Resolver as AddressResolver>::Address: Send + Sync + 'static,
  SC: Sidecar<Runtime = R>,
  R: Runtime,
{
  type Target = RaftInner<F, S, T, SC, R>;

  fn deref(&self) -> &Self::Target {
    &self.inner
  }
}

impl<F, S, T, SC, R> RaftRunner<F, S, T, SC, R>
where
  F: FinateStateMachine<
    Id = T::Id,
    Address = <T::Resolver as AddressResolver>::Address,
    Runtime = R,
  >,
  S: Storage<Id = T::Id, Address = <T::Resolver as AddressResolver>::Address, Runtime = R>,
  T: Transport<Runtime = R>,
  <T::Resolver as AddressResolver>::Address: Send + Sync + 'static,
  SC: Sidecar<Runtime = R>,
  R: Runtime,
{
  async fn run(&self) {
    loop {
      futures::select! {
        _ = self.shutdown_rx.recv().fuse() => {
          tracing::info!(target = "ruraft", "raft runner received shutdown signal, gracefully shutdown...");
          // Clear the leader to prevent forwarding
          self.inner.set_leader(None);
          self.stop_sidecar().await;
          return;
        }
        default => {
          match self.inner.role() {
            Role::Follower => {
              self.spawn_sidecar(Role::Follower);
              // self.run_follower().await;
              self.stop_sidecar().await;
            },
            Role::Candidate => todo!(),
            Role::Leader => todo!(),
            Role::Shutdown => {
              self.spawn_sidecar(Role::Shutdown);
            },
          }
        }
      }
    }
  }

  fn spawn_sidecar(&self, role: Role) {
    if SC::applicable(role) {
      if let Some(ref sidecar) = self.inner.sidecar {
        let sc = sidecar.clone();
        R::spawn_detach(async move {
          if let Err(e) = sc.run(role).await {
            tracing::error!(target = "ruraft", err=%e, "failed to run sidecar");
          }
        });
      }
    }
  }

  async fn stop_sidecar(&self) {
    if let Some(ref sidecar) = self.inner.sidecar {
      if sidecar.is_running() {
        if let Err(e) = sidecar.shutdown().await {
          tracing::error!(target = "ruraft", err=%e, "failed to shutdown sidecar");
        }
      }
    }
  }
}
