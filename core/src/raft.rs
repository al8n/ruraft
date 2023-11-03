use std::{
  borrow::Cow,
  collections::HashMap,
  fmt::Display,
  future::Future,
  sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
  },
  time::Instant,
};

use agnostic::Runtime;
use arc_swap::{ArcSwap, ArcSwapOption};
use async_lock::Mutex;
use atomic::Atomic;
use futures::channel::oneshot;
use wg::AsyncWaitGroup;

use crate::{
  error::{Error, RaftError},
  fsm::FinateStateMachine,
  membership::{Membership, Memberships, Server, ServerSuffrage},
  options::{Options, ReloadableOptions},
  raft::{fsm::FSMRunner, runner::RaftRunner, snapshot::SnapshotRunner},
  sidecar::{NoopSidecar, Sidecar},
  storage::{
    Log, LogKind, LogStorage, SnapshotMeta, SnapshotStorage, StableStorage, Storage, StorageError,
  },
  transport::{Address, AddressResolver, Id, Transport},
  FinateStateMachineError, FinateStateMachineLog, FinateStateMachineLogKind,
  FinateStateMachineSnapshot,
};

#[cfg(feature = "metrics")]
use crate::metrics::SaturationMetric;

mod api;
pub use api::*;

mod fsm;

mod observer;
pub use observer::*;

mod runner;

mod snapshot;
use snapshot::{CountingReader, SnapshotRestoreMonitor};

mod state;
pub use state::*;

#[derive(Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Node<I, A>(Arc<(I, A)>);

impl<I, A> Clone for Node<I, A> {
  fn clone(&self) -> Self {
    Self(self.0.clone())
  }
}

impl<I: Display, A: Display> core::fmt::Display for Node<I, A> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}({})", self.0 .0, self.0 .1)
  }
}

impl<I, A> Node<I, A> {
  /// Returns the id of the leader.
  #[inline]
  pub fn id(&self) -> &I {
    &self.0 .0
  }

  /// Returns the address of the leader.
  #[inline]
  pub fn addr(&self) -> &A {
    &self.0 .1
  }

  #[inline]
  pub fn new(id: I, addr: A) -> Self {
    Self(Arc::new((id, addr)))
  }
}

#[derive(Clone)]
#[repr(transparent)]
struct Contact(Arc<ArcSwap<Instant>>);

impl Default for Contact {
  #[inline]
  fn default() -> Self {
    Self::now()
  }
}

impl Contact {
  #[inline]
  fn now() -> Self {
    Self(Arc::new(ArcSwap::from_pointee(Instant::now())))
  }

  #[inline]
  fn update(&self) {
    self.0.store(Arc::new(Instant::now()));
  }

  #[inline]
  fn get(&self) -> Instant {
    **self.0.load()
  }
}

#[derive(Clone)]
#[repr(transparent)]
struct OptionalContact(Arc<ArcSwapOption<Instant>>);

impl OptionalContact {
  #[inline]
  fn none() -> Self {
    Self(Arc::new(ArcSwapOption::from_pointee(None)))
  }

  #[inline]
  fn update(&self) {
    self.0.store(Some(Arc::new(Instant::now())));
  }

  #[inline]
  fn get(&self) -> Option<Instant> {
    self.0.load().as_ref().map(|x| **x)
  }
}

struct Leader<I: Id, A: Address>(Arc<ArcSwapOption<Node<I, A>>>);

impl<I: Id, A: Address> Clone for Leader<I, A> {
  fn clone(&self) -> Self {
    Self(self.0.clone())
  }
}

impl<I: Id, A: Address> Leader<I, A> {
  #[inline]
  fn none() -> Self {
    Self(Arc::new(ArcSwapOption::new(None)))
  }

  async fn set(
    &self,
    leader: Option<Node<I, A>>,
    observers: &async_lock::RwLock<HashMap<ObserverId, Observer<I, A>>>,
  ) {
    let new = leader.map(Arc::new);
    let old = self.0.swap(new.clone());
    match (new, old) {
      (None, None) => {}
      (None, Some(_)) => {
        observe(observers, Observed::Leader(None)).await;
      }
      (Some(new), None) => {
        observe(observers, Observed::Leader(Some(new.as_ref().clone()))).await;
      }
      (Some(new), Some(old)) => {
        if old.addr() != new.addr() || old.id() != new.id() {
          observe(observers, Observed::Leader(Some(new.as_ref().clone()))).await;
        }
      }
    }
  }

  #[inline]
  fn load(&self) -> arc_swap::Guard<Option<Arc<Node<I, A>>>> {
    self.0.load()
  }
}

struct Shutdown {
  shutdown_tx: async_channel::Sender<()>,
  /// Used to prevent concurrent shutdown
  shutdown: AtomicBool,

  lock: async_lock::RwLock<()>,

  wg: AsyncWaitGroup,
}

impl Shutdown {
  #[inline]
  fn new(shutdown_tx: async_channel::Sender<()>, wg: AsyncWaitGroup) -> Self {
    Self {
      shutdown_tx,
      shutdown: AtomicBool::new(false),
      lock: async_lock::RwLock::new(()),
      wg,
    }
  }

  #[inline]
  fn is_shutdown(&self) -> bool {
    self.shutdown.load(Ordering::Acquire)
  }

  async fn shutdown<I: Id, A: Address>(
    &self,
    state: &State,
    observers: &async_lock::RwLock<HashMap<ObserverId, Observer<I, A>>>,
  ) -> bool {
    self.shutdown.store(true, Ordering::Release);
    let closed = self.shutdown_tx.close();
    let _mu = self.lock.write().await;
    state.set_role(Role::Shutdown, observers).await;
    self.wg.wait().await;
    closed
  }
}

struct Inner<F, S, T, SC, R>
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
  leader: Leader<T::Id, <T::Resolver as AddressResolver>::Address>,
  state: Arc<State>,
  /// last_contact is the last time we had contact from the
  /// leader node. This can be used to gauge staleness.
  last_contact: OptionalContact,
  memberships: Arc<Memberships<T::Id, <T::Resolver as AddressResolver>::Address>>,
  /// Used to prevent concurrent shutdown
  shutdown: Arc<Shutdown>,
  transport: Arc<T>,

  /// Stores the initial options to use. This is the most recent one
  /// provided. All reads of config values should use the options() helper method
  /// to read this safely.
  options: Arc<Options>,
  /// Stores the current reloadable options. This is the most recent one provided.
  reloadable_options: Arc<Atomic<ReloadableOptions>>,
  /// Ensures that only one thread can reload options at once since
  /// we need to read-modify-write the atomic. It is NOT necessary to hold this
  /// for any other operation e.g. reading config using options().
  reload_options_lock: Mutex<()>,

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

  user_restore_tx: async_channel::Sender<(
    <S::Snapshot as SnapshotStorage>::Source,
    oneshot::Sender<Result<(), Error<F, S, T>>>,
  )>,

  fsm_mutate_tx: async_channel::Sender<fsm::FSMRequest<F, S, T>>,

  apply_tx: async_channel::Sender<ApplyRequest<F, Error<F, S, T>>>,

  /// Used to request the leader to make membership changes.
  membership_change_tx: async_channel::Sender<MembershipChangeRequest<F, S, T>>,

  /// Used to tell leader that `reloadbale_options` has changed
  leader_notify_tx: async_channel::Sender<()>,

  /// Used to tell followers that `reloadbale_options` has changed
  follower_notify_tx: async_channel::Sender<()>,

  leader_transfer_tx: async_channel::Sender<(
    Option<Node<T::Id, <T::Resolver as AddressResolver>::Address>>,
    oneshot::Sender<Result<(), Error<F, S, T>>>,
  )>,
  verify_tx: async_channel::Sender<oneshot::Sender<Result<(), Error<F, S, T>>>>,

  leader_rx: async_channel::Receiver<bool>,
  leadership_change_rx: async_channel::Receiver<bool>,
  sidecar: Option<Arc<SC>>,
  observers: Arc<
    async_lock::RwLock<
      HashMap<ObserverId, Observer<T::Id, <T::Resolver as AddressResolver>::Address>>,
    >,
  >,
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
  inner: Arc<Inner<F, S, T, SC, R>>,
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
  <R::Interval as futures::Stream>::Item: Send + 'static,
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
    Self::new_in(fsm, storage, transport, None, opts).await
  }
}

impl<F, S, T, SC, R> RaftCore<F, S, T, SC, R>
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
  SC: Sidecar<Runtime = R>,
  R: Runtime,
  <R::Sleep as std::future::Future>::Output: Send,
  <R::Interval as futures::Stream>::Item: Send + 'static,
{
  /// Used to construct a new Raft node with sidecar. It takes a options, as well
  /// as implementations of various traits that are required.
  ///
  /// **N.B.**
  /// - If there is no old state, then will initialize a new Raft cluster which contains only one voter node(self), users can then invoke `add_voter` on it to add other servers to the cluster.
  /// - If there are any
  /// old state, such as snapshots, logs, peers, etc, all those will be restored
  /// when creating the Raft node.
  pub async fn with_sidecar(
    fsm: F,
    storage: S,
    transport: T,
    sidecar: SC,
    opts: Options,
  ) -> Result<Self, Error<F, S, T>> {
    Self::new_in(fsm, storage, transport, Some(sidecar), opts).await
  }

  /// `recover` is used to manually force a new membership in order to
  /// recover from a loss of quorum where the current membership cannot be
  /// restored, such as when several servers die at the same time. This works by
  /// reading all the current state for this server, creating a snapshot with the
  /// supplied membership, and then truncating the Raft log. This is the only
  /// safe way to force a given membership without actually altering the log to
  /// insert any new entries, which could cause conflicts with other servers with
  /// different state.
  ///
  /// **WARNING!** This operation implicitly commits all entries in the Raft log, so
  /// in general this is an extremely unsafe operation. If you've lost your other
  /// servers and are performing a manual recovery, then you've also lost the
  /// commit information, so this is likely the best you can do, but you should be
  /// aware that calling this can cause Raft log entries that were in the process
  /// of being replicated but not yet be committed to be committed.
  ///
  /// Note the [`FinateStateMachine`] passed here is used for the snapshot operations and will be
  /// left in a state that should not be used by the application. Be sure to
  /// discard this [`FinateStateMachine`] and any associated state and provide a fresh one when
  /// calling `new` or `with_sidecar` later.
  ///
  /// A typical way to recover the cluster is to shut down all servers and then
  /// run RecoverCluster on every server using an identical membership. When
  /// the cluster is then restarted, and election should occur and then Raft will
  /// resume normal operation. If it's desired to make a particular server the
  /// leader, this can be used to inject a new membership with that server as
  /// the sole voter, and then join up other new clean-state peer servers using
  /// the usual APIs in order to bring the cluster back into a known state.
  pub async fn recover(
    fsm: F,
    storage: S,
    membership: Membership<T::Id, <T::Resolver as AddressResolver>::Address>,
    opts: Options,
  ) -> Result<(), Error<F, S, T>> {
    // Validate the options
    opts.validate()?;

    // Sanity check the Raft peer configuration.
    membership
      .validate()
      .map_err(|e| Error::Raft(RaftError::Membership(e)))?;

    // Refuse to recover if there's no existing state. This would be safe to
    // do, but it is likely an indication of an operator error where they
    // expect data to be there and it's not. By refusing, we force them
    // to show intent to start a cluster fresh by explicitly doing a
    // bootstrap, rather than quietly fire up a fresh cluster here.
    let initial_state = Self::fetch_initial_state(&storage).await?;
    if initial_state.is_clean_state() {
      return Err(Error::Raft(RaftError::NoExistingState));
    }

    // Attempt to restore any snapshots we find, newest to oldest.
    let ss = storage.snapshot_store();
    let snaps = ss.list().await.map_err(|e| {
      Error::storage(
        <S::Error as StorageError>::snapshot(e)
          .with_message(Cow::Borrowed("failed to list snapshots")),
      )
    })?;
    let mut snapshot_index = 0;
    let mut snapshot_term = 0;
    let num_snapshots = snaps.len();

    for snap in snaps {
      let Ok(source) = ss.open(&snap.id()).await else {
        // Skip this one and try the next. We will detect if we
        // couldn't open any snapshots.
        continue;
      };

      // Note this is the one place we call fsm.Restore without the
      // fsmRestoreAndMeasure wrapper since this function should only be called to
      // reset state on disk and the FSM passed will not be used for a running
      // server instance. If the same process will eventually become a Raft peer
      // then it will call NewRaft and restore again from disk then which will
      // report metrics.
      let cr = CountingReader::from(source);
      let ctr = cr.ctr();
      let monitor = SnapshotRestoreMonitor::<R>::new(ctr, snap.size, false);
      let rst = fsm.restore(cr).await;
      monitor.stop_and_wait().await;
      match rst {
        Ok(_) => {
          snapshot_index = snap.index;
          snapshot_term = snap.term;
          break;
        }
        Err(_) => {
          // Same here, skip and try the next one.
          continue;
        }
      }
    }
    if num_snapshots > 0 && (snapshot_index == 0 || snapshot_term == 0) {
      return Err(Error::Raft(RaftError::FailedRestoreSnapshots));
    }

    let logs = storage.log_store();
    let mut last_index = snapshot_index;
    let mut last_term = snapshot_term;
    // Apply any Raft log entries past the snapshot.
    let Some(last_log_index) = logs.last_index().await.map_err(|e| {
      Error::storage(
        <S::Error as StorageError>::log(e).with_message(Cow::Borrowed("failed to find last log")),
      )
    })?
    else {
      return Err(Error::Raft(RaftError::FailedLoadLogIndex));
    };

    for index in (snapshot_index + 1)..=last_log_index {
      let Some(entry) = logs.get_log(index).await.map_err(|e| {
        Error::storage(
          <S::Error as StorageError>::log(e)
            .with_message(Cow::Owned(format!("failed to get log at index at {index}"))),
        )
      })?
      else {
        return Err(Error::log_not_found(index));
      };

      if let LogKind::User { data, extension } = entry.kind {
        fsm
          .apply(FinateStateMachineLog::new(
            entry.term,
            entry.index,
            FinateStateMachineLogKind::Log { data, extension },
          ))
          .await
          .map_err(|e| {
            Error::fsm(e.with_message(Cow::Owned(format!("failed to apply log at index {index}"))))
          })?;
      }

      last_index = entry.index;
      last_term = entry.term;
    }

    // Create a new snapshot, placing the configuration in as if it was
    // committed at index 1.
    let snapshot = fsm.snapshot().await.map_err(|e| {
      Error::fsm(e.with_message(Cow::Borrowed("failed to snapshot finate state machine")))
    })?;

    let sink = ss
      .create(
        Default::default(),
        last_term,
        last_index,
        Arc::new(membership),
        1,
      )
      .await
      .map_err(|e| {
        Error::storage(
          <S::Error as StorageError>::snapshot(e)
            .with_message(Cow::Borrowed("failed to create snapshot")),
        )
      })?;

    snapshot.persist(sink).await.map_err(|e| {
      Error::fsm(
        <F::Error as FinateStateMachineError>::snapshot(e)
          .with_message(Cow::Borrowed("failed to persist snapshot")),
      )
    })?;

    // Compact the log so that we don't get bad interference from any
    // membership change log entries that might be there.
    let first_log_index = match logs.first_index().await {
      Err(e) => {
        return Err(Error::storage(
          <S::Error as StorageError>::log(e).with_message(Cow::Borrowed("failed to get first log")),
        ));
      }
      Ok(None) => {
        return Err(Error::Raft(RaftError::FailedLoadLogIndex));
      }
      Ok(Some(index)) => index,
    };

    logs
      .remove_range(first_log_index..=last_log_index)
      .await
      .map_err(|e| {
        Error::storage(
          <S::Error as StorageError>::log(e).with_message(Cow::Borrowed("log compaction failed")),
        )
      })
  }

  /// Returns the sidecar, if any.
  #[inline]
  pub fn sidecar(&self) -> Option<&SC> {
    self.inner.sidecar.as_deref()
  }
}

impl<F, S, T, SC, R> RaftCore<F, S, T, SC, R>
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
  SC: Sidecar<Runtime = R>,
  R: Runtime,
  <R::Sleep as std::future::Future>::Output: Send,
  <R::Interval as futures::Stream>::Item: Send + 'static,
{
  async fn new_in(
    fsm: F,
    storage: S,
    transport: T,
    sidecar: Option<SC>,
    opts: Options,
  ) -> Result<Self, Error<F, S, T>> {
    // Validate the options
    opts.validate()?;

    let storage = Arc::new(storage);

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
      return Err(Error::Raft(RaftError::FailedLoadLogIndex));
    };

    // Get the last log entry.
    let Some(last_log) = ls.get_log(last_log_index).await.map_err(|e| {
      tracing::error!(target = "ruraft", err=%e, "failed to get last log at index {}", last_log_index);
      Error::log(e)
    })? else {
      tracing::error!(target = "ruraft", index = %last_log_index, "last log not found");
      return Err(Error::log_not_found(last_log_index));
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
        Ok(None) => {
          tracing::error!(target = "ruraft", index=%index, err=%Error::<F, S, T>::log_not_found(index), "log entry not found");
          return Err(Error::log_not_found(index));
        }
        Err(e) => {
          tracing::error!(target = "ruraft", index=%index, err=%e, "failed to get log");
          panic!("failed to get log: {e}");
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

    let last = Arc::new(parking_lot::Mutex::new(Last {
      log: LastLog::new(last_log.index(), last_log.term()),
      snapshot: last_snapshot,
    }));
    let reloadable_options = Arc::new(Atomic::new(ReloadableOptions::from_options(&opts)));

    let (user_snapshot_tx, user_snapshot_rx) = async_channel::unbounded();
    let (user_restore_tx, user_restore_rx) = async_channel::unbounded();
    let (leader_transfer_tx, leader_transfer_rx) = async_channel::bounded(1);
    let (verify_tx, verify_rx) = async_channel::bounded(64);
    let (leader_tx, leader_rx) = async_channel::bounded(1);
    let (leadership_change_tx, leadership_change_rx) = async_channel::unbounded();
    let state = Arc::new(State {
      current_term: AtomicU64::new(current_term),
      commit_index: Arc::new(AtomicU64::new(0)),
      last_applied: AtomicU64::new(last_applied),
      last: last.clone(),
      role: Atomic::new(Role::Follower),
    });
    let leader = Leader::none();
    let sidecar = sidecar.map(Arc::new);
    let last_contact = OptionalContact::none();
    let memberships = Arc::new(Memberships {
      committed: ArcSwap::from(membership.clone()),
      latest: ArcSwap::from(membership),
    });
    let options = Arc::new(opts);
    let wg = AsyncWaitGroup::new();
    let transport = Arc::new(transport);
    let observers = Arc::new(async_lock::RwLock::new(HashMap::new()));
    let shutdown = Arc::new(Shutdown::new(shutdown_tx, wg.clone()));
    RaftRunner::<F, S, T, SC, R> {
      options: options.clone(),
      reloadable_options: reloadable_options.clone(),
      memberships: memberships.clone(),
      rpc: transport.consumer(),
      candidate_from_leadership_transfer: AtomicBool::new(false),
      leader: leader.clone(),
      last_contact: last_contact.clone(),
      state: state.clone(),
      storage: storage.clone(),
      transport: transport.clone(),
      sidecar: sidecar.clone(),
      shutdown_rx: shutdown_rx.clone(),
      fsm_mutate_tx: fsm_mutate_tx.clone(),
      leader_notify_rx,
      follower_notify_rx,
      apply_rx,
      membership_change_rx,
      committed_membership_rx,
      leader_transfer_rx,
      leader_tx,
      leader_rx: leader_rx.clone(),
      leadership_change_tx,
      verify_rx,
      user_restore_rx,
      observers: observers.clone(),
      shutdown: shutdown.clone(),
      wg: wg.clone(),
    }
    .spawn(
      #[cfg(feature = "metrics")]
      SaturationMetric::new("ruraft.runner", std::time::Duration::from_secs(1)),
    );

    FSMRunner::<F, S, T, R> {
      fsm,
      storage: storage.clone(),
      mutate_rx: fsm_mutate_rx,
      snapshot_rx: fsm_snapshot_rx,
      wg: wg.clone(),
      shutdown_rx: shutdown_rx.clone(),
    }
    .spawn();

    SnapshotRunner::<F, S, T, R> {
      store: storage,
      state: state.clone(),
      fsm_snapshot_tx,
      committed_membership_tx,
      user_snapshot_rx,
      opts: reloadable_options.clone(),
      wg,
      shutdown_rx,
    }
    .spawn();

    let this = Inner {
      memberships,
      options,
      reloadable_options,
      reload_options_lock: async_lock::Mutex::new(()),
      leader,
      last_contact,
      sidecar,
      state,
      shutdown,
      transport,
      membership_change_tx,
      apply_tx,
      user_snapshot_tx,
      user_restore_tx,
      fsm_mutate_tx,
      leader_notify_tx,
      follower_notify_tx,
      leader_transfer_tx,
      verify_tx,
      leader_rx,
      leadership_change_rx,
      observers,
    };

    Ok(Self {
      inner: Arc::new(this),
    })
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
      if Self::try_restore_single_snapshot(fsm, snp, &snapshot, skip_restore_on_start).await {
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

pub(crate) fn spawn_local<R: Runtime, F: std::future::Future + Send + 'static>(
  wg: AsyncWaitGroup,
  f: F,
) {
  R::spawn_detach(async move {
    f.await;
    wg.done();
  });
}
