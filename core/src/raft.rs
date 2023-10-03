use std::{
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
  time::Duration,
};

use agnostic::Runtime;
use arc_swap::ArcSwapOption;
use async_lock::Mutex;
use atomic::Atomic;
use futures::FutureExt;

use crate::{
  error::Error,
  fsm::FinateStateMachine,
  options::{Options, ReloadableOptions},
  sidecar::{NoopSidecar, Sidecar},
  storage::Storage,
  transport::{Address, AddressResolver, Id, RequestConsumer, Transport},
};

mod candidate;
mod follower;
mod fsm;
mod leader;
mod state;
pub use state::*;

const MIN_CHECK_INTERVAL: Duration = Duration::from_millis(10);
const OLDEST_LOG_GAUGE_INTERVAL: Duration = Duration::from_secs(10);

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
  F: FinateStateMachine<Runtime = R>,
  S: Storage<Id = T::Id, Address = <T::Resolver as AddressResolver>::Address, Runtime = R>,
  T: Transport<Runtime = R>,
  SC: Sidecar<Runtime = R>,
  R: Runtime,
{
  state: State,
  /// The client state machine to apply commands to
  fsm: Arc<F>,
  storage: Arc<S>,
  transport: Arc<T>,
  leader: ArcSwapOption<Node<T::Id, <T::Resolver as AddressResolver>::Address>>,
  local: Node<T::Id, <T::Resolver as AddressResolver>::Address>,
  candidate_from_leadership_transfer: AtomicBool,
  /// Stores the initial options to use. This is the most recent one
  /// provided. All reads of config values should use the options() helper method
  /// to read this safely.
  options: Options,
  /// Stores the current reloadable options. This is the most recent one provided.
  reloadable_options: Atomic<ReloadableOptions>,
  /// Ensures that only one thread can reload options at once since
  /// we need to read-modify-write the atomic. It is NOT necessary to hold this
  /// for any other operation e.g. reading config using options().
  reload_options_lock: Mutex<()>,

  /// Used to tell leader that `reloadbale_options` has changed
  leader_notify_tx: async_channel::Sender<()>,
  /// Used to tell followers that `reloadbale_options` has changed
  follower_notify_tx: async_channel::Sender<()>,
  /// The sidecar to run alongside the Raft.
  sidecar: Option<Arc<SC>>,
  _marker: std::marker::PhantomData<R>,
}

impl<F, S, T, SC, R> core::ops::Deref for RaftInner<F, S, T, SC, R>
where
  F: FinateStateMachine<Runtime = R>,
  S: Storage<Id = T::Id, Address = <T::Resolver as AddressResolver>::Address, Runtime = R>,
  T: Transport<Runtime = R>,
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
  F: FinateStateMachine<Runtime = R>,
  S: Storage<Id = T::Id, Address = <T::Resolver as AddressResolver>::Address, Runtime = R>,
  T: Transport<Runtime = R>,
  SC: Sidecar<Runtime = R>,
  R: Runtime,
{
  #[inline]
  fn role(&self) -> Role {
    self.state.role()
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
}

pub struct RaftCore<F, S, T, SC, R>
where
  F: FinateStateMachine<Runtime = R>,
  S: Storage<Id = T::Id, Address = <T::Resolver as AddressResolver>::Address, Runtime = R>,
  T: Transport<Runtime = R>,
  SC: Sidecar<Runtime = R>,
  R: Runtime,
{
  inner: Arc<RaftInner<F, S, T, SC, R>>,
}

impl<F, S, T, SC, R> Clone for RaftCore<F, S, T, SC, R>
where
  F: FinateStateMachine<Runtime = R>,
  S: Storage<Id = T::Id, Address = <T::Resolver as AddressResolver>::Address, Runtime = R>,
  T: Transport<Runtime = R>,
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
  F: FinateStateMachine<Runtime = R>,
  S: Storage<Id = T::Id, Address = <T::Resolver as AddressResolver>::Address, Runtime = R>,
  T: Transport<Runtime = R>,
  R: Runtime,
{
  pub async fn new(_opts: Options) -> Result<Self, Error<F, S, T>> {
    todo!()
  }
}

impl<F, S, T, SC, R> RaftCore<F, S, T, SC, R>
where
  F: FinateStateMachine<Runtime = R>,
  S: Storage<Id = T::Id, Address = <T::Resolver as AddressResolver>::Address, Runtime = R>,
  T: Transport<Runtime = R>,
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
  F: FinateStateMachine<Runtime = R>,
  S: Storage<Id = T::Id, Address = <T::Resolver as AddressResolver>::Address, Runtime = R>,
  T: Transport<Runtime = R>,
  SC: Sidecar<Runtime = R>,
  R: Runtime,
{
  inner: Arc<RaftInner<F, S, T, SC, R>>,
  shutdown_rx: async_channel::Receiver<()>,
}

impl<F, S, T, SC, R> core::ops::Deref for RaftRunner<F, S, T, SC, R>
where
  F: FinateStateMachine<Runtime = R>,
  S: Storage<Id = T::Id, Address = <T::Resolver as AddressResolver>::Address, Runtime = R>,
  T: Transport<Runtime = R>,
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
  F: FinateStateMachine<Runtime = R>,
  S: Storage<Id = T::Id, Address = <T::Resolver as AddressResolver>::Address, Runtime = R>,
  T: Transport<Runtime = R>,
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
