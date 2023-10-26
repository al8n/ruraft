use std::{
  collections::HashMap,
  net::SocketAddr,
  sync::{atomic::AtomicBool, Arc},
  time::Instant,
};

use agnostic::Runtime;
use arc_swap::ArcSwapOption;
use atomic::Atomic;
use futures::{channel::oneshot, FutureExt};
use nodecraft::resolver::AddressResolver;

use super::{fsm::FSMRequest, Leader, MembershipChangeRequest};
use crate::{
  error::Error,
  membership::{Membership, Memberships},
  options::{Options, ReloadableOptions},
  sidecar::Sidecar,
  storage::{SnapshotStorage, Storage},
  transport::{RpcConsumer, Transport},
  FinateStateMachine, Node, Role, State,
};

#[cfg(feature = "metrics")]
use crate::metrics::SaturationMetric;

mod candidate;
mod follower;
mod leader;

pub(super) struct RaftRunner<F, S, T, SC, R>
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
{
  pub(super) options: Arc<Options>,
  pub(super) reloadable_options: Arc<Atomic<ReloadableOptions>>,
  pub(super) rpc: RpcConsumer<T::Id, <T::Resolver as AddressResolver>::Address>,
  pub(super) local: Arc<Node<T::Id, <T::Resolver as AddressResolver>::Address>>,
  pub(super) advertise_addr: SocketAddr,
  pub(super) memberships: Arc<Memberships<T::Id, <T::Resolver as AddressResolver>::Address>>,
  pub(super) candidate_from_leadership_transfer: AtomicBool,
  /// last_contact is the last time we had contact from the
  /// leader node. This can be used to gauge staleness.
  pub(super) last_contact: Arc<ArcSwapOption<Instant>>,
  pub(super) leader: Leader<T::Id, <T::Resolver as AddressResolver>::Address>,
  pub(super) state: Arc<State>,
  pub(super) storage: Arc<S>,
  pub(super) transport: T,
  /// The sidecar to run alongside the Raft.
  pub(super) sidecar: Option<Arc<SC>>,
  /// Used to send state-changing updates to the FSM. This
  /// receives pointers to commitTuple structures when applying logs or
  /// pointers to restoreFuture structures when restoring a snapshot. We
  /// need control over the order of these operations when doing user
  /// restores so that we finish applying any old log applies before we
  /// take a user snapshot on the leader, otherwise we might restore the
  /// snapshot and apply old logs to it that were in the pipe.
  pub(super) fsm_mutate_tx: async_channel::Sender<FSMRequest<F, S, T>>,
  /// Used to receive `reloadbale_options` has changed signal when the node is leader
  pub(super) leader_notify_rx: async_channel::Receiver<()>,
  /// Used to receive `reloadbale_options` has changed signal when the node is follower
  pub(super) follower_notify_rx: async_channel::Receiver<()>,
  pub(super) shutdown_rx: async_channel::Receiver<()>,
  pub(super) apply_rx: async_channel::Receiver<super::ApplyRequest<F, Error<F, S, T>>>,
  pub(super) membership_change_rx: async_channel::Receiver<MembershipChangeRequest<F, S, T>>,
  pub(super) committed_membership_rx: async_channel::Receiver<
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
  pub(super) leader_transfer_rx: async_channel::Receiver<(
    Option<Node<T::Id, <T::Resolver as AddressResolver>::Address>>,
    oneshot::Sender<Result<(), Error<F, S, T>>>,
  )>,
  pub(super) verify_rx: async_channel::Receiver<oneshot::Sender<Result<bool, Error<F, S, T>>>>,
  pub(super) user_restore_rx: async_channel::Receiver<(
    <S::Snapshot as SnapshotStorage>::Source,
    oneshot::Sender<Result<(), Error<F, S, T>>>,
  )>,
  pub(super) leader_tx: async_channel::Sender<bool>,

  #[cfg(feature = "metrics")]
  pub(super) saturation_metric: SaturationMetric,
}

impl<F, S, T, SC, R> core::ops::Deref for RaftRunner<F, S, T, SC, R>
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
{
  type Target = State;

  fn deref(&self) -> &Self::Target {
    &self.state
  }
}

impl<F, S, T, SC, R> RaftRunner<F, S, T, SC, R>
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
{
  pub(super) fn spawn(mut self) {
    R::spawn_detach(async move {
      loop {
        futures::select! {
          _ = self.shutdown_rx.recv().fuse() => {
            tracing::info!(target = "ruraft", "raft runner received shutdown signal, gracefully shutdown...");
            // Clear the leader to prevent forwarding
            self.leader.set(None);
            self.stop_sidecar().await;
            return;
          }
          default => {
            match self.state.role() {
              Role::Follower => {
                self.spawn_sidecar(Role::Follower);
                match self.run_follower().await {
                  Ok(true) => self.stop_sidecar().await,
                  Ok(false) | Err(_) => {
                    self.stop_sidecar().await;
                    self.set_role(Role::Shutdown);
                  }
                }
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
    })
  }

  #[inline]
  fn set_last_contact(&self, instant: Instant) {
    self.last_contact.store(Some(Arc::new(instant)));
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

  fn spawn_sidecar(&self, role: Role) {
    if SC::applicable(role) {
      if let Some(ref sidecar) = self.sidecar {
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
    if let Some(ref sidecar) = self.sidecar {
      if sidecar.is_running() {
        if let Err(e) = sidecar.shutdown().await {
          tracing::error!(target = "ruraft", err=%e, "failed to shutdown sidecar");
        }
      }
    }
  }
}
