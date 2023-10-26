use std::{
  collections::HashMap,
  net::SocketAddr,
  sync::{atomic::{AtomicBool, Ordering}, Arc},
  time::Instant,
};

use agnostic::Runtime;
use arc_swap::ArcSwapOption;
use atomic::Atomic;
use futures::{channel::oneshot, FutureExt};
use nodecraft::resolver::AddressResolver;
use wg::AsyncWaitGroup;

use super::{fsm::FSMRequest, Leader, MembershipChangeRequest, state::LastLog};
use crate::{
  error::Error,
  membership::{Membership, Memberships},
  options::{Options, ReloadableOptions},
  sidecar::Sidecar,
  storage::{SnapshotStorage, Storage, LogStorage},
  transport::{RpcConsumer, Transport, AppendEntriesRequest, AppendEntriesResponse, Request, Response},
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
  pub(super) transport: Arc<T>,
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

  pub(super) wg: AsyncWaitGroup,
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
  <R::Interval as futures::Stream>::Item: Send + 'static,
{
  pub(super) fn spawn(mut self) {
    super::spawn_local::<R, _>(self.wg.add(1), async move {
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
              Role::Candidate => {
                self.spawn_sidecar(Role::Candidate);
                match self.run_candidate().await {
                  Ok(true) => self.stop_sidecar().await,
                  Ok(false) | Err(_) => {
                    self.stop_sidecar().await;
                    self.set_role(Role::Shutdown);
                  }
                }
              },
              Role::Leader => todo!(),
              Role::Shutdown => {
                self.spawn_sidecar(Role::Shutdown);
              },
            }
          }
        }
      }
    });
  }

  #[inline]
  fn set_last_contact(&self, instant: Instant) {
    self.last_contact.store(Some(Arc::new(instant)));
  }

  async fn handle_request(
    &self,
    tx: oneshot::Sender<Response<T::Id, <T::Resolver as AddressResolver>::Address>>,
    req: Request<T::Id, <T::Resolver as AddressResolver>::Address>,
  ) {
    // TODO: validate the request header
    match req {
      Request::AppendEntries(req) => self.handle_append_entries(tx, req).await,
      Request::Vote(_) => todo!(),
      Request::InstallSnapshot(_) => todo!(),
      Request::TimeoutNow(_) => todo!(),
      Request::Heartbeat(_) => todo!(),
    }
  }

  async fn handle_append_entries(
    &self,
    tx: oneshot::Sender<Response<T::Id, <T::Resolver as AddressResolver>::Address>>,
    mut req: AppendEntriesRequest<T::Id, <T::Resolver as AddressResolver>::Address>,
  ) {
    // TODO: defer metrics.MeasureSince([]string{"raft", "rpc", "appendEntries"}, time.Now())

    macro_rules! respond {
      ($tx:ident.send($resp:ident)) => {
        if $tx.send(Response::append_entries($resp)).is_err() {
          tracing::error!(
            target = "ruraft.follower",
            err = "channel closed",
            "failed to respond to append entries request"
          );
        }
      };
    }

    let protocol_version = self.options.protocol_version;

    // Setup a response
    let mut resp = AppendEntriesResponse::new(
      protocol_version,
      self.local.id().clone(),
      self.local.addr().clone(),
    )
    .with_term(self.current_term())
    .with_last_log(self.last_index());

    // Ignore an older term
    if req.term < self.current_term() {
      respond!(tx.send(resp));
      return;
    }

    // Increase the term if we see a newer one, also transition to follower
    // if we ever get an appendEntries call
    if req.term > self.current_term()
      || (self.role() != Role::Follower
        && !self
          .candidate_from_leadership_transfer
          .load(Ordering::Acquire))
    {
      // Ensure transition to follower
      self.set_role(Role::Follower);
      self.set_current_term(req.term);
      resp.term = req.term;
    }

    // Save the current leader
    self
      .leader
      .set(Some(Node::new(req.header.id.clone(), req.header.addr)));

    // Verify the last log entry
    if req.prev_log_entry > 0 {
      let last = self.last_entry();
      let prev_log_term = if req.prev_log_entry == last.index {
        last.term
      } else {
        match self.storage.log_store().get_log(req.prev_log_entry).await {
          Ok(prev_log) => prev_log.term,
          Err(e) => {
            tracing::warn!(target = "ruraft.follower", previous_index = %req.prev_log_entry, last_index = %last.index, err=%e, "failed to get previous log");
            resp.no_retry_backoff = true;
            respond!(tx.send(resp));
            return;
          }
        }
      };

      if req.prev_log_term != prev_log_term {
        tracing::warn!(
          target = "ruraft.follower",
          "prev log term mismatch (local: {}, remote: {})",
          prev_log_term,
          req.prev_log_term
        );

        resp.no_retry_backoff = true;

        respond!(tx.send(resp));
        return;
      }
    }

    // Process any new entries
    if !req.entries.is_empty() {
      #[cfg(feature = "metrics")]
      let start = Instant::now();

      // Delete any conflicting entries, skip any duplicates
      let last_log = self.last_log();

      req.entries.sort_by(|a, b| a.index.cmp(&b.index));

      let mut pos = 0;

      let ls = self.storage.log_store();
      for (idx, entry) in req.entries.iter().enumerate() {
        let ent_idx = entry.index();
        if ent_idx > last_log.index {
          pos = idx;
          break;
        }

        match ls.get_log(ent_idx).await {
          Ok(stored_entry) => {
            if entry.term != stored_entry.term {
              tracing::warn!(target = "ruraft.follower", from=%ent_idx, to=%last_log.index, "clearing log suffix");
              if let Err(e) = ls.remove_range(ent_idx..=last_log.index).await {
                tracing::warn!(target = "ruraft.follower", from=%ent_idx, to=%last_log.index, err=%e, "failed to clear log suffix");
                resp.no_retry_backoff = true;
                respond!(tx.send(resp));
                return;
              }
              if ent_idx <= self.memberships.latest().0 {
                self
                  .memberships
                  .latest
                  .store(self.memberships.committed().clone());
              }
              pos = idx;
              break;
            }
          }
          Err(e) => {
            tracing::warn!(target = "ruraft.follower", index=%ent_idx, err=%e, "failed to get log entry");
            respond!(tx.send(resp));
            return;
          }
        }
      }

      if !req.entries[pos..].is_empty() {
        // Append the new entries
        if let Err(e) = ls.store_logs(&req.entries[pos..]).await {
          tracing::error!(target = "ruraft.follower", err=%e, "failed to append to logs");
          respond!(tx.send(resp));
          return;
        }

        let last = req.entries.last().unwrap();
        let last_log = LastLog::new(last.index, last.term);

        // Handle any new membership changes
        for entry in req.entries.drain(pos..) {
          if entry.is_membership() {
            //TODO: handle membership changes
            self.process_membership_log(entry);
          }
        }

        // Update the lastLog
        self.set_last_log(last_log);
      }

      #[cfg(feature = "metrics")]
      metrics::histogram!(
        "ruraft.follower.append_entries",
        start.elapsed().as_millis() as f64
      );
    }

    // Update the commit index
    if req.leader_commit > 0 && req.leader_commit > self.commit_index() {
      #[cfg(feature = "metrics")]
      let start = Instant::now();
      let idx = req.leader_commit.min(self.last_index());
      self.set_commit_index(idx);
      let latest = self.memberships.latest();
      if latest.0 <= idx {
        self.memberships.committed.store(latest.clone());
      }

      self.process_logs(idx, None).await;

      #[cfg(feature = "metrics")]
      metrics::histogram!(
        "ruraft.follower.commit_index",
        start.elapsed().as_millis() as f64
      );
    }

    // Everything went well, set success
    resp.success = true;
    self.set_last_contact(Instant::now());
    respond!(tx.send(resp));
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
