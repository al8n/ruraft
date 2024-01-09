use std::{
  borrow::Cow,
  collections::HashMap,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
  time::Instant,
};

use agnostic::Runtime;
use atomic::Atomic;
use futures::{channel::oneshot, AsyncWriteExt, FutureExt};
use nodecraft::{resolver::AddressResolver, Address, Id};
use smallvec::SmallVec;
use wg::AsyncWaitGroup;

use super::{
  api::ApplySender, fsm::FSMRequest, state::LastLog, CountingSnapshotSourceReader, Leader,
  MembershipChangeRequest, Observer, ObserverId, OptionalContact, Shutdown,
};
use crate::{
  error::Error,
  membership::{Membership, Memberships},
  observer::{observe, Observation},
  options::{Options, ReloadableOptions},
  options::{ProtocolVersion, SnapshotVersion},
  raft::snapshot::SnapshotRestoreMonitor,
  sidecar::Sidecar,
  storage::{
    compact_logs, remove_old_logs, Log, LogKind, LogStorage, SnapshotMeta, SnapshotSink, SnapshotSinkExt,
    SnapshotStorage, StableStorage, Storage, StorageError,
  },
  transport::{
    AppendEntriesRequest, AppendEntriesResponse, ErrorResponse, Header, HeartbeatRequest,
    HeartbeatResponse, InstallSnapshotRequest, InstallSnapshotResponse, Request, Response,
    RpcConsumer, RpcResponseSender, TimeoutNowRequest, TimeoutNowResponse, Transport, VoteRequest,
    VoteResponse,
  },
  FinateStateMachine, LastSnapshot, Node, Role, State,
};

#[cfg(feature = "metrics")]
use crate::metrics::SaturationMetric;

mod candidate;
mod follower;
mod leader;

/// Heartbeat RPC handler, can be used to handle heartbeat requests in a fast-path manner.
pub(super) struct DefaultHeartbeatHandler<I, A> {
  pub(super) state: Arc<State>,
  pub(super) last_contact: OptionalContact,
  pub(super) shutdown_rx: async_channel::Receiver<()>,
  pub(super) candidate_from_leadership_transfer: Arc<AtomicBool>,
  pub(super) observers: Arc<async_lock::RwLock<HashMap<ObserverId, Observer<I, A>>>>,
}

impl<I, A> DefaultHeartbeatHandler<I, A>
where
  I: Id,
  A: Address,
{
  /// Handle a heartbeat request in a fast-path manner.
  pub async fn handle_heartbeat(
    &self,
    header: Header<I, A>,
    req: HeartbeatRequest<I, A>,
    sender: RpcResponseSender<I, A>,
  ) {
    #[cfg(feature = "metrics")]
    let start = Instant::now();
    #[cfg(feature = "metrics")]
    scopeguard::defer!({
      let h = metrics::histogram!("ruraft.rpc.process_heartbeat");
      h.record(start.elapsed().as_millis() as f64);
    });

    handle_heartbeat_request(
      header,
      &self.state,
      &self.shutdown_rx,
      &self.last_contact,
      &self.candidate_from_leadership_transfer,
      &self.observers,
      sender.into(),
      req,
    )
    .await
  }
}

pub(super) struct RaftRunner<F, S, T, SC, R>
where
  F: FinateStateMachine<
    Id = T::Id,
    Address = <T::Resolver as AddressResolver>::Address,
    Data = T::Data,
    Runtime = R,
  >,
  S: Storage<
    Id = T::Id,
    Address = <T::Resolver as AddressResolver>::Address,
    Data = T::Data,
    Runtime = R,
  >,
  T: Transport<Runtime = R>,

  SC: Sidecar<Runtime = R>,
  R: Runtime,
{
  pub(super) options: Arc<Options>,
  pub(super) reloadable_options: Arc<Atomic<ReloadableOptions>>,
  pub(super) rpc: RpcConsumer<T::Id, <T::Resolver as AddressResolver>::Address, T::Data>,
  pub(super) memberships: Arc<Memberships<T::Id, <T::Resolver as AddressResolver>::Address>>,
  pub(super) candidate_from_leadership_transfer: Arc<AtomicBool>,
  /// last_contact is the last time we had contact from the
  /// leader node. This can be used to gauge staleness.
  pub(super) last_contact: OptionalContact,
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
  pub(super) shutdown: Arc<Shutdown>,
  pub(super) shutdown_rx: async_channel::Receiver<()>,
  pub(super) apply_rx: async_channel::Receiver<super::ApplyRequest<F, Error<F, S, T>>>,
  pub(super) membership_change_rx: async_channel::Receiver<MembershipChangeRequest<F, S, T>>,
  pub(super) committed_membership_rx: async_channel::Receiver<
    oneshot::Sender<
      Result<
        Arc<(
          u64,
          Membership<T::Id, <T::Resolver as AddressResolver>::Address>,
        )>,
        Error<F, S, T>,
      >,
    >,
  >,
  pub(super) leader_transfer_rx: async_channel::Receiver<(
    Option<Node<T::Id, <T::Resolver as AddressResolver>::Address>>,
    oneshot::Sender<Result<(), Error<F, S, T>>>,
  )>,
  pub(super) verify_rx: async_channel::Receiver<oneshot::Sender<Result<(), Error<F, S, T>>>>,
  pub(super) user_restore_rx: async_channel::Receiver<(
    (
      SnapshotMeta<T::Id, <T::Resolver as AddressResolver>::Address>,
      Box<dyn futures::AsyncRead + Unpin + Send + Sync + 'static>,
    ),
    oneshot::Sender<Result<(), Error<F, S, T>>>,
  )>,
  pub(super) leader_tx: async_channel::Sender<bool>,
  pub(super) leader_rx: async_channel::Receiver<bool>,
  pub(super) leadership_change_tx: async_channel::Sender<bool>,
  pub(super) observers: Arc<
    async_lock::RwLock<
      HashMap<ObserverId, Observer<T::Id, <T::Resolver as AddressResolver>::Address>>,
    >,
  >,
  pub(super) wg: AsyncWaitGroup,
}

impl<F, S, T, SC, R> core::ops::Deref for RaftRunner<F, S, T, SC, R>
where
  F: FinateStateMachine<
    Id = T::Id,
    Address = <T::Resolver as AddressResolver>::Address,
    Data = T::Data,
    Runtime = R,
  >,
  S: Storage<
    Id = T::Id,
    Address = <T::Resolver as AddressResolver>::Address,
    Data = T::Data,
    Runtime = R,
  >,
  T: Transport<Runtime = R>,

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
    Data = T::Data,
    Runtime = R,
  >,
  S: Storage<
    Id = T::Id,
    Address = <T::Resolver as AddressResolver>::Address,
    Data = T::Data,
    Runtime = R,
  >,
  T: Transport<Runtime = R>,

  SC: Sidecar<Runtime = R>,
  R: Runtime,
  <R::Sleep as std::future::Future>::Output: Send,
  <R::Interval as futures::Stream>::Item: Send + 'static,
{
  pub(super) fn spawn(
    mut self,
    #[cfg(feature = "metrics")] mut saturation_metric: SaturationMetric,
  ) {
    super::spawn_local::<R, _>(self.wg.add(1), async move {
      loop {
        futures::select! {
          _ = self.shutdown_rx.recv().fuse() => {
            tracing::info!(target = "ruraft", "raft runner received shutdown signal, gracefully shutdown...");
            // Clear the leader to prevent forwarding
            self.leader.set(None, &self.observers).await;
            self.stop_sidecar().await;
            return;
          }
          default => {
            match self.state.role() {
              Role::Follower => {
                self.spawn_sidecar(Role::Follower);
                match self.run_follower(
                  #[cfg(feature = "metrics")]
                  &mut saturation_metric
                ).await {
                  Ok(true) => self.stop_sidecar().await,
                  Ok(false) | Err(_) => {
                    self.stop_sidecar().await;
                    self.set_role(Role::Shutdown, &self.observers).await;
                  }
                }
              },
              Role::Candidate => {
                self.spawn_sidecar(Role::Candidate);
                match self.run_candidate(
                  #[cfg(feature = "metrics")]
                  &mut saturation_metric
                ).await {
                  Ok(true) => self.stop_sidecar().await,
                  Ok(false) | Err(_) => {
                    self.stop_sidecar().await;
                    self.set_role(Role::Shutdown, &self.observers).await;
                  }
                }
              },
              Role::Leader => {
                self.spawn_sidecar(Role::Leader);
                match self.run_leader(
                  #[cfg(feature = "metrics")]
                  &mut saturation_metric
                ).await {
                  Ok(true) => self.stop_sidecar().await,
                  Ok(false) | Err(_) => {
                    self.stop_sidecar().await;
                    self.set_role(Role::Shutdown, &self.observers).await;
                  }
                }
              },
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
  fn update_last_contact(&self) {
    self.last_contact.update();
  }

  #[inline]
  fn last_contact(&self) -> Option<Instant> {
    self.last_contact.get()
  }

  async fn handle_request(
    &self,
    tx: oneshot::Sender<Response<T::Id, <T::Resolver as AddressResolver>::Address>>,
    req: Request<T::Id, <T::Resolver as AddressResolver>::Address, T::Data>,
    rpc_reader: Option<Box<dyn futures::AsyncRead + Send + Sync + Unpin + 'static>>,
  ) {
    // TODO: validate the request header
    match req {
      Request::AppendEntries(req) => self.handle_append_entries(tx, req).await,
      Request::Vote(req) => self.handle_vote_request(tx, req).await,
      Request::InstallSnapshot(req) => {
        if let Some(rpc_reader) = rpc_reader {
          self
            .handle_install_snapshot_request(tx, req, rpc_reader)
            .await
        } else {
          panic!("ruraft: failed to handle install snapshot request since rpc reader is not available, this may be a bug in the transport implementation, if you are not using a custom transport, please report this issue to the http://github.com/al8n/ruraft/issues/new");
        }
      }
      Request::TimeoutNow(req) => self.handle_timeout_now_request(tx, req).await,
      Request::Heartbeat(req) => self.handle_heartbeat_request(tx, req).await,
    }
  }

  async fn handle_append_entries(
    &self,
    tx: oneshot::Sender<Response<T::Id, <T::Resolver as AddressResolver>::Address>>,
    mut req: AppendEntriesRequest<T::Id, <T::Resolver as AddressResolver>::Address, T::Data>,
  ) {
    #[cfg(feature = "metrics")]
    let start = Instant::now();

    #[cfg(feature = "metrics")]
    scopeguard::defer!({
      let h = metrics::histogram!("ruraft.rpc.append_entries");
      h.record(start.elapsed().as_millis() as f64);
    });

    // Setup a response
    let mut resp = AppendEntriesResponse {
      header: self.transport.header(),
      term: self.current_term(),
      last_log: self.last_index(),
      success: false,
      no_retry_backoff: false,
    };
    // Ignore an older term
    if req.term < self.current_term() {
      return respond_append_entries_request::<T>(resp, tx).await;
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
      self.set_role(Role::Follower, &self.observers).await;
      self.set_current_term(req.term);
      resp.term = req.term;
    }

    // Save the current leader
    self
      .leader
      .set(Some(req.header.from().clone()), &self.observers)
      .await;

    // Verify the last log entry
    if req.prev_log_entry > 0 {
      let last = self.last_entry();
      let prev_log_term = if req.prev_log_entry == last.index {
        last.term
      } else {
        match self.storage.log_store().get_log(req.prev_log_entry).await {
          Ok(Some(prev_log)) => prev_log.term,
          Ok(None) => {
            tracing::warn!(target = "ruraft.follower", previous_index = %req.prev_log_entry, last_index = %last.index, err=%Error::<F, S, T>::log_not_found(req.prev_log_entry), "previous log entry not found");
            resp.no_retry_backoff = true;
            return respond_append_entries_request::<T>(resp, tx).await;
          }
          Err(e) => {
            tracing::warn!(target = "ruraft.follower", previous_index = %req.prev_log_entry, last_index = %last.index, err=%e, "failed to get previous log");
            resp.no_retry_backoff = true;
            return respond_append_entries_request::<T>(resp, tx).await;
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

        return respond_append_entries_request::<T>(resp, tx).await;
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
          Ok(Some(stored_entry)) => {
            if entry.term != stored_entry.term {
              tracing::warn!(target = "ruraft.follower", from=%ent_idx, to=%last_log.index, "clearing log suffix");
              if let Err(e) = ls.remove_range(ent_idx..=last_log.index).await {
                tracing::warn!(target = "ruraft.follower", from=%ent_idx, to=%last_log.index, err=%e, "failed to clear log suffix");
                resp.no_retry_backoff = true;
                return respond_append_entries_request::<T>(resp, tx).await;
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
          Ok(None) => {
            tracing::warn!(target = "ruraft.follower", index=%ent_idx, err=%Error::<F, S, T>::log_not_found(ent_idx), "failed to get log entry");
            return respond_append_entries_request::<T>(resp, tx).await;
          }
          Err(e) => {
            tracing::warn!(target = "ruraft.follower", index=%ent_idx, err=%e, "failed to get log entry");
            return respond_append_entries_request::<T>(resp, tx).await;
          }
        }
      }

      if !req.entries[pos..].is_empty() {
        // Append the new entries
        if let Err(e) = ls.store_logs(&req.entries[pos..]).await {
          tracing::error!(target = "ruraft.follower", err=%e, "failed to append to logs");
          // TODO: leaving r.getLastLog() in the wrong
          // state if there was a truncation above
          return respond_append_entries_request::<T>(resp, tx).await;
        }

        let last = req.entries.last().unwrap();
        let last_log = LastLog::new(last.index, last.term);

        // Handle any new membership changes
        for entry in req.entries.drain(pos..) {
          if entry.is_membership() {
            self.process_membership_log(entry);
          }
        }

        // Update the lastLog
        self.set_last_log(last_log);
      }

      #[cfg(feature = "metrics")]
      metrics::histogram!("ruraft.follower.append_entries")
        .record(start.elapsed().as_millis() as f64);
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
      metrics::histogram!("ruraft.follower.commit_index")
        .record(start.elapsed().as_millis() as f64);
    }

    // Everything went well, set success
    resp.success = true;
    self.update_last_contact();
    respond_append_entries_request::<T>(resp, tx).await;
  }

  /// Takes a log entry and updates the latest
  /// membership if the entry results in a new membership. This must only be
  /// called from the main thread, or from constructors before any threads have begun.
  fn process_membership_log(
    &self,
    log: crate::storage::Log<T::Id, <T::Resolver as AddressResolver>::Address, T::Data>,
  ) {
    if let crate::storage::LogKind::Membership(m) = log.kind {
      self
        .memberships
        .committed
        .store(self.memberships.latest().clone());
      self.memberships.set_latest(m, log.index);
    }
  }

  /// Invoked when we get a request vote RPC call.
  async fn handle_vote_request(
    &self,
    tx: oneshot::Sender<Response<T::Id, <T::Resolver as AddressResolver>::Address>>,
    req: VoteRequest<T::Id, <T::Resolver as AddressResolver>::Address>,
  ) {
    macro_rules! return_and_respond {
      ($this:ident.$tx:ident($resp:ident)) => {{
        if $tx.send(Response::vote($resp)).is_err() {
          tracing::error!(
            target = "ruraft.rpc.vote_request",
            err = "channel closed",
            "failed to respond to vote request"
          );
        }
        return;
      }};
    }

    #[cfg(feature = "metrics")]
    let start = Instant::now();
    #[cfg(feature = "metrics")]
    scopeguard::defer!(
      metrics::histogram!("ruraft.rpc.vote_request").record(start.elapsed().as_millis() as f64)
    );

    observe(&self.observers, Observation::RequestVote(req.clone())).await;

    // Setup a response
    let mut resp = VoteResponse {
      header: self.transport.header(),
      term: self.current_term(),
      granted: false,
    };

    let candidate = req.header.from().clone();
    // if the Servers list is empty that mean the cluster is very likely trying to bootstrap,
    // Grant the vote
    let latest = self.memberships.latest().1.clone();
    if !latest.is_empty() && latest.contains_id(candidate.id()) {
      tracing::warn!(target = "ruraft.rpc.vote_request", candidate = %candidate, "rejecting vote request since node is not in membership");
      return_and_respond!(self.tx(resp))
    }

    if let Some(leader) = self.leader.load().as_ref() {
      tracing::warn!(target = "ruraft.rpc.vote_request", candidate = %candidate, leader=%leader, "rejecting vote request since we already have a leader");
      return_and_respond!(self.tx(resp))
    }

    // Ignore an older term
    if req.term < self.current_term() {
      return_and_respond!(self.tx(resp))
    }

    // Increase the term if we see a newer one
    if req.term > self.current_term() {
      tracing::debug!(target = "ruraft.rpc.vote_request", candidate = %candidate, "lost leadership because received a vote request with a newer term");
      // Ensure transition to follower
      self.set_role(Role::Follower, &self.observers).await;
      self.set_current_term(req.term);
      resp.term = req.term;
    }

    // if we get a request for vote from a nonVoter  and the request term is higher,
    // step down and update term, but reject the vote request
    // This could happen when a node, previously voter, is converted to non-voter
    // The reason we need to step in is to permit to the cluster to make progress in such a scenario
    // More details about that in https://github.com/hashicorp/raft/pull/526
    if !latest.is_empty() && !latest.is_voter(candidate.id()) {
      tracing::warn!(target = "ruraft.rpc.vote_request", candidate = %candidate, "rejecting vote request since node is not a voter");
      return_and_respond!(self.tx(resp))
    }

    // Check if we have voted yet
    let stable = self.storage.stable_store();
    let last_vote_term = match stable.last_vote_term().await {
      Ok(Some(v)) => v,
      Ok(None) => 0,
      Err(e) => {
        tracing::error!(target = "ruraft.rpc.vote_request", candidate = %candidate, err=%e, "failed to get last vote term");
        return_and_respond!(self.tx(resp))
      }
    };
    let last_vote_cand = match stable.last_vote_candidate().await {
      Ok(v) => v,
      Err(e) => {
        tracing::error!(target = "ruraft.rpc.vote_request", candidate = %candidate, err=%e, "failed to get last vote candidate");
        return_and_respond!(self.tx(resp))
      }
    };
    // Check if we've voted in this election before
    if last_vote_term == req.term {
      if let Some(last_vote_cand) = last_vote_cand {
        tracing::info!(
          target = "ruraft.rpc.vote_request",
          term = req.term,
          "duplicate vote request for same term"
        );
        if last_vote_cand == candidate {
          tracing::warn!(target = "ruraft.rpc.vote_request", candidate=%candidate, "duplicate vote request for candidate");
          resp.granted = true;
        }
        return_and_respond!(self.tx(resp))
      }
    }

    // Reject if their term is older
    let last = self.last_entry();
    if last.term > req.last_log_term {
      tracing::warn!(target = "ruraft.rpc.vote_request", candidate = %candidate, last_term = last.term, last_candidate_term = req.last_log_term, "rejecting vote request since our last term is greater");
      return_and_respond!(self.tx(resp))
    }

    if last.term == req.last_log_term && last.index > req.last_log_index {
      tracing::warn!(target = "ruraft.rpc.vote_request", candidate = %candidate, last_index = last.index, last_candidate_index = req.last_log_index, "rejecting vote request since our last index is greater");
      return_and_respond!(self.tx(resp))
    }

    // Persist a vote for safety
    if let Err(e) = Self::persist_vote(stable, req.term, candidate).await {
      tracing::error!(target = "ruraft.rpc.vote_request", err=%e, "failed to persist vote");
      return_and_respond!(self.tx(resp))
    }

    resp.granted = true;
    self.update_last_contact();
    return_and_respond!(self.tx(resp))
  }

  /// Invoked when we get a InstallSnapshot RPC call.
  /// We must be in the follower state for this, since it means we are
  /// too far behind a leader for log replay. This must only be called
  /// from the main thread.
  async fn handle_install_snapshot_request(
    &self,
    tx: oneshot::Sender<Response<T::Id, <T::Resolver as AddressResolver>::Address>>,
    req: InstallSnapshotRequest<T::Id, <T::Resolver as AddressResolver>::Address>,
    reader: Box<dyn futures::AsyncRead + Send + Sync + Unpin + 'static>,
  ) {
    #[cfg(feature = "metrics")]
    let start = Instant::now();
    #[cfg(feature = "metrics")]
    scopeguard::defer!(
      metrics::histogram!("ruraft.rpc.install_snapshot").record(start.elapsed().as_millis() as f64)
    );

    let req_size = *req.size();

    let mut resp = InstallSnapshotResponse {
      header: self.transport.header(),
      term: self.current_term(),
      success: false,
    };

    // Ignore an older term
    if req.term < self.current_term() {
      tracing::info!(
        target = "ruraft.rpc.install_snapshot",
        request_term = req.term,
        current_term = self.current_term(),
        "ignoring installSnapshot request with older term than current term"
      );
      return;
    }

    // Increase the term if we see a newer one
    if req.term > self.current_term() {
      // Ensure transition to follower
      self.set_role(Role::Follower, &self.observers).await;
      self.set_current_term(req.term);
      resp.term = req.term;
    }

    // Save the current leader
    self
      .leader
      .set(Some(req.header.from().clone()), &self.observers)
      .await;

    let snaps = self.storage.snapshot_store();
    // Create a new snapshot
    let mut sink = match snaps
      .create(
        snapshot_version(self.transport.version()),
        req.last_log_term,
        req.last_log_index,
        req.membership.clone(),
        req.membership_index,
      )
      .await
    {
      Ok(s) => s,
      Err(e) => {
        tracing::error!(target = "ruraft.rpc.install_snapshot", err=%e, "failed to create snapshot to install");
        let err = Error::<F, S, T>::storage(
          <S::Error as StorageError>::snapshot(e)
            .with_message(Cow::Borrowed("failed to create snapshot")),
        );

        return respond_install_snapshot_request(&self.transport, reader, Err(err), tx).await;
      }
    };

    let snapshot_id = sink.id();

    // Separately track the progress of streaming a snapshot over the network
    // because this too can take a long time.
    let mut counting_rpc_reader = CountingSnapshotSourceReader::from(reader);

    // Spill the remote snapshot to disk
    let ctr = counting_rpc_reader.ctr();
    let transfer_monitor = SnapshotRestoreMonitor::<R>::new(ctr, req_size, true);
    let copy_result = futures::io::copy(&mut counting_rpc_reader, &mut sink).await;
    transfer_monitor.stop_and_wait().await;
    let copied = match copy_result {
      Ok(n) => n,
      Err(e) => {
        if let Err(e) = sink.cancel().await {
          tracing::error!(target = "ruraft.rpc.install_snapshot", err=%e, "failed to cancel snapshot sink");
        }

        tracing::error!(target = "ruraft.rpc.install_snapshot", err=%e, "failed to copy snapshot");

        return respond_install_snapshot_request(
          &self.transport,
          counting_rpc_reader,
          Err(Error::<F, S, T>::storage(<S::Error as StorageError>::io(e))),
          tx,
        )
        .await;
      }
    };

    // Check that we received it all
    if copied != req_size {
      if let Err(e) = sink.cancel().await {
        tracing::error!(target = "ruraft.rpc.install_snapshot", err=%e, "failed to cancel snapshot sink");
      }

      tracing::error!(
        target = "ruraft.rpc.install_snapshot",
        received = copied / req_size,
        err = "short read",
        "failed to copy whole snapshot"
      );

      return respond_install_snapshot_request(
        &self.transport,
        counting_rpc_reader,
        Err("short read"),
        tx,
      )
      .await;
    }

    // Finalize the snapshot
    if let Err(e) = sink.close().await {
      tracing::error!(target = "ruraft.rpc.install_snapshot", err=%e, "failed to finalize snapshot");

      return respond_install_snapshot_request(
        &self.transport,
        counting_rpc_reader,
        Err(Error::<F, S, T>::storage(
          <S::Error as StorageError>::snapshot(<S::Snapshot as SnapshotStorage>::Error::from(e)),
        )),
        tx,
      )
      .await;
    }

    // Restore snapshot
    let (restore_tx, restore_rx) = futures::channel::oneshot::channel();
    futures::select! {
      res = self.fsm_mutate_tx.send(FSMRequest::Restore {
        id: snapshot_id,
        tx: restore_tx,
        shutdown_rx: self.shutdown_rx.clone(),
      }).fuse() => {
        if let Err(e) = res {
          tracing::error!(target = "ruraft.rpc.install_snapshot", err=%e, "failed to send restore request to fsm");

          return respond_install_snapshot_request(&self.transport, counting_rpc_reader, Err(e), tx).await;
        }
      },
      _ = self.shutdown_rx.recv().fuse() => {
        return respond_install_snapshot_request(&self.transport, counting_rpc_reader, Err(Error::<F, S, T>::shutdown()), tx).await;
      }
    }

    // Wait for the restore to happen
    match restore_rx.await {
      Ok(Ok(())) => {}
      Ok(Err(e)) => {
        tracing::error!(target = "ruraft.rpc.install_snapshot", err=%e, "failed to restore snapshot");
        return respond_install_snapshot_request(&self.transport, counting_rpc_reader, Err(e), tx)
          .await;
      }
      Err(e) => {
        tracing::error!(target = "ruraft.rpc.install_snapshot", err=%e, "failed to receive restore response");
        return respond_install_snapshot_request(&self.transport, counting_rpc_reader, Err(e), tx)
          .await;
      }
    }

    // Update the lastApplied so we don't replay old logs
    self.set_last_applied(req.last_log_index);

    // Update the last stable snapshot info
    self.set_last_snapshot(LastSnapshot::new(req.last_log_index, req.last_log_term));

    // Restore the peer set
    let updated_membership = Arc::new((req.membership_index, req.membership));
    self.memberships.latest.store(updated_membership.clone());
    self.memberships.committed.store(updated_membership);

    // Clear old logs if req.logs is a MonotonicLogStore. Otherwise compact the
    // logs. In both cases, log any errors and continue.
    if <S::Log as LogStorage>::is_monotonic() {
      if let Err(err) = remove_old_logs::<S>(self.storage.log_store()).await {
        tracing::error!(target = "ruraft.rpc.install_snapshot", err=%err, "failed to remove old logs");

        return respond_install_snapshot_request(
          &self.transport,
          counting_rpc_reader,
          Err(err),
          tx,
        )
        .await;
      }
    } else if let Err(e) = compact_logs::<S>(
      self.storage.log_store(),
      &self.state,
      req.last_log_index,
      self
        .reloadable_options
        .load(Ordering::Acquire)
        .trailing_logs(),
    )
    .await
    {
      tracing::error!(target = "ruraft.rpc.install_snapshot", err=%e, "failed to compact logs");

      return respond_install_snapshot_request(&self.transport, counting_rpc_reader, Err(e), tx)
        .await;
    }

    tracing::info!(
      target = "ruraft.rpc.install_snapshot",
      "successfully installed remote snapshot"
    );
    self.update_last_contact();
    resp.success = true;
    respond_install_snapshot_request::<_, _, &str>(
      &self.transport,
      counting_rpc_reader,
      Ok(resp),
      tx,
    )
    .await;
  }

  /// What happens when a server receives a [`TimeoutNowRequest`].
  async fn handle_timeout_now_request(
    &self,
    tx: oneshot::Sender<Response<T::Id, <T::Resolver as AddressResolver>::Address>>,
    _: TimeoutNowRequest<T::Id, <T::Resolver as AddressResolver>::Address>,
  ) {
    self.leader.set(None, &self.observers).await;
    self.set_role(Role::Candidate, &self.observers).await;
    self
      .candidate_from_leadership_transfer
      .store(true, Ordering::Release);
    if tx
      .send(Response::TimeoutNow(TimeoutNowResponse {
        header: self.transport.header(),
      }))
      .is_err()
    {
      tracing::error!(
        target = "ruraft.rpc.timeout",
        err = "receiver channel closed",
        "failed to respond to timeout now request"
      );
    }
  }

  /// What happens when a server receives a [`HeartbeatRequest`].
  async fn handle_heartbeat_request(
    &self,
    tx: oneshot::Sender<Response<T::Id, <T::Resolver as AddressResolver>::Address>>,
    req: HeartbeatRequest<T::Id, <T::Resolver as AddressResolver>::Address>,
  ) {
    #[cfg(feature = "metrics")]
    let start = Instant::now();
    #[cfg(feature = "metrics")]
    scopeguard::defer!(
      metrics::histogram!("ruraft.rpc.heartbeat").record(start.elapsed().as_millis() as f64)
    );

    handle_heartbeat_request::<_, _>(
      self.transport.header(),
      &self.state,
      &self.shutdown_rx,
      &self.last_contact,
      &self.candidate_from_leadership_transfer,
      &self.observers,
      tx,
      req,
    )
    .await;
  }

  /// Used to apply all the committed entries that haven't been
  /// applied up to the given index limit.
  /// This can be called from both leaders and followers.
  /// Followers call this from `append_entries`, for `n` entries at a time, and always
  /// pass futures = `None`.
  /// Leaders call this when entries are committed. They pass the futures from any
  /// inflight logs.
  async fn process_logs(
    &self,
    index: u64,
    futures: Option<HashMap<u64, Inflight<F, Error<F, S, T>>>>,
  ) {
    // Reject logs we've applied already
    let last_applied = self.last_applied();
    if index <= last_applied {
      tracing::warn!(
        target = "ruraft",
        index = index,
        "skipping application of old log"
      );
    }

    let batch_enabled = self.options.batch_apply();
    let apply_batch = |batch: BatchCommit<F, Error<F, S, T>>| async {
      let req = if batch_enabled {
        FSMRequest::Batch(batch)
      } else {
        FSMRequest::AdHoc(batch)
      };

      futures::select! {
        res = self.fsm_mutate_tx.send(req).fuse() => {
          if let Err(e) = res {
            tracing::error!(target = "ruraft", err=%e, "failed to send batch to fsm");
          }
        },
        _ = self.shutdown_rx.recv().fuse() => {},
      }
    };

    // Store max_append_entries for this call in case it ever becomes reloadable. We
    // need to use the same value for all lines here to get the expected result.
    let max_append_entries = self.options.max_append_entries();

    // Apply all the preceding logs
    if let Some(mut futures) = futures {
      let mut batch = BatchCommit::with_capacity(max_append_entries);

      for idx in (last_applied + 1)..=index {
        // Get the log, either from the future or from our log store

        let inflight = futures.remove(&idx);
        let prepared_log = if let Some(inflight) = inflight {
          if inflight.log.is_noop() {
            inflight.tx.respond_noop();
            continue;
          }

          self.prepare_log(inflight.log, Some(inflight.tx))
        } else {
          let log = match self.storage.log_store().get_log(idx).await {
            Ok(Some(log)) => log,
            Ok(None) => {
              tracing::error!(
                target = "ruraft",
                index = idx,
                err = "log not found",
                "failed to get log"
              );
              panic!("ruraft: log not found");
            }
            Err(e) => {
              tracing::error!(target = "ruraft", index = idx, err=%e, "failed to get log");
              panic!("{e}");
            }
          };

          self.prepare_log(log, None)
        };

        if let Some(prepared_log) = prepared_log {
          // If we have a log ready to send to the FSM add it to the batch.
          // The FSM thread will respond to the future.
          batch.push(prepared_log);

          // If we have filled up a batch, send it to the FSM
          if batch.len() >= max_append_entries {
            let mut old_batch = BatchCommit::with_capacity(max_append_entries);
            std::mem::swap(&mut batch, &mut old_batch);
            apply_batch(old_batch).await;
          }
        }
      }

      // If there are any remaining logs in the batch apply them
      if !batch.is_empty() {
        apply_batch(batch).await;
      }
    }

    // Update the lastApplied index and term
    self.set_last_applied(index);
  }

  fn prepare_log(
    &self,
    l: Log<T::Id, <T::Resolver as AddressResolver>::Address, T::Data>,
    fut: Option<ApplySender<F, Error<F, S, T>>>,
  ) -> Option<CommitTuple<F, Error<F, S, T>>> {
    match &l.kind {
      LogKind::Noop => None,
      _ => Some(CommitTuple { log: l, tx: fut }),
    }
  }

  async fn persist_vote(
    s: &S::Stable,
    term: u64,
    candidate: Node<T::Id, <T::Resolver as AddressResolver>::Address>,
  ) -> Result<(), <S::Stable as StableStorage>::Error> {
    s.store_last_vote_term(term).await?;
    s.store_last_vote_candidate(candidate).await
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

struct Inflight<F: FinateStateMachine, E> {
  #[cfg(feature = "metrics")]
  dispatch: Instant,
  log: Log<F::Id, F::Address, F::Data>,
  tx: ApplySender<F, E>,
}

pub(crate) struct CommitTuple<F: FinateStateMachine, E> {
  pub(crate) log: Log<F::Id, F::Address, F::Data>,
  pub(crate) tx: Option<ApplySender<F, E>>,
}

const INLINE: usize = 2;

pub(crate) type BatchCommit<F, E> = SmallVec<[CommitTuple<F, E>; INLINE]>;

fn snapshot_version(proto: ProtocolVersion) -> SnapshotVersion {
  match proto {
    ProtocolVersion::V1 => SnapshotVersion::V1,
  }
}

async fn respond_install_snapshot_request<
  T: Transport,
  R: futures::AsyncRead + Unpin,
  E: ToString,
>(
  transport: &Arc<T>,
  reader: R,
  resp: Result<InstallSnapshotResponse<T::Id, <T::Resolver as AddressResolver>::Address>, E>,
  tx: oneshot::Sender<Response<T::Id, <T::Resolver as AddressResolver>::Address>>,
) {
  // ensure we always consume all the snapshot data from the stream
  let _ = futures::io::copy(reader, &mut ruraft_utils::io::Discard).await;

  match resp {
    Ok(resp) => {
      if tx.send(Response::InstallSnapshot(resp)).is_err() {
        tracing::error!(
          target = "ruraft.rpc.install_snapshot",
          err = "channel closed",
          "failed to respond to install snapshot request"
        );
      }
    }
    Err(e) => {
      let resp = ErrorResponse {
        header: transport.header(),
        error: e.to_string(),
      };
      if tx.send(Response::Error(resp)).is_err() {
        tracing::error!(
          target = "ruraft.rpc.install_snapshot",
          err = "channel closed",
          "failed to respond to install snapshot request"
        );
      }
    }
  }
}

async fn respond_append_entries_request<T: Transport>(
  resp: AppendEntriesResponse<T::Id, <T::Resolver as AddressResolver>::Address>,
  tx: oneshot::Sender<Response<T::Id, <T::Resolver as AddressResolver>::Address>>,
) {
  if tx.send(Response::AppendEntries(resp)).is_err() {
    tracing::error!(
      target = "ruraft.rpc.append_entries",
      err = "channel closed",
      "failed to respond to append entries request"
    );
  }
}

/// What happens when a server receives a [`HeartbeatRequest`].
#[allow(clippy::too_many_arguments)]
async fn handle_heartbeat_request<I: Id, A: Address>(
  header: Header<I, A>,
  state: &State,
  shutdown_rx: &async_channel::Receiver<()>,
  last_contact: &OptionalContact,
  candidate_from_leadership_transfer: &AtomicBool,
  observers: &async_lock::RwLock<HashMap<ObserverId, Observer<I, A>>>,
  tx: oneshot::Sender<Response<I, A>>,
  req: HeartbeatRequest<I, A>,
) {
  // Check if we are shutdown, just ignore the RPC
  futures::select! {
    _ = shutdown_rx.recv().fuse() => {
      return;
    },
    default => {}
  }

  // Setup a response
  let mut resp = HeartbeatResponse {
    header,
    success: false,
  };

  // Ignore an older term
  if req.term < state.current_term() {
    return respond_heartbeat_request(resp, tx).await;
  }

  // Increase the term if we see a newer one, also transition to follower
  // if we ever get an appendEntries call
  if req.term > state.current_term()
    || (state.role() != Role::Follower
      && !candidate_from_leadership_transfer.load(Ordering::Acquire))
  {
    // Ensure transition to follower
    state.set_role(Role::Follower, observers).await;
    state.set_current_term(req.term);
  }

  // Everything went well, set success
  resp.success = true;
  last_contact.update();
  respond_heartbeat_request(resp, tx).await;
}

async fn respond_heartbeat_request<I: Id, A: Address>(
  resp: HeartbeatResponse<I, A>,
  tx: oneshot::Sender<Response<I, A>>,
) {
  if tx.send(Response::Heartbeat(resp)).is_err() {
    tracing::error!(
      target = "ruraft.rpc.heartbeat",
      err = "channel closed",
      "failed to respond to heartbeat request"
    );
  }
}
