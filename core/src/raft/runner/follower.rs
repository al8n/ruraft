use std::{sync::atomic::Ordering, time::Instant};

use crate::{
  storage::LogStorage,
  transport::{AppendEntriesRequest, AppendEntriesResponse, Request, Response},
};

use super::{super::state::LastLog, *};
use futures::{channel::oneshot, StreamExt};

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
  pub(super) async fn run_follower(&mut self) -> Result<bool, ()> {
    let mut did_warn = false;
    let leader = self.leader.load();
    let local = &self.local;

    match leader.as_ref() {
      Some(l) => {
        tracing::info!(target = "ruraft.follower", leader = %l.as_ref(), local = %local, "entering follower state");
      }
      None => {
        tracing::warn!(target = "ruraft.follower", local = %local, "entering follower state without a leader");
      }
    }

    while self.state.role() == Role::Follower {
      #[cfg(feature = "metrics")]
      self.saturation_metric.sleeping();

      futures::select! {
        rpc = self.rpc.recv().fuse() => {
          #[cfg(feature = "metrics")]
          self.saturation_metric.working();

          match rpc {
            Ok(rpc) => {
              let (tx, req) = rpc.into_components();
              self.handle_request(tx, req).await;
            }
            Err(e) => {
              tracing::error!(target = "ruraft.follower", err=%e, "failed to receive rpc request, producer has been dropped unexpectedly, shutting down...");
              return Err(());
            }
          }
        }
        _ = self.shutdown_rx.recv().fuse() => {
          tracing::info!(target = "ruraft.follower", "follower received shutdown signal, gracefully shutdown...");
          // Clear the leader to prevent forwarding
          self.leader.set(None);
          return Ok(false);
        }
      }
    }

    Ok(true)
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
}
