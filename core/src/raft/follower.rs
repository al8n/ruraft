use std::sync::atomic::Ordering;

use crate::{
  storage::LogStorage,
  transport::{
    AppendEntriesRequest, AppendEntriesResponse, Command, Request, RequestKind, Response,
  },
};

use super::*;
use futures::{channel::oneshot, StreamExt};

impl<F, S, T, SC, R> RaftRunner<F, S, T, SC, R>
where
  F: FinateStateMachine<Runtime = R>,
  S: Storage<Runtime = R>,
  T: Transport<Runtime = R>,
  SC: Sidecar<Runtime = R>,
  R: Runtime,
{
  pub(super) async fn run_follower(&self) {
    let mut did_warn = false;
    let leader = self.inner.leader.load();
    let local = &self.inner.local;

    match leader.as_ref() {
      Some(l) => {
        tracing::info!(target = "ruraft.follower", leader = %l.as_ref(), local = %local, "entering follower state");
      }
      None => {
        tracing::warn!(target = "ruraft.follower", local = %local, "entering follower state without a leader");
      }
    }

    let mut request_consumer = self.inner.transport.consumer();

    while self.inner.role() == Role::Follower {
      // r.mainThreadSaturation.sleeping()

      futures::select! {
        req = request_consumer.next().fuse() => {
          // r.mainThreadSaturation.working()
          if let Some(req) = req {
            let (tx, req) = req.into_components();
            self.handle_request(tx, req).await;
          }
        }
        _ = self.shutdown_rx.recv().fuse() => {
          tracing::info!(target = "ruraft.follower", "follower received shutdown signal, gracefully shutdown...");
          // Clear the leader to prevent forwarding
          self.inner.set_leader(None);
          return;
        }
      }
    }
  }

  async fn handle_request(&self, tx: oneshot::Sender<Response>, req: Request) {
    // TODO: validate the request header
    match req.kind {
      RequestKind::AppendEntries(req) => self.handle_append_entries(tx, req).await,
      RequestKind::Vote(_) => todo!(),
      RequestKind::InstallSnapshot(_) => todo!(),
      RequestKind::TimeoutNow(_) => todo!(),
      RequestKind::Heartbeat(_) => todo!(),
    }
  }

  async fn handle_append_entries(&self, tx: oneshot::Sender<Response>, req: AppendEntriesRequest) {
    // TODO: defer metrics.MeasureSince([]string{"raft", "rpc", "appendEntries"}, time.Now())

    macro_rules! respond {
      ($tx:ident.send($version: ident, $resp:ident)) => {
        if $tx.send(Response::append_entries($version, $resp)).is_err() {
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
    let mut resp = AppendEntriesResponse::new(self.local.id().clone(), self.local.addr)
      .with_term(self.current_term())
      .with_last_log(self.last_index().await);

    // Ignore an older term
    if req.term < self.current_term() {
      respond!(tx.send(protocol_version, resp));
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
    self.set_leader(Some(Node::new(req.header.id.clone(), req.header.addr)));

    // Verify the last log entry
    if req.prev_log_entry > 0 {
      let last = self.last_entry().await;
      let mut prev_log_term = 0;
      if req.prev_log_entry == last.index {
        prev_log_term = last.term;
      } else {
        match self.storage.log_store().get_log(req.prev_log_entry).await {
          Ok(Some(prev_log)) => prev_log_term = prev_log.term,
          Ok(None) => {}
          Err(e) => {
            tracing::warn!(target = "ruraft.follower", previous_index = %req.prev_log_entry, last_index = %last.index, err=%e, "failed to get previous log");
            resp.no_retry_backoff = true;
            respond!(tx.send(protocol_version, resp));
            return;
          }
        }
      }

      if req.prev_log_term != prev_log_term {
        tracing::warn!(
          target = "ruraft.follower",
          "prev log term mismatch (local: {}, remote: {})",
          prev_log_term,
          req.prev_log_term
        );

        resp.no_retry_backoff = true;

        respond!(tx.send(protocol_version, resp));
        return;
      }
    }

    // Process any new entries
    if !req.entries.is_empty() {}
  }
}
