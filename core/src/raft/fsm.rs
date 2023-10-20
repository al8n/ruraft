#![allow(clippy::type_complexity)]

use std::{sync::Arc, time::Instant};

use agnostic::Runtime;
use async_channel::Receiver;
use futures::{channel::oneshot, Future, FutureExt};

use crate::{
  fsm::{
    FSMError, FinateStateMachine, FinateStateMachineLog, FinateStateMachineLogKind,
    FinateStateMachineResponse, FinateStateMachineSnapshot,
  },
  storage::{Log, LogKind, SnapshotId, SnapshotSource, SnapshotStorage, Storage},
};

pub(super) struct FSMSnapshot<S: FinateStateMachineSnapshot> {
  term: u64,
  index: u64,
  snapshot: S,
}

pub(crate) enum FSMLogResponse<R: FinateStateMachineResponse> {
  One(R),
  More(Vec<R>),
}

pub(crate) enum FSMResponse<R: FinateStateMachineResponse> {
  Log(FSMLogResponse<R>),
  Membership,
}

impl<R: FinateStateMachineResponse> FSMResponse<R> {
  fn one(resp: R) -> Self {
    Self::Log(FSMLogResponse::One(resp))
  }

  fn more(resp: Vec<R>) -> Self {
    Self::Log(FSMLogResponse::More(resp))
  }

  fn membership() -> Self {
    Self::Membership
  }
}

pub(crate) enum FSMLogRequest<F: FinateStateMachine> {
  One(Log<F::Id, F::Address>),
  More(Vec<Log<F::Id, F::Address>>),
}

pub(crate) enum FSMRequest<F: FinateStateMachine, S: Storage> {
  Log {
    req: FSMLogRequest<F>,
    tx: oneshot::Sender<Result<FSMResponse<F::Response>, FSMError<F, S>>>,
  },
  Restore {
    id: SnapshotId,
    tx: oneshot::Sender<Result<(), FSMError<F, S>>>,
  },
}

// TODO: fix viewit crate and use it
pub(super) struct FSMRunner<F, S, R>
where
  F: FinateStateMachine<Runtime = R>,
  S: Storage<Id = F::Id, Address = F::Address, Runtime = R>,
  R: Runtime,
{
  pub(super) fsm: F,
  pub(super) storage: Arc<S>,
  pub(super) mutate_rx: Receiver<FSMRequest<F, S>>,
  pub(super) snapshot_rx:
    Receiver<oneshot::Sender<Result<FSMSnapshot<F::Snapshot>, FSMError<F, S>>>>,
  pub(super) batching_apply: bool,
  pub(super) shutdown_rx: Receiver<()>,
}

impl<F, S, R> FSMRunner<F, S, R>
where
  F: FinateStateMachine<Runtime = R>,
  S: Storage<Id = F::Id, Address = F::Address, Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
{
  /// A long running task responsible for applying logs
  /// to the FSM. This is done async of other logs since we don't want
  /// the FSM to block our internal operations.
  pub(super) async fn run(self) {
    let Self {
      fsm,
      storage,
      mutate_rx,
      snapshot_rx,
      shutdown_rx,
      batching_apply,
    } = self;

    // TODO: saturation metrics

    R::spawn_detach(async move {
      let mut last_index = 0;
      let mut last_term = 0;

      loop {
        futures::select! {
          req = mutate_rx.recv().fuse() => {
            // TODO: stauration metrics
            match req {
              Ok(FSMRequest::Log {
                req,
                tx,
              }) => {
                let ApplyResult {
                  term,
                  index,
                } = match req {
                  FSMLogRequest::One(log) => Self::apply_single(&fsm, log, tx).await,
                  FSMLogRequest::More(logs) => {
                    if batching_apply {
                      Self::apply_batch(&fsm, logs, tx).await
                    } else {
                      let mut last_batch_index = 0;
                      let mut last_batch_term = 0;
                      for log in logs {
                        let ApplyResult {
                          term,
                          index,
                        } = Self::apply_single(&fsm, log, tx.clone()).await;
                        last_batch_index = index;
                        last_batch_term = term;
                      }
                      ApplyResult {
                        term: last_batch_term,
                        index: last_batch_index,
                      }
                    }
                  },
                };
                last_index = index;
                last_term = term;
              }
              Ok(FSMRequest::Restore {
                id,
                tx,
              }) => {
                // Open the snapshot
                let store = storage.snapshot_store();
                match store.open(&id).await {
                  Ok(source) => {
                    let meta = source.meta();
                    let size = meta.size();
                    let index = meta.index();
                    let term = meta.term();

                    if let Err(e) = Self::fsm_restore_and_measure(&fsm, source, size).await {
                      tracing::error!(target = "ruraft.fsm", id=%e, err=%e, "failed to restore snapshot");
                      if tx.send(Err(FSMError::StateMachine(e))).is_err() {
                        tracing::error!(target = "ruraft.fsm", "failed to send finate state machine response, receiver closed");
                      }
                      continue;
                    }

                    // Update the last index and term
                    last_index = index;
                    last_term = term;
                    if tx.send(Ok(())).is_err() {
                      tracing::error!(target = "ruraft.fsm", "failed to send finate state machine response, receiver closed");
                    }
                  },
                  Err(e) => {
                    tracing::error!(target="ruraft.fsm", id = %id, err=%e, "failed to open snapshot");
                    if tx.send(Err(FSMError::snapshot(e))).is_err() {
                      tracing::error!(target = "ruraft.fsm", "failed to send finate state machine response, receiver closed");
                    }
                  }
                }
              }
              Err(e) => {
                tracing::error!(target = "ruraft.fsm", err=%e, "failed to receive finate state machine request, stop finate state machine listener...");
                return;
              }
            }
          }
          tx = snapshot_rx.recv().fuse() => {
            // TODO: stauration metrics
            match tx {
              Ok(tx) => {
                // Is there something to snapshot?
                if last_index == 0 {
                  if tx.send(Err(FSMError::NothingNew)).is_err() {
                    tracing::error!(target = "ruraft.fsm", "failed to send finate state machine snapshot response, receiver closed");
                  }
                  continue;
                }

                // Start a snapshot
                let _start = Instant::now();
                match fsm.snapshot().await {
                  Ok(snapshot) => {
                    let resp = FSMSnapshot {
                      term: last_term,
                      index: last_index,
                      snapshot,
                    };

                    if tx.send(Ok(resp)).is_err() {
                      tracing::error!(target = "ruraft.fsm", "failed to send finate state machine snapshot response, receiver closed");
                    }
                  },
                  Err(e) => {
                    if tx.send(Err(FSMError::StateMachine(e))).is_err() {
                      tracing::error!(target = "ruraft.fsm", "failed to send finate state machine snapshot response, receiver closed");
                    }
                  }
                }
              },
              Err(e) => {
                tracing::error!(target = "ruraft.fsm", err=%e, "failed to receive finate state machine snapshot request, stop finate state machine listener...");
                return;
              }
            }
          }
          _ = shutdown_rx.recv().fuse() => {
            tracing::info!(target = "ruraft.fsm", "shutdown finate state machine...");
            return;
          }
        }
      }
    });
  }

  pub(super) async fn fsm_restore_and_measure(
    fsm: &F,
    source: <S::Snapshot as SnapshotStorage>::Source,
    snapshot_size: u64,
  ) -> Result<(), F::Error> {
    let start = Instant::now();

    fsm.restore(source).await?;

    // TODO: metrics
    Ok(())
  }

  async fn apply_batch(
    fsm: &F,
    logs: Vec<Log<F::Id, F::Address>>,
    tx: oneshot::Sender<Result<FSMResponse<F::Response>, FSMError<F, S>>>,
  ) -> ApplyResult {
    let mut last_batch_index = 0;
    let mut last_batch_term = 0;
    let mut should_send = 0;

    let logs = logs.into_iter().filter_map(|l| {
      last_batch_index = l.index;
      last_batch_term = l.term;
      Some(match l.kind {
        LogKind::User { data, extension } => {
          should_send += 1;
          FinateStateMachineLog {
            index: l.index,
            term: l.term,
            kind: FinateStateMachineLogKind::Log { data, extension },
          }
        }
        LogKind::Membership(m) => {
          should_send += 1;
          FinateStateMachineLog {
            index: l.index,
            term: l.term,
            kind: FinateStateMachineLogKind::Membership(m),
          }
        }
        _ => return None,
      })
    });

    if logs.size_hint().0 > 0 {
      let _start = Instant::now();
      match fsm.apply_batch(logs).await {
        Ok(resps) => {
          // Ensure we get the expected responses
          if resps.len() != should_send {
            panic!("ruraft: finate state machine apply batch response length mismatch, expected {}, got {}", should_send, resps.len());
          }

          // TODO: metrics
          if tx.send(Ok(FSMResponse::more(resps))).is_err() {
            tracing::error!(
              target = "ruraft.fsm",
              "failed to send finate state machine response, receiver closed"
            );
          }
        }
        Err(e) => {
          // TODO: metrics
          if tx.send(Err(FSMError::state_machine(e))).is_err() {
            tracing::error!(
              target = "ruraft.fsm",
              "failed to send finate state machine response, receiver closed"
            );
          }
        }
      }
    }

    ApplyResult {
      term: last_batch_term,
      index: last_batch_index,
    }
  }

  async fn apply_single(
    fsm: &F,
    log: Log<F::Id, F::Address>,
    tx: oneshot::Sender<Result<FSMResponse<F::Response>, FSMError<F, S>>>,
  ) -> ApplyResult {
    let rst = ApplyResult {
      term: log.term,
      index: log.index,
    };
    // Apply the log if a command or config change
    match log.kind {
      LogKind::User { data, extension } => {
        let _start = Instant::now();
        let resp = fsm
          .apply(FinateStateMachineLog {
            index: log.index,
            term: log.term,
            kind: FinateStateMachineLogKind::Log { data, extension },
          })
          .await;
        // TODO: metrics
        if tx
          .send(resp.map(FSMResponse::one).map_err(FSMError::state_machine))
          .is_err()
        {
          tracing::error!(
            target = "ruraft.fsm",
            "failed to send finate state machine response, receiver closed"
          );
        }
      }
      LogKind::Membership(membership) => {
        let resp = fsm
          .apply(FinateStateMachineLog {
            index: log.index,
            term: log.term,
            kind: FinateStateMachineLogKind::Membership(membership),
          })
          .await;
        // TODO: metrics
        if tx
          .send(resp.map(FSMResponse::one).map_err(FSMError::state_machine))
          .is_err()
        {
          tracing::error!(
            target = "ruraft.fsm",
            "failed to send finate state machine response, receiver closed"
          );
        }
      }
      _ => {}
    }

    rst
  }
}

struct ApplyResult {
  term: u64,
  index: u64,
}
