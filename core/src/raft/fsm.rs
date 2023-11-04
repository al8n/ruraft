#![allow(clippy::type_complexity)]

use std::sync::Arc;
#[cfg(feature = "metrics")]
use std::time::Instant;

use agnostic::Runtime;
use async_channel::Receiver;
use futures::{channel::oneshot, Future, FutureExt};
use nodecraft::resolver::AddressResolver;
use smallvec::SmallVec;
use wg::AsyncWaitGroup;

use crate::{
  error::Error,
  fsm::{
    FinateStateMachine, FinateStateMachineLog, FinateStateMachineLogKind,
    FinateStateMachineResponse, FinateStateMachineSnapshot,
  },
  storage::{Log, LogKind, SnapshotId, SnapshotSource, SnapshotStorage, Storage},
  transport::Transport,
};

#[cfg(feature = "metrics")]
use crate::metrics::SaturationMetric;

use super::snapshot::{CountingReader, SnapshotRestoreMonitor};

pub(crate) struct FSMSnapshot<S: FinateStateMachineSnapshot> {
  pub(crate) term: u64,
  pub(crate) index: u64,
  pub(crate) snapshot: S,
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

const INLINE: usize = 2;

pub(crate) struct FSMLogRequest<F: FinateStateMachine, S: Storage, T: Transport> {
  log: Log<F::Id, F::Address, F::Data>,
  tx: oneshot::Sender<Result<FSMResponse<F::Response>, Error<F, S, T>>>,
}

pub(crate) enum FSMRequest<F: FinateStateMachine, S: Storage, T: Transport> {
  AdHoc(SmallVec<[FSMLogRequest<F, S, T>; INLINE]>),
  Batch {
    logs: SmallVec<[Log<F::Id, F::Address, F::Data>; INLINE]>,
    tx: oneshot::Sender<Result<FSMResponse<F::Response>, Error<F, S, T>>>,
  },
  Restore {
    id: SnapshotId,
    tx: oneshot::Sender<Result<(), Error<F, S, T>>>,
    shutdown_rx: async_channel::Receiver<()>,
  },
}

// TODO: fix viewit crate and use it
pub(super) struct FSMRunner<F, S, T, R>
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
  <T::Resolver as AddressResolver>::Address: Send + Sync + 'static,
  R: Runtime,
{
  pub(super) fsm: F,
  pub(super) storage: Arc<S>,
  pub(super) mutate_rx: Receiver<FSMRequest<F, S, T>>,
  pub(super) snapshot_rx:
    Receiver<oneshot::Sender<Result<FSMSnapshot<F::Snapshot>, Error<F, S, T>>>>,
  pub(super) wg: AsyncWaitGroup,
  pub(super) shutdown_rx: Receiver<()>,
}

impl<F, S, T, R> FSMRunner<F, S, T, R>
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
  <T::Resolver as AddressResolver>::Address: Send + Sync + 'static,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
{
  /// A long running task responsible for applying logs
  /// to the FSM. This is done async of other logs since we don't want
  /// the FSM to block our internal operations.
  pub(super) fn spawn(self) {
    let Self {
      fsm,
      storage,
      mutate_rx,
      snapshot_rx,
      shutdown_rx,
      wg,
    } = self;

    #[cfg(feature = "metrics")]
    let mut saturation =
      SaturationMetric::new("ruraft.fsm.runner", std::time::Duration::from_secs(1));

    super::spawn_local::<R, _>(wg.add(1), async move {
      let mut last_index = 0;
      let mut last_term = 0;

      loop {
        #[cfg(feature = "metrics")]
        saturation.sleeping();

        futures::select! {
          req = mutate_rx.recv().fuse() => {
            #[cfg(feature = "metrics")]
            saturation.working();

            match req {
              Ok(FSMRequest::AdHoc(reqs)) => {
                let mut last_batch_index = 0;
                let mut last_batch_term = 0;
                for FSMLogRequest { log, tx } in reqs {
                  let ApplyResult {
                    term,
                    index,
                  } = Self::apply_single(&fsm, log, tx).await;
                  last_batch_index = index;
                  last_batch_term = term;
                }
                last_index = last_batch_index;
                last_term = last_batch_term;
              },
              Ok(FSMRequest::Batch {
                logs,
                tx,
              }) => {
                let ApplyResult {
                  term,
                  index,
                } = Self::apply_batch(&fsm, logs, tx).await;
                last_index = index;
                last_term = term;
              }
              Ok(FSMRequest::Restore {
                id,
                tx,
                shutdown_rx,
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
                      tracing::error!(target = "ruraft.fsm.runner", id=%e, err=%e, "failed to restore snapshot");
                      if tx.send(Err(Error::fsm(e))).is_err() {
                        tracing::error!(target = "ruraft.fsm.runner", "failed to send finate state machine response, receiver closed");
                      }
                      continue;
                    }

                    // Update the last index and term
                    last_index = index;
                    last_term = term;
                    let resp = if shutdown_rx.is_closed() {
                      Err(Error::shutdown())
                    } else {
                      Ok(())
                    };

                    if tx.send(resp).is_err() {
                      tracing::error!(target = "ruraft.fsm.runner", "failed to send finate state machine response, receiver closed");
                    }
                  },
                  Err(e) => {
                    tracing::error!(target="ruraft.fsm.runner", id = %id, err=%e, "failed to open snapshot");
                    let resp = if shutdown_rx.is_closed() {
                      Err(Error::shutdown())
                    } else {
                      Err(Error::snapshot(e))
                    };
                    if tx.send(resp).is_err() {
                      tracing::error!(target = "ruraft.fsm.runner", "failed to send finate state machine response, receiver closed");
                    }
                  }
                }
              }
              Err(e) => {
                tracing::error!(target = "ruraft.fsm.runner", err=%e, "failed to receive finate state machine request, stop finate state machine listener...");
                return;
              }
            }
          }
          tx = snapshot_rx.recv().fuse() => {
            #[cfg(feature = "metrics")]
            saturation.working();
            match tx {
              Ok(tx) => {
                // Is there something to snapshot?
                if last_index == 0 {
                  if tx.send(Err(Error::nothing_new_to_snapshot())).is_err() {
                    tracing::error!(target = "ruraft.fsm.runner", "failed to send finate state machine snapshot response, receiver closed");
                  }
                  continue;
                }

                // Start a snapshot
                #[cfg(feature = "metrics")]
                let start = Instant::now();
                match fsm.snapshot().await {
                  Ok(snapshot) => {
                    #[cfg(feature = "metrics")]
                    metrics::histogram!("ruraft.fsm.snapshot", start.elapsed().as_millis() as f64);

                    let resp = FSMSnapshot {
                      term: last_term,
                      index: last_index,
                      snapshot,
                    };

                    if tx.send(Ok(resp)).is_err() {
                      tracing::error!(target = "ruraft.fsm.runner", "failed to send finate state machine snapshot response, receiver closed");
                    }
                  },
                  Err(e) => {
                    #[cfg(feature = "metrics")]
                    metrics::histogram!("ruraft.fsm.snapshot", start.elapsed().as_millis() as f64);

                    if tx.send(Err(Error::fsm(e))).is_err() {
                      tracing::error!(target = "ruraft.fsm.runner", "failed to send finate state machine snapshot response, receiver closed");
                    }
                  }
                }
              },
              Err(e) => {
                tracing::error!(target = "ruraft.fsm.runner", err=%e, "failed to receive finate state machine snapshot request, stop finate state machine listener...");
                return;
              }
            }
          }
          _ = shutdown_rx.recv().fuse() => {
            tracing::info!(target = "ruraft.fsm.runner", "shutdown finate state machine...");
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
    #[cfg(feature = "metrics")]
    let start = Instant::now();

    let cr = CountingReader::from(source);
    let ctr = cr.ctr();
    let monitor = SnapshotRestoreMonitor::<R>::new(ctr, snapshot_size, false);
    match fsm.restore(cr).await {
      Ok(_) => {
        #[cfg(feature = "metrics")]
        {
          metrics::histogram!("ruraft.fsm.restore", start.elapsed().as_millis() as f64);

          metrics::gauge!(
            "ruraft.fsm.last_restore_duration",
            start.elapsed().as_millis() as f64
          );
        }
        monitor.stop_and_wait().await;
        Ok(())
      }
      Err(e) => {
        monitor.stop_and_wait().await;
        Err(e)
      }
    }
  }

  async fn apply_batch(
    fsm: &F,
    logs: SmallVec<[Log<F::Id, F::Address, F::Data>; INLINE]>,
    tx: oneshot::Sender<Result<FSMResponse<F::Response>, Error<F, S, T>>>,
  ) -> ApplyResult {
    let mut last_batch_index = 0;
    let mut last_batch_term = 0;
    let mut should_send = 0;

    let logs = logs.into_iter().filter_map(|l| {
      last_batch_index = l.index;
      last_batch_term = l.term;
      Some(match l.kind {
        LogKind::Data(data) => {
          should_send += 1;
          FinateStateMachineLog {
            index: l.index,
            term: l.term,
            kind: FinateStateMachineLogKind::Log(data),
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

    let len = logs.size_hint().0;
    if len > 0 {
      #[cfg(feature = "metrics")]
      let start = Instant::now();
      match fsm.apply_batch(logs).await {
        Ok(resps) => {
          // Ensure we get the expected responses
          if resps.len() != should_send {
            panic!("ruraft: finate state machine apply batch response length mismatch, expected {}, got {}", should_send, resps.len());
          }

          #[cfg(feature = "metrics")]
          {
            metrics::histogram!("raft.fsm.apply_batch", start.elapsed().as_millis() as f64);

            metrics::counter!("raft.fsm.apply_batch_num", len as u64);
          }

          if tx.send(Ok(FSMResponse::more(resps))).is_err() {
            tracing::error!(
              target = "ruraft.fsm.runner",
              "failed to send finate state machine response, receiver closed"
            );
          }
        }
        Err(e) => {
          #[cfg(feature = "metrics")]
          {
            metrics::histogram!("raft.fsm.apply_batch", start.elapsed().as_millis() as f64);

            metrics::counter!("raft.fsm.apply_batch_num", len as u64);
          }

          if tx.send(Err(Error::fsm(e))).is_err() {
            tracing::error!(
              target = "ruraft.fsm.runner",
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
    log: Log<F::Id, F::Address, F::Data>,
    tx: oneshot::Sender<Result<FSMResponse<F::Response>, Error<F, S, T>>>,
  ) -> ApplyResult {
    let rst = ApplyResult {
      term: log.term,
      index: log.index,
    };
    // Apply the log if a command or config change
    match log.kind {
      LogKind::Data(data) => {
        #[cfg(feature = "metrics")]
        let start = Instant::now();
        let resp = fsm
          .apply(FinateStateMachineLog {
            index: log.index,
            term: log.term,
            kind: FinateStateMachineLogKind::Log(data),
          })
          .await;

        #[cfg(feature = "metrics")]
        metrics::histogram!("ruraft.fsm.apply", start.elapsed().as_millis() as f64);

        if tx
          .send(resp.map(FSMResponse::one).map_err(Error::fsm))
          .is_err()
        {
          tracing::error!(
            target = "ruraft.fsm.runner",
            "failed to send finate state machine response, receiver closed"
          );
        }
      }
      LogKind::Membership(membership) => {
        #[cfg(feature = "metrics")]
        let start = Instant::now();
        let resp = fsm
          .apply(FinateStateMachineLog {
            index: log.index,
            term: log.term,
            kind: FinateStateMachineLogKind::Membership(membership),
          })
          .await;
        #[cfg(feature = "metrics")]
        metrics::histogram!("ruraft.fsm.apply", start.elapsed().as_millis() as f64);

        if tx
          .send(resp.map(FSMResponse::one).map_err(Error::fsm))
          .is_err()
        {
          tracing::error!(
            target = "ruraft.fsm.runner",
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
