use std::{
  borrow::Cow,
  future::Future,
  sync::{atomic::Ordering, Arc},
  time::{Duration, Instant},
};

use agnostic::Runtime;
use atomic::Atomic;
use futures::{channel::oneshot, FutureExt};
use nodecraft::resolver::AddressResolver;

use crate::{
  error::{Error, RaftError},
  membership::Membership,
  options::ReloadableOptions,
  storage::{LogStorage, SnapshotId, SnapshotSink, SnapshotStorage, Storage, StorageError},
  transport::Transport,
  FinateStateMachine, FinateStateMachineError, FinateStateMachineSnapshot, LastSnapshot,
};

use super::fsm::FSMSnapshot;

mod monitor;
pub(crate) use monitor::*;

pub(super) struct SnapshotRunner<F, S, T, R>
where
  F: FinateStateMachine<
    Id = T::Id,
    Address = <T::Resolver as AddressResolver>::Address,
    SnapshotSink = <S::Snapshot as SnapshotStorage>::Sink,
  >,
  S: Storage<Id = T::Id, Address = <T::Resolver as AddressResolver>::Address, Runtime = R>,
  T: Transport<Runtime = R>,
  <T::Resolver as AddressResolver>::Address: Send + Sync + 'static,
  R: Runtime,
  <R::Sleep as std::future::Future>::Output: Send,
  R: Runtime,
{
  pub(super) store: Arc<S>,
  pub(super) last: Arc<parking_lot::Mutex<super::Last>>,
  /// Used to trigger a new snapshot being taken
  pub(super) fsm_snapshot_tx: async_channel::Sender<
    oneshot::Sender<Result<FSMSnapshot<<F as FinateStateMachine>::Snapshot>, Error<F, S, T>>>,
  >,
  pub(super) committed_membership_tx: async_channel::Sender<
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
  pub(super) user_snapshot_rx: async_channel::Receiver<
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
  pub(super) opts: Arc<Atomic<ReloadableOptions>>,
  pub(super) shutdown_rx: async_channel::Receiver<()>,
}

impl<F, S, T, R> SnapshotRunner<F, S, T, R>
where
  F: FinateStateMachine<
    Id = T::Id,
    Address = <T::Resolver as AddressResolver>::Address,
    SnapshotSink = <S::Snapshot as SnapshotStorage>::Sink,
  >,
  S: Storage<Id = T::Id, Address = <T::Resolver as AddressResolver>::Address, Runtime = R>,
  T: Transport<Runtime = R>,
  <T::Resolver as AddressResolver>::Address: Send + Sync + 'static,
  R: Runtime,
  <R::Sleep as std::future::Future>::Output: Send,
  R: Runtime,
{
  pub(super) fn spawn(self) {
    R::spawn_detach(async move {
      loop {
        futures::select! {
          // unwrap safe here, because snapshot_interval cannot be zero.
          _ = R::sleep(crate::utils::random_timeout(self.snapshot_interval()).unwrap()).fuse() => {
            // Check if we should snapshot
            if !self.should_snapshot().await {
              continue;
            }

            // Trigger a snapshot
            if let Err(e) = self.take_snapshot().await {
              tracing::error!(target = "ruraft.snapshot.runner", err=%e, "failed to take snapshot");
            }
          },
          rst = self.user_snapshot_rx.recv().fuse() => match rst {
            // User-triggered, run immediately
            Ok(tx) => {
              match self.take_snapshot().await {
                Ok(id) => {
                  let s = self.store.clone();
                  let _ = tx.send(Ok(Box::new(async move {
                    s.snapshot_store().open(&id).await
                  })));
                }
                Err(e) => {
                  tracing::error!(target = "ruraft.snapshot.runner", err=%e, "failed to take snapshot");
                  let _ = tx.send(Err(e));
                }
              }
            }
            Err(_) => {
              tracing::error!(target = "ruraft.snapshot.runner", "user snapshot channel closed unexpectedly, shutting down runner...");
              return;
            }
          },
          _ = self.shutdown_rx.recv().fuse() => {
            tracing::info!(target = "ruraft.snapshot.runner", "shutting down...");
            return;
          }
        }
      }
    });
  }

  async fn take_snapshot(&self) -> Result<SnapshotId, Error<F, S, T>> {
    #[cfg(feature = "metrics")]
    let start = Instant::now();

    #[cfg(feature = "metrics")]
    scopeguard::defer!(metrics::histogram!(
      "ruraft.snapshot.take_snapshot",
      start.elapsed().as_millis() as f64
    ));

    let (tx, rx) = oneshot::channel();

    // Wait for dispatch or shutdown.
    futures::select! {
      rst = self.fsm_snapshot_tx.send(tx).fuse() => {
        let snap = match rst {
          Ok(_) => {
            // Wait until we get a response
            match rx.await {
              Ok(Ok(snap)) => snap,
              Ok(Err(e)) => {
                return match e {
                  Error::Raft(RaftError::NothingNewToSnapshot) => Err(Error::Raft(RaftError::NothingNewToSnapshot)),
                  e => {
                    // TODO: with_message
                    // Err(e.with_message(Cow::Borrowed("failed to start snapshot")))
                    Err(e)
                  },
                };
              }
              Err(_) => return Err(Error::Raft(RaftError::Closed("finate state mechine snapshot request sender closed"))),
            }
          }
          Err(_) => return Err(Error::Raft(RaftError::Closed("finate state mechine snapshot receiver closed"))),
        };

        // Make a request for the memberships and extract the committed info.
        // We have to use the future here to safely get this information since
        // it is owned by the main thread.
        let (tx, rx) = oneshot::channel();
        futures::select! {
          rst = self.committed_membership_tx.send(tx).fuse() => {
            let committed_membership = match rst {
              Ok(_) => {
                // Wait until we get a response
                match rx.await {
                  Ok(Ok(membership)) => membership,
                  Ok(Err(e)) => return Err(e),
                  Err(_) => return Err(Error::Raft(RaftError::Closed("memberships request channel closed"))),
                }
              }
              Err(_) => return Err(Error::Raft(RaftError::Closed("memberships channel closed"))),
            };

            // We don't support snapshots while there's a membership change outstanding
            // since the snapshot doesn't have a means to represent this state. This
            // is a little weird because we need the FSM to apply an index that's
            // past the membership change, even though the FSM itself doesn't see
            // the membership changes. It should be ok in practice with normal
            // application traffic flowing through the FSM. If there's none of that
            // then it's not crucial that we snapshot, since there's not much going
            // on Raft-wise.
            if snap.index < committed_membership.0 {
              return Err(Error::Raft(RaftError::CantTakeSnapshot {
                committed: committed_membership.0,
                snapshot: snap.index,
              }));
            }


            // Create a new snapshot.
            tracing::info!(target = "ruraft.snapshot.runner", index = %snap.index, "starting snapshot up");
            #[cfg(feature = "metrics")]
            let create_start = Instant::now();
            let sink = match self.store.snapshot_store().create(
              Default::default(),
              snap.term,
              snap.index,
              committed_membership.1.clone(),
              committed_membership.0,
            ).await {
              Ok(sink) => sink,
              Err(e) => {
                return Err(Error::storage(<S::Error as StorageError>::snapshot(e).with_message(Cow::Borrowed("failed to create snapshot"))));
              },
            };

            #[cfg(feature = "metrics")]
            metrics::histogram!("ruraft.snapshot.create", create_start.elapsed().as_millis() as f64);

            // Try to persist the snapshot
            #[cfg(feature = "metrics")]
            let persist_start = Instant::now();
            let id = sink.id();
            if let Err(e) = snap.snapshot.persist(sink).await {
              return Err(Error::fsm(<F::Error as FinateStateMachineError>::snapshot(e).with_message(Cow::Borrowed("failed to create snapshot"))));
            }

            #[cfg(feature = "metrics")]
            metrics::histogram!("ruraft.snapshot.persist", persist_start.elapsed().as_millis() as f64);

            // Update the last stable snapshot info.
            self.last.lock().snapshot = LastSnapshot::new(snap.term, snap.index);

            // Compact the logs.
            self.compact_logs(snap.index).await
              .map(|_| {
                tracing::info!(target = "ruraft.snapshot.runner", index=%snap.index, "snapshot complete up");
                id
              })
              .map_err(Error::storage)
          }
          _ = self.shutdown_rx.recv().fuse() => Err(Error::Raft(RaftError::Shutdown)),
        }
      }
      _ = self.shutdown_rx.recv().fuse() => Err(Error::Raft(RaftError::Shutdown)),
    }
  }

  /// Checks if we meet the conditions to take
  /// a new snapshot.
  async fn should_snapshot(&self) -> bool {
    // Check the last snapshot index
    let last_snapshot = self.last.lock().snapshot;

    // Check the last log index
    match self.store.log_store().last_index().await {
      Ok(None) | Ok(Some(0)) => false,
      Ok(Some(last_index)) => {
        // Compare the delta to the threshold
        let delta = last_index.saturating_sub(last_snapshot.index);
        delta > self.snapshot_threshold()
      }
      Err(e) => {
        tracing::error!(target = "ruraft.snapshot.runner", err=%e, "failed to get last log index");
        false
      }
    }
  }

  /// Takes the last inclusive index of a snapshot,
  /// the lastLogIdx, and and the trailingLogs and trims the logs that
  /// are no longer needed.
  async fn compact_logs_with_trailing(
    &self,
    snap_idx: u64,
    last_log_idx: u64,
    traling_logs: u64,
  ) -> Result<(), S::Error> {
    let ls = self.store.log_store();
    // Determine log ranges to compact
    let Some(min_log) = ls.first_index().await.map_err(|e| {
      tracing::error!(target = "ruraft.snapshot.runner", err = %e, "failed to get first log index");
      <S::Error as StorageError>::log(e)
    })?
    else {
      return Ok(());
    };

    // Check if we have enough logs to truncate
    // Use a consistent value for traling_logs for the duration of this method
    // call to avoid surprising behaviour.
    if last_log_idx <= traling_logs {
      return Ok(());
    }

    // Truncate up to the end of the snapshot, or `traling_logs`
    // back from the head, which ever is further back. This ensures
    // at least `traling_logs` entries, but does not allow logs
    // after the snapshot to be removed.
    let max_log = snap_idx.min(last_log_idx.saturating_sub(traling_logs));
    if min_log > max_log {
      tracing::info!(target = "ruraft.snapshot.runner", "no logs to truncate");
      return Ok(());
    }

    tracing::info!(target="ruraft.snapshot.runner", from = %min_log, to = %max_log, "compacting logs");

    // Compact the logs
    ls.remove_range(min_log..=min_log).await.map_err(|e| {
      tracing::error!(target = "ruraft.snapshot.runner", err = %e, "log compaction failed");
      <S::Error as StorageError>::log(e).with_message(Cow::Borrowed("log compaction failed"))
    })
  }

  /// Takes the last inclusive index of a snapshot
  /// and trims the logs that are no longer needed.
  async fn compact_logs(&self, snap_idx: u64) -> Result<(), S::Error> {
    #[cfg(feature = "metrics")]
    let start = Instant::now();

    #[cfg(feature = "metrics")]
    scopeguard::defer!(metrics::histogram!(
      "ruraft.snapshot.compact_logs",
      start.elapsed().as_millis() as f64
    ));

    let last_log = self.last.lock().log;
    self
      .compact_logs_with_trailing(snap_idx, last_log.index, self.trailing_logs())
      .await
  }

  /// Removes all old logs from the store. This is used for
  /// MonotonicLogStores after restore. Callers should verify that the store
  /// implementation is monotonic prior to calling.
  pub(super) async fn remove_old_logs(&self) -> Result<(), S::Error> {
    #[cfg(feature = "metrics")]
    let start = Instant::now();

    #[cfg(feature = "metrics")]
    scopeguard::defer!(metrics::histogram!(
      "ruraft.snapshot.remove_old_logs",
      start.elapsed().as_millis() as f64
    ));

    match self.store.log_store().last_index().await {
      Ok(None) | Ok(Some(0)) => Ok(()),
      Ok(Some(last_log_index)) => {
        tracing::info!(
          target = "ruraft.snapshot.runner",
          "removing all old logs from log store"
        );
        // call compactLogsWithTrailing with lastLogIdx for snapIdx since
        // it will take the lesser of lastLogIdx and snapIdx to figure out
        // the end for which to apply trailingLogs.
        self
          .compact_logs_with_trailing(last_log_index, last_log_index, 0)
          .await
      }
      Err(e) => Err(
        <S::Error as StorageError>::log(e)
          .with_message(Cow::Borrowed("failed to get last log index")),
      ),
    }
  }

  fn trailing_logs(&self) -> u64 {
    self.opts.load(Ordering::Acquire).trailing_logs()
  }

  fn snapshot_threshold(&self) -> u64 {
    self.opts.load(Ordering::Acquire).snapshot_threshold()
  }

  fn snapshot_interval(&self) -> Duration {
    self.opts.load(Ordering::Acquire).snapshot_interval()
  }
}
