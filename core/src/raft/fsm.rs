use std::time::Instant;

use agnostic::Runtime;
use async_channel::Receiver;
use futures::FutureExt;

use crate::{sidecar::Sidecar, transport::AddressResolver, storage::{SnapshotSource, SnapshotStorage}, error::Error};

use super::{FinateStateMachine, RaftCore, Storage, Transport};

impl<F, S, T, SC, R> RaftCore<F, S, T, SC, R>
where
  F: FinateStateMachine<Runtime = R>,
  S: Storage<Id = T::Id, Address = <T::Resolver as AddressResolver>::Address, Runtime = R>,
  T: Transport<Runtime = R>,
  SC: Sidecar<Runtime = R>,
  R: Runtime,
{
  /// A long running task responsible for applying logs
  /// to the FSM. This is done async of other logs since we don't want
  /// the FSM to block our internal operations.
  pub(super) async fn run_fsm(&self, shutdown_rx: Receiver<()>) {
    // R::spawn_detach(async move {

    // });
    let mut last_index = 0;
    let mut last_term = 0;

    loop {
      futures::select! {
        _ = shutdown_rx.recv().fuse() => {
          tracing::info!(target = "ruraft", "shutdown finate state machine...");
          return;
        }
      }
    }
  }

  pub(super) async fn fsm_restore_and_measure(fsm: &F, source: <S::Snapshot as SnapshotStorage>::Source, snapshot_size: u64) -> Result<(), Error<F, S::Error, T::Error>> {
    let start = Instant::now();
  

    fsm.restore(source).await.map_err(Error::fsm)?;

    // TODO: metrics
    Ok(())
  }
}


