use agnostic::Runtime;
use async_channel::Receiver;
use futures::FutureExt;

use super::{FinateStateMachine, Raft, Storage, Transport};

impl<F, S, T, R> Raft<F, S, T, R>
where
  F: FinateStateMachine,
  S: Storage,
  T: Transport,
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
}
