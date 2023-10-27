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
  <R::Interval as futures::Stream>::Item: Send + 'static,
{
  pub(super) async fn run_follower(
    &mut self,
    #[cfg(feature = "metrics")]
    saturation_metric: &mut SaturationMetric
  ) -> Result<bool, ()> {
    let mut did_warn = false;
    let leader = self.leader.load();
    let local_id = self.transport.local_id();
    let local_addr = self.transport.local_addr();

    match leader.as_ref() {
      Some(l) => {
        tracing::info!(target = "ruraft.follower", leader = %l.as_ref(), id=%local_id, addr=%local_addr, "entering follower state");
      }
      None => {
        tracing::warn!(target = "ruraft.follower", id=%local_id, addr=%local_addr, "entering follower state without a leader");
      }
    }

    while self.state.role() == Role::Follower {
      #[cfg(feature = "metrics")]
      saturation_metric.sleeping();

      futures::select! {
        rpc = self.rpc.recv().fuse() => {
          #[cfg(feature = "metrics")]
          saturation_metric.working();

          match rpc {
            Ok(rpc) => {
              let (tx, req) = rpc.into_components();
              self.handle_request(tx, req).await;
            }
            Err(e) => {
              tracing::error!(target = "ruraft.follower", err=%e, "rpc consumer closed unexpectedly, shutting down...");
              self.state.set_role(Role::Shutdown);
              return Err(());
            }
          }
        }
        _ = self.shutdown_rx.recv().fuse() => {
          tracing::info!(target = "ruraft.follower", "follower received shutdown signal, shutdown...");
          // Clear the leader to prevent forwarding
          self.leader.set(None);
          return Ok(false);
        }
      }
    }

    Ok(true)
  }
}
