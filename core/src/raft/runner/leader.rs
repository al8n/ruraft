use std::{
  collections::{HashSet, LinkedList},
  sync::atomic::Ordering,
  time::Duration,
};

use futures::StreamExt;
use nodecraft::{Address, Id};

use super::*;
use crate::{error::RaftError, storage::Log, utils::random_timeout};

mod replication;
use replication::Replication;

#[derive(Debug, PartialEq, Eq, Hash)]
struct Verify {
  quorum_size: usize,
  votes: usize,
}

struct LeaderState<I: Id, A: Address> {
  // indicates that a leadership transfer is in progress.
  leadership_transfer_in_progress: AtomicBool,
  commit_rx: async_channel::Receiver<()>,
  // list of log in log index order
  inflight: LinkedList<Log<I, A>>,
  repl_state: HashMap<I, Replication<I, A>>,
  nofity: HashSet<Verify>,
  step_down: async_channel::Receiver<()>,
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
  pub(super) async fn run_leader(
    &mut self,
    #[cfg(feature = "metrics")] saturation_metric: &mut SaturationMetric,
  ) -> Result<bool, ()> {
    // override_notify_bool(self.leader_tx, true);

    todo!()
  }
}
