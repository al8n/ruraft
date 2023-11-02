use agnostic::Runtime;
use arc_swap::ArcSwap;
use atomic::Atomic;
use futures::{channel::oneshot, FutureExt, Stream, StreamExt};
use nodecraft::resolver::AddressResolver;
use parking_lot::Mutex;
use std::{
  collections::{HashMap, HashSet},
  net::SocketAddr,
  sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
  },
  time::{Duration, Instant},
};
use wg::AsyncWaitGroup;

use super::{super::super::spawn_local, Commitment, Node, Verify};
use crate::{
  error::{Error, RaftError},
  observe,
  options::ReloadableOptions,
  raft::Contact,
  storage::{LogStorage, SnapshotSource, SnapshotStorage, Storage},
  transport::{
    AppendEntriesPipeline, AppendEntriesPipelineFuture, AppendEntriesRequest, Header,
    HeartbeatRequest, InstallSnapshotRequest, PipelineAppendEntriesResponse, Transport,
  },
  utils::{backoff, capped_exponential_backoff, random_timeout},
  FinateStateMachine, Last, LastSnapshot, Observed, Observer, ObserverId,
};

const MAX_FAILURE_SCALE: u64 = 12;
const FAILURE_WAIT: Duration = Duration::from_millis(10);

pub(super) struct Replication<F: FinateStateMachine, S: Storage, T: Transport> {
  /// The index of the next log entry to send to the follower,
  /// which may fall past the end of the log.
  next_index: Arc<AtomicU64>,

  /// Contains the network address and ID of the remote follower
  pub(super) peer: Arc<ArcSwap<Node<T::Id, <T::Resolver as AddressResolver>::Address>>>,

  /// closed when this leader steps down or the follower is
  /// removed from the cluster. In the follower removed case, it carries a log
  /// index; replication should be attempted with a best effort up through that
  /// index, before exiting.
  stop_tx: async_channel::Sender<u64>,

  /// Notify every time new entries are appended to the log.
  pub(super) trigger_tx: async_channel::Sender<()>,

  /// Used to provide a backchannel. By sending a
  /// deferErr, the sender can be notifed when the replication is done.
  trigger_defer_error_tx: async_channel::Sender<oneshot::Sender<Result<(), Error<F, S, T>>>>,

  /// Notify the runner to send out a heartbeat, which is used to check that
  /// this server is still leader.
  pub(super) heartbeat_notify: async_channel::Sender<()>,

  /// a map of futures to be resolved upon receipt of an
  /// acknowledgement, then cleared from this map.
  pub(super) notify: Arc<Mutex<HashMap<u64, Verify<F, S, T>>>>,

  /// Updated to the current time whenever any response is
  /// received from the follower (successful or not). This is used to check
  /// whether the leader should step down (`check_leader_lease`).
  last_contact: Contact,
}

impl<F: FinateStateMachine, S: Storage, T: Transport> Replication<F, S, T>
where
  <T::Resolver as AddressResolver>::Address: Send + Sync + 'static,
{
  pub(super) async fn new<R: Runtime>(
    wg: &AsyncWaitGroup,
    peer: Node<T::Id, <T::Resolver as AddressResolver>::Address>,
    commitment: Commitment<T::Id, <T::Resolver as AddressResolver>::Address>,
    current_term: u64,
    next_index: u64,
    step_down_tx: async_channel::Sender<()>,
  ) -> (Self, ReplicationRunner<F, S, T>) {
    let next_index = Arc::new(AtomicU64::new(next_index));
    let peer = Arc::new(ArcSwap::from_pointee(peer));
    let notify = Arc::new(Mutex::new(HashMap::new()));

    let (stop_tx, stop_rx) = async_channel::bounded(1);
    let (trigger_tx, trigger_rx) = async_channel::bounded(1);
    let (trigger_defer_error_tx, trigger_defer_error_rx) = async_channel::bounded(1);
    let (heartbeat_notify, heartbeat_signal) = async_channel::bounded(1);

    let last_contact = Arc::new(ArcSwap::from_pointee(Instant::now()));

    // let runner = ReplicationRunner::<F, S, T> {
    //   commitment,
    //   current_term,
    //   next_index: next_index.clone(),
    //   peer: peer.clone(),
    //   stop_rx,
    //   trigger_rx,
    //   trigger_defer_error_rx,
    //   last_contact: last_contact.clone(),
    //   failures: 0,
    //   heartbeat_signal,
    //   notify: notify.clone(),
    //   step_down_tx,
    //   allow_pipeline: false,
    //   shutdown_rx: todo!(),
    //   transport: todo!(),
    // };

    // (
    //   Self {
    //     next_index,
    //     peer,
    //     stop_tx,
    //     trigger_tx,
    //     trigger_defer_error_tx,
    //     heartbeat_notify,
    //     notify,
    //     last_contact,
    //   },
    //   runner,
    // )
    todo!()
  }

  /// Used to notify all the waiting verify futures
  /// if the follower believes we are still the leader.
  pub(super) async fn notify_all(notify: &Mutex<HashMap<u64, Verify<F, S, T>>>, leader: bool) {
    // Clear the waiting notifies minimizing lock time
    let n = notify.lock().drain().collect::<Vec<_>>();

    // Submit our votes
    futures::future::join_all(
      n.into_iter()
        .map(|(_, v)| async move { v.vote(leader).await }),
    )
    .await;
  }

  /// Used to delete notify
  pub(super) fn clean_notify(&self, id: u64) {
    self.notify.lock().remove(&id);
  }

  /// Returns the time of last contact.
  pub(super) fn last_contact(&self) -> Instant {
    self.last_contact.get()
  }
}

pub(super) struct ReplicationRunner<F: FinateStateMachine, S: Storage, T: Transport> {
  /// commitment tracks the entries acknowledged by followers so that the
  /// leader's commit index can advance. It is updated on successful
  /// AppendEntries responses.
  commitment: Commitment<T::Id, <T::Resolver as AddressResolver>::Address>,

  /// The term of this leader, to be included in `append_entries`
  /// requests.
  current_term: u64,

  /// The index of the next log entry to send to the follower,
  /// which may fall past the end of the log.
  next_index: Arc<AtomicU64>,

  /// Highest committed log entry
  committed_index: Arc<AtomicU64>,

  last: Arc<Mutex<Last>>,

  /// Contains the network address and ID of the remote follower
  peer: Arc<ArcSwap<Node<T::Id, <T::Resolver as AddressResolver>::Address>>>,

  /// notified when this leader steps down or the follower is
  /// removed from the cluster. In the follower removed case, it carries a log
  /// index; replication should be attempted with a best effort up through that
  /// index, before exiting.
  stop_rx: async_channel::Receiver<u64>,

  /// Notified every time new entries are appended to the log.
  trigger_rx: async_channel::Receiver<()>,

  /// Used to provide a backchannel. By sending a
  /// deferErr, the sender can be notifed when the replication is done.
  trigger_defer_error_rx: async_channel::Receiver<oneshot::Sender<Result<(), Error<F, S, T>>>>,

  /// Updated to the current time whenever any response is
  /// received from the follower (successful or not). This is used to check
  /// whether the leader should step down (`check_leader_lease`).
  last_contact: Contact,

  /// Counts the number of failed RPCs since the last success, which is
  /// used to apply backoff.
  failures: u64,

  /// Signal to send out a heartbeat, which is used to check that
  /// this server is still leader.
  heartbeat_signal: async_channel::Receiver<()>,

  /// a map of futures to be resolved upon receipt of an
  /// acknowledgement, then cleared from this map.
  notify: Arc<Mutex<HashMap<u64, Verify<F, S, T>>>>,

  /// Used to indicate to the leader that we
  /// should step down based on information from a follower.
  step_down_tx: async_channel::Sender<()>,

  /// The global shutdown channel for the Raft node.
  shutdown_rx: async_channel::Receiver<()>,

  /// The transport used to send RPCs to the follower.
  transport: Arc<T>,

  /// The storage used to read and write the Raft log.
  storage: Arc<S>,

  /// Used to determine when to pipeline the `append_entries` RPCs.
  /// It is private to this replication task.
  allow_pipeline: bool,

  max_append_entries: u64,

  commit_timeout: Duration,
}

impl<F: FinateStateMachine, S, T: Transport> ReplicationRunner<F, S, T>
where
  S: Storage<Id = T::Id, Address = <T::Resolver as AddressResolver>::Address>,
  <T::Resolver as AddressResolver>::Address: Send + Sync + 'static,
{
  fn spawn(self, wg: &AsyncWaitGroup) {
    let runner_wg = wg.clone();
    spawn_local::<T::Runtime, _>(wg.add(1), async move {
      self.run(runner_wg).await;
    });
  }

  async fn run(mut self, wg: AsyncWaitGroup) {
    let mut should_stop = false;
    while !should_stop {
      futures::select! {
        match_index = self.stop_rx.recv().fuse() => {
          match match_index {
            Ok(match_index) if match_index > 0 => {
              self.replicate_to(match_index).await;
              return;
            }
            _ => return,
          }
        },
        tx = self.trigger_defer_error_rx.recv().fuse() => {
          match tx {
            Ok(tx) => {
              let last_log_idx = self.last.lock().log.index;
              should_stop = self.replicate_to(last_log_idx).await;
              if !should_stop {
                let _ = tx.send(Ok(()));
              } else {
                let _ = tx.send(Err(Error::replication_failed()));
              }
            }
            _ => return,
          }
        }
        _ = self.trigger_rx.recv().fuse() => {
          let last_log_idx = self.last.lock().log.index;
          should_stop = self.replicate_to(last_log_idx).await;
        }
        // This is _not_ our heartbeat mechanism but is to ensure
        // followers quickly learn the leader's commit index when
        // raft commits stop flowing naturally. The actual heartbeats
        // can't do this to keep them unblocked by disk IO on the
        // follower. See https://github.com/hashicorp/raft/issues/282.
        _ = async {
          let timeout = random_timeout(self.commit_timeout).unwrap();
          <T::Runtime as Runtime>::sleep(timeout)
        }.fuse() => {
          let last_log_idx = self.last.lock().log.index;
          should_stop = self.replicate_to(last_log_idx).await;
        }
      }

      // If things looks healthy, switch to pipeline mode
      if !should_stop && self.allow_pipeline {
        // Disable until re-enabled
        self.allow_pipeline = false;

        // Replicates using a pipeline for high performance. This method
        // is not able to gracefully recover from errors, and so we fall back
        // to standard mode on failure.
        if let Err(e) = self.pipeline_replicate().await {}
        continue;
      }
    }
  }

  /// Used when we have synchronized our state with the follower,
  /// and want to switch to a higher performance pipeline mode of replication.
  /// We only pipeline AppendEntries commands, and if we ever hit an error, we fall
  /// back to the standard replication which can handle more complex situations.
  async fn pipeline_replicate(&mut self) -> Result<(), Error<F, S, T>> {
    let remote = self.peer.load_full();

    // Create a new pipeline
    let pipeline = self
      .transport
      .append_entries_pipeline((*remote).clone())
      .await
      .map_err(Error::transport)?;

    let consumer = pipeline.consumer();

    // Log start and stop of pipeline
    tracing::info!(target = "ruraft.repl", remote=%remote, "starting pipeline");
    scopeguard::defer!(
      tracing::info!(target = "ruraft.repl", remote=%remote, "aborting pipeline replication")
    );

    // Create a shutdown and finish channel
    let (stop_tx, stop_rx) = async_channel::bounded(1);
    let (finish_tx, finish_rx) = async_channel::bounded(1);

    // Start pipeline sends at the last good nextIndex
    let next_idx = self.next_index.load(Ordering::Acquire);

    let mut should_stop = false;
    Ok(())
  }

  /// Used to send data over a pipeline. It is a helper to
  /// pipelineReplicate.
  async fn pipeline_send() -> bool {
    
    false
  }

  async fn pipeline_decode(
    peer: Arc<ArcSwap<Node<T::Id, <T::Resolver as AddressResolver>::Address>>>,
    mut consumer: impl Stream<Item = PipelineAppendEntriesResponse<T::Id, <T::Resolver as AddressResolver>::Address>>
      + Unpin,
    notify: Arc<Mutex<HashMap<u64, Verify<F, S, T>>>>,
    next_index: Arc<AtomicU64>,
    commitment: Commitment<T::Id, <T::Resolver as AddressResolver>::Address>,
    last_contact: Contact,
    stop_rx: async_channel::Receiver<()>,
    finish_tx: async_channel::Sender<()>,
    step_down_tx: async_channel::Sender<()>,
  ) {
    scopeguard::defer!(let _ = finish_tx.close(););

    loop {
      futures::select! {
        resp = consumer.next().fuse() => {
          match resp {
            Some(resp) => {
              let remote = peer.load_full();

              #[cfg(feature = "metrics")]
              append_stats(&remote, resp.start(), resp.num_entries as u64);

              // Check for a newer term, stop running
              if resp.resp.term > resp.term {
                Self::handle_stale_term(
                  &remote,
                  &notify,
                  &step_down_tx,
                ).await;
                return;
              }

              // Update the last contact
              last_contact.update();

              // Abort pipeline if not successful
              if !resp.resp().success {
                return;
              }

              // Update our replication state
              Self::update_last_appended(remote.id(), &next_index, &commitment, &notify, resp.highest_log_index()).await;

            }
            None => {
              tracing::error!(target = "ruraft.repl", err="pipeline closed", "failed to get next item from pipeline");
              return;
            }
          }
        },
        _ = stop_rx.recv().fuse() => {
          return;
        }
      }
    }
  }

  /// used to replicate the logs up to a
  /// given last index.
  /// If the follower log is behind, we take care to bring them up to date.
  ///
  /// Returns true if the replication should stop.
  async fn replicate_to(&mut self, last_index: u64) -> bool {
    macro_rules! check_more {
      ($this:ident) => {{
        // Check if there is more to replicate
        // Poll the stop channel here in case we are looping and have been asked
        // to stop, or have stepped down as leader. Even for the best effort case
        // where we are asked to replicate to a given index and then shutdown,
        // it's better to not loop in here to send lots of entries to a straggler
        // that's leaving the cluster anyways.

        futures::select! {
          _ = $this.stop_rx.recv().fuse() => {
            return true;
          }
          default => {}
        }

        // Check if there are more logs to replicate
        if $this.next_index.load(Ordering::Acquire) <= last_index {
          continue;
        }
        return false;
      }};
    }

    loop {
      // Prevent an excessive retry rate on errors
      if self.failures > 0 {
        let timeout = backoff(FAILURE_WAIT, self.failures, MAX_FAILURE_SCALE);
        futures::select! {
          _ = <T::Runtime as Runtime>::sleep(timeout).fuse() => {}
          _ = self.shutdown_rx.recv().fuse() => return true,
        }
      }

      let remote = self.peer.load_full();

      match self
        .setup_append_entries(self.next_index.load(Ordering::Acquire), last_index)
        .await
      {
        Err(Error::Raft(RaftError::LogNotFound(_))) => match self.send_latest_snapshot().await {
          Err(e) => {
            tracing::error!(target = "ruraft.repl", remote=%remote, err=%e, "failed to send snapshot");
            return false;
          }
          Ok(true) => return true,
          Ok(false) => check_more!(self),
        },
        Err(_) => return false,
        Ok(req) => {
          // Make the RPC call
          let start = Instant::now();
          #[cfg(feature = "metrics")]
          let num_entries = req.entries.len();
          let req_last_index = req.entries.last().map(|l| l.index);

          match self.transport.append_entries(req).await {
            Ok(resp) => {
              #[cfg(feature = "metrics")]
              append_stats(&remote, start, num_entries as u64);
              // Check for a newer term, stop running
              if resp.term > self.current_term {
                Self::handle_stale_term(&remote, &self.notify, &self.step_down_tx).await;
                return true;
              }

              // Update the last contact
              self.set_last_contact();

              // Update s based on success
              if resp.success {
                // Update our replication state
                Self::update_last_appended(
                  remote.id(),
                  &self.next_index,
                  &self.commitment,
                  &self.notify,
                  req_last_index,
                );

                // Clear any failures, allow pipelining
                self.failures = 0;
                self.allow_pipeline = true;
              } else {
                self
                  .next_index
                  .fetch_update(Ordering::Release, Ordering::Acquire, |next_idx| {
                    Some((next_idx - 1).min(resp.last_log + 1).max(1))
                  });
                if resp.no_retry_backoff {
                  self.failures = 0;
                } else {
                  self.failures += 1;
                }
                tracing::warn!(target = "ruraft.repl", remote=%remote, next = self.next_index.load(Ordering::Acquire), "append entries rejected, sending older logs");
              }

              check_more!(self);
            }
            Err(e) => {
              tracing::error!(target = "ruraft.repl", remote=%remote, err=%e, "failed to append entries");
              self.failures += 1;
              return false;
            }
          }
        }
      }
    }
  }

  /// Used to send the latest snapshot we have
  /// down to our follower.
  async fn send_latest_snapshot(&mut self) -> Result<bool, Error<F, S, T>> {
    // Get the snapshots
    let snap_store = self.storage.snapshot_store();
    let snapshots = snap_store.list().await.map_err(|e| {
      tracing::error!(target = "ruraft.repl", err=%e, "failed to list snapshots");
      Error::snapshot(e)
    })?;

    // Check we have at least a single snapshot
    if snapshots.is_empty() {
      tracing::error!(target = "ruraft.repl", err = "no snapshots found");
      return Err(Error::no_snapshots());
    }
    // Open the most recent snapshot
    let snap_id = snapshots[0].id();
    let snap = snap_store.open(&snap_id).await.map_err(|e| {
      tracing::error!(target = "ruraft.repl", id=%snap_id, err=%e, "failed to open snapshot");
      Error::snapshot(e)
    })?;

    // Setup the request
    let meta = snap.meta();
    let req = InstallSnapshotRequest {
      header: self.transport.header(),
      snapshot_version: meta.version(),
      term: self.current_term,
      last_log_index: meta.index(),
      last_log_term: meta.term(),
      membership: meta.membership().clone(),
      membership_index: meta.membership_index(),
      size: meta.size(),
    };

    let remote = self.peer.load_full();

    // make the call
    let start = Instant::now();
    let meta_idx = meta.index();
    match self.transport.install_snapshot(req, snap).await {
      Err(e) => {
        tracing::error!(target = "ruraft.repl", remote=%remote, id=%snap_id, err=%e, "failed to install snapshot");
        self.failures += 1;
        return Err(Error::transport(e));
      }
      Ok(resp) => {
        #[cfg(feature = "metrics")]
        metrics::gauge!(
          format!("ruraft.repl.install_snapshot.{}", remote.id()),
          start.elapsed().as_millis() as f64
        );

        // Check for a newer term, stop running
        if resp.term > self.current_term {
          Self::handle_stale_term(&remote, &self.notify, &self.step_down_tx).await;
          return Ok(true);
        }

        // Update the last contact
        self.set_last_contact();

        // Check for success
        if resp.success {
          // update the indexes
          self.next_index.store(meta_idx + 1, Ordering::Release);
          self.commitment.match_index(remote.id(), meta_idx).await;

          // clear any failures
          self.failures = 0;

          // notify we are still leader
          Replication::notify_all(&self.notify, true).await;
        } else {
          self.failures += 1;
          tracing::warn!(target = "ruraft.repl", remote=%remote, id=%snap_id, "install snapshot rejected");
        }
        Ok(false)
      }
    }
  }

  async fn handle_stale_term(
    remote: &Node<T::Id, <T::Resolver as AddressResolver>::Address>,
    notify: &Mutex<HashMap<u64, Verify<F, S, T>>>,
    step_down_tx: &async_channel::Sender<()>,
  ) {
    tracing::error!(target = "ruraft.repl", remote=%remote, "peer has newer term, stopping replication");
    Replication::notify_all(notify, false).await;
    let _ = step_down_tx.send(()).await;
  }

  /// used to update follower replication state after a
  /// successful AppendEntries RPC.
  async fn update_last_appended(
    id: &T::Id,
    next_idx: &AtomicU64,
    commitment: &Commitment<T::Id, <T::Resolver as AddressResolver>::Address>,
    notify: &Mutex<HashMap<u64, Verify<F, S, T>>>,
    last_index: Option<u64>,
  ) {
    // Mark any inflight logs as committed
    if let Some(idx) = last_index {
      next_idx.store(idx + 1, Ordering::Release);
      commitment.match_index(id, idx).await;
    }
    // Notify still leader
    Replication::notify_all(notify, true).await;
  }

  /// used to setup an append entries request.
  async fn setup_append_entries(
    &self,
    next_idx: u64,
    last_idx: u64,
  ) -> Result<AppendEntriesRequest<T::Id, <T::Resolver as AddressResolver>::Address>, Error<F, S, T>>
  {
    let header = self.transport.header();
    let term = self.current_term;
    let ls = self.storage.log_store();
    let mut req = AppendEntriesRequest {
      header,
      term,
      prev_log_entry: 0,
      prev_log_term: 0,
      entries: Vec::with_capacity(self.max_append_entries as usize),
      leader_commit: self.committed_index.load(Ordering::Acquire),
    };
    self.set_previous_log(ls, next_idx, &mut req).await?;
    self
      .set_new_logs(ls, next_idx, last_idx, &mut req)
      .await
      .map(|_| req)
  }

  /// Used to setup the PrevLogEntry and PrevLogTerm for an
  /// [`AppendEntriesRequest`] given the next index to replicate.
  async fn set_previous_log(
    &self,
    ls: &S::Log,
    next_idx: u64,
    req: &mut AppendEntriesRequest<T::Id, <T::Resolver as AddressResolver>::Address>,
  ) -> Result<(), Error<F, S, T>> {
    let LastSnapshot {
      term: last_snapshot_term,
      index: last_snapshot_index,
    } = self.last.lock().snapshot;

    if next_idx == 1 {
      req.prev_log_entry = 0;
      req.prev_log_term = 0;
      Ok(())
    } else if (next_idx - 1) == last_snapshot_index {
      req.prev_log_entry = last_snapshot_index;
      req.prev_log_term = last_snapshot_term;
      Ok(())
    } else {
      match ls.get_log(next_idx - 1).await {
        Ok(Some(log)) => {
          req.prev_log_entry = log.index;
          req.prev_log_term = log.term;
          Ok(())
        }
        Ok(None) => {
          tracing::error!(target = "ruraft.repl", index=%(next_idx - 1), "failed to get log");
          Err(Error::log_not_found(next_idx - 1))
        }
        Err(e) => {
          tracing::error!(target = "ruraft.repl", index=%(next_idx - 1), err=%e, "failed to get log");
          Err(Error::log(e))
        }
      }
    }
  }

  /// Used to setup the logs which should be appended for a request.
  async fn set_new_logs(
    &self,
    ls: &S::Log,
    next_idx: u64,
    last_idx: u64,
    req: &mut AppendEntriesRequest<T::Id, <T::Resolver as AddressResolver>::Address>,
  ) -> Result<(), Error<F, S, T>> {
    // Append up to MaxAppendEntries or up to the lastIndex. we need to use a
    // consistent value for maxAppendEntries in the lines below in case it ever
    // becomes reloadable.
    let match_index = (next_idx + self.max_append_entries - 1).min(last_idx);
    for i in next_idx..=match_index {
      match ls.get_log(i).await {
        Ok(Some(log)) => {
          req.entries.push(log);
        }
        Ok(None) => {
          tracing::error!(target = "ruraft.repl", index=%i, "failed to get log");
          return Err(Error::log_not_found(i));
        }
        Err(e) => {
          tracing::error!(target = "ruraft.repl", index=%i, err=%e, "failed to get log");
          return Err(Error::log(e));
        }
      }
    }
    Ok(())
  }

  fn set_last_contact(&self) {
    self.last_contact.update();
  }
}

impl<F, S, T, SC, R> super::RaftRunner<F, S, T, SC, R>
where
  F: FinateStateMachine<
    Id = T::Id,
    Address = <T::Resolver as AddressResolver>::Address,
    SnapshotSink = <S::Snapshot as super::SnapshotStorage>::Sink,
    Runtime = R,
  >,
  S: Storage<Id = T::Id, Address = <T::Resolver as AddressResolver>::Address, Runtime = R>,
  T: Transport<Runtime = R>,
  <T::Resolver as AddressResolver>::Address: Send + Sync + 'static,
  SC: super::Sidecar<Runtime = R>,
  R: Runtime,
  <R::Sleep as std::future::Future>::Output: Send,
  <R::Interval as futures::Stream>::Item: Send + 'static,
{
  /// A long running task that replicates log entries to a single
  /// follower.
  pub(super) async fn replicate(&self, mut repl: ReplicationRunner<F, S, T>) {
    // let runner_wg = self.wg.clone();
    // spawn_local::<R, _>(self.wg.add(1), async move {
    //   // Start an async heartbeating routing
    //   let (stop_heartbeat_tx, stop_heartbeat_rx) = async_channel::bounded(1);
    //   spawn_local::<R, _>(runner_wg.add(1), async move {

    //   });

    // });
  }
}

struct HeartbeatRunner<F: FinateStateMachine, S: Storage, T: Transport> {
  current_term: u64,
  trans: Arc<T>,
  peer: Arc<ArcSwap<Node<T::Id, <T::Resolver as AddressResolver>::Address>>>,
  observers: Arc<
    async_lock::RwLock<
      HashMap<ObserverId, Observer<T::Id, <T::Resolver as AddressResolver>::Address>>,
    >,
  >,
  notify_all: Arc<Mutex<HashMap<u64, Verify<F, S, T>>>>,
  opts: Arc<Atomic<ReloadableOptions>>,
  last_contact: Contact,
  heartbeat_signal: async_channel::Receiver<()>,
  stop_heartbeat_rx: async_channel::Receiver<()>,
}

impl<F: FinateStateMachine, S: Storage, T: Transport> HeartbeatRunner<F, S, T>
where
  <T::Resolver as AddressResolver>::Address: Send + Sync + 'static,
{
  async fn run(self) {
    let Self {
      current_term,
      trans,
      peer,
      observers,
      notify_all,
      opts,
      last_contact,
      heartbeat_signal,
      stop_heartbeat_rx,
    } = self;
    let mut failures = 0;
    loop {
      // Wait for the next heartbeat interval or forced notify
      futures::select! {
        _ = heartbeat_signal.recv().fuse() => {}
        _ = stop_heartbeat_rx.recv().fuse() => return,
        _ = {
          let timeout = random_timeout(opts.load(Ordering::Acquire).heartbeat_timeout()).unwrap();
          <T::Runtime as Runtime>::sleep(timeout)
        }.fuse() => {}
      }

      let remote = peer.load_full();

      #[cfg(feature = "metrics")]
      let start = Instant::now();

      let req = HeartbeatRequest {
        header: trans.header(),
        term: current_term,
      };
      match trans.heartbeat(req).await {
        Ok(resp) => {
          if failures > 0 {
            observe(
              &observers,
              Observed::HeartbeatResumed(resp.header.id().clone()),
            )
            .await;
          }
          last_contact.update();
          failures = 0;
          #[cfg(feature = "metrics")]
          metrics::gauge!(
            format!("ruraft.repl.heartbeat.{}", remote.id()),
            start.elapsed().as_millis() as f64
          );
          Replication::notify_all(&notify_all, resp.success).await;
        }
        Err(e) => {
          let next_backoff_time = capped_exponential_backoff(
            FAILURE_WAIT,
            failures,
            MAX_FAILURE_SCALE,
            random_timeout(opts.load(Ordering::Acquire).heartbeat_timeout()).unwrap() / 2,
          );
          tracing::error!(target = "ruraft.repl", remote=%remote, backoff_time = %humantime::Duration::from(next_backoff_time), err=%e, "failed to heartbeat");
          observe(
            &observers,
            Observed::HeartbeatFailed {
              id: remote.id().clone(),
              last_contact: last_contact.get(),
            },
          )
          .await;
          failures += 1;
          futures::select! {
            _ = <T::Runtime as Runtime>::sleep(next_backoff_time).fuse() => {}
            _ = stop_heartbeat_rx.recv().fuse() => return,
          }
        }
      }
    }
  }
}

#[cfg(feature = "metrics")]
fn append_stats<I: nodecraft::Id, A: nodecraft::Address>(
  remote: &Node<I, A>,
  start: Instant,
  logs: u64,
) {
  enum Kind<'a, I: nodecraft::Id> {
    Rpc(&'a I),
    Log(&'a I),
  }

  impl<'a, I: nodecraft::Id> core::fmt::Display for Kind<'a, I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
      match self {
        Kind::Rpc(id) => write!(f, "ruraft.repl.append_entries.rpc.{}", id),
        Kind::Log(id) => write!(f, "ruraft.repl.append_entries.logs.{}", id),
      }
    }
  }

  impl<'a, I: nodecraft::Id> Into<metrics::KeyName> for Kind<'a, I> {
    fn into(self) -> metrics::KeyName {
      metrics::KeyName::from(self.to_string())
    }
  }

  let id = remote.id();
  let log = Kind::Log(id);
  let rpc = Kind::Rpc(id);
  metrics::gauge!(rpc, start.elapsed().as_millis() as f64);
  // metrics::counter!(log, logs as f64);
}
