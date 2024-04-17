use std::{
  future::Future,
  pin::Pin,
  sync::{atomic::Ordering, Arc},
  task::{Context, Poll},
  time::{Duration, Instant},
};

use agnostic_lite::RuntimeLite;
use futures::{channel::oneshot, future::Either, Stream};
use nodecraft::resolver::AddressResolver;

use crate::{
  error::Error,
  membership::{Membership, MembershipChangeCommand},
  options::{Options, ProtocolVersion, ReloadableOptions, SnapshotVersion},
  sidecar::Sidecar,
  storage::{LogKind, SnapshotMeta, SnapshotSource, Storage},
  transport::Transport,
  FinateStateMachine, Node, RaftCore, Role,
};

pub use futures::{FutureExt, StreamExt};

impl<F, S, T, SC, R> RaftCore<F, S, T, SC, R>
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
  SC: Sidecar<Runtime = R>,
  R: RuntimeLite,
{
  /// Returns the current state of the reloadable fields in Raft's
  /// options. This is useful for programs to discover the current state for
  /// reporting to users or tests. It is safe to call concurrently. It is
  /// intended for reporting and testing purposes primarily; external
  /// synchronization would be required to safely use this in a read-modify-write
  /// pattern for reloadable options.
  pub fn reloadable_options(&self) -> ReloadableOptions {
    self.inner.reloadable_options.load(Ordering::Acquire)
  }

  /// Returns the current options in use by the Raft instance.
  pub fn options(&self) -> Options {
    self.inner.options.apply(self.reloadable_options())
  }

  /// Provides the local unique identifier, helping in distinguishing this node from its peers.
  pub fn local_id(&self) -> &T::Id {
    self.inner.transport.local_id()
  }

  /// Provides the local address, helping in distinguishing this node from its peers.
  pub fn local_addr(&self) -> &<T::Resolver as AddressResolver>::Address {
    self.inner.transport.local_addr()
  }

  /// Returns the current role of the node.
  pub fn role(&self) -> Role {
    self.inner.state.role()
  }

  /// Returns the current term the node.
  pub fn current_term(&self) -> u64 {
    self.inner.state.current_term()
  }

  /// Returns the latest membership. This may not yet be
  /// committed.
  pub fn latest_membership(
    &self,
  ) -> LatestMembership<T::Id, <T::Resolver as AddressResolver>::Address> {
    let membership = self.inner.memberships.latest();
    LatestMembership {
      index: membership.0,
      membership: membership.1.clone(),
    }
  }

  /// Returns the time of last contact by a leader.
  /// This only makes sense if we are currently a follower.
  pub fn last_contact(&self) -> Option<Instant> {
    self.inner.last_contact.get()
  }

  /// Returns the last index in stable storage,
  /// either from the last log or from the last snapshot.
  pub fn last_index(&self) -> u64 {
    self.inner.state.last_index()
  }

  /// Returns the committed index.
  /// This API maybe helpful for server to implement the read index optimization
  /// as described in the Raft paper.
  pub fn commit_index(&self) -> u64 {
    self.inner.state.commit_index()
  }

  /// Returns the last index applied to the [`FinateStateMachine`]. This is generally
  /// lagging behind the last index, especially for indexes that are persisted but
  /// have not yet been considered committed by the leader.
  ///
  /// **NOTE** - this reflects
  /// the last index that was sent to the application's [`FinateStateMachine`] over the apply channel
  /// but DOES NOT mean that the application's [`FinateStateMachine`] has yet consumed it and applied
  /// it to its internal state. Thus, the application's state may lag behind this
  /// index.
  pub fn applied_index(&self) -> u64 {
    self.inner.state.last_applied()
  }

  /// Used to return the current leader address and ID of the cluster.
  /// It may return `None` if there is no current leader
  /// or the leader is unknown.
  pub fn leader(&self) -> Option<Arc<Node<T::Id, <T::Resolver as AddressResolver>::Address>>> {
    self.inner.leader.load().clone()
  }

  /// Used to get a stream which delivers signals on acquiring or
  /// losing leadership. It sends true if we become the leader, and false if we
  /// lose it.
  ///
  /// Receivers can expect to receive a notification only if leadership
  /// transition has occured.
  ///
  /// If receivers aren't ready for the signal, signals may drop and only the
  /// latest leadership transition. For example, if a receiver receives subsequent
  /// `true` values, they may deduce that leadership was lost and regained while
  /// the the receiver was processing first leadership transition.
  ///
  /// If you want to watch all leadership transitions, use [`leadership_change_watcher`].
  pub fn leadership_watcher(&self) -> LeadershipWatcher {
    LeadershipWatcher(self.inner.leader_rx.clone())
  }

  /// Used to get a stream which will receive all of leadership changes.
  /// Unlike [`leadership_watcher`], no change will be overriden,
  /// which means subsequent `true` values will never happen.
  ///
  /// [`leadership_watcher`]: struct.RaftCore.html#method.leader_watcher
  #[must_use = "The `LeadershipWatcher` returned by `leadership_change_watcher` must be aggressively consumed. Otherwise, you should consider to use `leadership_watcher`."]
  pub fn leadership_change_watcher(&self) -> LeadershipWatcher {
    LeadershipWatcher(self.inner.leadership_change_rx.clone())
  }
}

impl<F, S, T, SC, R> RaftCore<F, S, T, SC, R>
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
  SC: Sidecar<Runtime = R>,
  R: RuntimeLite,
{
  /// Used to apply a command to the [`FinateStateMachine`] in a highly consistent
  /// manner. This returns a future that can be used to wait on the application.
  ///
  /// This must be run on the leader or it
  /// will fail.
  ///
  /// - If the node discovers it is no longer the leader while applying the command,
  /// it will return `Err(Error::Raft(RaftError::LeadershipLost))`. There is no way to guarantee whether the
  /// write succeeded or failed in this case. For example, if the leader is
  /// partitioned it can't know if a quorum of followers wrote the log to disk. If
  /// at least one did, it may survive into the next leader's term.
  ///
  /// - If a user snapshot is restored while the command is in-flight, an
  /// `Err(Error::Raft(RaftError::AbortedByRestore))` is returned. In this case the write effectively failed
  /// since its effects will not be present in the [`FinateStateMachine`] after the restore.
  ///
  /// See also [`apply_timeout`].
  ///
  /// [`apply_timeout`]: struct.RaftCore.html#method.apply_timeout
  pub async fn apply(&self, data: T::Data) -> ApplyFuture<F, S, T> {
    self.apply_in(data, None).await
  }

  /// Used to apply a command to the [`FinateStateMachine`] in a highly consistent
  /// manner. This returns a future that can be used to wait on the application.
  ///
  /// An optional timeout can be provided to limit the amount of time we wait
  /// for the command to be started. This must be run on the leader or it
  /// will fail.
  ///
  /// - If the node discovers it is no longer the leader while applying the command,
  /// it will return `Err(Error::Raft(RaftError::LeadershipLost))`. There is no way to guarantee whether the
  /// write succeeded or failed in this case. For example, if the leader is
  /// partitioned it can't know if a quorum of followers wrote the log to disk. If
  /// at least one did, it may survive into the next leader's term.
  ///
  /// - If a user snapshot is restored while the command is in-flight, an
  /// `Err(Error::Raft(RaftError::AbortedByRestore))` is returned. In this case the write effectively failed
  /// since its effects will not be present in the [`FinateStateMachine`] after the restore.
  ///
  /// See also [`apply`].
  ///
  /// [`apply`]: struct.RaftCore.html#method.apply
  pub async fn apply_timeout(&self, data: T::Data, timeout: Duration) -> ApplyFuture<F, S, T> {
    self.apply_in(data, Some(timeout)).await
  }

  /// Used to issue a command that blocks until all preceding
  /// operations have been applied to the [`FinateStateMachine`]. It can be used to ensure the
  /// [`FinateStateMachine`] reflects all queued writes. This
  /// must be run on the leader, or it will fail.
  ///
  /// See also [`barrier_timeout`].
  ///
  /// [`barrier_timeout`]: struct.RaftCore.html#method.barrier_timeout
  pub async fn barrier(&self) -> BarrierFuture<F, S, T> {
    self.barrier_in(None).await
  }

  /// Used to issue a command that blocks until all preceding
  /// operations have been applied to the [`FinateStateMachine`]. It can be used to ensure the
  /// [`FinateStateMachine`] reflects all queued writes. An optional timeout can be provided to
  /// limit the amount of time we wait for the command to be started. This
  /// must be run on the leader, or it will fail.
  ///
  /// See also [`barrier`].
  ///
  /// [`barrier`]: struct.RaftCore.html#method.barrier
  pub async fn barrier_timeout(&self, timeout: Duration) -> BarrierFuture<F, S, T> {
    self.barrier_in(Some(timeout)).await
  }

  /// Used to ensure this peer is still the leader. It may be used
  /// to prevent returning stale data from the FSM after the peer has lost
  /// leadership.
  pub async fn verify_leader(&self) -> VerifyFuture<F, S, T> {
    if let Err(e) = self.is_shutdown() {
      return e;
    }

    #[cfg(feature = "metrics")]
    metrics::counter!("ruraft.verify_leader").increment(1);

    let (tx, rx) = oneshot::channel();
    match self.inner.verify_tx.send(tx).await {
      Ok(_) => VerifyFuture::ok(rx),
      Err(_) => VerifyFuture::err(Error::shutdown()),
    }
  }

  /// Add the given server to the cluster as a staging server. If the
  /// server is already in the cluster as a voter, this updates the server's address.
  /// This must be run on the leader or it will fail. The leader will promote the
  /// staging server to a voter once that server is ready.
  ///
  /// - If `prev_index` is non-zero,
  /// the index of the only membership upon which this change may be applied.
  ///
  /// - If another membership entry has been added in the meantime,
  /// this request will fail.
  ///
  /// See also [`add_voter_timeout`].
  ///
  /// [`add_voter_timeout`]: struct.RaftCore.html#method.add_voter_timeout
  pub async fn add_voter(
    &self,
    id: T::Id,
    addr: <T::Resolver as AddressResolver>::Address,
    prev_index: u64,
  ) -> MembershipChangeFuture<F, S, T> {
    self
      .request_membership_change(
        MembershipChangeCommand::add_voter(id, addr, prev_index),
        None,
      )
      .await
  }

  /// Add the given server to the cluster as a staging server. If the
  /// server is already in the cluster as a voter, this updates the server's address.
  /// This must be run on the leader or it will fail. The leader will promote the
  /// staging server to a voter once that server is ready.
  ///
  /// - If `prev_index` is non-zero,
  /// the index of the only membership upon which this change may be applied;
  /// and `timeout`` is how long this server should wait before the
  /// membership change log entry is appended.
  ///
  /// - If another membership entry has been added in the meantime,
  /// this request will fail.
  ///
  /// See also [`add_voter`].
  ///
  /// [`add_voter`]: struct.RaftCore.html#method.add_voter
  pub async fn add_voter_timeout(
    &self,
    id: T::Id,
    addr: <T::Resolver as AddressResolver>::Address,
    prev_index: u64,
    timeout: Duration,
  ) -> MembershipChangeFuture<F, S, T> {
    self
      .request_membership_change(
        MembershipChangeCommand::add_voter(id, addr, prev_index),
        Some(timeout),
      )
      .await
  }

  /// Add the given server to the cluster but won't assign it a
  /// vote. The server will receive log entries, but it won't participate in
  /// elections or log entry commitment. If the server is already in the cluster,
  /// this updates the server's address. This must be run on the leader or it will
  /// fail.
  ///
  /// For `prev_index`, see [`add_voter`].
  ///
  /// See also [`add_nonvoter_timeout`].
  ///  
  /// [`add_voter`]: struct.RaftCore.html#method.add_voter
  /// [`add_nonvoter_timeout`]: struct.RaftCore.html#method.add_nonvoter_timeout
  pub async fn add_nonvoter(
    &self,
    id: T::Id,
    addr: <T::Resolver as AddressResolver>::Address,
    prev_index: u64,
  ) -> MembershipChangeFuture<F, S, T> {
    self
      .request_membership_change(
        MembershipChangeCommand::add_nonvoter(id, addr, prev_index),
        None,
      )
      .await
  }

  /// Add the given server to the cluster but won't assign it a
  /// vote. The server will receive log entries, but it won't participate in
  /// elections or log entry commitment. If the server is already in the cluster,
  /// this updates the server's address. This must be run on the leader or it will
  /// fail.
  ///  
  /// For `prev_index` and `timeout`, see [`add_voter_timeout`].
  ///
  /// See also [`add_nonvoter`].
  ///  
  /// [`add_voter_timeout`]: struct.RaftCore.html#method.add_voter_timeout
  /// [`add_nonvoter`]: struct.RaftCore.html#method.add_nonvoter
  pub async fn add_nonvoter_timeout(
    &self,
    id: T::Id,
    addr: <T::Resolver as AddressResolver>::Address,
    prev_index: u64,
    timeout: Duration,
  ) -> MembershipChangeFuture<F, S, T> {
    self
      .request_membership_change(
        MembershipChangeCommand::add_nonvoter(id, addr, prev_index),
        Some(timeout),
      )
      .await
  }

  /// Remove the given server from the cluster. If the current
  /// leader is being removed, it will cause a new election to occur. This must be
  /// run on the leader or it will fail.
  ///
  /// For `prev_index`, see [`add_voter`].
  ///
  /// See also [`remove_timeout`].
  ///  
  /// [`add_voter`]: struct.RaftCore.html#method.add_voter
  /// [`remove_timeout`]: struct.RaftCore.html#method.remove_timeout
  pub async fn remove(&self, id: T::Id, prev_index: u64) -> MembershipChangeFuture<F, S, T> {
    self
      .request_membership_change(MembershipChangeCommand::remove_server(id, prev_index), None)
      .await
  }

  /// Remove the given server from the cluster. If the current
  /// leader is being removed, it will cause a new election to occur. This must be
  /// run on the leader or it will fail.
  ///
  /// For `prev_index` and `timeout`, see [`add_voter_timeout`].
  ///
  /// See also [`remove`].
  ///  
  /// [`add_voter_timeout`]: struct.RaftCore.html#method.add_voter_timeout
  /// [`remove`]: struct.RaftCore.html#method.remove
  pub async fn remove_timeout(
    &self,
    id: T::Id,
    prev_index: u64,
    timeout: Duration,
  ) -> MembershipChangeFuture<F, S, T> {
    self
      .request_membership_change(
        MembershipChangeCommand::remove_server(id, prev_index),
        Some(timeout),
      )
      .await
  }

  /// Take away a server's vote, if it has one. If present, the
  /// server will continue to receive log entries, but it won't participate in
  /// elections or log entry commitment. If the server is not in the cluster, this
  /// does nothing. This must be run on the leader or it will fail.
  ///
  /// For `prev_index`, see [`add_voter`].
  ///
  /// See also [`demote_voter_timeout`].
  ///
  /// [`add_voter`]: struct.RaftCore.html#method.add_voter
  /// [`demote_voter_timeout`]: struct.RaftCore.html#method.demote_voter_timeout
  pub async fn demote_voter(&self, id: T::Id, prev_index: u64) -> MembershipChangeFuture<F, S, T> {
    self
      .request_membership_change(MembershipChangeCommand::demote_voter(id, prev_index), None)
      .await
  }

  /// Take away a server's vote, if it has one. If present, the
  /// server will continue to receive log entries, but it won't participate in
  /// elections or log entry commitment. If the server is not in the cluster, this
  /// does nothing. This must be run on the leader or it will fail.
  ///
  /// For `prev_index` and `timeout`, see [`add_voter_timeout`].
  ///
  /// See also [`demote_voter`].
  ///  
  /// [`add_voter_timeout`]: struct.RaftCore.html#method.add_voter_timeout
  /// [`demote_voter`]: struct.RaftCore.html#method.demote_voter
  pub async fn demote_voter_timeout(
    &self,
    id: T::Id,
    prev_index: u64,
    timeout: Duration,
  ) -> MembershipChangeFuture<F, S, T> {
    self
      .request_membership_change(
        MembershipChangeCommand::demote_voter(id, prev_index),
        Some(timeout),
      )
      .await
  }

  /// Updates the options of a running raft node. If the new
  /// options is invalid an error is returned and no changes made to the
  /// instance. All fields will be copied from rc into the new options, even
  /// if they are zero valued.
  pub async fn reload_options(&self, rc: ReloadableOptions) -> Result<(), Error<F, S, T>> {
    self.is_shutdown_error()?;

    rc.validate(self.inner.options.leader_lease_timeout)?;
    let _mu = self.inner.reload_options_lock.lock().await;
    let old = self.inner.reloadable_options.swap(rc, Ordering::Release);

    if rc.heartbeat_timeout() < old.heartbeat_timeout() {
      // On leader, ensure replication loops running with a longer
      // timeout than what we want now discover the change.
      // On follower, update current timer to use the shorter new value.
      let (lres, fres) = futures::future::join(
        self.inner.leader_notify_tx.send(()),
        self.inner.follower_notify_tx.send(()),
      )
      .await;
      if let Err(e) = lres {
        tracing::error!(target = "ruraft", err=%e, "failed to notify leader the options has been changed");
      }

      if let Err(e) = fres {
        tracing::error!(target = "ruraft", err=%e, "failed to notify followers the options has been changed");
      }
    }
    Ok(())
  }

  /// Used to stop the Raft background routines.
  /// This is not a graceful operation.
  /// It is safe to call this multiple times.
  ///
  /// Returns `true` if this call has shutdown the Raft and it was not shutdown already.
  pub async fn shutdown(&self) -> bool {
    if self.inner.shutdown.is_shutdown() {
      return false;
    }

    self
      .inner
      .shutdown
      .shutdown(&self.inner.state, &self.inner.observers)
      .await
  }

  /// Used to manually force Raft to take a snapshot. Returns a future
  /// that can be used to block until complete, and that contains a function that
  /// can be used to open the snapshot.
  ///
  /// See also [`snapshot_timeout`].
  ///
  /// [`snapshot_timeout`]: struct.RaftCore.html#method.snapshot_timeout
  pub async fn snapshot(&self) -> SnapshotFuture<F, S, T> {
    self.snapshot_in(None).await
  }

  /// Used to manually force Raft to take a snapshot. Returns a future
  /// that can be used to block until complete, and that contains a function that
  /// can be used to open the snapshot. A timeout can be provided to limit the amount of time we wait.
  ///
  /// See also [`snapshot`].
  ///
  /// [`snapshot`]: struct.RaftCore.html#method.snapshot
  pub async fn snapshot_timeout(&self, timeout: Duration) -> SnapshotFuture<F, S, T> {
    self.snapshot_in(Some(timeout)).await
  }

  /// Used to manually force Raft to consume an external snapshot, such
  /// as if restoring from a backup. We will use the current Raft membership,
  /// not the one from the snapshot, so that we can restore into a new cluster. We
  /// will also use the max of the index of the snapshot, or the current index,
  /// and then add 1 to that, so we force a new state with a hole in the Raft log,
  /// so that the snapshot will be sent to followers and used for any new joiners.
  /// This can only be run on the leader, and blocks until the restore is complete
  /// or an error occurs.
  ///
  /// **WARNING!** This operation has the leader take on the state of the snapshot and
  /// then sets itself up so that it replicates that to its followers though the
  /// install snapshot process. This involves a potentially dangerous period where
  /// the leader commits ahead of its followers, so should only be used for disaster
  /// recovery into a fresh cluster, and should not be used in normal operations.
  ///
  /// See also [`restore_timeout`].
  ///
  /// [`restore_timeout`]: struct.RaftCore.html#method.restore_timeout
  pub async fn restore(
    &self,
    meta: SnapshotMeta<T::Id, <T::Resolver as AddressResolver>::Address>,
    source: impl futures::AsyncRead + Send + Sync + Unpin + 'static,
  ) -> Result<(), Error<F, S, T>> {
    self.restore_in(meta, source, None).await
  }

  /// Used to manually force Raft to consume an external snapshot, such
  /// as if restoring from a backup. We will use the current Raft membership,
  /// not the one from the snapshot, so that we can restore into a new cluster. We
  /// will also use the max of the index of the snapshot, or the current index,
  /// and then add 1 to that, so we force a new state with a hole in the Raft log,
  /// so that the snapshot will be sent to followers and used for any new joiners.
  /// This can only be run on the leader, and blocks until the restore is complete
  /// or an error occurs.
  ///
  /// **WARNING!** This operation has the leader take on the state of the snapshot and
  /// then sets itself up so that it replicates that to its followers though the
  /// install snapshot process. This involves a potentially dangerous period where
  /// the leader commits ahead of its followers, so should only be used for disaster
  /// recovery into a fresh cluster, and should not be used in normal operations.
  ///
  /// A timeout can be provided to limit the amount of time we wait.
  ///
  /// See also [`restore`].
  ///
  /// [`restore`]: struct.RaftCore.html#method.restore
  pub async fn restore_timeout(
    &self,
    meta: SnapshotMeta<T::Id, <T::Resolver as AddressResolver>::Address>,
    source: impl futures::AsyncRead + Send + Sync + Unpin + 'static,
    timeout: Duration,
  ) -> Result<(), Error<F, S, T>> {
    self.restore_in(meta, source, Some(timeout)).await
  }

  /// Transfer leadership to a node in the cluster.
  /// This can only be called from the leader, or it will fail. The leader will
  /// stop accepting client requests, make sure the target server is up to date
  /// and starts the transfer with a [`TimeoutNowRequest`](crate::transport::TimeoutNowRequest) message. This message has the same
  /// effect as if the election timeout on the target server fires. Since
  /// it is unlikely that another server is starting an election, it is very
  /// likely that the target server is able to win the election. If a follower cannot be promoted, it will fail
  /// gracefully.
  ///
  /// See also [`leadership_transfer_to_node`].
  ///
  /// [`leadership_transfer_to_node`]: struct.RaftCore.html#method.leadership_transfer_to_node
  pub async fn leadership_transfer(&self) -> LeadershipTransferFuture<F, S, T> {
    self.initiate_leadership_transfer(None).await
  }

  /// The same as [`leadership_transfer`] but takes a
  /// server in the arguments in case a leadership should be transitioned to a
  /// specific server in the cluster. If a
  /// follower cannot be promoted, it will fail gracefully.
  ///
  /// See also [`leadership_transfer`].
  ///
  /// [`leadership_transfer`]: struct.RaftCore.html#method.leadership_transfer
  pub async fn leadership_transfer_to_node(
    &self,
    id: T::Id,
    addr: <T::Resolver as AddressResolver>::Address,
  ) -> LeadershipTransferFuture<F, S, T> {
    self
      .initiate_leadership_transfer(Some(Node::new(id, addr)))
      .await
  }

  /// Return various internal stats. This
  /// should only be used for informative purposes or debugging.
  pub async fn stats(&self) -> RaftStats<T::Id, <T::Resolver as AddressResolver>::Address> {
    let last_log = self.inner.state.last_log();
    let last_snapshot = self.inner.state.last_snapshot();
    let membership = self.latest_membership();

    let mut num_peers = 0;
    let mut has_us = false;
    for (id, (_, suffrage)) in membership.membership.iter() {
      if suffrage.is_voter() {
        if id.eq(self.inner.transport.local_id()) {
          has_us = true;
        } else {
          num_peers += 1;
        }
      }

      if !has_us {
        num_peers = 0;
      }
    }

    RaftStats {
      last_log_index: last_log.index,
      last_log_term: last_log.term,
      last_snapshot_index: last_snapshot.index,
      last_snapshot_term: last_snapshot.term,
      latest_membership_index: membership.index,
      latest_membership: membership.membership,
      role: self.inner.state.role(),
      term: self.inner.state.current_term(),
      commit_index: self.inner.state.commit_index(),
      applied_index: self.inner.state.last_applied(),
      last_contact: self.last_contact().map(|t| t.elapsed()),
      num_peers,
      fsm_pending: self.inner.fsm_mutate_tx.len() as u64,
      snapshot_version: SnapshotVersion::V1,
      protocol_version: ProtocolVersion::V1,
    }
  }
}

impl<F, S, T, SC, R> RaftCore<F, S, T, SC, R>
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
  SC: Sidecar<Runtime = R>,
  R: RuntimeLite,
{
  async fn apply_in(&self, data: T::Data, timeout: Option<Duration>) -> ApplyFuture<F, S, T> {
    if let Err(e) = self.is_shutdown() {
      return e;
    }

    #[cfg(feature = "metrics")]
    metrics::counter!("ruraft.apply").increment(1);

    let (tx, rx) = oneshot::channel();
    let req = ApplyRequest {
      log: LogKind::Data(Arc::new(data)),
      tx: ApplySender::Log(tx),
    };
    match timeout {
      Some(timeout) => {
        futures::select! {
          _ = R::sleep(timeout).fuse() => {
            ApplyFuture::err(Error::enqueue_timeout())
          }
          rst = self.inner.apply_tx.send(req).fuse() => {
            if let Err(e) = rst {
              tracing::error!(target="ruraft", err=%e, "failed to send apply request to the raft: apply channel closed");
              ApplyFuture::err(Error::closed("apply channel closed"))
            } else {
              ApplyFuture::ok(rx)
            }
          },
        }
      }
      None => {
        if let Err(e) = self.inner.apply_tx.send(req).await {
          tracing::error!(target="ruraft", err=%e, "failed to send apply request to the raft: apply channel closed");
          ApplyFuture::err(Error::closed("apply channel closed"))
        } else {
          ApplyFuture::ok(rx)
        }
      }
    }
  }

  async fn barrier_in(&self, timeout: Option<Duration>) -> BarrierFuture<F, S, T> {
    if let Err(e) = self.is_shutdown() {
      return e;
    }

    #[cfg(feature = "metrics")]
    metrics::counter!("ruraft.barrier").increment(1);

    let (tx, rx) = oneshot::channel();
    let req = ApplyRequest {
      log: LogKind::Barrier,
      tx: ApplySender::Barrier(tx),
    };

    if let Some(timeout) = timeout {
      futures::select! {
        _ = R::sleep(timeout).fuse() => {
          BarrierFuture::err(Error::enqueue_timeout())
        }
        rst = self.inner.apply_tx.send(req).fuse() => {
          if let Err(e) = rst {
            tracing::error!(target="ruraft", err=%e, "failed to send apply request to the raft: apply channel closed");
            BarrierFuture::err(Error::closed("apply channel closed"))
          } else {
            BarrierFuture::ok(rx)
          }
        },
      }
    } else if let Err(e) = self.inner.apply_tx.send(req).await {
      tracing::error!(target="ruraft", err=%e, "failed to send apply request to the raft: apply channel closed");
      BarrierFuture::err(Error::closed("apply channel closed"))
    } else {
      BarrierFuture::ok(rx)
    }
  }

  async fn request_membership_change(
    &self,
    cmd: MembershipChangeCommand<T::Id, <T::Resolver as AddressResolver>::Address>,
    timeout: Option<Duration>,
  ) -> MembershipChangeFuture<F, S, T> {
    if let Err(e) = self.is_shutdown() {
      return e;
    }

    #[cfg(feature = "metrics")]
    metrics::counter!("ruraft.membership_change").increment(1);

    let (tx, rx) = oneshot::channel();
    let req = MembershipChangeRequest { cmd, tx };

    match timeout {
      Some(Duration::ZERO) | None => {
        if let Err(e) = self.inner.membership_change_tx.send(req).await {
          tracing::error!(target="ruraft", err=%e, "failed to send membership change request to the raft: membership change channel closed");
          return MembershipChangeFuture::err(Error::closed("membership change channel closed"));
        }
        MembershipChangeFuture::ok(rx)
      }
      Some(timeout) => {
        futures::select! {
          rst = self.inner.membership_change_tx.send(req).fuse() => {
            if let Err(e) = rst {
              tracing::error!(target="ruraft", err=%e, "failed to send membership change request to the raft: membership change channel closed");
              return MembershipChangeFuture::err(Error::closed("membership change channel closed"));
            }

            MembershipChangeFuture::ok(rx)
          }
          _ = R::sleep(timeout).fuse() => {
            MembershipChangeFuture::err(Error::enqueue_timeout())
          }
        }
      }
    }
  }

  async fn snapshot_in(&self, timeout: Option<Duration>) -> SnapshotFuture<F, S, T> {
    if let Err(e) = self.is_shutdown() {
      return e;
    }

    #[cfg(feature = "metrics")]
    metrics::counter!("ruraft.snapshot").increment(1);

    let (tx, rx) = oneshot::channel();

    match timeout {
      Some(Duration::ZERO) | None => match self.inner.user_snapshot_tx.send(tx).await {
        Ok(_) => SnapshotFuture::ok(rx),
        Err(e) => {
          tracing::error!(target = "ruraft", err=%e, "failed to send snapshot request: user snapshot channel closed");
          SnapshotFuture::err(Error::closed("user snapshot channel closed"))
        }
      },
      Some(timeout) => {
        futures::select! {
          _ = R::sleep(timeout).fuse() => {
            tracing::error!(target = "ruraft", "failed to send snapshot request: user snapshot channel closed");
            SnapshotFuture::err(Error::enqueue_timeout())
          }
          rst = self.inner.user_snapshot_tx.send(tx).fuse() => {
            if let Err(e) = rst {
              tracing::error!(target = "ruraft", err=%e, "failed to send snapshot request: user snapshot channel closed");
              return SnapshotFuture::err(Error::closed("user snapshot channel closed"));
            }

            SnapshotFuture::ok(rx)
          },
        }
      }
    }
  }

  async fn restore_in(
    &self,
    meta: SnapshotMeta<T::Id, <T::Resolver as AddressResolver>::Address>,
    source: impl futures::AsyncRead + Send + Sync + Unpin + 'static,
    timeout: Option<Duration>,
  ) -> Result<(), Error<F, S, T>> {
    self.is_shutdown_error()?;

    #[cfg(feature = "metrics")]
    metrics::counter!("ruraft.restore").increment(1);

    let (tx, rx) = oneshot::channel();

    // Perform the restore.
    match timeout {
      None => {
        if let Err(e) = self
          .inner
          .user_restore_tx
          .send(((meta, Box::new(source) as Box<_>), tx))
          .await
        {
          tracing::error!(target="ruraft", err=%e, "failed to send restore request to the raft: user restore channel closed");
          return Err(Error::closed("user restore channel closed"));
        }

        match rx.await {
          Ok(Err(e)) => Err(e),
          Err(_) => Err(Error::canceled()),
          Ok(Ok(_)) => {
            self.is_shutdown_error()?;

            // Apply a no-op log entry. Waiting for this allows us to wait until the
            // followers have gotten the restore and replicated at least this new
            // entry, which shows that we've also faulted and installed the
            // snapshot with the contents of the restore.
            let (tx, rx) = oneshot::channel();
            if let Err(e) = self
              .inner
              .apply_tx
              .send(ApplyRequest {
                log: LogKind::Noop,
                tx: ApplySender::Noop(tx),
              })
              .await
            {
              tracing::error!(target="ruraft", err=%e, "failed to send apply request to the raft: apply channel closed");
              return Err(Error::closed("apply channel closed"));
            }

            match rx.await {
              Ok(Err(e)) => Err(e),
              Err(_) => Err(Error::canceled()),
              Ok(Ok(_)) => Ok(()),
            }
          }
        }
      }
      Some(timeout) => {
        let timer = R::sleep(timeout);
        futures::pin_mut!(timer);

        futures::select! {
          _ = timer.as_mut().fuse() => {
            tracing::error!(target="ruraft", "failed to send restore request to the raft: restore channel closed");
            Err(Error::enqueue_timeout())
          }
          rst = self.inner.user_restore_tx.send(((meta, Box::new(source) as Box<_>), tx)).fuse() => {
            if let Err(e) = rst {
              tracing::error!(target="ruraft", err=%e, "failed to send restore request to the raft: restore channel closed");
              return Err(Error::closed("user restore channel closed"));
            }

            match rx.await {
              Ok(Err(e)) => Err(e),
              Err(_) => Err(Error::canceled()),
              Ok(Ok(_)) => {
                self.is_shutdown_error()?;

                // Apply a no-op log entry. Waiting for this allows us to wait until the
                // followers have gotten the restore and replicated at least this new
                // entry, which shows that we've also faulted and installed the
                // snapshot with the contents of the restore.
                let (tx, rx) = oneshot::channel();

                futures::select! {
                  _ = timer.as_mut().fuse() => {
                    tracing::error!(target="ruraft", "failed to send apply request to the raft: apply channel closed");
                    Err(Error::enqueue_timeout())
                  }
                  rst = self.inner.apply_tx.send(ApplyRequest { log: LogKind::Noop, tx: ApplySender::Noop(tx) }).fuse() => {
                    if let Err(e) = rst {
                      tracing::error!(target="ruraft", err=%e, "failed to send apply request to the raft: apply channel closed");
                      return Err(Error::closed("apply channel closed"));
                    }

                    match rx.await {
                      Ok(Err(e)) => Err(e),
                      Err(_) => Err(Error::canceled()),
                      Ok(Ok(_)) => Ok(()),
                    }
                  }
                }
              }
            }
          },
        }
      }
    }
  }

  /// Starts the leadership on the leader side, by
  /// sending a message to the `leadership_transfer_tx`, to make sure it runs in the
  /// mainloop.
  async fn initiate_leadership_transfer(
    &self,
    target: Option<Node<T::Id, <T::Resolver as AddressResolver>::Address>>,
  ) -> LeadershipTransferFuture<F, S, T> {
    if let Err(e) = self.is_shutdown() {
      return e;
    }

    let (tx, rx) = oneshot::channel();

    if let Some(ref node) = target {
      if node.id().eq(self.inner.transport.local_id()) {
        tracing::error!(target = "ruraft", "cannot transfer leadership to itself");
        return LeadershipTransferFuture::err(Error::transfer_to_self());
      }
    }

    futures::select! {
      rst = self.inner.leader_transfer_tx.send((target, tx)).fuse() => {
        match rst {
          Ok(_) => LeadershipTransferFuture::ok(rx),
          Err(e) => {
            tracing::error!(target="ruraft", err=%e, "failed to send leadership transfer request to the raft: leadership transfer channel closed");
            LeadershipTransferFuture::err(Error::closed("leadership transfer channel closed"))
          }
        }
      }
      default => {
        LeadershipTransferFuture::err(Error::enqueue_timeout())
      }
    }
  }

  fn is_shutdown<Future: Fut<F, S, T>>(&self) -> Result<(), Future> {
    if self.inner.shutdown.is_shutdown() {
      Err(Future::err(Error::shutdown()))
    } else {
      Ok(())
    }
  }

  fn is_shutdown_error(&self) -> Result<(), Error<F, S, T>> {
    if self.inner.shutdown.is_shutdown() {
      Err(Error::shutdown())
    } else {
      Ok(())
    }
  }
}

/// The information about the current stats of the Raft node.
#[viewit::viewit(vis_all = "", getters(vis_all = "pub"), setters(skip))]
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[cfg_attr(
  feature = "serde",
  serde(bound = "I: Eq + core::hash::Hash + serde::Serialize, A: serde::Serialize")
)]
pub struct RaftStats<I, A> {
  #[viewit(getter(const, attrs(doc = "Returns the role of the raft.")))]
  role: Role,
  #[viewit(getter(const, attrs(doc = "Returns the term of the raft.")))]
  term: u64,
  #[viewit(getter(const, attrs(doc = "Returns the last log index of the raft.")))]
  last_log_index: u64,
  #[viewit(getter(const, attrs(doc = "Returns the last log term of the raft.")))]
  last_log_term: u64,
  #[viewit(getter(const, attrs(doc = "Returns the committed index of the raft.")))]
  commit_index: u64,
  #[viewit(getter(const, attrs(doc = "Returns the applied index of the raft.")))]
  applied_index: u64,
  #[viewit(getter(const, attrs(doc = "Returns the number of pending fsm requests.")))]
  fsm_pending: u64,
  #[viewit(getter(const, attrs(doc = "Returns the last snapshot index of the raft.")))]
  last_snapshot_index: u64,
  #[viewit(getter(const, attrs(doc = "Returns the last snapshot term of the raft.")))]
  last_snapshot_term: u64,
  #[viewit(getter(const, attrs(doc = "Returns the protocol version of the raft.")))]
  protocol_version: ProtocolVersion,
  #[viewit(getter(const, attrs(doc = "Returns the version of the snapshot.")))]
  snapshot_version: SnapshotVersion,
  #[viewit(getter(const, attrs(doc = "Returns the last contact time of the raft.")))]
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde::option"))]
  last_contact: Option<Duration>,
  #[viewit(getter(
    style = "ref",
    const,
    attrs(doc = "Returns the latest membership in use by Raft.")
  ))]
  latest_membership: Membership<I, A>,
  #[viewit(getter(
    const,
    attrs(doc = "Returns the index of the latest membership in use by Raft.")
  ))]
  latest_membership_index: u64,
  #[viewit(getter(const, attrs(doc = "Returns the number of peers in the cluster.")))]
  num_peers: u64,
}

/// The latest membership in use by Raft, the membership may not yet be committed.
#[derive(Clone)]
pub struct LatestMembership<I, A> {
  index: u64,
  membership: Membership<I, A>,
}

impl<I, A> LatestMembership<I, A> {
  /// Returns the index of the latest membership in use by Raft.
  pub fn index(&self) -> u64 {
    self.index
  }

  /// Returns the latest membership in use by Raft.
  pub fn membership(&self) -> &Membership<I, A> {
    &self.membership
  }

  /// Consumes the `LatestMembership` and returns the membership and the index.
  pub fn into_components(self) -> (u64, Membership<I, A>) {
    (self.index, self.membership)
  }
}

/// A stream which can be used to receive leadership changes.
///
/// - `true` indicates the node becomes the leader.
/// - `false` indicates the node is not the leader anymore.
#[derive(Clone)]
#[pin_project::pin_project]
pub struct LeadershipWatcher(#[pin] async_channel::Receiver<bool>);

impl Stream for LeadershipWatcher {
  type Item = <async_channel::Receiver<bool> as Stream>::Item;

  fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    <async_channel::Receiver<bool> as Stream>::poll_next(self.project().0, cx)
  }
}

trait Fut<F: FinateStateMachine, S: Storage, T: Transport> {
  type Ok;

  fn err(err: Error<F, S, T>) -> Self;

  fn ok(rst: Self::Ok) -> Self;
}

#[pin_project::pin_project]
#[repr(transparent)]
struct ErrorFuture<F: FinateStateMachine, S: Storage, T: Transport> {
  error: Option<Error<F, S, T>>,
}

impl<F: FinateStateMachine, S: Storage, T: Transport> Future for ErrorFuture<F, S, T> {
  type Output = Error<F, S, T>;

  fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
    Poll::Ready(self.project().error.take().unwrap())
  }
}

pub(super) struct MembershipChangeRequest<F: FinateStateMachine, S: Storage, T: Transport> {
  pub(super) cmd: MembershipChangeCommand<T::Id, <T::Resolver as AddressResolver>::Address>,
  pub(super) tx: oneshot::Sender<Result<F::Response, Error<F, S, T>>>,
}

pub(super) enum ApplySender<F: FinateStateMachine, E> {
  Membership(oneshot::Sender<Result<F::Response, E>>),
  Log(oneshot::Sender<Result<F::Response, E>>),
  Barrier(oneshot::Sender<Result<F::Response, E>>),
  Noop(oneshot::Sender<Result<(), E>>),
}

impl<F: FinateStateMachine, E> ApplySender<F, E> {
  pub(super) fn send_err(self, err: E) -> Result<(), ()> {
    match self {
      Self::Membership(tx) => tx.send(Err(err)).map_err(|_| ()),
      Self::Log(tx) => tx.send(Err(err)).map_err(|_| ()),
      Self::Barrier(tx) => tx.send(Err(err)).map_err(|_| ()),
      Self::Noop(tx) => tx.send(Err(err)).map_err(|_| ()),
    }
  }

  pub(super) fn respond_fsm(
    self,
    resp: Result<F::Response, E>,
  ) -> Result<(), Result<F::Response, E>> {
    match self {
      Self::Noop(_) => panic!("unexpected noop sender, please report this bug to https://github.com/al8n/ruraft/issues/new"),
      Self::Barrier(tx) | Self::Log(tx) | Self::Membership(tx) => tx.send(resp),
    }
  }

  pub(super) fn respond_noop(self) {
    match self {
      Self::Noop(tx) => if tx.send(Ok(())).is_err() {
        panic!("unexpectedly fail to send noop response to the sender, please report this bug to https://github.com/al8n/ruraft/issues/new");
      },
      _ => panic!("expect a noop sender, but got other. please report this bug to https://github.com/al8n/ruraft/issues/new"),
    }
  }
}

pub(super) struct ApplyRequest<F: FinateStateMachine, E> {
  pub(super) log: LogKind<F::Id, F::Address, F::Data>,
  pub(super) tx: ApplySender<F, E>,
}

macro_rules! resp {
  ($(
    $(#[$attr:meta])*
    $name:ident<$ty: ty>
  ), + $(,)?) => {
    $(
      $(#[$attr])*
      #[pin_project::pin_project]
      #[repr(transparent)]
      pub struct $name<F: FinateStateMachine, S: Storage, T: Transport>(
        #[pin] Either<ErrorFuture<F, S, T>, oneshot::Receiver<Result<$ty, Error<F, S, T>>>>,
      );

      impl<F: FinateStateMachine, S: Storage, T: Transport> Fut<F, S, T> for $name<F, S, T> {
        type Ok = oneshot::Receiver<Result<$ty, Error<F, S, T>>>;

        fn err(err: Error<F, S, T>) -> Self {
          Self(Either::Left(ErrorFuture { error: Some(err) }))
        }

        fn ok(rst: oneshot::Receiver<Result<$ty, Error<F, S, T>>>) -> Self {
          Self(Either::Right(rst))
        }
      }

      impl<F: FinateStateMachine, S: Storage, T: Transport> Future for $name<F, S, T> {
        type Output = Result<$ty, Error<F, S, T>>;

        fn poll(
          self: std::pin::Pin<&mut Self>,
          cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
          let this = self.project();
          match this.0.as_pin_mut() {
            Either::Left(fut) => fut.poll(cx).map(Err),
            Either::Right(fut) => match fut.poll(cx) {
              Poll::Ready(Ok(rst)) => Poll::Ready(rst),
              Poll::Ready(Err(_)) => Poll::Ready(Err(Error::canceled())),
              Poll::Pending => Poll::Pending,
            },
          }
        }
      }
    )*
  };
}

resp!(
  #[doc = "A future that can be used to wait on the result of a membership change. The index is the output of the future."]
  MembershipChangeFuture<F::Response>,
  #[doc = "A future that can be used for apply and can return the [`FinateStateMachineResponse`](crate::fsm::FinateStateMachineResponse) response."]
  ApplyFuture<F::Response>,
  #[doc = "A future that can be used for barrier and can return the [`FinateStateMachineResponse`](crate::fsm::FinateStateMachineResponse) response."]
  BarrierFuture<F::Response>,
  #[doc = "A future that can be used to verify the current node is still the leader. This is to prevent a stale read."]
  VerifyFuture<()>,
  #[doc = "A future that can be used to wait on the result of a snapshot. The returned future whose output is a [`SnapshotSource`](crate::storage::SnapshotSource)."]
  SnapshotFuture<SnapshotSource<S>>,
  #[doc = "A future that can be used to wait on the result of a leadership transfer response."]
  LeadershipTransferFuture<()>,
);
