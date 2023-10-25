use std::{
  future::Future,
  pin::Pin,
  sync::{atomic::Ordering, Arc},
  task::{Context, Poll},
  time::{Duration, Instant},
};

use agnostic::Runtime;
use bytes::Bytes;
use futures::{channel::oneshot, future::Either, Stream};
use nodecraft::{resolver::AddressResolver, Address, Id};

use crate::{
  error::{Error, RaftError},
  membership::{Membership, MembershipChangeCommand},
  options::{Options, ReloadableOptions},
  sidecar::Sidecar,
  storage::{LogKind, SnapshotMeta, SnapshotStorage, Storage},
  transport::Transport,
  FinateStateMachine, FinateStateMachineResponse, Node, RaftCore, Role,
};

pub use futures::{FutureExt, StreamExt};

impl<F, S, T, SC, R> RaftCore<F, S, T, SC, R>
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
  <R::Sleep as std::future::Future>::Output: Send,
{
  /// Returns the current state of the reloadable fields in Raft's
  /// options. This is useful for programs to discover the current state for
  /// reporting to users or tests. It is safe to call concurrently. It is
  /// intended for reporting and testing purposes primarily; external
  /// synchronization would be required to safely use this in a read-modify-write
  /// pattern for reloadable options.
  pub fn reloadable_options(&self) -> ReloadableOptions {
    self.reloadable_options.load(Ordering::Acquire)
  }

  /// Returns the current options in use by the Raft instance.
  pub fn options(&self) -> Options {
    self.options.apply(self.reloadable_options())
  }

  /// Returns the latest membership. This may not yet be
  /// committed.
  pub fn membership(&self) -> LatestMembership<T::Id, <T::Resolver as AddressResolver>::Address> {
    let membership = self.memberships.latest();
    LatestMembership {
      index: membership.0,
      membership: membership.1.clone(),
    }
  }

  /// Returns the time of last contact by a leader.
  /// This only makes sense if we are currently a follower.
  pub fn last_contact(&self) -> Option<Instant> {
    self.last_contact.load().as_ref().map(|i| **i)
  }

  /// Returns the last index in stable storage,
  /// either from the last log or from the last snapshot.
  pub fn last_index(&self) -> u64 {
    self.state.last_index()
  }

  /// Returns the committed index.
  /// This API maybe helpful for server to implement the read index optimization
  /// as described in the Raft paper.
  pub fn commit_index(&self) -> u64 {
    self.state.commit_index()
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
    self.state.last_applied()
  }

  /// Used to return the current leader address and ID of the cluster.
  /// It may return `None` if there is no current leader
  /// or the leader is unknown.
  pub fn leader(&self) -> Option<Arc<Node<T::Id, <T::Resolver as AddressResolver>::Address>>> {
    self.leader.load().clone()
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
  pub fn leader_watcher(&self) -> LeaderWatcher {
    LeaderWatcher(self.leader_rx.clone())
  }

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
  pub async fn apply(&self, data: Bytes) -> ApplyResponse<F, S, T> {
    self.apply_in(data, None, None).await
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
  pub async fn apply_timeout(&self, data: Bytes, timeout: Duration) -> ApplyResponse<F, S, T> {
    self.apply_in(data, None, Some(timeout)).await
  }

  /// Used to issue a command that blocks until all preceding
  /// operations have been applied to the [`FinateStateMachine`]. It can be used to ensure the
  /// [`FinateStateMachine`] reflects all queued writes. This
  /// must be run on the leader, or it will fail.
  ///
  /// See also [`barrier_timeout`].
  ///
  /// [`barrier_timeout`]: struct.RaftCore.html#method.barrier_timeout
  pub async fn barrier(&self) -> Barrier<F, S, T> {
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
  pub async fn barrier_timeout(&self, timeout: Duration) -> Barrier<F, S, T> {
    self.barrier_in(Some(timeout)).await
  }

  /// Used to ensure this peer is still the leader. It may be used
  /// to prevent returning stale data from the FSM after the peer has lost
  /// leadership.
  pub async fn verify_leader(&self) -> Verify<F, S, T> {
    // TODO: metrics

    let (tx, rx) = oneshot::channel();
    match self.verify_tx.send(tx).await {
      Ok(_) => Verify::ok(rx),
      Err(_) => Verify::err(Error::Raft(RaftError::Shutdown)),
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
  ) -> MembershipChangeResponse<F, S, T> {
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
  ) -> MembershipChangeResponse<F, S, T> {
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
  ) -> MembershipChangeResponse<F, S, T> {
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
  ) -> MembershipChangeResponse<F, S, T> {
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
  pub async fn remove(&self, id: T::Id, prev_index: u64) -> MembershipChangeResponse<F, S, T> {
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
  ) -> MembershipChangeResponse<F, S, T> {
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
  pub async fn demote_voter(
    &self,
    id: T::Id,
    prev_index: u64,
  ) -> MembershipChangeResponse<F, S, T> {
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
  ) -> MembershipChangeResponse<F, S, T> {
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
    rc.validate(self.options.leader_lease_timeout)?;
    let _mu = self.reload_options_lock.lock().await;
    let old = self.reloadable_options.swap(rc, Ordering::Release);

    if rc.heartbeat_timeout() < old.heartbeat_timeout() {
      // On leader, ensure replication loops running with a longer
      // timeout than what we want now discover the change.
      // On follower, update current timer to use the shorter new value.
      let (lres, fres) = futures::future::join(
        self.leader_notify_tx.send(()),
        self.follower_notify_tx.send(()),
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
  /// Returns `true`` if this call has shutdown the Raft and it was not shutdown already.
  pub fn shutdown(&self) -> bool {
    if self.shutdown.load(Ordering::Acquire) {
      return false;
    }

    self.state.set_role(Role::Shutdown);
    self.shutdown_tx.close();
    self.shutdown.store(true, Ordering::Release);
    true
  }

  /// Used to manually force Raft to take a snapshot. Returns a future
  /// that can be used to block until complete, and that contains a function that
  /// can be used to open the snapshot.
  pub async fn snapshot(&self) -> SnapshotResponse<F, S, T> {
    let (tx, rx) = oneshot::channel();

    match self.user_snapshot_tx.send(tx).await {
      Ok(_) => SnapshotResponse::ok(rx),
      Err(e) => {
        tracing::error!(target = "ruraft", err=%e, "failed to send snapshot request: user snapshot channel closed");
        SnapshotResponse::err(Error::Raft(RaftError::Closed(
          "user snapshot channel closed",
        )))
      }
    }
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
    source: <S::Snapshot as SnapshotStorage>::Source,
  ) -> Result<(), Error<F, S, T>> {
    self.restore_in(source, None).await
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
    source: <S::Snapshot as SnapshotStorage>::Source,
    timeout: Duration,
  ) -> Result<(), Error<F, S, T>> {
    self.restore_in(source, Some(timeout)).await
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
  pub async fn leadership_transfer(&self) -> LeadershipTransferResponse<F, S, T> {
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
  ) -> LeadershipTransferResponse<F, S, T> {
    self
      .initiate_leadership_transfer(Some(Node::new(id, addr)))
      .await
  }

  /// Return various internal stats. This
  /// should only be used for informative purposes or debugging.
  pub async fn stats(&self) -> RaftStats {
    todo!()
  }
}

impl<F, S, T, SC, R> RaftCore<F, S, T, SC, R>
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
  <R::Sleep as std::future::Future>::Output: Send,
{
  async fn apply_in(
    &self,
    data: Bytes,
    extension: Option<Bytes>,
    timeout: Option<Duration>,
  ) -> ApplyResponse<F, S, T> {
    // TODO: metrics
    let (tx, rx) = oneshot::channel();
    let req = ApplyRequest {
      log: LogKind::User {
        data,
        extension: extension.unwrap_or_default(),
      },
      tx,
    };
    match timeout {
      Some(timeout) => {
        futures::select! {
          _ = R::sleep(timeout).fuse() => {
            ApplyResponse::err(Error::Raft(RaftError::EnqueueTimeout))
          }
          rst = self.apply_tx.send(req).fuse() => {
            if let Err(e) = rst {
              tracing::error!(target="ruraft", err=%e, "failed to send apply request to the raft: apply channel closed");
              ApplyResponse::err(Error::Raft(RaftError::Closed("apply channel closed")))
            } else {
              ApplyResponse::ok(rx)
            }
          },
        }
      }
      None => {
        if let Err(e) = self.apply_tx.send(req).await {
          tracing::error!(target="ruraft", err=%e, "failed to send apply request to the raft: apply channel closed");
          ApplyResponse::err(Error::Raft(RaftError::Closed("apply channel closed")))
        } else {
          ApplyResponse::ok(rx)
        }
      }
    }
  }

  async fn barrier_in(&self, timeout: Option<Duration>) -> Barrier<F, S, T> {
    // TODO: metrics
    let (tx, rx) = oneshot::channel();
    let req = ApplyRequest {
      log: LogKind::Barrier,
      tx,
    };

    if let Some(timeout) = timeout {
      futures::select! {
        _ = R::sleep(timeout).fuse() => {
          Barrier::err(Error::Raft(RaftError::EnqueueTimeout))
        }
        rst = self.apply_tx.send(req).fuse() => {
          if let Err(e) = rst {
            tracing::error!(target="ruraft", err=%e, "failed to send apply request to the raft: apply channel closed");
            Barrier::err(Error::Raft(RaftError::Closed("apply channel closed")))
          } else {
            Barrier::ok(rx)
          }
        },
      }
    } else if let Err(e) = self.apply_tx.send(req).await {
      tracing::error!(target="ruraft", err=%e, "failed to send apply request to the raft: apply channel closed");
      Barrier::err(Error::Raft(RaftError::Closed("apply channel closed")))
    } else {
      Barrier::ok(rx)
    }
  }

  async fn request_membership_change(
    &self,
    cmd: MembershipChangeCommand<T::Id, <T::Resolver as AddressResolver>::Address>,
    timeout: Option<Duration>,
  ) -> MembershipChangeResponse<F, S, T> {
    // TODO: metrics
    let (tx, rx) = oneshot::channel();
    let req = MembershipChangeRequest { cmd, tx };

    match timeout {
      Some(Duration::ZERO) | None => {
        if let Err(e) = self.membership_change_tx.send(req).await {
          tracing::error!(target="ruraft", err=%e, "failed to send membership change request to the raft: membership change channel closed");
          return MembershipChangeResponse::err(Error::Raft(RaftError::Closed(
            "membership change channel closed",
          )));
        }
        MembershipChangeResponse::ok(rx)
      }
      Some(timeout) => {
        futures::select! {
          rst = self.membership_change_tx.send(req).fuse() => {
            if let Err(e) = rst {
              tracing::error!(target="ruraft", err=%e, "failed to send membership change request to the raft: membership change channel closed");
              return MembershipChangeResponse::err(Error::Raft(RaftError::Closed("membership change channel closed")));
            }

            MembershipChangeResponse::ok(rx)
          }
          _ = R::sleep(timeout).fuse() => {
            MembershipChangeResponse::err(Error::Raft(RaftError::EnqueueTimeout))
          }
        }
      }
    }
  }

  async fn restore_in(
    &self,
    source: <S::Snapshot as SnapshotStorage>::Source,
    timeout: Option<Duration>,
  ) -> Result<(), Error<F, S, T>> {
    // TODO: metrics

    let (tx, rx) = oneshot::channel();

    // Perform the restore.
    match timeout {
      None => {
        if let Err(e) = self.user_restore_tx.send((source, tx)).await {
          tracing::error!(target="ruraft", err=%e, "failed to send restore request to the raft: user restore channel closed");
          return Err(Error::Raft(RaftError::Closed(
            "user restore channel closed",
          )));
        }

        match rx.await {
          Ok(Err(e)) => Err(e),
          Err(_) => Err(Error::Raft(RaftError::Canceled)),
          Ok(Ok(_)) => {
            // Apply a no-op log entry. Waiting for this allows us to wait until the
            // followers have gotten the restore and replicated at least this new
            // entry, which shows that we've also faulted and installed the
            // snapshot with the contents of the restore.
            let (tx, rx) = oneshot::channel();
            if let Err(e) = self
              .apply_tx
              .send(ApplyRequest {
                log: LogKind::Noop,
                tx,
              })
              .await
            {
              tracing::error!(target="ruraft", err=%e, "failed to send apply request to the raft: apply channel closed");
              return Err(Error::Raft(RaftError::Closed("apply channel closed")));
            }

            match rx.await {
              Ok(Err(e)) => Err(e),
              Err(_) => Err(Error::Raft(RaftError::Canceled)),
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
            Err(Error::Raft(RaftError::EnqueueTimeout))
          }
          rst = self.user_restore_tx.send((source, tx)).fuse() => {
            if let Err(e) = rst {
              tracing::error!(target="ruraft", err=%e, "failed to send restore request to the raft: restore channel closed");
              return Err(Error::Raft(RaftError::Closed("user restore channel closed")));
            }

            match rx.await {
              Ok(Err(e)) => Err(e),
              Err(_) => Err(Error::Raft(RaftError::Canceled)),
              Ok(Ok(_)) => {
                // Apply a no-op log entry. Waiting for this allows us to wait until the
                // followers have gotten the restore and replicated at least this new
                // entry, which shows that we've also faulted and installed the
                // snapshot with the contents of the restore.
                let (tx, rx) = oneshot::channel();

                futures::select! {
                  _ = timer.as_mut().fuse() => {
                    tracing::error!(target="ruraft", "failed to send apply request to the raft: apply channel closed");
                    Err(Error::Raft(RaftError::EnqueueTimeout))
                  }
                  rst = self.apply_tx.send(ApplyRequest { log: LogKind::Noop, tx }).fuse() => {
                    if let Err(e) = rst {
                      tracing::error!(target="ruraft", err=%e, "failed to send apply request to the raft: apply channel closed");
                      return Err(Error::Raft(RaftError::Closed("apply channel closed")));
                    }

                    match rx.await {
                      Ok(Err(e)) => Err(e),
                      Err(_) => Err(Error::Raft(RaftError::Canceled)),
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
  ) -> LeadershipTransferResponse<F, S, T> {
    // TODO: metrics
    let (tx, rx) = oneshot::channel();

    if let Some(ref node) = target {
      if node.id.eq(&self.local.id) {
        tracing::error!(target = "ruraft", "cannot transfer leadership to itself");
        return LeadershipTransferResponse::err(Error::Raft(RaftError::TransferToSelf));
      }
    }

    futures::select! {
      rst = self.leader_transfer_tx.send((target, tx)).fuse() => {
        match rst {
          Ok(_) => LeadershipTransferResponse::ok(rx),
          Err(e) => {
            tracing::error!(target="ruraft", err=%e, "failed to send leadership transfer request to the raft: leadership transfer channel closed");
            LeadershipTransferResponse::err(Error::Raft(RaftError::Closed("leadership transfer channel closed")))
          }
        }
      }
      default => {
        LeadershipTransferResponse::err(Error::Raft(RaftError::EnqueueTimeout))
      }
    }
  }
}

/// The information about the current stats of the Raft node.
#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RaftStats {}

/// The latest membership in use by Raft, the membership may not yet be committed.
#[derive(PartialEq, Eq)]
pub struct LatestMembership<I: Id, A: Address> {
  index: u64,
  membership: Arc<Membership<I, A>>,
}

impl<I: Id, A: Address> Clone for LatestMembership<I, A> {
  fn clone(&self) -> Self {
    Self {
      index: self.index,
      membership: self.membership.clone(),
    }
  }
}

impl<I: Id, A: Address> LatestMembership<I, A> {
  /// Returns the index of the latest membership in use by Raft.
  pub fn index(&self) -> u64 {
    self.index
  }

  /// Returns the latest membership in use by Raft.
  pub fn membership(&self) -> &Membership<I, A> {
    &self.membership
  }
}

/// A stream which can be used to receive leadership changes.
///
/// - `true` indicates the node becomes the leader.
/// - `false` indicates the node is not the leader anymore.
#[derive(Clone)]
#[pin_project::pin_project]
pub struct LeaderWatcher(#[pin] async_channel::Receiver<bool>);

impl Stream for LeaderWatcher {
  type Item = <async_channel::Receiver<bool> as Stream>::Item;

  fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    <async_channel::Receiver<bool> as Stream>::poll_next(self.project().0, cx)
  }
}

trait Fut<F: FinateStateMachine, S: Storage, T: Transport> {
  type Ok;
  fn err(err: ErrorFuture<F, S, T>) -> Self;

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
  pub(super) tx: oneshot::Sender<Result<u64, Error<F, S, T>>>,
}

/// A future that can be used to wait on the result of a membership change. The index is the output of the future.
#[pin_project::pin_project]
#[repr(transparent)]
pub struct MembershipChangeResponse<F: FinateStateMachine, S: Storage, T: Transport>(
  #[pin] Either<ErrorFuture<F, S, T>, oneshot::Receiver<Result<u64, Error<F, S, T>>>>,
);

impl<F: FinateStateMachine, S: Storage, T: Transport> MembershipChangeResponse<F, S, T> {
  fn err(err: Error<F, S, T>) -> Self {
    Self(Either::Left(ErrorFuture { error: Some(err) }))
  }

  fn ok(rst: oneshot::Receiver<Result<u64, Error<F, S, T>>>) -> Self {
    Self(Either::Right(rst))
  }
}

impl<F: FinateStateMachine, S: Storage, T: Transport> Future for MembershipChangeResponse<F, S, T> {
  type Output = Result<u64, Error<F, S, T>>;

  fn poll(
    self: std::pin::Pin<&mut Self>,
    cx: &mut std::task::Context<'_>,
  ) -> std::task::Poll<Self::Output> {
    let this = self.project();
    match this.0.as_pin_mut() {
      Either::Left(fut) => fut.poll(cx).map(Err),
      Either::Right(fut) => match fut.poll(cx) {
        Poll::Ready(Ok(rst)) => Poll::Ready(rst),
        Poll::Ready(Err(_)) => Poll::Ready(Err(Error::Raft(RaftError::Canceled))),
        Poll::Pending => Poll::Pending,
      },
    }
  }
}

/// Used for apply and can return the [`FinateStateMachine`] response.
#[pin_project::pin_project]
#[repr(transparent)]
pub struct ApplyResponse<F: FinateStateMachine, S: Storage, T: Transport>(
  #[pin] Either<ErrorFuture<F, S, T>, oneshot::Receiver<Result<F::Response, Error<F, S, T>>>>,
);

impl<F: FinateStateMachine, S: Storage, T: Transport> ApplyResponse<F, S, T> {
  fn err(err: Error<F, S, T>) -> Self {
    Self(Either::Left(ErrorFuture { error: Some(err) }))
  }

  fn ok(rst: oneshot::Receiver<Result<F::Response, Error<F, S, T>>>) -> Self {
    Self(Either::Right(rst))
  }
}

impl<F: FinateStateMachine, S: Storage, T: Transport> Future for ApplyResponse<F, S, T> {
  type Output = Result<F::Response, Error<F, S, T>>;

  fn poll(
    self: std::pin::Pin<&mut Self>,
    cx: &mut std::task::Context<'_>,
  ) -> std::task::Poll<Self::Output> {
    let this = self.project();
    match this.0.as_pin_mut() {
      Either::Left(fut) => fut.poll(cx).map(Err),
      Either::Right(fut) => match fut.poll(cx) {
        Poll::Ready(Ok(rst)) => Poll::Ready(rst),
        Poll::Ready(Err(_)) => Poll::Ready(Err(Error::Raft(RaftError::Canceled))),
        Poll::Pending => Poll::Pending,
      },
    }
  }
}

pub(super) struct ApplyRequest<F: FinateStateMachine, E> {
  log: LogKind<F::Id, F::Address>,
  tx: oneshot::Sender<Result<F::Response, E>>,
}

/// Used for barrier and can return the [`FinateStateMachine`] response.
#[pin_project::pin_project]
#[repr(transparent)]
pub struct Barrier<F: FinateStateMachine, S: Storage, T: Transport>(
  #[pin] Either<ErrorFuture<F, S, T>, oneshot::Receiver<Result<F::Response, Error<F, S, T>>>>,
);

impl<F: FinateStateMachine, S: Storage, T: Transport> Barrier<F, S, T> {
  fn err(err: Error<F, S, T>) -> Self {
    Self(Either::Left(ErrorFuture { error: Some(err) }))
  }

  fn ok(rst: oneshot::Receiver<Result<F::Response, Error<F, S, T>>>) -> Self {
    Self(Either::Right(rst))
  }
}

impl<F: FinateStateMachine, S: Storage, T: Transport> Future for Barrier<F, S, T> {
  type Output = Result<u64, Error<F, S, T>>;

  fn poll(
    self: std::pin::Pin<&mut Self>,
    cx: &mut std::task::Context<'_>,
  ) -> std::task::Poll<Self::Output> {
    let this = self.project();

    match this.0.as_pin_mut() {
      Either::Left(fut) => fut.poll(cx).map(Err),
      Either::Right(fut) => match fut.poll(cx) {
        Poll::Ready(Ok(rst)) => Poll::Ready(rst.map(|resp| resp.index())),
        Poll::Ready(Err(_)) => Poll::Ready(Err(Error::Raft(RaftError::Canceled))),
        Poll::Pending => Poll::Pending,
      },
    }
  }
}

#[pin_project::pin_project]
pub struct Verify<F: FinateStateMachine, S: Storage, T: Transport>(
  #[pin] Either<ErrorFuture<F, S, T>, oneshot::Receiver<Result<bool, Error<F, S, T>>>>,
);

impl<F: FinateStateMachine, S: Storage, T: Transport> Verify<F, S, T> {
  fn err(err: Error<F, S, T>) -> Self {
    Self(Either::Left(ErrorFuture { error: Some(err) }))
  }

  fn ok(rst: oneshot::Receiver<Result<bool, Error<F, S, T>>>) -> Self {
    Self(Either::Right(rst))
  }
}

impl<F: FinateStateMachine, S: Storage, T: Transport> Future for Verify<F, S, T> {
  type Output = Result<bool, Error<F, S, T>>;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    let this = self.project();
    match this.0.as_pin_mut() {
      Either::Left(fut) => fut.poll(cx).map(Err),
      Either::Right(fut) => match fut.poll(cx) {
        Poll::Ready(Ok(rst)) => Poll::Ready(rst),
        Poll::Ready(Err(_)) => Poll::Ready(Err(Error::Raft(RaftError::Canceled))),
        Poll::Pending => Poll::Pending,
      },
    }
  }
}

/// A future that can be used to wait on the result of a snapshot. The returned future whose output is a [`SnapshotSource`](crate::storage::SnapshotSource).
#[pin_project::pin_project]
pub struct SnapshotResponse<F: FinateStateMachine, S: Storage, T: Transport>(
  #[pin]
  Either<
    ErrorFuture<F, S, T>,
    oneshot::Receiver<
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
);

impl<F: FinateStateMachine, S: Storage, T: Transport> SnapshotResponse<F, S, T> {
  fn err(err: Error<F, S, T>) -> Self {
    Self(Either::Left(ErrorFuture { error: Some(err) }))
  }

  fn ok(
    rst: oneshot::Receiver<
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
  ) -> Self {
    Self(Either::Right(rst))
  }
}

impl<F: FinateStateMachine, S: Storage, T: Transport> Future for SnapshotResponse<F, S, T> {
  type Output = Result<
    Box<
      dyn Future<
          Output = Result<
            <S::Snapshot as SnapshotStorage>::Source,
            <S::Snapshot as SnapshotStorage>::Error,
          >,
        > + Send,
    >,
    Error<F, S, T>,
  >;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    let this = self.project();
    match this.0.as_pin_mut() {
      Either::Left(fut) => fut.poll(cx).map(Err),
      Either::Right(fut) => match fut.poll(cx) {
        Poll::Ready(Ok(rst)) => Poll::Ready(rst),
        Poll::Ready(Err(_)) => Poll::Ready(Err(Error::Raft(RaftError::Canceled))),
        Poll::Pending => Poll::Pending,
      },
    }
  }
}

/// A future that can be used to wait on the result of a leadership transfer response.
#[pin_project::pin_project]
pub struct LeadershipTransferResponse<F: FinateStateMachine, S: Storage, T: Transport>(
  #[pin] Either<ErrorFuture<F, S, T>, oneshot::Receiver<Result<(), Error<F, S, T>>>>,
);

impl<F: FinateStateMachine, S: Storage, T: Transport> LeadershipTransferResponse<F, S, T> {
  fn err(err: Error<F, S, T>) -> Self {
    Self(Either::Left(ErrorFuture { error: Some(err) }))
  }

  fn ok(rx: oneshot::Receiver<Result<(), Error<F, S, T>>>) -> Self {
    Self(Either::Right(rx))
  }
}

impl<F: FinateStateMachine, S: Storage, T: Transport> Future
  for LeadershipTransferResponse<F, S, T>
{
  type Output = Result<(), Error<F, S, T>>;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    let this = self.project();
    match this.0.as_pin_mut() {
      Either::Left(fut) => fut.poll(cx).map(Err),
      Either::Right(fut) => match fut.poll(cx) {
        Poll::Ready(Ok(rst)) => Poll::Ready(rst),
        Poll::Ready(Err(_)) => Poll::Ready(Err(Error::Raft(RaftError::Canceled))),
        Poll::Pending => Poll::Pending,
      },
    }
  }
}
