use super::*;
use crate::{options::*, types::*};

macro_rules! raft {
  ($($rt: literal), +$(,)?) => {
    $(
      paste::paste! {
        #[pyclass(name = "Raft")]
        #[cfg(feature = $rt)]
        pub struct [< $rt:camel Raft >](Raft<::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]>);

        #[cfg(feature = $rt)]
        #[pymethods]
        impl [< $rt:camel Raft >] {
          #[staticmethod]
          pub fn new() -> pyo3::PyResult<Self> {
            todo!()
          }

          /// Provides the local unique identifier, helping in distinguishing this node from its peers.
          #[getter]
          pub fn local_id(&self) -> NodeId {
            self.0.local_id().clone().into()
          }

          /// Provides the local address, helping in distinguishing this node from its peers.
          #[getter]
          pub fn local_addr(&self) -> NodeAddress {
            self.0.local_addr().clone().into()
          }

          /// Returns the latest membership. This may not yet be
          /// committed.
          #[getter]
          pub fn latest_membership(&self) -> LatestMembership {
            self.0.latest_membership().into()
          }

          /// Returns the current role of the node.
          #[getter]
          pub fn role(&self) -> Role {
            self.0.role().into()
          }

          /// Returns the current term of the node.
          #[getter]
          pub fn current_term(&self) -> u64 {
            self.0.current_term()
          }

          /// Returns the last index in stable storage,
          /// either from the last log or from the last snapshot.
          #[getter]
          pub fn last_index(&self) -> u64 {
            self.0.last_index()
          }

          /// Returns the committed index.
          /// This API maybe helpful for server to implement the read index optimization
          /// as described in the Raft paper.
          #[getter]
          pub fn commit_index(&self) -> u64 {
            self.0.commit_index()
          }

          /// Returns the last index applied to the `FinateStateMachine`. This is generally
          /// lagging behind the last index, especially for indexes that are persisted but
          /// have not yet been considered committed by the leader.
          ///
          /// **NOTE** - this reflects
          /// the last index that was sent to the application's `FinateStateMachine` over the apply channel
          /// but DOES NOT mean that the application's `FinateStateMachine` has yet consumed it and applied
          /// it to its internal state. Thus, the application's state may lag behind this
          /// index.
          #[getter]
          pub fn applied_index(&self) -> u64 {
            self.0.applied_index()
          }

          /// Used to return the current leader address and ID of the cluster.
          /// It may return `None` if there is no current leader
          /// or the leader is unknown.
          #[getter]
          pub fn leader(&self) -> Option<Node> {
            self.0.leader().map(|l| l.as_ref().clone().into())
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
          /// If you want to watch all leadership transitions, use `leadership_change_watcher`.
          pub fn leadership_watcher(&self) -> [< $rt:camel LeadershipWatcher >] {
            [< $rt:camel LeadershipWatcher >](self.0.leadership_watcher())
          }

          /// Used to get a stream which will receive all of leadership changes.
          /// Unlike `leadership_watcher`, no change will be overriden,
          /// which means subsequent `true` values will never happen.
          pub fn leadership_change_watcher(&self) -> [< $rt:camel LeadershipWatcher >] {
            [< $rt:camel LeadershipWatcher >](self.0.leadership_change_watcher())
          }

          /// Used to apply a command to the `FinateStateMachine` in a highly consistent
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
          /// since its effects will not be present in the `FinateStateMachine` after the restore.
          ///
          /// See also `apply_timeout`.
          pub fn apply<'a>(&'a self, py: pyo3::Python<'a>, data: RaftData) -> pyo3::PyResult<&'a pyo3::PyAny> {
            let this = self.0.clone();
            ::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]::into_supported().future_into_py(py, async move {
              Ok([< $rt:camel ApplyFuture >]::from(this.apply(data).await))
            })
          }

          /// Used to apply a command to the `FinateStateMachine` in a highly consistent
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
          /// since its effects will not be present in the `FinateStateMachine` after the restore.
          ///
          /// See also `apply`.
          pub fn apply_timeout<'a>(
            &'a self,
            py: pyo3::Python<'a>,
            data: RaftData,
            timeout: ::chrono::Duration,
          ) -> pyo3::PyResult<&'a pyo3::PyAny> {
            let this = self.0.clone();
            ::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]::into_supported().future_into_py(py, async move {
              Ok([< $rt:camel ApplyFuture >]::from(
                this
                  .apply_timeout(
                    data,
                    timeout
                      .to_std()
                      .map_err(|e| PyErr::new::<pyo3::exceptions::PyTypeError, _>(e.to_string()))?,
                  )
                  .await,
              ))
            })
          }

          /// Used to issue a command that blocks until all preceding
          /// operations have been applied to the `FinateStateMachine`. It can be used to ensure the
          /// `FinateStateMachine` reflects all queued writes. This
          /// must be run on the leader, or it will fail.
          ///
          /// See also `barrier_timeout`.
          pub fn barrier<'a>(&'a self, py: pyo3::Python<'a>) -> pyo3::PyResult<&'a pyo3::PyAny> {
            let this = self.0.clone();
            ::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]::into_supported().future_into_py(py, async move {
              Ok([< $rt:camel BarrierFuture >]::from(this.barrier().await))
            })
          }

          /// Used to issue a command that blocks until all preceding
          /// operations have been applied to the `FinateStateMachine`. It can be used to ensure the
          /// `FinateStateMachine` reflects all queued writes. An optional timeout can be provided to
          /// limit the amount of time we wait for the command to be started. This
          /// must be run on the leader, or it will fail.
          ///
          /// See also `barrier`.
          ///
          pub fn barrier_timeout<'a>(
            &'a self,
            py: pyo3::Python<'a>,
            timeout: ::chrono::Duration,
          ) -> pyo3::PyResult<&'a pyo3::PyAny> {
            let this = self.0.clone();
            ::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]::into_supported().future_into_py(py, async move {
              Ok([< $rt:camel BarrierFuture >]::from(
                this
                  .barrier_timeout(
                    timeout
                      .to_std()
                      .map_err(|e| PyErr::new::<pyo3::exceptions::PyTypeError, _>(e.to_string()))?,
                  )
                  .await,
              ))
            })
          }

          /// Used to ensure this peer is still the leader. It may be used
          /// to prevent returning stale data from the FSM after the peer has lost
          /// leadership.
          pub fn verify_leader<'a>(&'a self, py: pyo3::Python<'a>) -> pyo3::PyResult<&'a pyo3::PyAny> {
            let this = self.0.clone();
            ::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]::into_supported().future_into_py(py, async move {
              Ok([< $rt:camel VerifyFuture >]::from(this.verify_leader().await))
            })
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
          /// See also `add_voter_timeout`.
          pub fn add_voter<'a>(
            &'a self,
            py: pyo3::Python<'a>,
            id: NodeId,
            addr: NodeAddress,
            prev_index: u64,
          ) -> pyo3::PyResult<&'a pyo3::PyAny> {
            let this = self.0.clone();
            ::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]::into_supported().future_into_py(py, async move {
              Ok([< $rt:camel MembershipChangeFuture >]::from(
                this.add_voter(id.into(), addr.into(), prev_index).await,
              ))
            })
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
          /// See also `add_voter`.
          pub fn add_voter_timeout<'a>(
            &'a self,
            py: pyo3::Python<'a>,
            id: NodeId,
            addr: NodeAddress,
            prev_index: u64,
            timeout: ::chrono::Duration,
          ) -> pyo3::PyResult<&'a pyo3::PyAny> {
            let this = self.0.clone();
            ::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]::into_supported().future_into_py(py, async move {
              Ok([< $rt:camel MembershipChangeFuture >]::from(
                this
                  .add_voter_timeout(
                    id.into(),
                    addr.into(),
                    prev_index,
                    timeout
                      .to_std()
                      .map_err(|e| PyErr::new::<pyo3::exceptions::PyTypeError, _>(e.to_string()))?,
                  )
                  .await,
              ))
            })
          }

          /// Add the given server to the cluster but won't assign it a
          /// vote. The server will receive log entries, but it won't participate in
          /// elections or log entry commitment. If the server is already in the cluster,
          /// this updates the server's address. This must be run on the leader or it will
          /// fail.
          ///
          /// For `prev_index`, see `add_voter`.
          ///
          /// See also `add_nonvoter_timeout`.
          pub fn add_nonvoter<'a>(
            &'a self,
            py: pyo3::Python<'a>,
            id: NodeId,
            addr: NodeAddress,
            prev_index: u64,
          ) -> pyo3::PyResult<&'a pyo3::PyAny> {
            let this = self.0.clone();
            ::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]::into_supported().future_into_py(py, async move {
              Ok([< $rt:camel MembershipChangeFuture >]::from(
                this.add_nonvoter(id.into(), addr.into(), prev_index).await,
              ))
            })
          }

          /// Add the given server to the cluster but won't assign it a
          /// vote. The server will receive log entries, but it won't participate in
          /// elections or log entry commitment. If the server is already in the cluster,
          /// this updates the server's address. This must be run on the leader or it will
          /// fail.
          ///
          /// For `prev_index` and `timeout`, see `add_voter_timeout`.
          ///
          /// See also `add_nonvoter`.
          pub fn add_nonvoter_timeout<'a>(
            &'a self,
            py: pyo3::Python<'a>,
            id: NodeId,
            addr: NodeAddress,
            prev_index: u64,
            timeout: ::chrono::Duration,
          ) -> pyo3::PyResult<&'a pyo3::PyAny> {
            let this = self.0.clone();
            ::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]::into_supported().future_into_py(py, async move {
              Ok([< $rt:camel MembershipChangeFuture >]::from(
                this
                  .add_nonvoter_timeout(
                    id.into(),
                    addr.into(),
                    prev_index,
                    timeout
                      .to_std()
                      .map_err(|e| PyErr::new::<pyo3::exceptions::PyTypeError, _>(e.to_string()))?,
                  )
                  .await,
              ))
            })
          }

          /// Remove the given server from the cluster. If the current
          /// leader is being removed, it will cause a new election to occur. This must be
          /// run on the leader or it will fail.
          ///
          /// For `prev_index`, see `add_voter`.
          ///
          /// See also `remove_timeout`.
          pub fn remove<'a>(&'a self, py: pyo3::Python<'a>, id: NodeId, prev_index: u64) -> pyo3::PyResult<&'a pyo3::PyAny> {
            let this = self.0.clone();
            ::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]::into_supported().future_into_py(py, async move {
              Ok([< $rt:camel MembershipChangeFuture >]::from(
                this.remove(id.into(), prev_index).await,
              ))
            })
          }

          /// Remove the given server from the cluster. If the current
          /// leader is being removed, it will cause a new election to occur. This must be
          /// run on the leader or it will fail.
          ///
          /// For `prev_index` and `timeout`, see `add_voter_timeout`.
          ///
          /// See also `remove`.
          pub fn remove_timeout<'a>(
            &'a self,
            py: pyo3::Python<'a>,
            id: NodeId,
            prev_index: u64,
            timeout: ::chrono::Duration,
          ) -> pyo3::PyResult<&'a pyo3::PyAny> {
            let this = self.0.clone();
            ::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]::into_supported().future_into_py(py, async move {
              Ok([< $rt:camel MembershipChangeFuture >]::from(
                this
                  .remove_timeout(
                    id.into(),
                    prev_index,
                    timeout
                      .to_std()
                      .map_err(|e| PyErr::new::<pyo3::exceptions::PyTypeError, _>(e.to_string()))?,
                  )
                  .await,
              ))
            })
          }

          /// Take away a server's vote, if it has one. If present, the
          /// server will continue to receive log entries, but it won't participate in
          /// elections or log entry commitment. If the server is not in the cluster, this
          /// does nothing. This must be run on the leader or it will fail.
          ///
          /// For `prev_index`, see `add_voter`.
          ///
          /// See also `demote_voter_timeout`.
          pub fn demote_voter<'a>(
            &'a self,
            py: pyo3::Python<'a>,
            id: NodeId,
            prev_index: u64,
          ) -> pyo3::PyResult<&'a pyo3::PyAny> {
            let this = self.0.clone();
            ::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]::into_supported().future_into_py(py, async move {
              Ok([< $rt:camel MembershipChangeFuture >]::from(
                this.demote_voter(id.into(), prev_index).await,
              ))
            })
          }

          /// Take away a server's vote, if it has one. If present, the
          /// server will continue to receive log entries, but it won't participate in
          /// elections or log entry commitment. If the server is not in the cluster, this
          /// does nothing. This must be run on the leader or it will fail.
          ///
          /// For `prev_index` and `timeout`, see `add_voter_timeout`.
          ///
          /// See also `demote_voter`.
          pub fn demote_voter_timeout<'a>(
            &'a self,
            py: pyo3::Python<'a>,
            id: NodeId,
            prev_index: u64,
            timeout: ::chrono::Duration,
          ) -> pyo3::PyResult<&'a pyo3::PyAny> {
            let this = self.0.clone();
            ::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]::into_supported().future_into_py(py, async move {
              Ok([< $rt:camel MembershipChangeFuture >]::from(
                this
                  .demote_voter_timeout(
                    id.into(),
                    prev_index,
                    timeout
                      .to_std()
                      .map_err(|e| PyErr::new::<pyo3::exceptions::PyTypeError, _>(e.to_string()))?,
                  )
                  .await,
              ))
            })
          }

          /// Updates the options of a running raft node. If the new
          /// options is invalid an error is returned and no changes made to the
          /// instance. All fields will be copied from rc into the new options, even
          /// if they are zero valued.
          pub fn reload_options<'a>(
            &'a self,
            py: pyo3::Python<'a>,
            options: ReloadableOptions,
          ) -> pyo3::PyResult<&'a pyo3::PyAny> {
            let this = self.0.clone();
            ::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]::into_supported().future_into_py(py, async move {
              this
                .reload_options(options.into())
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyTypeError, _>(e.to_string()))
            })
          }

          /// Used to stop the Raft background routines.
          /// This is not a graceful operation.
          /// It is safe to call this multiple times.
          ///
          /// Returns `true` if this call has shutdown the Raft and it was not shutdown already.
          pub fn shutdown<'a>(&'a self, py: pyo3::Python<'a>) -> pyo3::PyResult<&'a pyo3::PyAny> {
            let this = self.0.clone();
            ::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]::into_supported().future_into_py(py, async move { Ok(this.shutdown().await) })
          }

          /// Used to manually force Raft to take a snapshot. Returns a future
          /// that can be used to block until complete, and that contains a function that
          /// can be used to open the snapshot.
          ///
          /// See also `snapshot_timeout`.
          pub fn snapshot<'a>(&'a self, py: pyo3::Python<'a>) -> pyo3::PyResult<&'a pyo3::PyAny> {
            let this = self.0.clone();
            ::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]::into_supported().future_into_py(py, async move {
              Ok([< $rt:camel SnapshotFuture >]::from(this.snapshot().await))
            })
          }

          /// Used to manually force Raft to take a snapshot. Returns a future
          /// that can be used to block until complete, and that contains a function that
          /// can be used to open the snapshot. A timeout can be provided to limit the amount of time we wait.
          ///
          /// See also `snapshot`.
          pub fn snapshot_timeout<'a>(
            &'a self,
            py: pyo3::Python<'a>,
            timeout: ::chrono::Duration,
          ) -> pyo3::PyResult<&'a pyo3::PyAny> {
            let this = self.0.clone();
            ::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]::into_supported().future_into_py(py, async move {
              Ok([< $rt:camel SnapshotFuture >]::from(
                this
                  .snapshot_timeout(
                    timeout
                      .to_std()
                      .map_err(|e| PyErr::new::<pyo3::exceptions::PyTypeError, _>(e.to_string()))?,
                  )
                  .await,
              ))
            })
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
          /// See also `restore_timeout`.
          pub fn restore<'a>(&'a self, py: pyo3::Python<'a>, id: SnapshotId) -> pyo3::PyResult<&'a pyo3::PyAny> {
            let this = self.0.clone();
            ::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]::into_supported().future_into_py(py, async move {
              let src = this
                .open_snapshot(id.into())
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyTypeError, _>(e.to_string()))?;

              this
                .restore(src)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyTypeError, _>(e.to_string()))
            })
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
          /// See also `restore`.
          pub fn restore_timeout<'a>(
            &'a self,
            py: pyo3::Python<'a>,
            id: SnapshotId,
            timeout: ::chrono::Duration,
          ) -> pyo3::PyResult<&'a pyo3::PyAny> {
            let this = self.0.clone();
            ::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]::into_supported().future_into_py(py, async move {
              let src = this
                .open_snapshot(id.into())
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyTypeError, _>(e.to_string()))?;

              this
                .restore_timeout(
                  src,
                  timeout
                    .to_std()
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyTypeError, _>(e.to_string()))?,
                )
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyTypeError, _>(e.to_string()))
            })
          }

          /// Transfer leadership to a node in the cluster.
          /// This can only be called from the leader, or it will fail. The leader will
          /// stop accepting client requests, make sure the target server is up to date
          /// and starts the transfer with a `TimeoutNowRequest` message. This message has the same
          /// effect as if the election timeout on the target server fires. Since
          /// it is unlikely that another server is starting an election, it is very
          /// likely that the target server is able to win the election. If a follower cannot be promoted, it will fail
          /// gracefully.
          ///
          /// See also `leadership_transfer_to_node`.
          pub fn leadership_transfer<'a>(&'a self, py: pyo3::Python<'a>, id: NodeId) -> pyo3::PyResult<&'a pyo3::PyAny> {
            let this = self.0.clone();
            ::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]::into_supported().future_into_py(py, async move {
              Ok([< $rt:camel LeadershipTransferFuture >]::from(
                this.leadership_transfer().await,
              ))
            })
          }

          /// The same as `leadership_transfer` but takes a
          /// server in the arguments in case a leadership should be transitioned to a
          /// specific server in the cluster. If a
          /// follower cannot be promoted, it will fail gracefully.
          ///
          /// See also `leadership_transfer`.
          pub fn leadership_transfer_to_node<'a>(
            &'a self,
            py: pyo3::Python<'a>,
            id: NodeId,
            addr: NodeAddress,
          ) -> pyo3::PyResult<&'a pyo3::PyAny> {
            let this = self.0.clone();
            ::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]::into_supported().future_into_py(py, async move {
              Ok([< $rt:camel LeadershipTransferFuture >]::from(
                this
                  .leadership_transfer_to_node(id.into(), addr.into())
                  .await,
              ))
            })
          }

          /// Return various internal stats. This
          /// should only be used for informative purposes or debugging.
          pub fn stats<'a>(&'a self, py: pyo3::Python<'a>) -> pyo3::PyResult<&'a pyo3::PyAny> {
            let this = self.0.clone();
            ::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]::into_supported()
              .future_into_py(py, async move { Ok(RaftStats::from(this.stats().await)) })
          }
        }
      }
    )*
  };
}

raft!("tokio", "async-std");
