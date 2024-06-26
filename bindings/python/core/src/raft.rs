use super::*;
use crate::{fsm::*, options::*, storage::*, types::*};
use pyo3::exceptions::PyTypeError;
use ruraft_bindings_common::{storage::SupportedStorage, transport::SupportedTransport};

mod futs;
use futs::*;

macro_rules! register_futs {
  ($($rt: literal), +$(,)?) => {
    $(
      paste::paste! {
        #[cfg(feature = $rt)]
        pub fn [< register_ $rt:snake >](m: &PyModule) -> pyo3::PyResult<()> {
          m.add_class::<[< $rt:camel ApplyFuture >]>()?;
          m.add_class::<[< $rt:camel BarrierFuture >]>()?;
          m.add_class::<[< $rt:camel MembershipChangeFuture >]>()?;
          m.add_class::<[< $rt:camel VerifyFuture >]>()?;
          m.add_class::<[< $rt:camel LeadershipTransferFuture >]>()?;
          m.add_class::<[< $rt:camel SnapshotFuture >]>()?;
          m.add_class::<[< $rt:camel LeadershipWatcher >]>()?;
          m.add_class::<[< $rt:camel Raft >]>()?;

          Ok(())
        }

        #[cfg(feature = $rt)]
        pub fn [< pyi_ $rt:snake >]() -> String {
          use crate::Pyi;

          let mut pyi = r#"

from typing import AsyncIterable, Optional
from datetime import timedelta
from .types import *
from .fsm import *
from .membership import *
from .options import *

                    "#.to_string();

          pyi.push_str(&[< $rt:camel ApplyFuture >]::pyi());
          pyi.push_str(&[< $rt:camel BarrierFuture >]::pyi());
          pyi.push_str(&[< $rt:camel MembershipChangeFuture >]::pyi());
          pyi.push_str(&[< $rt:camel VerifyFuture >]::pyi());
          pyi.push_str(&[< $rt:camel LeadershipTransferFuture >]::pyi());
          pyi.push_str(&[< $rt:camel SnapshotFuture >]::pyi());
          pyi.push_str(&crate::storage:: [< $rt:camel SnapshotSource >]::pyi());
          pyi.push_str(&[< $rt:camel AsyncReader >]::pyi());
          pyi.push_str(&[< $rt:camel SnapshotSink >]::pyi());
          pyi.push_str(&[< $rt:camel Snapshot >]::pyi());
          pyi.push_str(&[< $rt:camel Raft >]::pyi());

          pyi
        }
      }
    )*
  };
}

register_futs!("tokio", "async-std");

macro_rules! raft {
  ($($rt: literal), +$(,)?) => {
    $(
      paste::paste! {
        #[pyclass(name = "Raft")]
        #[cfg(feature = $rt)]
        pub struct [< $rt:camel Raft >](Raft<::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]>);

        #[cfg(feature = $rt)]
        impl Clone for [< $rt:camel Raft >] {
          fn clone(&self) -> Self {
            Self(self.0.clone())
          }
        }

        #[cfg(feature = $rt)]
        impl crate::Pyi for [< $rt:camel Raft >] {
          fn pyi() -> std::borrow::Cow<'static, str> {
r#"

class Raft:
  async def new(fsm: fsm.FinateStateMachine, options: Options) -> Raft: ...

  async def recover(fsm: fsm.FinateStateMachine, membership: Membership, options: Options) -> None:...

  def reloadable_options(self) -> ReloadableOptions: ...
  
  def options(self) -> Options: ...
  
  def local_id(self) -> NodeId: ...
  
  def local_addr(self) -> NodeAddress: ...
  
  def role(self) -> Role: ...
  
  def current_term(self) -> int: ...
  
  def latest_membership(self) -> LatestMembership: ...
  
  def last_contact(self) -> Optional[timedelta]: ...
  
  def last_index(self) -> int: ...
  
  def commit_index(self) -> int: ...
  
  def applied_index(self) -> int: ...
  
  def leader(self) -> Node: ...

  async def leadership_watcher(self) -> AsyncIterable[bool] : ...
  
  async def leadership_change_watcher(self) -> AsyncIterable[bool] : ...
  
  async def apply(self, data: bytes) -> ApplyFuture : ...

  async def apply_timeout(self, timeout: timedelta) -> ApplyFuture : ...

  async def barrier(self) -> BarrierFuture: ...
  
  async def barrier_timeout(self, timeout: timedelta) -> BarrierFuture: ...

  async def snapshot(self) -> SnapshotFuture: ...

  async def snapshot_timeout(self, timeout: timedelta) -> SnapshotFuture: ...

  async def verify_leader(self) -> VerifyFuture: ...
  
  async def add_voter(self, id: NodeId, addr: NodeAddress, prev_index: int) -> MembershipChangeFuture:...
  
  async def add_voter_timeout(self, id: NodeId, addr: NodeAddress, prev_index: int, timeout: timedelta) -> MembershipChangeFuture:...
  
  async def add_nonvoter(self, id: NodeId, addr: NodeAddress, prev_index: int) -> MembershipChangeFuture:...

  async def add_nonvoter_timeout(self, id: NodeId, addr: NodeAddress, prev_index: int, timeout: timedelta) -> MembershipChangeFuture:...

  async def demote_voter(self, id: NodeId, prev_index: int) -> MembershipChangeFuture:...

  async def demote_voter_timeout(self, id: NodeId, prev_index: int, timeout: timedelta) -> MembershipChangeFuture:...
  
  async def remove(self, id: NodeId, prev_index: int) -> MembershipChangeFuture:...

  async def remove_timeout(self, id: NodeId, prev_index: int, timeout: timedelta) -> MembershipChangeFuture:...
  
  async def reload_options(self, options: ReloadableOptions) -> None :...
  
  async def restore(self, meta: SnapshotMeta, reader: AsyncReader) -> None :...

  async def restore_timeout(self, meta: SnapshotMeta, reader: AsyncReader, timeout: timedelta) -> None :...

  async def leadership_transfer(self) -> LeadershipTransferFuture:...
  
  async def leadership_transfer_to_node(self, id: NodeId, addr: NodeAddress) -> LeadershipTransferFuture:...

  async def stats(self) -> RaftStats:...

  async def shutdown(self) -> bool :...

"#.into()
          }
        }

        #[cfg(feature = $rt)]
        #[pymethods]
        impl [< $rt:camel Raft >] {
          /// Creates a new Raft node.
          ///
          /// **N.B.**
          /// - If there is no old state, then will initialize a new Raft cluster which contains only one voter node(self), users can then invoke `add_voter` on it to add other servers to the cluster.
          /// - If there are any
          /// old state, such as snapshots, logs, peers, etc, all those will be restored
          /// when creating the Raft node.
          ///
          /// Arguments:
          ///   fsm: The `FinateStateMachine` to use.
          ///   storage_opts: The options used to configure the storage.
          ///   transport_opts: The options used to configure the transport.
          ///   opts: The options used to configure the Raft node.
          #[staticmethod]
          pub fn new(
            fsm: Py<PyAny>,
            storage_opts: StorageOptions,
            transport_opts: PythonTransportOptions,
            opts: Options,
            py: pyo3::Python<'_>
          ) -> pyo3::PyResult<Self> {
            let fsm = FinateStateMachine::new(fsm);
            ::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >] ::into_supported().future_into_py(py, async move {
              let storage = SupportedStorage::new(storage_opts.into()).await.map_err(|e| $crate::RaftError::storage(e)).map_err(|e| PyTypeError::new_err(e.to_string()))?;
              let transport = SupportedTransport::new(transport_opts.into()).await.map_err(|e| $crate::RaftError::transport(e)).map_err(|e| PyTypeError::new_err(e.to_string()))?;
              Raft::< ::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >] > :: new(fsm, storage, transport, opts.into()).await.map(Self).map_err(|e| PyTypeError::new_err(e.to_string()))
            })
            .and_then(|raft| raft.extract())
          }

          /// `recover` is used to manually force a new membership in order to
          /// recover from a loss of quorum where the current membership cannot be
          /// restored, such as when several servers die at the same time. This works by
          /// reading all the current state for this server, creating a snapshot with the
          /// supplied membership, and then truncating the Raft log. This is the only
          /// safe way to force a given membership without actually altering the log to
          /// insert any new entries, which could cause conflicts with other servers with
          /// different state.
          ///
          /// **WARNING!** This operation implicitly commits all entries in the Raft log, so
          /// in general this is an extremely unsafe operation. If you've lost your other
          /// servers and are performing a manual recovery, then you've also lost the
          /// commit information, so this is likely the best you can do, but you should be
          /// aware that calling this can cause Raft log entries that were in the process
          /// of being replicated but not yet be committed to be committed.
          ///
          /// Note the `FinateStateMachine` passed here is used for the snapshot operations and will be
          /// left in a state that should not be used by the application. Be sure to
          /// discard this `FinateStateMachine` and any associated state and provide a fresh one when
          /// calling `new` later.
          ///
          /// A typical way to recover the cluster is to shut down all servers and then
          /// run RecoverCluster on every server using an identical membership. When
          /// the cluster is then restarted, and election should occur and then Raft will
          /// resume normal operation. If it's desired to make a particular server the
          /// leader, this can be used to inject a new membership with that server as
          /// the sole voter, and then join up other new clean-state peer servers using
          /// the usual APIs in order to bring the cluster back into a known state.
          #[staticmethod]
          pub fn recover(
            fsm: Py<PyAny>,
            storage: StorageOptions,
            membership: Membership,
            opts: Options,
            py: pyo3::Python<'_>
          ) -> PyResult<&pyo3::PyAny> {
            let fsm = FinateStateMachine::new(fsm);
            ::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >] ::into_supported().future_into_py(py, async move {
              let storage = SupportedStorage::new(storage.into()).await.map_err(|e| $crate::RaftError::storage(e)).map_err(|e| PyTypeError::new_err(e.to_string()))?;
              Raft::< ::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >] > :: recover(fsm, storage, membership.into(), opts.into()).await.map_err(|e| PyTypeError::new_err(e.to_string()))
            })
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
          /// it will return `Err(Error::Raft($crate::RaftError::LeadershipLost))`. There is no way to guarantee whether the
          /// write succeeded or failed in this case. For example, if the leader is
          /// partitioned it can't know if a quorum of followers wrote the log to disk. If
          /// at least one did, it may survive into the next leader's term.
          ///
          /// - If a user snapshot is restored while the command is in-flight, an
          /// `Err(Error::Raft($crate::RaftError::AbortedByRestore))` is returned. In this case the write effectively failed
          /// since its effects will not be present in the `FinateStateMachine` after the restore.
          ///
          /// See also `apply_timeout`.
          pub fn apply<'a>(&'a self, data: RaftData, py: pyo3::Python<'a>) -> pyo3::PyResult<&'a pyo3::PyAny> {
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
          /// it will return `Err(Error::Raft($crate::RaftError::LeadershipLost))`. There is no way to guarantee whether the
          /// write succeeded or failed in this case. For example, if the leader is
          /// partitioned it can't know if a quorum of followers wrote the log to disk. If
          /// at least one did, it may survive into the next leader's term.
          ///
          /// - If a user snapshot is restored while the command is in-flight, an
          /// `Err(Error::Raft($crate::RaftError::AbortedByRestore))` is returned. In this case the write effectively failed
          /// since its effects will not be present in the `FinateStateMachine` after the restore.
          ///
          /// See also `apply`.
          pub fn apply_timeout<'a>(
            &'a self,
            data: RaftData,
            timeout: ::chrono::Duration,
            py: pyo3::Python<'a>,
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
            timeout: ::chrono::Duration,
            py: pyo3::Python<'a>,
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
            id: NodeId,
            addr: NodeAddress,
            prev_index: u64,
            py: pyo3::Python<'a>,
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
            id: NodeId,
            addr: NodeAddress,
            prev_index: u64,
            timeout: ::chrono::Duration,
            py: pyo3::Python<'a>,
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
            id: NodeId,
            addr: NodeAddress,
            prev_index: u64,
            py: pyo3::Python<'a>,
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
            id: NodeId,
            addr: NodeAddress,
            prev_index: u64,
            timeout: ::chrono::Duration,
            py: pyo3::Python<'a>,
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
          pub fn remove<'a>(&'a self, id: NodeId, prev_index: u64, py: pyo3::Python<'a>) -> pyo3::PyResult<&'a pyo3::PyAny> {
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
            id: NodeId,
            prev_index: u64,
            timeout: ::chrono::Duration,
            py: pyo3::Python<'a>,
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
            id: NodeId,
            prev_index: u64,
            py: pyo3::Python<'a>,
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
            id: NodeId,
            prev_index: u64,
            timeout: ::chrono::Duration,
            py: pyo3::Python<'a>,
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
            options: ReloadableOptions,
            py: pyo3::Python<'a>,
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
            timeout: ::chrono::Duration,
            py: pyo3::Python<'a>,
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
          pub fn restore<'a>(&'a self, meta: SnapshotMeta, src: [< $rt:camel AsyncReader >], py: pyo3::Python<'a>) -> pyo3::PyResult<&'a pyo3::PyAny> {
            let this = self.0.clone();
            ::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]::into_supported().future_into_py(py, async move {
              this
                .restore(meta.into(), src)
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
            meta: SnapshotMeta,
            src: [< $rt:camel AsyncReader >],
            timeout: ::chrono::Duration,
            py: pyo3::Python<'a>,
          ) -> pyo3::PyResult<&'a pyo3::PyAny> {
            let this = self.0.clone();
            ::agnostic:: [< $rt:snake >] :: [< $rt:camel Runtime >]::into_supported().future_into_py(py, async move {
              this
                .restore_timeout(
                  meta.into(),
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
          pub fn leadership_transfer<'a>(&'a self, py: pyo3::Python<'a>) -> pyo3::PyResult<&'a pyo3::PyAny> {
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
            id: NodeId,
            addr: NodeAddress,
            py: pyo3::Python<'a>,
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
