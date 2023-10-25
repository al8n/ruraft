use std::{sync::{atomic::Ordering, Arc}, time::Duration};

use agnostic::Runtime;
use bytes::Bytes;
use futures::{channel::oneshot, FutureExt};
use nodecraft::resolver::AddressResolver;

use crate::{RaftCore, FinateStateMachine, storage::{SnapshotStorage, Storage, LogKind}, transport::Transport, sidecar::Sidecar, options::{ReloadableOptions, Options}, error::{Error, RaftError}, Node};

use super::{ApplyResponse, ApplyRequest};

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

  /// Used to return the current leader address and ID of the cluster.
  /// It may return `None` if there is no current leader
// or the leader is unknown.
  pub fn leader(&self) -> Option<Arc<Node<T::Id, <T::Resolver as AddressResolver>::Address>>> {
    self.leader.load().clone()
  }

  /// Used to apply a command to the [`FinateStateMachine`] in a highly consistent
  /// manner. This returns a future that can be used to wait on the application.
  /// 
  /// An optional timeout can be provided to limit the amount of time we wait
  /// for the command to be started by using [`apply_timeout`]. This must be run on the leader or it
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
  /// [`apply_timeout`]: struct.RaftCore.html#method.apply_timeout
  pub async fn apply(&self, data: Bytes) -> Result<ApplyResponse<F, S, T>, Error<F, S, T>> {
    self.apply_in(data, None, None).await
  }

  /// Used to apply a command to the [`FinateStateMachine`] in a highly consistent
  /// manner with a timeout to limit the amount of time we wait
  /// for the command to be started. This returns a future that can be used to wait on the application.
  /// 
  pub async fn apply_timeout(&self, data: Bytes, timeout: Duration) -> Result<ApplyResponse<F, S, T>, Error<F, S, T>> {
    self.apply_in(data, None, Some(timeout)).await
  }

  /// Used to issue a command that blocks until all preceding
  /// operations have been applied to the [`FinateStateMachine`]. It can be used to ensure the
  /// [`FinateStateMachine`] reflects all queued writes. An optional timeout can be provided to
  /// limit the amount of time we wait for the command to be started. This
  /// must be run on the leader, or it will fail.
  pub async fn barrier(&self, timeout: Duration) {

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
  async fn apply_in(&self, data: Bytes, extension: Option<Bytes>, timeout: Option<Duration>) -> Result<ApplyResponse<F, S, T>, Error<F, S, T>> {
    // TODO: metrics
    let (tx, rx) = oneshot::channel();
    let req = ApplyRequest {
      log: LogKind::User { data, extension: extension.unwrap_or_default(), },
      tx,
    };
    match timeout {
      Some(timeout) => {
        futures::select! {
          _ = R::sleep(timeout).fuse() => {
            Err(Error::Raft(RaftError::EnqueueTimeout))
          }
          rst = self.apply_tx.send(req).fuse() => {
            if let Err(e) = rst {
              tracing::error!(target="ruraft", err=%e, "failed to send apply request to the raft: apply channel closed");
              Err(Error::Raft(RaftError::Shutdown))
            } else {
              Ok(ApplyResponse {
                rx
              })
            }
          },
        }
      },
      None => {
        if let Err(e) = self.apply_tx.send(req).await {
          tracing::error!(target="ruraft", err=%e, "failed to send apply request to the raft: apply channel closed");
          Err(Error::Raft(RaftError::Shutdown))
        } else {
          Ok(ApplyResponse {
            rx
          })
        }
      },
    }
  }
}
