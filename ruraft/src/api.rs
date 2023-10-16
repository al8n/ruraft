use std::{
  future::Future,
  time::{Duration, Instant},
};

use ruraft_core::{
  Node, Role,
  transport::{Address, Id},
};

/// Raft is the API for the Raft consensus algorithm.
pub trait API {
  /// The error type returned by the Raft.
  type Error: std::error::Error + Send + Sync + 'static;

  /// The log type should by applied to the finate state machine.
  type Log: Send + Sync + 'static;

  /// The id node.
  type Id: Id;

  /// The address of the node.
  type Address: Address;

  /// Used to return the current leader of the cluster.
  fn leader(&self) -> impl Future<Output = Option<Node<Self::Id, Self::Address>>> + Send;

  /// Used to apply a `Log` to the finate state machine in a highly consistent
  /// manner. This returns a future that can be used to wait on the application.
  /// An optional timeout can be provided to limit the amount of time we wait
  /// for the log to be started. This must be run on the leader or it
  /// will fail.
  ///
  /// There is no way to guarantee whether the
  /// write succeeded or failed in this case. For example, if the leader is
  /// partitioned it can't know if a quorum of followers wrote the log to disk. If
  /// at least one did, it may survive into the next leader's term.
  fn apply(
    &self,
    log: Self::Log,
    timeout: Option<Duration>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Used to issue a command that blocks until all preceding
  /// operations have been applied to the finate state machine. It can be used to ensure the
  /// finate state machine reflects all queued writes. An optional timeout can be provided to
  /// limit the amount of time we wait for the command to be started. This
  /// must be run on the leader, or it will fail.
  fn barrier(
    &self,
    timeout: Option<Duration>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Used to ensure this peer is still the leader. It may be used
  /// to prevent returning stale data from the finate state machine after the peer has lost
  /// leadership.
  fn verify_leader(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Returns the latest membership. This may not yet be
  /// committed.
  fn latest_memberhsip(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Returns the last committed membership.
  fn committed_memberhsip(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Add the given server to the cluster as a staging server. If the
  /// server is already in the cluster as a voter, this updates the server's address.
  /// This must be run on the leader or it will fail. The leader will promote the
  /// staging server to a voter once that server is ready. If nonzero, prevIndex is
  /// the index of the only membership upon which this change may be applied; if
  /// another membership entry has been added in the meantime, this request will
  /// fail. If nonnull, timeout is how long this server should wait before the
  /// membership change log entry is appended.
  fn add_voter(
    &self,
    id: Self::Id,
    addr: Self::Address,
    prev_index: u64,
    timeout: Option<Duration>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Add the given server to the cluster but won't assign it a
  /// vote. The server will receive log entries, but it won't participate in
  /// elections or log entry commitment. If the server is already in the cluster,
  /// this updates the server's address. This must be run on the leader or it will
  /// fail. For prevIndex and timeout, see [`Raft::add_voter`].
  fn add_nonvoter(
    &self,
    id: Self::Id,
    addr: Self::Address,
    prev_index: u64,
    timeout: Option<Duration>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Remove the given server from the cluster. If the current
  /// leader is being removed, it will cause a new election to occur. This must be
  /// run on the leader or it will fail. For `prev_index` and `timeout`, see [`Raft::add_voter`].
  fn remove_server(
    &self,
    id: Self::Id,
    prev_index: u64,
    timeout: Option<Duration>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Take away a server's vote, if it has one. If present, the
  /// server will continue to receive log entries, but it won't participate in
  /// elections or log entry commitment. If the server is not in the cluster, this
  /// does nothing. This must be run on the leader or it will fail. For `prev_index` and `timeout`, see [`Raft::add_voter`].
  fn demote_voter(
    &self,
    id: Self::Id,
    prev_index: u64,
    timeout: Option<Duration>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Used to stop the Raft background tasks.
  fn shutdown(self) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Returns the role of this raft node.
  fn role(&self) -> Role;

  /// Returns the time of last contact by a leader.
  /// This only makes sense if we are currently a follower.
  fn last_contact(&self) -> impl Future<Output = Option<Instant>> + Send;
}
