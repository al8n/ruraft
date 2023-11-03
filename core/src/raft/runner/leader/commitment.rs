use std::sync::Arc;

use async_channel::Sender;
use async_lock::Mutex;
use std::collections::HashMap;

use crate::{
  membership::Membership,
  transport::{Address, Id},
};

struct Inner<I: Id, A: Address> {
  /// notified when commit_index increases
  commit_tx: Sender<()>,
  /// voter ID to log index: the server stores up through this log entry
  match_indexes: HashMap<I, u64>,
  /// a quorum stores up through this log entry. monotonically increases.
  commit_index: u64,

  _marker: std::marker::PhantomData<A>,
}

impl<I: Id, A: Address> Inner<I, A> {
  /// Called once a server completes writing entries to disk: either the
  /// leader has written the new entry or a follower has replied to an
  /// `append_entries` RPC. The given server's disk agrees with this server's log up
  /// through the given index.
  async fn match_index(&mut self, id: &I, match_index: u64, start_index: u64) {
    match self.match_indexes.get_mut(id) {
      Some(prev) if match_index > *prev => {
        *prev = match_index;
        self.recalculate(start_index).await;
      }
      _ => {}
    }
  }

  /// Called when a new cluster membership is created: it will be
  /// used to determine commitment from now on. 'membership' is the servers in
  /// the cluster.
  async fn set_membership(&mut self, start_index: u64, membership: &Membership<I, A>) {
    let mut old_match_indexes = core::mem::replace(
      &mut self.match_indexes,
      HashMap::with_capacity(membership.len()),
    );
    for (id, (_, suffrage)) in membership.iter() {
      if suffrage.is_voter() {
        if let Some((id, index)) = old_match_indexes.remove_entry(id) {
          self.match_indexes.insert(id, index);
        } else {
          self.match_indexes.insert(id.clone(), 0);
        }
      }
    }
    self.recalculate(start_index).await;
  }

  /// Internal helper to calculate new commitIndex from matchIndexes.
  /// Must be called with lock held.
  async fn recalculate(&mut self, start_index: u64) {
    if self.match_indexes.is_empty() {
      return;
    }

    let mut matched = self.match_indexes.values().copied().collect::<Vec<_>>();
    matched.sort();
    let quorum_match_index = matched[(matched.len() - 1) / 2];
    if quorum_match_index > self.commit_index && quorum_match_index >= start_index {
      self.commit_index = quorum_match_index;
      if self.commit_tx.send(()).await.is_err() {
        tracing::error!(
          target = "ruraft",
          "commitment: failed to send commit notification"
        );
      }
    }
  }
}

/// Used to advance the leader's commit index. The leader and
/// replication task reports in newly written entries with match(), and
/// this notifies on commit channel when the commit index has advanced.
pub(crate) struct Commitment<I: Id, A: Address> {
  inner: Arc<Mutex<Inner<I, A>>>,
  /// the first index of this leader's term: this needs to be replicated to a
  /// majority of the cluster before this leader may mark anything committed
  /// (per Raft's commitment rule)
  start_index: u64,
}

impl<I: Id, A: Address> Commitment<I, A> {
  /// Returns a [`Commitment`] that notifies the provided
  /// channel when log entries have been committed. A new [`Commitment`] is
  /// created each time this server becomes leader for a particular term.
  /// `membership` is the servers in the cluster.
  /// `start_index` is the first index created in this term (see
  // its description above).
  pub(crate) fn new(
    commit_tx: Sender<()>,
    membership: &Membership<I, A>,
    start_index: u64,
  ) -> Self {
    let mut match_indexes = HashMap::with_capacity(membership.len());
    for (id, (_, suffrage)) in membership.iter() {
      if suffrage.is_voter() {
        match_indexes.insert(id.clone(), 0);
      }
    }
    Self {
      inner: Arc::new(Mutex::new(Inner {
        commit_tx,
        match_indexes,
        commit_index: 0,

        _marker: Default::default(),
      })),
      start_index,
    }
  }

  pub(crate) async fn set_membership(&self, membership: &Membership<I, A>) {
    self
      .inner
      .lock()
      .await
      .set_membership(self.start_index, membership)
      .await;
  }

  /// Called by leader after `commit_rx` is notified
  pub(crate) async fn get_commit_index(&self) -> u64 {
    self.inner.lock().await.commit_index
  }

  pub(crate) async fn match_index(&self, server: &I, index: u64) {
    self
      .inner
      .lock()
      .await
      .match_index(server, index, self.start_index)
      .await;
  }

  pub(super) fn start_index(&self) -> u64 {
    self.start_index
  }
}

impl<I: Id, A: Address> Clone for Commitment<I, A> {
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
      start_index: self.start_index,
    }
  }
}
