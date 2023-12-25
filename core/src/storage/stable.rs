use std::future::Future;

use nodecraft::{Address, Id};

use crate::Node;

/// Used to provide stable storage
/// of key configurations to ensure safety.
/// e.g. votes are persisted to this storage.
pub trait StableStorage: Send + Sync + 'static {
  /// The error type returned by the stable storage.
  type Error: std::error::Error + Send + Sync + 'static;
  /// The async runtime used by the storage.
  type Runtime: agnostic::Runtime;
  /// The id type used to identify node.
  type Id: Id;
  /// The address type of node.
  type Address: Address;

  /// Returns the current term. `Some(0)` or `None` means
  /// no term has been persisted yet.
  fn current_term(&self) -> impl Future<Output = Result<Option<u64>, Self::Error>> + Send;

  /// Stores the current term.
  fn store_current_term(&self, term: u64) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Returns the last vote term. `Some(0)` or `None` means
  /// no term has been persisted yet.
  fn last_vote_term(&self) -> impl Future<Output = Result<Option<u64>, Self::Error>> + Send;

  /// Stores the last vote term.
  fn store_last_vote_term(&self, term: u64)
    -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Returns the last vote candidate. `None` means no candidate has been persisted yet.
  fn last_vote_candidate(
    &self,
  ) -> impl Future<Output = Result<Option<Node<Self::Id, Self::Address>>, Self::Error>> + Send;

  /// Stores the last vote candidate.
  fn store_last_vote_candidate(
    &self,
    candidate: Node<Self::Id, Self::Address>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
