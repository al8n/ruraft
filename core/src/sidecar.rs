use std::{convert::Infallible, future::Future};

use crate::raft::Role;

mod autopilot;
pub use autopilot::*;

#[auto_impl::auto_impl(Box, Arc)]
/// Represents a sidecar that can be run alongside the Raft according
/// to the different [`Role`].
///
/// e.g. [`Autopilot`](crate::sidecar::Autopilot) will be run alongside when the Raft node
/// becomes the leader.
pub trait Sidecar: Send + Sync + 'static {
  /// The error type returned by the sidecar.
  type Error: std::error::Error + Send + Sync + 'static;
  /// The async runtime used by the sidecar.
  type Runtime: agnostic::Runtime;

  /// Runs the sidecar.
  ///
  /// - The `role` parameter is the current role of the Raft node.
  /// The implementor should determine whether to run the sidecar or not based on
  /// the role.
  fn run(&self, role: Role) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Shutdowns the sidecar, this will be called when the Raft node receives shutdown signal.
  fn shutdown(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// This method is used to determine whether the sidecar should run or not.
  fn applicable(role: Role) -> bool;

  /// Returns whether the sidecar is running or not.
  fn is_running(&self) -> bool;
}

/// A noop sidecar, the default sidecar for the [`Raft`]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct NoopSidecar<R>(std::marker::PhantomData<R>);

impl<R> NoopSidecar<R> {
  /// Returns a new noop sidecar.
  #[inline]
  pub const fn new() -> Self {
    Self(std::marker::PhantomData)
  }
}

impl<R: agnostic::Runtime> Sidecar for NoopSidecar<R> {
  type Error = Infallible;
  type Runtime = R;

  async fn run(&self, _role: Role) -> Result<(), Self::Error> {
    Ok(())
  }

  async fn shutdown(&self) -> Result<(), Self::Error> {
    Ok(())
  }

  fn applicable(_role: Role) -> bool {
    false
  }

  fn is_running(&self) -> bool {
    false
  }
}
