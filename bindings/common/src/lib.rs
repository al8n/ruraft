#![allow(clippy::type_complexity)]

use ruraft_core::{sidecar::NoopSidecar, RaftCore};

pub mod storage;
pub mod transport;

pub type Raft<F, D, R> = RaftCore<
  F,
  storage::SupportedStorage<D, R>,
  transport::SupportedTransport<D, R>,
  NoopSidecar<R>,
  R,
>;

pub type RaftError<F, D, R> = ruraft_core::error::Error<
  F,
  storage::SupportedStorage<D, R>,
  transport::SupportedTransport<D, R>,
>;
