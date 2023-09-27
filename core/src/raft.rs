use std::{sync::Arc, time::Duration};

use agnostic::Runtime;

use crate::{fsm::FinateStateMachine, storage::Storage, transport::Transport};

mod fsm;
mod state;
pub use state::*;

const MIN_CHECK_INTERVAL: Duration = Duration::from_millis(10);
const OLDEST_LOG_GAUGE_INTERVAL: Duration = Duration::from_secs(10);

pub struct Raft<F, S, T, R>
where
  F: FinateStateMachine,
  S: Storage,
  T: Transport,
  R: Runtime,
{
  /// The client state machine to apply commands to
  fsm: Arc<F>,
  storage: Arc<S>,
  transport: Arc<T>,
  _marker: std::marker::PhantomData<R>,
}
