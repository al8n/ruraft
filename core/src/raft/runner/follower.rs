use std::{sync::atomic::Ordering, time::Duration};

use futures::StreamExt;

use super::*;
use crate::utils::random_timeout;

impl<F, S, T, SC, R> RaftRunner<F, S, T, SC, R>
where
  F: FinateStateMachine<
    Id = T::Id,
    Address = <T::Resolver as AddressResolver>::Address,
    Data = T::Data,
    SnapshotSink = <S::Snapshot as SnapshotStorage>::Sink,
    Runtime = R,
  >,
  S: Storage<
    Id = T::Id,
    Address = <T::Resolver as AddressResolver>::Address,
    Data = T::Data,
    Runtime = R,
  >,
  T: Transport<Runtime = R>,
  <T::Resolver as AddressResolver>::Address: Send + Sync + 'static,
  SC: Sidecar<Runtime = R>,
  R: Runtime,
  <R::Sleep as std::future::Future>::Output: Send,
  <R::Interval as futures::Stream>::Item: Send + 'static,
{
  pub(super) async fn run_follower(
    &mut self,
    #[cfg(feature = "metrics")] saturation_metric: &mut SaturationMetric,
  ) -> Result<bool, ()> {
    let mut did_warn = false;
    let leader = self.leader.load();
    let local_id = self.transport.local_id();
    let local_addr = self.transport.local_addr();

    match leader.as_ref() {
      Some(l) => {
        tracing::info!(target = "ruraft.follower", leader = %l.as_ref(), id=%local_id, addr=%local_addr, "entering follower state");
      }
      None => {
        tracing::warn!(target = "ruraft.follower", id=%local_id, addr=%local_addr, "entering follower state without a leader");
      }
    }

    let opts = self.reloadable_options.load(Ordering::Acquire);
    let mut heartbeat_timeout = opts.heartbeat_timeout();
    let mut heartbeat_timer = R::interval(random_timeout(heartbeat_timeout).unwrap());

    while self.state.role() == Role::Follower {
      #[cfg(feature = "metrics")]
      saturation_metric.sleeping();

      futures::select! {
        rpc = self.rpc.recv().fuse() => {
          #[cfg(feature = "metrics")]
          saturation_metric.working();

          match rpc {
            Ok(rpc) => {
              let (tx, req) = rpc.into_components();
              self.handle_request(tx, req).await;
            }
            Err(e) => {
              tracing::error!(target = "ruraft.follower", err=%e, "rpc consumer closed unexpectedly, shutting down...");
              return Err(());
            }
          }
        }
        mc = self.membership_change_rx.recv().fuse() => {
          #[cfg(feature = "metrics")]
          saturation_metric.working();

          // Reject any operations since we are not the leader
          match mc {
            Ok(mc) => {
              if mc.tx.send(Err(Error::not_leader())).is_err() {
                tracing::error!(target = "ruraft.follower", "receive membership change request, but fail to send error response, receiver closed");
              }
            }
            Err(e) => {
              tracing::error!(target = "ruraft.follower", err=%e, "membership change sender closed unexpectedly, shutting down...");
              return Err(());
            }
          }
        }
        a = self.apply_rx.recv().fuse() => {
          #[cfg(feature = "metrics")]
          saturation_metric.working();

          // Reject any operations since we are not the leader
          match a {
            Ok(a) => {
              if a.tx.send_err(Error::not_leader()).is_err() {
                tracing::error!(target = "ruraft.follower", "receive apply request, but fail to send error response, receiver closed");
              }
            }
            Err(e) => {
              tracing::error!(target = "ruraft.follower", err=%e, "apply sender closed unexpectedly, shutting down...");
              return Err(());
            }
          }
        }
        v = self.verify_rx.recv().fuse() => {
          #[cfg(feature = "metrics")]
          saturation_metric.working();

          // Reject any operations since we are not the leader
          match v {
            Ok(v) => {
              if v.send(Err(Error::not_leader())).is_err() {
                tracing::error!(target = "ruraft.follower", "receive verify leader request, but fail to send error response, receiver closed");
              }
            }
            Err(e) => {
              tracing::error!(target = "ruraft.follower", err=%e, "verify sender closed unexpectedly, shutting down...");
              return Err(());
            }
          }
        }
        ur = self.user_restore_rx.recv().fuse() => {
          #[cfg(feature = "metrics")]
          saturation_metric.working();

          // Reject any operations since we are not the leader
          match ur {
            Ok(ur) => {
              if ur.1.send(Err(Error::not_leader())).is_err() {
                tracing::error!(target = "ruraft.follower", "receive user restore request, but fail to send error response, receiver closed");
              }
            }
            Err(e) => {
              tracing::error!(target = "ruraft.follower", err=%e, "user restore sender closed unexpectedly, shutting down...");
              return Err(());
            }
          }
        }
        l = self.leader_transfer_rx.recv().fuse() => {
          #[cfg(feature = "metrics")]
          saturation_metric.working();

          // Reject any operations since we are not the leader
          match l {
            Ok(l) => {
              if l.1.send(Err(Error::not_leader())).is_err() {
                tracing::error!(target = "ruraft.follower", "receive leader transfer request, but fail to send error response, receiver closed");
              }
            },
            Err(e) => {
              tracing::error!(target = "ruraft.follower", err=%e, "leader transfer sender closed unexpectedly, shutting down...");
              return Err(());
            }
          }
        }
        c = self.committed_membership_rx.recv().fuse() => {
          #[cfg(feature = "metrics")]
          saturation_metric.working();

          // Reject any operations since we are not the leader
          match c {
            Ok(c) => {
              if c.send(Ok(self.memberships.committed().clone())).is_err() {
                tracing::error!(target = "ruraft.candidate", "receive committed membership request, but fail to send response, receiver closed");
              }
            },
            Err(e) => {
              tracing::error!(target = "ruraft.follower", err=%e, "membership sender closed unexpectedly, shutting down...");
              return Err(());
            }
          }
        }
        _ = self.leader_notify_rx.recv().fuse() => {
          // ignore since we are not the leader
        },
        _ = self.follower_notify_rx.recv().fuse() => {
          heartbeat_timer = R::interval(Duration::ZERO);
        }
        _ = heartbeat_timer.next().fuse() => {
          #[cfg(feature = "metrics")]
          saturation_metric.working();

          // Restart the heartbeat timer
          let opts = self.reloadable_options.load(Ordering::Acquire);
          heartbeat_timeout = opts.heartbeat_timeout();
          heartbeat_timer = R::interval(random_timeout(heartbeat_timeout).unwrap());

          // Check if we have had a successful contact
          if let Some(last_contact) = self.last_contact() {
            if last_contact.elapsed() < heartbeat_timeout {
              continue;
            }

            // Heartbeat failed! Transition to the candidate state
            let last_leader = self.leader.load().clone();
            self.leader.set(None, &self.observers).await;

            let (latest_index, latest) = {
              let latest = self.memberships.latest();
              (latest.0, latest.1.clone())
            };
            let committed_index = self.memberships.committed().0;
            if latest_index == 0 {
              if !did_warn {
                tracing::warn!(target = "ruraft.follower", "no known peers, aborting election");
                did_warn = true;
              }
            } else {
              let has_vote = latest.is_voter(self.transport.local_id());
              if latest_index == committed_index && !has_vote {
                if !did_warn {
                  tracing::warn!(target = "ruraft.follower", "not part of stable configuration, aborting election");
                  did_warn = true;
                }
              } else {
                #[cfg(feature = "metrics")]
                metrics::increment_counter!("ruraft.transition.heartbeat_timeout");
                if has_vote {
                  if let Some(last_leader) = last_leader {
                    tracing::warn!(target = "ruraft.follower", last_leader_id = %last_leader.id(), last_leader_addr=%last_leader.addr(), "heartbeat timeout reached, starting election");
                  } else {
                    tracing::warn!(target = "ruraft.follower", last_leader_id = "", last_leader_addr="", "heartbeat timeout reached, starting election");
                  }
                  self.state.set_role(Role::Candidate, &self.observers).await;
                  return Ok(true);
                } else if !did_warn {
                  tracing::warn!(target = "ruraft.follower", "heartbeat timeout reached, not part of a stable membership or a non-voter, not triggering a leader election");
                  did_warn = true;
                }
              }
            }
          }
        }
        _ = self.shutdown_rx.recv().fuse() => {
          tracing::info!(target = "ruraft.follower", "follower received shutdown signal, shutdown...");
          // Clear the leader to prevent forwarding
          self.leader.set(None, &self.observers).await;
          return Ok(false);
        }
      }
    }

    Ok(true)
  }
}
