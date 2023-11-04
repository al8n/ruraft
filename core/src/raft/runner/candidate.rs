use std::sync::atomic::Ordering;

use crate::{
  membership::ServerSuffrage,
  storage::StableStorage,
  transport::{Header, VoteRequest, VoteResponse},
  utils::random_timeout,
};

use super::*;
use futures::{future::Either, StreamExt};
use nodecraft::{Address, Id};

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
  pub(super) async fn run_candidate(
    &mut self,
    #[cfg(feature = "metrics")] saturation_metric: &mut SaturationMetric,
  ) -> Result<bool, ()> {
    let term = self.current_term() + 1;
    let local_id = self.transport.local_id();
    let local_addr = self.transport.local_addr();
    tracing::info!(target = "ruraft", id=%local_id, addr=%local_addr, term = %term, "entering candidate state");

    #[cfg(feature = "metrics")]
    metrics::increment_counter!("ruraft.state.candidate");

    // Start vote for us, and set a timeout
    let (votes_needed, vote_rx) = self.elect_self(local_id, local_addr).await;

    // Make sure the leadership transfer flag is reset after each run. Having this
    // flag will set the field LeadershipTransfer in a RequestVoteRequst to true,
    // which will make other servers vote even though they have a leader already.
    // It is important to reset that flag, because this priviledge could be abused
    // otherwise.
    scopeguard::defer!(self
      .candidate_from_leadership_transfer
      .store(false, Ordering::Release));

    let opts = self.reloadable_options.load(Ordering::Acquire);
    let mut election_timeout = opts.election_timeout();
    let mut election_timer = R::interval(random_timeout(election_timeout).unwrap());

    // Tally the votes, need a simple majority
    let mut granted_votes = 0;

    tracing::debug!(target = "ruraft.candidate", needed = %votes_needed, term = %term, "calculated votes needed");
    while self.role() == Role::Candidate {
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
              tracing::error!(target = "ruraft.candidate", err=%e, "rpc consumer closed unexpectedly, shutting down...");
              return Err(());
            }
          }
        }
        vote = vote_rx.recv().fuse() => {
          #[cfg(feature = "metrics")]
          saturation_metric.working();

          if let Ok(vote) = vote {
            // Check if the term is greater than ours, bail
            if vote.resp.term > self.current_term() {
              tracing::debug!(target = "ruraft.candidate", term = %vote.resp.term, "newer term discovered, fallback to follower");
              self.state.set_role(Role::Follower, &self.observers).await;
              self.set_current_term(vote.resp.term);
              return Ok(true);
            }

            // Check if the vote is granted
            if vote.resp.granted {
              granted_votes += 1;
              tracing::debug!(target = "ruraft.candidate", from = %vote.voter_id, term = %vote.resp.term, tally = %granted_votes, "vote granted");
            }

            // Check if we've become the leader
            if granted_votes >= votes_needed {
              tracing::info!(target = "ruraft.candidate", term = %term, tally = %granted_votes, "election won");
              self.state.set_role(Role::Leader, &self.observers).await;
              self.leader.set(Some(Node::new(local_id.clone(), local_addr.clone())), &self.observers).await;
              return Ok(true);
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
                tracing::error!(target = "ruraft.candidate", "receive membership change request, but fail to send error response, receiver closed");
              }
            }
            Err(e) => {
              tracing::error!(target = "ruraft.candidate", err=%e, "membership change sender closed unexpectedly, shutting down...");
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
              tracing::error!(target = "ruraft.candidate", err=%e, "membership sender closed unexpectedly, shutting down...");
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
                tracing::error!(target = "ruraft.candidate", "receive apply request, but fail to send error response, receiver closed");
              }
            }
            Err(e) => {
              tracing::error!(target = "ruraft.candidate", err=%e, "apply sender closed unexpectedly, shutting down...");
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
                tracing::error!(target = "ruraft.candidate", "receive verify leader request, but fail to send error response, receiver closed");
              }
            }
            Err(e) => {
              tracing::error!(target = "ruraft.candidate", err=%e, "verify sender closed unexpectedly, shutting down...");
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
                tracing::error!(target = "ruraft.candidate", "receive user restore request, but fail to send error response, receiver closed");
              }
            }
            Err(e) => {
              tracing::error!(target = "ruraft.candidate", err=%e, "user restore sender closed unexpectedly, shutting down...");
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
                tracing::error!(target = "ruraft.candidate", "receive leader transfer request, but fail to send error response, receiver closed");
              }
            },
            Err(e) => {
              tracing::error!(target = "ruraft.candidate", err=%e, "leader transfer sender closed unexpectedly, shutting down...");
              return Err(());
            }
          }
        }
        _ = self.leader_notify_rx.recv().fuse() => {
          // ignore since we are not the leader
        },
        notify = self.follower_notify_rx.recv().fuse() => {
          match notify {
            Ok(_) => {
              let n_election_timeout = self.reloadable_options.load(Ordering::Acquire).election_timeout();
              if n_election_timeout != election_timeout {
                election_timeout = n_election_timeout;
                election_timer = R::interval(random_timeout(election_timeout).unwrap());
              }
            },
            Err(e) => {
              tracing::error!(target = "ruraft.candidate", err=%e, "follower notify sender closed unexpectedly, shutting down...");
              return Err(());
            }
          }
        }
        _ = election_timer.next().fuse() => {
          #[cfg(feature = "metrics")]
          saturation_metric.working();

          // Election failed! Restart the election. We simply return,
          // which will kick us back into runCandidate
          tracing::warn!(target = "ruraft.candidate", term = %term, "election timeout reached, restarting election");
          return Ok(true);
        }
        _ = self.shutdown_rx.recv().fuse() => {
          tracing::info!(target = "ruraft.candidate", "candidate received shutdown signal, shutdown...");
          return Ok(false);
        }
      }
    }
    Ok(true)
  }

  /// Used to send a [`vote`] RPC to all peers, and vote for
  /// ourself. This has the side affecting of incrementing the current term. The
  /// response channel returned is used to wait for all the responses (including a
  /// vote for ourself). This must only be called from the main thread.
  ///
  /// [`vote`]: crate::transport::Transport::vote
  async fn elect_self(
    &self,
    local_id: &T::Id,
    local_addr: &<T::Resolver as AddressResolver>::Address,
  ) -> (
    usize,
    async_channel::Receiver<VoteResult<T::Id, <T::Resolver as AddressResolver>::Address>>,
  ) {
    let latest = self.memberships.latest().1.clone();

    // Create a response channel
    let (tx, rx) = async_channel::bounded(latest.len());

    // Increment the term
    self.current_term.fetch_add(1, Ordering::AcqRel);

    // Construct the request
    let last = self.last_entry();
    let term = self.current_term();
    let leadership_transfer = self
      .candidate_from_leadership_transfer
      .load(Ordering::Acquire);
    let protocol_version = self.options.protocol_version;

    // For each peer, request a vote
    let vote_futs = latest.iter().filter_map(|(id, (addr, suffrage))| {
      if *suffrage != ServerSuffrage::Voter {
        None
      } else {
        Some((id.clone(), (addr.clone(), *suffrage)))
      }
    }).map(|(id, (addr, _))| {
      if local_id.eq(&id) {
        let node = Node::new(local_id.clone(), local_addr.clone());
        let s = self.storage.clone();
        let txx = tx.clone();
        Either::Left(async move {
          tracing::debug!(target = "ruraft", term=%term, id=%id, "voting for self");
          // Persist a vote for ourselves
          if let Err(err) = Self::persist_vote(s.stable_store(), term, node).await {
            tracing::error!(target = "ruraft", err=%err, "failed to persist vote");
            return;
          }

          // Include our own vote
          if txx.send(VoteResult {
            resp: VoteResponse {
              header: Header::new(protocol_version, id.clone(), addr.clone()),
              term,
              granted: true,
            },
            voter_id: id.clone(),
          }).await.is_err() {
            unreachable!("receiver should not be dropped");
          }
        })
      } else {
        let wg = self.wg.clone();
        let txx = tx.clone();
        let trans = self.transport.clone();
        let target = Node::new(id, addr);
        Either::Right(async move {
          tracing::debug!(target = "ruraft", term = %term, from = %local_id, address = %local_addr, "asking for vote");

          super::super::spawn_local::<R, _>(wg.add(1), async move {
            #[cfg(feature = "metrics")]
            let start = std::time::Instant::now();

            let res = match trans.vote(&target, VoteRequest {
              header: trans.header(),
              term,
              last_log_index: last.index,
              last_log_term: last.term,
              leadership_transfer,
            }).await {
              Ok(resp) => VoteResult {
                resp,
                voter_id: target.id().clone(),
              },
              Err(e) => {
                tracing::error!(target = "ruraft", to = %target, err=%e, "failed to make vote rpc");
                VoteResult {
                  resp: VoteResponse {
                    header: Header {
                      protocol_version,
                      from: target.clone(),
                    },
                    term,
                    granted: false,
                  },
                  voter_id: target.id().clone(),
                }
              }
            };


            if let Err(e) = txx.send(res).await {
              tracing::error!(target = "ruraft", to = %target, err=%e, "failed to send back vote result, receiver closed");
            }

            #[cfg(feature = "metrics")]
            metrics::histogram!("ruraft.candidate.elect_self", start.elapsed().as_millis() as f64);
          });
        })
      }
    });
    let quorum = vote_futs.size_hint().0 / 2 + 1;
    futures::future::join_all(vote_futs).await;
    (quorum, rx)
  }

  async fn persist_vote(
    stable: &S::Stable,
    term: u64,
    cand: Node<T::Id, <T::Resolver as AddressResolver>::Address>,
  ) -> Result<(), <S::Stable as StableStorage>::Error> {
    stable.store_last_vote_term(term).await?;
    stable.store_last_vote_candidate(cand).await
  }
}

struct VoteResult<I: Id, A: Address> {
  resp: VoteResponse<I, A>,
  voter_id: I,
}
