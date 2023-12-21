use super::*;

/// The response returned from an
/// [`AppendEntriesRequest`].
#[viewit::viewit(setters(prefix = "with"))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]

pub struct AppendEntriesResponse<I, A> {
  /// The header of the response
  #[viewit(getter(const))]
  header: Header<I, A>,

  /// Newer term if leader is out of date
  term: u64,

  /// A hint to help accelerate rebuilding slow nodes
  last_log: u64,

  /// We may not succeed if we have a conflicting entry
  success: bool,

  /// There are scenarios where this request didn't succeed
  /// but there's no need to wait/back-off the next attempt.
  no_retry_backoff: bool,
}

impl<I, A> AppendEntriesResponse<I, A> {
  pub fn new(version: ProtocolVersion, id: I, addr: A) -> Self {
    Self {
      header: Header::new(version, id, addr),
      term: 0,
      last_log: 0,
      success: false,
      no_retry_backoff: false,
    }
  }
}

impl<I: CheapClone, A: CheapClone> CheapClone for AppendEntriesResponse<I, A> {}

/// The response returned by a pipeline.
///
/// The difference between this and [`AppendEntriesResponse`] is that this
/// keeps some extra information:
///
/// 1. the time that the append request was started
/// 2. the original request's `term`
/// 3. the number of entries the original request has
/// 4. highest log index of the original request's entries
#[viewit::viewit(getters(vis_all = "pub"), setters(prefix = "with"))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PipelineAppendEntriesResponse<I, A> {
  /// The term of the request
  term: u64,

  /// The highest log index of the [`AppendEntriesRequest`]'s entries
  highest_log_index: Option<u64>,

  /// The number of entries in the [`AppendEntriesRequest`]'s
  num_entries: usize,

  /// The time that the original request was started
  #[cfg_attr(feature = "serde", serde(with = "serde_millis"))]
  start: Instant,

  /// The response of the [`AppendEntriesRequest`]
  #[viewit(getter(const, style = "ref"))]
  resp: AppendEntriesResponse<I, A>,
}

impl<I: CheapClone, A: CheapClone> CheapClone for PipelineAppendEntriesResponse<I, A> {}
