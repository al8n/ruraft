use std::time::SystemTime;

use super::*;

/// The response returned from an
/// [`AppendEntriesRequest`].
#[viewit::viewit(setters(prefix = "with"))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AppendEntriesResponse<I, A> {
  /// The header of the response
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the header of the response"),),
    setter(attrs(doc = "Set the header of the response"),)
  )]
  header: Header<I, A>,

  /// Newer term if leader is out of date
  #[viewit(
    getter(const, attrs(doc = "Get the newer term if leader is out of date"),),
    setter(attrs(doc = "Set the newer term if leader is out of date"),)
  )]
  term: u64,

  /// A hint to help accelerate rebuilding slow nodes
  #[viewit(
    getter(const, attrs(doc = "Get a hint to help accelerate rebuilding slow nodes"),),
    setter(attrs(doc = "Set a hint to help accelerate rebuilding slow nodes"),)
  )] 
  last_log: u64,

  /// We may not succeed if we have a conflicting entry
  #[viewit(
    getter(const, attrs(doc = "Get if we have a conflicting entry"),),
    setter(attrs(doc = "Set if we have a conflicting entry"),)
  )]
  success: bool,

  /// There are scenarios where this request didn't succeed
  /// but there's no need to wait/back-off the next attempt.
  #[viewit(
    getter(const, attrs(doc = "Get if there's no need to wait/back-off the next attempt"),),
    setter(attrs(doc = "Set if there's no need to wait/back-off the next attempt"),)
  )]
  no_retry_backoff: bool,
}

impl<I, A> AppendEntriesResponse<I, A> {
  /// Create a new [`AppendEntriesResponse`] with the given `id` and `addr` and `version`. Other fields
  /// are set to their default values.
  #[inline]
  pub const fn new(version: ProtocolVersion, id: I, addr: A) -> Self {
    Self {
      header: Header::new(version, id, addr),
      term: 0,
      last_log: 0,
      success: false,
      no_retry_backoff: false,
    }
  }

  /// Create a new [`AppendEntriesResponse`] with the given protocol version, node and default values.
  #[inline]
  pub const fn from_node(version: ProtocolVersion, node: Node<I, A>) -> Self {
    Self {
      header: Header::from_node(version, node),
      term: 0,
      last_log: 0,
      success: false,
      no_retry_backoff: false,
    }
  }

  /// Create a new [`AppendEntriesResponse`] with the given header and default values.
  #[inline]
  pub const fn from_header(header: Header<I, A>) -> Self {
    Self {
      header,
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
  #[viewit(
    getter(const, attrs(doc = "Get the term of the original [`AppendEntriesRequest`](crate::transport::AppendEntriesRequest)."),),
    setter(attrs(doc = "Set the term which comes from the original [`AppendEntriesRequest`](crate::transport::AppendEntriesRequest)."),)
  )]
  term: u64,

  /// The highest log index of the [`AppendEntriesRequest`]'s entries
  #[viewit(
    getter(const, attrs(doc = "Get the highest log index of the original [`AppendEntriesRequest`](crate::transport::AppendEntriesRequest)."),),
    setter(attrs(doc = "Set the highest log index which comes from the original [`AppendEntriesRequest`](crate::transport::AppendEntriesRequest)."),)
  )]
  highest_log_index: Option<u64>,

  /// The number of entries in the [`AppendEntriesRequest`]'s
  #[viewit(
    getter(const, attrs(doc = "Get the number of entries of the original [`AppendEntriesRequest`](crate::transport::AppendEntriesRequest)."),),
    setter(attrs(doc = "Set the number of entries which comes from the original [`AppendEntriesRequest`](crate::transport::AppendEntriesRequest)."),)
  )]
  num_entries: usize,

  /// The time that the original request was started
  #[cfg_attr(feature = "serde", serde(with = "serde_millis"))]
  #[viewit(
    getter(const, attrs(doc = "Get the time that the original [`AppendEntriesRequest`](crate::transport::AppendEntriesRequest) was started."),),
    setter(attrs(doc = "Set the time that the original [`AppendEntriesRequest`](crate::transport::AppendEntriesRequest) was started."),)
  )]
  start: SystemTime,

  /// The response of the [`AppendEntriesRequest`]
  #[viewit(
    getter(const, style = "ref", rename = "response", attrs(doc = "Get the [`AppendEntriesResponse`].")),
    setter(attrs(doc = "Set the [`AppendEntriesResponse`]."),)
  )]
  resp: AppendEntriesResponse<I, A>,
}

impl<I, A> PipelineAppendEntriesResponse<I, A> {
  /// Create a new [`PipelineAppendEntriesResponse`] with the given [`AppendEntriesResponse`]. Other fields
  /// are set to their default values.
  #[inline]
  pub fn new(resp: AppendEntriesResponse<I, A>) -> Self {
    Self {
      term: 0,
      highest_log_index: None,
      num_entries: 0,
      start: SystemTime::now(),
      resp,
    }
  }

  /// Returns the mutable reference to the response.
  #[inline]
  pub fn response_mut(&mut self) -> &mut AppendEntriesResponse<I, A> {
    &mut self.resp
  }
}

impl<I, A> From<PipelineAppendEntriesResponse<I, A>> for AppendEntriesResponse<I, A> {
  fn from(value: PipelineAppendEntriesResponse<I, A>) -> Self {
    value.resp
  }
}

impl<I: CheapClone, A: CheapClone> CheapClone for PipelineAppendEntriesResponse<I, A> {
  fn cheap_clone(&self) -> Self {
    Self {
      term: self.term,
      highest_log_index: self.highest_log_index,
      num_entries: self.num_entries,
      start: self.start,
      resp: self.resp.cheap_clone(),
    }
  }
}
