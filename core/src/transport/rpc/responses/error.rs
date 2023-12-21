use super::*;

/// The response returned from an
/// [`HeartbeatRequest`].
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ErrorResponse<I, A> {
  /// The header of the response
  #[viewit(getter(const))]
  header: Header<I, A>,
  /// The error message
  error: String,
}

impl<I, A> From<ErrorResponse<I, A>> for String {
  fn from(value: ErrorResponse<I, A>) -> Self {
    value.error
  }
}

impl<I, A> ErrorResponse<I, A> {
  /// Create a new ErrorResponse
  pub const fn new(header: Header<I, A>, error: String) -> Self {
    Self { header, error }
  }
}
