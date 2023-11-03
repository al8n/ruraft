use std::borrow::Cow;

use nodecraft::{resolver::AddressResolver, Transformable};

use super::{Id, Wire};

/// Represents a comprehensive set of errors arising from operations within the [`Transport`](crate::transport::Transport) trait.
///
/// This trait encapsulates a range of error types, providing a structured approach to categorizing
/// and handling transport-level errors. Implementers can leverage this to define both generic and
/// transport-specific error scenarios.
pub trait TransportError: std::error::Error + From<std::io::Error> + Send + Sync + 'static {
  /// Denotes the unique identifier type associated with nodes from the parent transport.
  type Id: Id;

  /// Specifies the type used to resolve node addresses within the parent transport.
  type Resolver: AddressResolver;

  /// Defines the encoding and decoding mechanism used in the parent transport.
  type Wire: Wire;

  /// Constructs an error specifically associated with node identifier ([`Id`]) transformation issues.
  fn id(err: <Self::Id as Transformable>::Error) -> Self;

  /// Constructs an error associated with address ([`Address`](nodecraft::Address)) transformation issues.
  fn address(err: <<Self::Resolver as AddressResolver>::Address as Transformable>::Error) -> Self;

  /// Constructs an error originating from address resolution processes within the transport.
  fn resolver(err: <Self::Resolver as AddressResolver>::Error) -> Self;

  /// Creates an error arising from encoding/decoding processes.
  fn wire(err: <Self::Wire as Wire>::Error) -> Self;

  /// Constructs an error resulting from IO operations within the transport.
  fn io(err: std::io::Error) -> Self;

  /// With extra message to explain the error.
  fn with_message(self, msg: Cow<'static, str>) -> Self;

  /// Provides a flexible mechanism to define custom errors using a descriptive message.
  ///
  /// The resulting error message will be straightforward, avoiding capitalization or a trailing period.
  fn custom<T>(msg: T) -> Self
  where
    Self: Sized,
    T: core::fmt::Display;
}
