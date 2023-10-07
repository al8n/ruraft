use nodecraft::{resolver::AddressResolver, Transformable};

use super::{Decoder, Encoder, Id};

/// The Error trait allows to create descriptive error messages belonging to the `Transport` against which they are currently running.
///
/// Every `Transport` declares an Error type that encompasses both general-purpose transport errors as well as errors specific to the particular transport implementation.
pub trait Error: std::error::Error + From<std::io::Error> + Send + Sync + 'static {
  /// The id type of the parent transport.
  type Id: Id;
  /// The resolver type of the parent transport.
  type Resolver: AddressResolver;
  /// The encoder type of the parent transport.
  type Encoder: Encoder;
  /// The decoder type of the parent transport.
  type Decoder: Decoder;

  /// Creates a new error from an id error.
  fn id(err: <Self::Id as Transformable>::Error) -> Self
  where
    Self: Sized;

  /// Creates a new error from an address error.
  fn address(err: <<Self::Resolver as AddressResolver>::Address as Transformable>::Error) -> Self
  where
    Self: Sized;

  /// Creates a new error from a resolver error.
  fn resolver(err: <Self::Resolver as AddressResolver>::Error) -> Self
  where
    Self: Sized;

  /// Creates a new error from an encoder error.
  fn encoder(err: <Self::Encoder as Encoder>::Error) -> Self
  where
    Self: Sized;

  /// Creates a new error from a decoder error.
  fn decoder(err: <Self::Decoder as Decoder>::Error) -> Self
  where
    Self: Sized;

  /// Creates a new error from an io error.
  fn io(err: std::io::Error) -> Self
  where
    Self: Sized;

  /// Raised when there is general error happens in a transport type.
  ///
  /// The message should not be capitalized and should not end with a period.
  fn custom<T>(msg: T) -> Self
  where
    Self: Sized,
    T: core::fmt::Display;
}
