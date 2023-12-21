use std::{io, marker::PhantomData};

use futures_util::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use ruraft_core::{
  transport::{Address, Id, Request, Response, Transformable, Wire, WireError},
  Data,
};

#[derive(thiserror::Error)]
pub enum Error<I: Transformable, A: Transformable> {
  #[error("{0}")]
  Id(<I as Transformable>::Error),
  #[error("{0}")]
  Address(<A as Transformable>::Error),
  #[error("{0}")]
  Custom(String),
}

impl<I: Id, A: Address> WireError for Error<I, A>
where
  I::Error: Send + Sync + 'static,
  A::Error: Send + Sync + 'static,
{
  type Id = I;

  type Address = A;

  fn id(err: <I as Transformable>::Error) -> Self {
    Self::Id(err)
  }

  fn address(err: <A as Transformable>::Error) -> Self {
    Self::Address(err)
  }

  fn custom<T>(msg: T) -> Self
  where
    T: core::fmt::Display,
  {
    Self::Custom(msg.to_string())
  }
}

impl<I: Transformable, A: Transformable> core::fmt::Debug for Error<I, A> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::Id(arg0) => f.debug_tuple("Id").field(arg0).finish(),
      Self::Address(arg0) => f.debug_tuple("Address").field(arg0).finish(),
      Self::Custom(arg0) => f.debug_tuple("Custom").field(arg0).finish(),
    }
  }
}

/// A [`Wire`] implementor that aims for high-performance.
pub struct PlainWire<I, A, D>(PhantomData<(I, A, D)>);

impl<I, A, D> PlainWire<I, A, D> {
  /// Creates a new [`PlainWire`].
  pub const fn new() -> Self {
    Self(PhantomData)
  }
}

impl<I, A, D> Default for PlainWire<I, A, D> {
  fn default() -> Self {
    Self::new()
  }
}

impl<I, A, D> Clone for PlainWire<I, A, D> {
  fn clone(&self) -> Self {
    *self
  }
}

impl<I, A, D> Copy for PlainWire<I, A, D> {}

impl<I, A, D> Wire for PlainWire<I, A, D>
where
  I: Id + Send + Sync + 'static,
  I::Error: Send + Sync + 'static,
  A: Address + Send + Sync + 'static,
  A::Error: Send + Sync + 'static,
  D: Data,
{
  type Error = Error<I, A>;

  type Id = I;

  type Address = A;

  type Data = D;

  type Bytes = bytes::Bytes;

  fn encode_request(
    req: &Request<Self::Id, Self::Address, Self::Data>,
  ) -> Result<Self::Bytes, Self::Error> {
    todo!()
  }

  fn encode_response(resp: &Response<Self::Id, Self::Address>) -> Result<Self::Bytes, Self::Error> {
    todo!()
  }

  async fn decode_request(
    reader: impl AsyncRead + Send + Unpin,
  ) -> Result<Request<Self::Id, Self::Address, Self::Data>, Self::Error> {
    todo!()
  }

  async fn decode_response(
    reader: impl AsyncRead + Send + Unpin,
  ) -> Result<Response<Self::Id, Self::Address>, Self::Error> {
    todo!()
  }
}
