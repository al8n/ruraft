use std::{io, marker::PhantomData};

use bytes::Bytes;
use futures_util::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use ruraft_core::{transport::*, Data};

/// The error kind of [`LpeWire`].
pub enum ErrorKind {
  /// Unknown RPC tag
  UnknownRpc(u8),
  /// Transform error
  Transform(TransformError),
  /// IO error
  IO(io::Error),
  /// Custom error
  Custom(String),
}

impl core::fmt::Debug for ErrorKind {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::UnknownRpc(tag) => f.debug_tuple("UnknownRpc").field(tag).finish(),
      Self::Transform(err) => f.debug_tuple("Transform").field(err).finish(),
      Self::Custom(msg) => f.debug_tuple("Custom").field(msg).finish(),
      Self::IO(err) => f.debug_tuple("IO").field(err).finish(),
    }
  }
}

impl core::fmt::Display for ErrorKind {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::UnknownRpc(tag) => write!(f, "unknown rpc tag: {tag}"),
      Self::Transform(err) => write!(f, "transform error: {err}"),
      Self::Custom(msg) => write!(f, "{msg}"),
      Self::IO(err) => write!(f, "io error: {err}"),
    }
  }
}

/// The error type of [`LpeWire`].
pub struct Error {
  kind: ErrorKind,
}

impl From<TransformError> for Error {
  fn from(err: TransformError) -> Self {
    Self {
      kind: ErrorKind::Transform(err),
    }
  }
}

impl From<io::Error> for Error {
  fn from(err: io::Error) -> Self {
    Self {
      kind: ErrorKind::IO(err),
    }
  }
}

impl core::fmt::Debug for Error {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    self.kind.fmt(f)
  }
}

impl core::fmt::Display for Error {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", self.kind)
  }
}

impl std::error::Error for Error {}

impl Error {
  /// Creates a new [`Error`].
  #[inline]
  pub const fn new(kind: ErrorKind) -> Self {
    Self { kind }
  }

  /// Returns the kind of the error.
  #[inline]
  pub const fn kind(&self) -> &ErrorKind {
    &self.kind
  }

  /// Consumes self and returns the kind of the error.
  #[inline]
  pub fn into_kind(self) -> ErrorKind {
    self.kind
  }
}

impl WireError for Error {
  fn io(err: std::io::Error) -> Self {
    Self {
      kind: ErrorKind::IO(err),
    }
  }

  fn custom<T>(msg: T) -> Self
  where
    T: core::fmt::Display,
  {
    Self {
      kind: ErrorKind::Custom(msg.to_string()),
    }
  }
}

/// A length-prefix-encoding [`Wire`] implementation that aims for high-performance.
///
/// **Note**: It is recommended to use a [`BufWriter`](futures_util::io::BufWriter)/[`BufReader`](futures_util::io::BufWriter) to
/// wrap the writer/reader when using the async APIs of this implementation.
#[repr(transparent)]
pub struct LpeWire<I, A, D>(PhantomData<(I, A, D)>);

impl<I, A, D> LpeWire<I, A, D> {
  /// Creates a new [`LpeWire`].
  pub const fn new() -> Self {
    Self(PhantomData)
  }
}

impl<I, A, D> Default for LpeWire<I, A, D> {
  fn default() -> Self {
    Self::new()
  }
}

impl<I, A, D> Clone for LpeWire<I, A, D> {
  fn clone(&self) -> Self {
    *self
  }
}

impl<I, A, D> Copy for LpeWire<I, A, D> {}

macro_rules! encode {
  ($tag: ident: $target: ident) => {{
    let encoded_len = 1 + $target.encoded_len();
    let mut buf = vec![0u8; encoded_len];
    buf[0] = $tag;
    $target.encode(&mut buf[1..])?;
    Ok(Bytes::from(buf))
  }};
}

macro_rules! decode {
  ($kind:ident::$variant:ident($src: ident)) => {{
    paste::paste! {
      [< $variant $kind >]::decode($src).map(|(_, val)| $kind::$variant(val)).map_err(Into::into)
    }
  }};
}

macro_rules! encode_to_writer {
  ($writer:ident($tag: ident, $target: ident)) => {{
    $writer.write_all(&[$tag]).await?;
    $target
      .encode_to_async_writer(&mut $writer)
      .await
      .map(|_| ())
  }};
}

macro_rules! decode_from_reader {
  ($kind:ident::$variant:ident($reader: ident)) => {{
    paste::paste! {
      [< $variant $kind >]::decode_from_async_reader(&mut $reader).await.map(|(_, val)| $kind::$variant(val))
    }
  }}
}

impl<I, A, D> Wire for LpeWire<I, A, D>
where
  I: Id,
  A: Address,
  D: Data,
{
  type Error = Error;

  type Id = I;

  type Address = A;

  type Data = D;

  type Bytes = bytes::Bytes;

  fn encode_request(
    req: &Request<Self::Id, Self::Address, Self::Data>,
  ) -> Result<Self::Bytes, Self::Error> {
    let tag = req.tag();
    match req {
      Request::AppendEntries(req) => encode!(tag: req),
      Request::Vote(req) => encode!(tag: req),
      Request::InstallSnapshot(req) => encode!(tag: req),
      Request::TimeoutNow(req) => encode!(tag: req),
      Request::Heartbeat(req) => encode!(tag: req),
      _ => unreachable!(),
    }
  }

  fn encode_response(resp: &Response<Self::Id, Self::Address>) -> Result<Self::Bytes, Self::Error> {
    let tag = resp.tag();
    match resp {
      Response::AppendEntries(response) => encode!(tag: response),
      Response::Vote(response) => encode!(tag: response),
      Response::InstallSnapshot(response) => encode!(tag: response),
      Response::TimeoutNow(response) => encode!(tag: response),
      Response::Heartbeat(response) => encode!(tag: response),
      Response::Error(response) => encode!(tag: response),
      _ => unreachable!(),
    }
  }

  async fn encode_request_to_writer(
    req: &Request<Self::Id, Self::Address, Self::Data>,
    mut writer: impl AsyncWrite + Send + Unpin,
  ) -> std::io::Result<()> {
    let tag = req.tag();
    match req {
      Request::AppendEntries(req) => encode_to_writer!(writer(tag, req)),
      Request::Vote(req) => encode_to_writer!(writer(tag, req)),
      Request::InstallSnapshot(req) => encode_to_writer!(writer(tag, req)),
      Request::TimeoutNow(req) => encode_to_writer!(writer(tag, req)),
      Request::Heartbeat(req) => encode_to_writer!(writer(tag, req)),
      _ => unreachable!(),
    }
  }

  async fn encode_response_to_writer(
    resp: &Response<Self::Id, Self::Address>,
    mut writer: impl AsyncWrite + Send + Unpin,
  ) -> std::io::Result<()> {
    let tag = resp.tag();
    match resp {
      Response::AppendEntries(response) => encode_to_writer!(writer(tag, response)),
      Response::Vote(response) => encode_to_writer!(writer(tag, response)),
      Response::InstallSnapshot(response) => encode_to_writer!(writer(tag, response)),
      Response::TimeoutNow(response) => encode_to_writer!(writer(tag, response)),
      Response::Heartbeat(response) => encode_to_writer!(writer(tag, response)),
      Response::Error(response) => encode_to_writer!(writer(tag, response)),
      _ => unreachable!(),
    }
  }

  fn decode_request(
    mut src: &[u8],
  ) -> Result<Request<Self::Id, Self::Address, Self::Data>, Self::Error> {
    let tag = src[0];
    src = &src[1..];

    match tag {
      AppendEntriesRequest::<I, A, D>::TAG => decode!(Request::AppendEntries(src)),
      VoteRequest::<I, A>::TAG => decode!(Request::Vote(src)),
      InstallSnapshotRequest::<I, A>::TAG => decode!(Request::InstallSnapshot(src)),
      TimeoutNowRequest::<I, A>::TAG => decode!(Request::TimeoutNow(src)),
      HeartbeatRequest::<I, A>::TAG => decode!(Request::Heartbeat(src)),
      tag => Err(Error::new(ErrorKind::UnknownRpc(tag))),
    }
  }

  fn decode_response(mut src: &[u8]) -> Result<Response<Self::Id, Self::Address>, Self::Error> {
    let tag = src[0];
    src = &src[1..];
    match tag {
      AppendEntriesResponse::<I, A>::TAG => {
        decode!(Response::AppendEntries(src))
      }
      VoteResponse::<I, A>::TAG => decode!(Response::Vote(src)),
      InstallSnapshotResponse::<I, A>::TAG => {
        decode!(Response::InstallSnapshot(src))
      }
      TimeoutNowResponse::<I, A>::TAG => decode!(Response::TimeoutNow(src)),
      HeartbeatResponse::<I, A>::TAG => decode!(Response::Heartbeat(src)),
      ErrorResponse::<I, A>::TAG => decode!(Response::Error(src)),
      tag => Err(Error::new(ErrorKind::UnknownRpc(tag))),
    }
  }

  async fn decode_request_from_reader(
    mut reader: impl AsyncRead + Send + Unpin,
  ) -> io::Result<Request<Self::Id, Self::Address, Self::Data>> {
    let mut tag = [0; 1];
    reader.read_exact(&mut tag).await?;
    let tag = tag[0];
    match tag {
      AppendEntriesRequest::<I, A, D>::TAG => decode_from_reader!(Request::AppendEntries(reader)),
      VoteRequest::<I, A>::TAG => decode_from_reader!(Request::Vote(reader)),
      InstallSnapshotRequest::<I, A>::TAG => {
        decode_from_reader!(Request::InstallSnapshot(reader))
      }
      TimeoutNowRequest::<I, A>::TAG => decode_from_reader!(Request::TimeoutNow(reader)),
      HeartbeatRequest::<I, A>::TAG => decode_from_reader!(Request::Heartbeat(reader)),
      tag => Err(io::Error::new(
        io::ErrorKind::InvalidData,
        Error::new(ErrorKind::UnknownRpc(tag)),
      )),
    }
  }

  async fn decode_response_from_reader(
    mut reader: impl AsyncRead + Send + Unpin,
  ) -> io::Result<Response<Self::Id, Self::Address>> {
    let mut tag = [0; 1];
    reader.read_exact(&mut tag).await?;
    let tag = tag[0];
    match tag {
      AppendEntriesResponse::<I, A>::TAG => {
        decode_from_reader!(Response::AppendEntries(reader))
      }
      VoteResponse::<I, A>::TAG => decode_from_reader!(Response::Vote(reader)),
      InstallSnapshotResponse::<I, A>::TAG => {
        decode_from_reader!(Response::InstallSnapshot(reader))
      }
      TimeoutNowResponse::<I, A>::TAG => decode_from_reader!(Response::TimeoutNow(reader)),
      HeartbeatResponse::<I, A>::TAG => decode_from_reader!(Response::Heartbeat(reader)),
      ErrorResponse::<I, A>::TAG => decode_from_reader!(Response::Error(reader)),
      tag => Err(io::Error::new(
        io::ErrorKind::InvalidData,
        Error::new(ErrorKind::UnknownRpc(tag)),
      )),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::LpeWire;

  #[tokio::test]
  async fn test_encode_decode_request() {
    unittest_encode_decode_request!(LpeWire {
      AppendEntries,
      Vote,
      InstallSnapshot,
      TimeoutNow,
      Heartbeat
    });
  }

  #[tokio::test]
  async fn test_encode_decode_response() {
    unittest_encode_decode_response!(LpeWire {
      AppendEntries,
      Vote,
      InstallSnapshot,
      TimeoutNow,
      Heartbeat,
      Error
    });
  }
}
