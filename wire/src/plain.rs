use std::{io, marker::PhantomData};

use bytes::Bytes;
use futures_util::{AsyncRead, AsyncReadExt};
use ruraft_core::{
  transport::*,
  Data,
};

/// The error kind of [`PlainWire`].
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

/// The error type of [`PlainWire`].
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
    Self {
      kind,
    }
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
  fn custom<T>(msg: T) -> Self
  where
    T: core::fmt::Display,
  {
    Self {
      kind: ErrorKind::Custom(msg.to_string()),
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

const MAX_INLINED_BYTES: usize = 256;
const MESSAGE_LEN_SIZE: usize = core::mem::size_of::<u32>();

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
  ($kind:ident::$variant:ident($reader: ident, $buf: ident, $len: ident)) => {{
    if $len <= MAX_INLINED_BYTES {
      let mut buf = [0u8; MAX_INLINED_BYTES];
      buf[..MESSAGE_LEN_SIZE].copy_from_slice(&$buf[1..]);
      $reader.read_exact(&mut buf[MESSAGE_LEN_SIZE..$len]).await?;
      paste::paste! {
        [< $variant $kind >]::decode(&buf).map(|(_, val)| $kind::$variant(val)).map_err(Into::into) 
      }
    } else {
      let mut buf = vec![0u8; $len];
      buf[..MESSAGE_LEN_SIZE].copy_from_slice(&$buf[1..]);
      $reader.read_exact(&mut buf[MESSAGE_LEN_SIZE..]).await?;
      paste::paste! {
        [< $variant $kind >]::decode(&buf).map(|(_, val)| $kind::$variant(val)).map_err(Into::into) 
      }
    }
  }};
}

impl<I, A, D> Wire for PlainWire<I, A, D>
where
  I: Id + Send + Sync + 'static,
  I::Error: Send + Sync + 'static,
  A: Address + Send + Sync + 'static,
  A::Error: Send + Sync + 'static,
  D: Data,
  D::Error: Send + Sync + 'static,
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

  async fn decode_request(
    mut reader: impl AsyncRead + Send + Unpin,
  ) -> Result<Request<Self::Id, Self::Address, Self::Data>, Self::Error> {
    let mut tag_and_len = [0; 5];
    reader.read_exact(&mut tag_and_len).await?;
    let tag = tag_and_len[0];
    let len = u32::from_be_bytes(tag_and_len[1..].try_into().unwrap()) as usize;
    match tag {
      AppendEntriesRequest::<I, A, D>::TAG => decode!(Request::AppendEntries(reader, tag_and_len, len)),
      VoteRequest::<I, A>::TAG => decode!(Request::Vote(reader, tag_and_len, len)),
      InstallSnapshotRequest::<I, A>::TAG => decode!(Request::InstallSnapshot(reader, tag_and_len, len)),
      TimeoutNowRequest::<I, A>::TAG => decode!(Request::TimeoutNow(reader, tag_and_len, len)),
      HeartbeatRequest::<I, A>::TAG => decode!(Request::Heartbeat(reader, tag_and_len, len)),
      tag => Err(Error::new(ErrorKind::UnknownRpc(tag))),
    }
  }

  async fn decode_response(
    mut reader: impl AsyncRead + Send + Unpin,
  ) -> Result<Response<Self::Id, Self::Address>, Self::Error> {
    let mut tag_and_len = [0; 5];
    reader.read_exact(&mut tag_and_len).await?;
    let tag = tag_and_len[0];
    let len = u32::from_be_bytes(tag_and_len[1..].try_into().unwrap()) as usize;
    match tag {
      AppendEntriesResponse::<I, A>::TAG => decode!(Response::AppendEntries(reader, tag_and_len, len)),
      VoteResponse::<I, A>::TAG => decode!(Response::Vote(reader, tag_and_len, len)),
      InstallSnapshotResponse::<I, A>::TAG => decode!(Response::InstallSnapshot(reader, tag_and_len, len)),
      TimeoutNowResponse::<I, A>::TAG => decode!(Response::TimeoutNow(reader, tag_and_len, len)),
      HeartbeatResponse::<I, A>::TAG => decode!(Response::Heartbeat(reader, tag_and_len, len)),
      ErrorResponse::<I, A>::TAG => decode!(Response::Error(reader, tag_and_len, len)), 
      tag => Err(Error::new(ErrorKind::UnknownRpc(tag))),
    } 
  }
}
