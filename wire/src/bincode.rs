use std::{io, marker::PhantomData};

use async_bincode::futures::{AsyncBincodeReader, AsyncBincodeWriter};
use bincode::{
  config::{AllowTrailing, Bounded, WithOtherLimit, WithOtherTrailing},
  DefaultOptions, Options,
};
use byteorder::{ByteOrder, NetworkEndian, WriteBytesExt};
use bytes::Bytes;
use futures_util::{AsyncRead, SinkExt, TryStreamExt};
use ruraft_core::{transport::*, Data};

/// Error type of [`BincodeWire`].
#[derive(thiserror::Error, Debug)]
pub enum Error {
  /// Encode error
  #[error("encode error: {0}")]
  Encode(bincode::Error),
  /// Decode error
  #[error("decode error: {0}")]
  Decode(bincode::Error),
  /// IO error
  #[error("io error: {0}")]
  IO(#[from] io::Error),
  /// Custom error
  #[error("custom error: {0}")]
  Custom(String),
}

impl WireError for Error {
  fn io(err: std::io::Error) -> Self {
    Self::IO(err)
  }

  fn custom<T>(msg: T) -> Self
  where
    T: core::fmt::Display,
  {
    Self::Custom(msg.to_string())
  }
}

fn bincoder() -> WithOtherTrailing<WithOtherLimit<DefaultOptions, Bounded>, AllowTrailing> {
  bincode::options()
    .with_limit(u32::max_value() as u64)
    .allow_trailing_bytes()
}

/// A [`Wire`] implementation based on [`bincode`](bincode) for sync APIs and [`async-bincode`](async_bincode) for async APIs.
///
/// **Important:** When using async write APIs, only one element at a time is written to the output writer.
/// It is recommended to use a [`BufWriter`](futures_util::io::BufWriter) in front of the output to batch write operations to the underlying writer.
#[repr(transparent)]
pub struct BincodeWire<I, A, D>(PhantomData<(I, A, D)>);

impl<I, A, D> Wire for BincodeWire<I, A, D>
where
  I: Id + serde::Serialize + for<'a> serde::Deserialize<'a> + Send + Sync + 'static,
  A: Address + serde::Serialize + for<'a> serde::Deserialize<'a> + Send + Sync + 'static,
  D: Data + serde::Serialize + for<'a> serde::Deserialize<'a> + Send + Sync + 'static,
{
  type Error = Error;

  type Id = I;

  type Address = A;

  type Data = D;

  type Bytes = Bytes;

  fn encode_request(
    req: &Request<Self::Id, Self::Address, Self::Data>,
  ) -> Result<Self::Bytes, Self::Error> {
    let c = bincoder();
    let size = c.serialized_size(req).map_err(Self::Error::Encode)? as u32;
    let mut buffer = Vec::with_capacity(size as usize + 4);
    buffer.write_u32::<NetworkEndian>(size)?;
    c.serialize_into(&mut buffer, &req)
      .map(|_| Bytes::from(buffer))
      .map_err(Error::Encode)
  }

  fn encode_response(resp: &Response<Self::Id, Self::Address>) -> Result<Self::Bytes, Self::Error> {
    let c = bincoder();
    let size = c.serialized_size(resp).map_err(Self::Error::Encode)? as u32;
    let mut buffer = Vec::with_capacity(size as usize + 4);
    buffer.write_u32::<NetworkEndian>(size)?;
    c.serialize_into(&mut buffer, &resp)
      .map(|_| Bytes::from(buffer))
      .map_err(Error::Encode)
  }

  async fn encode_request_to_writer(
    req: &Request<Self::Id, Self::Address, Self::Data>,
    writer: impl futures_util::AsyncWrite + Send + Unpin,
  ) -> std::io::Result<()> {
    let mut writer = AsyncBincodeWriter::from(writer).for_async();
    writer
      .send(req)
      .await
      .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
  }

  async fn encode_response_to_writer(
    resp: &Response<Self::Id, Self::Address>,
    writer: impl futures_util::AsyncWrite + Send + Unpin,
  ) -> std::io::Result<()> {
    let mut writer = AsyncBincodeWriter::from(writer).for_async();
    writer
      .send(resp)
      .await
      .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
  }

  fn decode_request(
    src: &[u8],
  ) -> Result<Request<Self::Id, Self::Address, Self::Data>, Self::Error> {
    let message_size: u32 = NetworkEndian::read_u32(&src[..4]);
    bincoder()
      .deserialize(&src[4..4 + message_size as usize])
      .map_err(Error::Decode)
  }

  fn decode_response(src: &[u8]) -> Result<Response<Self::Id, Self::Address>, Self::Error> {
    let message_size: u32 = NetworkEndian::read_u32(&src[..4]);
    bincoder()
      .deserialize(&src[4..4 + message_size as usize])
      .map_err(Error::Decode)
  }

  async fn decode_request_from_reader(
    reader: impl AsyncRead + Send + Unpin,
  ) -> std::io::Result<Request<Self::Id, Self::Address, Self::Data>> {
    let mut reader = AsyncBincodeReader::from(reader);
    reader
      .try_next()
      .await
      .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
      .and_then(|opt| {
        opt.ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "corrupted stream"))
      })
  }

  async fn decode_response_from_reader(
    reader: impl AsyncRead + Send + Unpin,
  ) -> std::io::Result<Response<Self::Id, Self::Address>> {
    let mut reader = AsyncBincodeReader::from(reader);
    reader
      .try_next()
      .await
      .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
      .and_then(|opt| {
        opt.ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "corrupted stream"))
      })
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test]
  async fn test_encode_decode_request() {
    unittest_encode_decode_request!(BincodeWire {
      AppendEntries,
      Vote,
      InstallSnapshot,
      TimeoutNow,
      Heartbeat
    });
  }

  #[tokio::test]
  async fn test_encode_decode_response() {
    unittest_encode_decode_response!(BincodeWire {
      AppendEntries,
      Vote,
      InstallSnapshot,
      TimeoutNow,
      Heartbeat,
      Error
    });
  }
}
