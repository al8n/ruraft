use std::{io, marker::PhantomData};

use bytes::Bytes;
use futures_util::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use ruraft_core::{transport::*, Data};

/// Error type of [`RmpWire`].
#[derive(thiserror::Error, Debug)]
pub enum Error {
  /// Encode error
  #[error("encode error: {0}")]
  Encode(#[from] rmp_serde::encode::Error),
  /// Decode error
  #[error("decode error: {0}")]
  Decode(#[from] rmp_serde::decode::Error),
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

/// A [`Wire`] implementation based on [`rmp-serde`](rmp_serde).
#[repr(transparent)]
pub struct RmpWire<I, A, D>(PhantomData<(I, A, D)>);

impl<I, A, D> Wire for RmpWire<I, A, D>
where
  I: Id + serde::Serialize + for<'a> serde::Deserialize<'a>,
  A: Address + serde::Serialize + for<'a> serde::Deserialize<'a>,
  D: Data + serde::Serialize + for<'a> serde::Deserialize<'a>,
{
  type Error = Error;

  type Id = I;

  type Address = A;

  type Data = D;

  type Bytes = Bytes;

  fn encode_request(
    req: &Request<Self::Id, Self::Address, Self::Data>,
  ) -> Result<Self::Bytes, Self::Error> {
    rmp_serde::to_vec(req).map(Into::into).map_err(Into::into)
  }

  fn encode_response(resp: &Response<Self::Id, Self::Address>) -> Result<Self::Bytes, Self::Error> {
    rmp_serde::to_vec(resp).map(Into::into).map_err(Into::into)
  }

  async fn encode_request_to_writer(
    req: &Request<Self::Id, Self::Address, Self::Data>,
    mut writer: impl futures_util::AsyncWrite + Send + Unpin,
  ) -> std::io::Result<()> {
    writer
      .write_all(
        &rmp_serde::to_vec(req)
          .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?,
      )
      .await
  }

  async fn encode_response_to_writer(
    resp: &Response<Self::Id, Self::Address>,
    mut writer: impl futures_util::AsyncWrite + Send + Unpin,
  ) -> std::io::Result<()> {
    writer
      .write_all(
        &rmp_serde::to_vec(resp)
          .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?,
      )
      .await
  }

  fn decode_request(
    src: &[u8],
  ) -> Result<Request<Self::Id, Self::Address, Self::Data>, Self::Error> {
    rmp_serde::from_slice(src).map_err(Into::into)
  }

  fn decode_response(src: &[u8]) -> Result<Response<Self::Id, Self::Address>, Self::Error> {
    rmp_serde::from_slice(src).map_err(Into::into)
  }

  async fn decode_request_from_reader(
    mut reader: impl AsyncRead + Send + Unpin,
  ) -> std::io::Result<Request<Self::Id, Self::Address, Self::Data>> {
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await?;
    rmp_serde::from_slice(&buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
  }

  async fn decode_response_from_reader(
    mut reader: impl AsyncRead + Send + Unpin,
  ) -> std::io::Result<Response<Self::Id, Self::Address>> {
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await?;
    rmp_serde::from_slice(&buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
  }
}

#[cfg(test)]
mod tests {
  use super::RmpWire;

  #[tokio::test]
  async fn test_encode_decode_request() {
    unittest_encode_decode_request!(RmpWire {
      AppendEntries,
      Vote,
      InstallSnapshot,
      TimeoutNow,
      Heartbeat
    });
  }

  #[tokio::test]
  async fn test_encode_decode_response() {
    unittest_encode_decode_response!(RmpWire {
      AppendEntries,
      Vote,
      InstallSnapshot,
      TimeoutNow,
      Heartbeat,
      Error
    });
  }
}
