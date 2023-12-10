use super::*;

#[derive(thiserror::Error)]
pub enum LogTransformError<I: Transformable, A: Transformable, D: Transformable> {
  #[error("{0}")]
  Id(I::Error),
  #[error("{0}")]
  Address(A::Error),
  #[error("{0}")]
  Data(D::Error),
  #[error("{0}")]
  Membership(crate::membership::MembershipTransformError<I, A>),
  #[error("dst buffer is too small")]
  EncodeBufferTooSmall,
  #[error("unknown log kind {0}")]
  UnknownLogKind(u8),
  #[error("{0}")]
  Corrupted(&'static str),
}

impl<I, A, D> core::fmt::Debug for LogTransformError<I, A, D>
where
  I: Id,
  A: Address,
  D: Transformable,
{
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::Id(arg0) => f.debug_tuple("Id").field(arg0).finish(),
      Self::Address(arg0) => f.debug_tuple("Address").field(arg0).finish(),
      Self::Data(arg0) => f.debug_tuple("Data").field(arg0).finish(),
      Self::Membership(arg0) => f.debug_tuple("Membership").field(arg0).finish(),
      Self::EncodeBufferTooSmall => f.debug_tuple("EncodeBufferTooSmall").finish(),
      Self::Corrupted(arg0) => f.debug_tuple("Corrupted").field(arg0).finish(),
      Self::UnknownLogKind(arg0) => f.debug_tuple("UnknownLogKind").field(arg0).finish(),
    }
  }
}

const LEN_SIZE: usize = mem::size_of::<u32>();
const LOG_HEADER_SIZE: usize = LEN_SIZE + 1 + 8 + 8 + 12;
// inlined max 64 bytes on stack when encoding/decoding
const INLINED: usize = 256;

#[derive(Debug, Copy, Clone)]
struct Header {
  encoded_len: usize,
  tag: u8,
  index: u64,
  term: u64,
  appended_at: Option<SystemTime>,
}

impl Header {
  fn from_bytes(b: [u8; LOG_HEADER_SIZE]) -> Self {
    let encoded_len = u32::from_be_bytes([b[0], b[1], b[2], b[3]]) as usize;
    let tag = b[4];
    let index = u64::from_be_bytes([b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12]]);
    let term = u64::from_be_bytes([b[13], b[14], b[15], b[16], b[17], b[18], b[19], b[20]]);
    let appended_at = &b[21..33];
    let appended_at = if appended_at == [0; 12] {
      None
    } else {
      Some(SystemTime::decode(appended_at).unwrap().1)
    };

    Self {
      encoded_len,
      tag,
      index,
      term,
      appended_at,
    }
  }
}

fn encode_header(header: Header, dst: &mut [u8]) {
  let Header {
    encoded_len,
    tag,
    index,
    term,
    appended_at,
  } = header;

  let mut cur = 0;
  dst[cur..cur + 4].copy_from_slice((encoded_len as u32).to_be_bytes().as_slice());
  cur += 4;
  dst[cur] = tag;
  cur += 1;
  dst[cur..cur + 8].copy_from_slice(index.to_be_bytes().as_slice());
  cur += 8;
  dst[cur..cur + 8].copy_from_slice(term.to_be_bytes().as_slice());
  cur += 8;

  const ENCODED_SYSTEMTIME_LEN: usize = 12;

  match appended_at {
    None => {
      dst[cur..cur + ENCODED_SYSTEMTIME_LEN].copy_from_slice(&[0; ENCODED_SYSTEMTIME_LEN]);
    }
    Some(instant) => {
      instant
        .encode(&mut dst[cur..cur + ENCODED_SYSTEMTIME_LEN])
        .unwrap();
    }
  }
}

fn decode_log_data<I, A, D>(
  index: u64,
  term: u64,
  appended_at: Option<SystemTime>,
  src: &[u8],
) -> Result<Log<I, A, D>, LogTransformError<I, A, D>>
where
  I: Transformable,
  A: Transformable,
  D: Transformable,
{
  let (_, data) = D::decode(src).map_err(|e| LogTransformError::Data(e))?;
  Ok(Log {
    kind: LogKind::Data(Arc::new(data)),
    index,
    term,
    appended_at,
  })
}

fn decode_log_membership<I, A, D>(
  index: u64,
  term: u64,
  appended_at: Option<SystemTime>,
  src: &[u8],
) -> Result<Log<I, A, D>, LogTransformError<I, A, D>>
where
  I: Id + Send + Sync + 'static,
  <I as Transformable>::Error: Send + Sync + 'static,
  A: Address + Send + Sync + 'static,
  <A as Transformable>::Error: Send + Sync + 'static,
  D: Transformable,
{
  let (_, data) = Membership::decode(src).map_err(|e| LogTransformError::Membership(e))?;
  Ok(Log {
    kind: LogKind::Membership(data),
    index,
    term,
    appended_at,
  })
}

// Log Binary Format
//
// --------------------------------------------------------------------------------------------------------
// | total_len (4 bytes) | kind (1 byte) | index (8 bytes) | term (8 bytes) | append_at (8 bytes) | data |
// --------------------------------------------------------------------------------------------------------
impl<I, A, D> Transformable for Log<I, A, D>
where
  I: Id + Send + Sync + 'static,
  <I as Transformable>::Error: Send + Sync + 'static,
  A: Address + Send + Sync + 'static,
  <A as Transformable>::Error: Send + Sync + 'static,
  D: Data,
  <D as Transformable>::Error: Send + Sync + 'static,
{
  type Error = LogTransformError<I, A, D>;

  fn encode(&self, dst: &mut [u8]) -> Result<(), Self::Error> {
    let encoded_len = self.encoded_len();

    if dst.len() < encoded_len {
      return Err(LogTransformError::EncodeBufferTooSmall);
    }

    let tag = self.kind.tag();
    let header = Header {
      encoded_len,
      tag,
      index: self.index,
      term: self.term,
      appended_at: self.appended_at,
    };
    match &self.kind {
      LogKind::Data(d) => {
        encode_header(header, dst);
        let data_encoded_len = d.encoded_len();
        d.encode(&mut dst[LOG_HEADER_SIZE..LOG_HEADER_SIZE + data_encoded_len])
          .map_err(LogTransformError::Data)
      }
      LogKind::Noop => {
        encode_header(header, dst);
        Ok(())
      }
      LogKind::Barrier => {
        encode_header(header, dst);
        Ok(())
      }
      LogKind::Membership(m) => {
        encode_header(header, dst);
        let membership_encoded_len = m.encoded_len();
        m.encode(&mut dst[LOG_HEADER_SIZE..LOG_HEADER_SIZE + membership_encoded_len])
          .map_err(LogTransformError::Membership)
      }
    }
  }

  fn encode_to_writer<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
    let encoded_len = self.encoded_len();
    if encoded_len <= INLINED {
      let mut buf = [0; INLINED];
      self.encode(&mut buf).map_err(invalid_data)?;
      writer.write_all(&buf[..encoded_len])
    } else {
      let mut buf = vec![0; encoded_len];
      self.encode(&mut buf).map_err(invalid_data)?;
      writer.write_all(&buf[..])
    }
  }

  async fn encode_to_async_writer<W: AsyncWrite + Send + Unpin>(
    &self,
    writer: &mut W,
  ) -> io::Result<()>
  where
    Self::Error: Send + Sync + 'static,
  {
    use futures::AsyncWriteExt;

    let encoded_len = self.encoded_len();
    if encoded_len <= INLINED {
      let mut buf = [0; INLINED];
      self.encode(&mut buf).map_err(invalid_data)?;
      writer.write_all(&buf[..encoded_len]).await
    } else {
      let mut buf = vec![0; encoded_len];
      self.encode(&mut buf).map_err(invalid_data)?;
      writer.write_all(&buf[..]).await
    }
  }

  fn encoded_len(&self) -> usize {
    match &self.kind {
      LogKind::Data(d) => LOG_HEADER_SIZE + d.encoded_len(),
      LogKind::Noop => LOG_HEADER_SIZE,
      LogKind::Barrier => LOG_HEADER_SIZE,
      LogKind::Membership(m) => LOG_HEADER_SIZE + m.encoded_len(),
    }
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    if src.len() < LOG_HEADER_SIZE {
      return Err(LogTransformError::Corrupted("corrupted log"));
    }

    let mut header = [0; LOG_HEADER_SIZE];
    header.copy_from_slice(&src[..LOG_HEADER_SIZE]);
    let Header {
      encoded_len,
      tag,
      index,
      term,
      appended_at,
    } = Header::from_bytes(header);

    if src.len() < encoded_len {
      return Err(LogTransformError::Corrupted("corrupted log"));
    }

    match tag {
      0 => decode_log_data(index, term, appended_at, &src[LOG_HEADER_SIZE..encoded_len])
        .map(|l| (encoded_len, l)),
      1 => Ok((
        encoded_len,
        Log {
          kind: LogKind::Noop,
          index,
          term,
          appended_at,
        },
      )),
      2 => Ok((
        encoded_len,
        Log {
          kind: LogKind::Barrier,
          index,
          term,
          appended_at,
        },
      )),
      3 => decode_log_membership(index, term, appended_at, &src[LOG_HEADER_SIZE..encoded_len])
        .map(|l| (encoded_len, l)),
      _ => Err(LogTransformError::UnknownLogKind(tag)),
    }
  }

  fn decode_from_reader<R: io::Read>(reader: &mut R) -> io::Result<(usize, Self)>
  where
    Self: Sized,
  {
    let mut buf = [0; LOG_HEADER_SIZE];
    reader.read_exact(&mut buf)?;
    let Header {
      encoded_len: total_len,
      tag,
      index,
      term,
      appended_at,
    } = Header::from_bytes(buf);
    let remaining = total_len - LOG_HEADER_SIZE;
    match tag {
      0 => {
        if remaining <= INLINED {
          let mut dst = [0; INLINED];
          reader.read_exact(&mut dst[..remaining])?;
          decode_log_data(index, term, appended_at, &dst[..remaining])
            .map(|l| (total_len, l))
            .map_err(invalid_data)
        } else {
          let mut dst = vec![0; remaining];
          reader.read_exact(&mut dst)?;
          decode_log_data(index, term, appended_at, &dst)
            .map(|l| (total_len, l))
            .map_err(invalid_data)
        }
      }
      1 => Ok((
        total_len,
        Log {
          kind: LogKind::Noop,
          index,
          term,
          appended_at,
        },
      )),
      2 => Ok((
        total_len,
        Log {
          kind: LogKind::Barrier,
          index,
          term,
          appended_at,
        },
      )),
      3 => {
        if remaining <= INLINED {
          let mut dst = [0; INLINED];
          reader.read_exact(&mut dst[..remaining])?;
          decode_log_membership(index, term, appended_at, &dst[..remaining])
            .map(|l| (total_len, l))
            .map_err(invalid_data)
        } else {
          let mut dst = vec![0; remaining];
          reader.read_exact(&mut dst)?;
          decode_log_membership(index, term, appended_at, &dst)
            .map(|l| (total_len, l))
            .map_err(invalid_data)
        }
      }
      _ => Err(invalid_data(LogTransformError::<I, A, D>::UnknownLogKind(
        tag,
      ))),
    }
  }

  async fn decode_from_async_reader<R: futures::io::AsyncRead + Send + Unpin>(
    reader: &mut R,
  ) -> io::Result<(usize, Self)>
  where
    Self: Sized,
    Self::Error: Send + Sync + 'static,
  {
    use futures::AsyncReadExt;

    let mut buf = [0; LOG_HEADER_SIZE];
    reader.read_exact(&mut buf).await?;
    let Header {
      encoded_len: total_len,
      tag,
      index,
      term,
      appended_at,
    } = Header::from_bytes(buf);

    let remaining = total_len - LOG_HEADER_SIZE;
    match tag {
      0 => {
        if remaining <= INLINED {
          let mut dst = [0; INLINED];
          reader.read_exact(&mut dst[..remaining]).await?;
          decode_log_data(index, term, appended_at, &dst[..remaining])
            .map(|l| (total_len, l))
            .map_err(invalid_data)
        } else {
          let mut dst = vec![0; remaining];
          reader.read_exact(&mut dst).await?;
          decode_log_data(index, term, appended_at, &dst)
            .map(|l| (total_len, l))
            .map_err(invalid_data)
        }
      }
      1 => Ok((
        total_len,
        Log {
          kind: LogKind::Noop,
          index,
          term,
          appended_at,
        },
      )),
      2 => Ok((
        total_len,
        Log {
          kind: LogKind::Barrier,
          index,
          term,
          appended_at,
        },
      )),
      3 => {
        if remaining <= INLINED {
          let mut dst = [0; INLINED];
          reader.read_exact(&mut dst[..remaining]).await?;
          decode_log_membership(index, term, appended_at, &dst[..remaining])
            .map(|l| (total_len, l))
            .map_err(invalid_data)
        } else {
          let mut dst = vec![0; remaining];
          reader.read_exact(&mut dst).await?;
          decode_log_membership(index, term, appended_at, &dst)
            .map(|l| (total_len, l))
            .map_err(invalid_data)
        }
      }
      _ => Err(invalid_data(LogTransformError::UnknownLogKind(tag))),
    }
  }
}

#[cfg(test)]
mod tests {
  use std::net::SocketAddr;

  use crate::membership::sample_membership;

  use super::*;
  use nodecraft::{NodeAddress, NodeId};
  use smol_str::SmolStr;

  async fn test_log_transformable_in<I, A, D>(log: Log<I, A, D>)
  where
    I: Id + Send + Sync + 'static,
    <I as Transformable>::Error: Send + Sync + 'static,
    A: Address + Send + Sync + 'static,
    <A as Transformable>::Error: Send + Sync + 'static,
    D: Data + PartialEq + core::fmt::Debug,
    <D as Transformable>::Error: Send + Sync + 'static,
  {
    let mut buf = vec![0; log.encoded_len()];
    log.encode(&mut buf).unwrap();

    let (_, decoded) = Log::<I, A, D>::decode(&buf).unwrap();
    assert_eq!(log, decoded);

    let mut buf = Vec::with_capacity(log.encoded_len());
    log.encode_to_writer(&mut buf).unwrap();

    let (_, decoded) = Log::<I, A, D>::decode_from_reader(&mut std::io::Cursor::new(&buf)).unwrap();
    assert_eq!(log, decoded);

    let mut buf = Vec::with_capacity(log.encoded_len());
    log.encode_to_async_writer(&mut buf).await.unwrap();

    let (_, decoded) = Log::<I, A, D>::decode_from_async_reader(&mut buf.as_slice())
      .await
      .unwrap();
    assert_eq!(log, decoded);
  }

  #[tokio::test]
  async fn test_log_transformable() {
    let log: Log<NodeId, NodeAddress, Vec<u8>> = Log {
      kind: LogKind::Data(Arc::new(vec![1, 2, 3])),
      index: 1,
      term: 1,
      appended_at: None,
    };

    test_log_transformable_in(log).await;

    let log: Log<NodeId, NodeAddress, Vec<u8>> = Log {
      kind: LogKind::Data(Arc::new((0..=u8::MAX).collect())),
      index: 1,
      term: 1,
      appended_at: Some(SystemTime::now()),
    };

    test_log_transformable_in(log).await;

    let log: Log<NodeId, NodeAddress, Vec<u8>> = Log {
      kind: LogKind::Noop,
      index: 1,
      term: 1,
      appended_at: None,
    };

    test_log_transformable_in(log).await;

    let log: Log<NodeId, NodeAddress, Vec<u8>> = Log {
      kind: LogKind::Noop,
      index: 1,
      term: 1,
      appended_at: Some(SystemTime::now()),
    };

    test_log_transformable_in(log).await;

    let log: Log<NodeId, NodeAddress, Vec<u8>> = Log {
      kind: LogKind::Barrier,
      index: 1,
      term: 1,
      appended_at: None,
    };

    test_log_transformable_in(log).await;

    let log: Log<NodeId, NodeAddress, Vec<u8>> = Log {
      kind: LogKind::Barrier,
      index: 1,
      term: 1,
      appended_at: Some(SystemTime::now()),
    };

    test_log_transformable_in(log).await;

    let log: Log<SmolStr, SocketAddr, Membership<SmolStr, SocketAddr>> = Log {
      kind: LogKind::Membership(sample_membership()),
      index: 1,
      term: 1,
      appended_at: None,
    };
    test_log_transformable_in(log).await;

    let log: Log<SmolStr, SocketAddr, Membership<SmolStr, SocketAddr>> = Log {
      kind: LogKind::Membership(sample_membership()),
      index: 1,
      term: 1,
      appended_at: Some(SystemTime::now()),
    };

    test_log_transformable_in(log).await;
  }
}
