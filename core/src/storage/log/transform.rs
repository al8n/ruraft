use byteorder::{ByteOrder, NetworkEndian};
use ruraft_utils::{decode_varint, encode_varint, encoded_len_varint};

use crate::MESSAGE_SIZE_LEN;

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
  #[error("{0}")]
  EncodeVarint(#[from] ruraft_utils::EncodeVarintError),
  #[error("{0}")]
  DecodeVarint(#[from] ruraft_utils::DecodeVarintError),
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
      Self::EncodeVarint(arg0) => f.debug_tuple("EncodeVarint").field(arg0).finish(),
      Self::DecodeVarint(arg0) => f.debug_tuple("DecodeVarint").field(arg0).finish(),
    }
  }
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

  fn encode(&self, dst: &mut [u8]) -> Result<usize, Self::Error> {
    let encoded_len = self.encoded_len();

    if dst.len() < encoded_len {
      return Err(LogTransformError::EncodeBufferTooSmall);
    }

    let mut cur = 0;
    NetworkEndian::write_u32(&mut dst[..MESSAGE_SIZE_LEN], encoded_len as u32);
    cur += MESSAGE_SIZE_LEN;
    dst[cur] = self.kind.tag();
    cur += 1;

    cur += encode_varint(self.index, &mut dst[cur..])?;
    cur += encode_varint(self.term, &mut dst[cur..])?;

    const ENCODED_SYSTEMTIME_LEN: usize = 12;

    match self.appended_at {
      None => {
        dst[cur..cur + ENCODED_SYSTEMTIME_LEN].copy_from_slice(&[0; ENCODED_SYSTEMTIME_LEN]);
        cur += ENCODED_SYSTEMTIME_LEN;
      }
      Some(instant) => {
        instant
          .encode(&mut dst[cur..cur + ENCODED_SYSTEMTIME_LEN])
          .unwrap();
        cur += ENCODED_SYSTEMTIME_LEN;
      }
    }

    match &self.kind {
      LogKind::Data(d) => {
        cur += d.encode(&mut dst[cur..]).map_err(LogTransformError::Data)?;
      }
      LogKind::Membership(m) => {
        cur += m
          .encode(&mut dst[cur..])
          .map_err(LogTransformError::Membership)?;
      }
      LogKind::Noop | LogKind::Barrier => {}
    }
    debug_assert_eq!(
      cur, encoded_len,
      "expected bytes wrote ({}) not match actual bytes wrote ({})",
      encoded_len, cur
    );
    Ok(cur)
  }

  fn encoded_len(&self) -> usize {
    MESSAGE_SIZE_LEN
      + 1
      + encoded_len_varint(self.index)
      + encoded_len_varint(self.term)
      + 12
      + match &self.kind {
        LogKind::Data(d) => d.encoded_len(),
        LogKind::Noop => 0,
        LogKind::Barrier => 0,
        LogKind::Membership(m) => m.encoded_len(),
      }
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    let src_len = src.len();
    if src_len < MESSAGE_SIZE_LEN {
      return Err(LogTransformError::Corrupted("corrupted log"));
    }

    let mut cur = 0;
    let encoded_len = NetworkEndian::read_u32(&src[..MESSAGE_SIZE_LEN]) as usize;
    cur += MESSAGE_SIZE_LEN;
    if src_len < encoded_len {
      return Err(LogTransformError::Corrupted("corrupted log"));
    }

    let tag = src[cur];
    cur += 1;

    let (readed, index) = decode_varint(&src[cur..])?;
    cur += readed;
    let (readed, term) = decode_varint(&src[cur..])?;
    cur += readed;

    let appended_at = &src[cur..cur + 12];
    let (readed, appended_at) = if appended_at == [0; 12] {
      (12, None)
    } else {
      let (readed, appended_at) = SystemTime::decode(appended_at).unwrap();
      (readed, Some(appended_at))
    };
    cur += readed;

    let log = match tag {
      0 => {
        let (readed, data) = D::decode(&src[cur..]).map_err(LogTransformError::Data)?;
        cur += readed;
        Log {
          kind: LogKind::Data(Arc::new(data)),
          index,
          term,
          appended_at,
        }
      }
      1 => Log {
        kind: LogKind::Noop,
        index,
        term,
        appended_at,
      },
      2 => Log {
        kind: LogKind::Barrier,
        index,
        term,
        appended_at,
      },
      3 => {
        let (readed, membership) =
          Membership::decode(&src[cur..]).map_err(LogTransformError::Membership)?;
        cur += readed;
        Log {
          kind: LogKind::Membership(membership),
          index,
          term,
          appended_at,
        }
      }
      _ => return Err(LogTransformError::UnknownLogKind(tag)),
    };
    debug_assert_eq!(
      cur, encoded_len,
      "expected bytes read ({}) not match actual bytes read ({})",
      encoded_len, cur
    );
    Ok((cur, log))
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
