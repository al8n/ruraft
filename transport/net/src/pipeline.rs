use std::time::SystemTime;

use super::{stream::Connection, *};

struct Event {
  /// The term of the request
  term: u64,

  /// The highest log index of the [`AppendEntriesRequest`]'s entries
  highest_log_index: Option<u64>,

  /// The number of entries in the [`AppendEntriesRequest`]'s
  num_entries: usize,

  /// The time that the original request was started
  start: SystemTime,
}

/// [`AppendEntriesPipeline`] implementation for [`NetTransport`].
pub struct NetAppendEntriesPipeline<I, A, D, S, W>
where
  I: Id,
  A: AddressResolver,
  D: Data,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::Address, Data = D>,
{
  conn: <S::Stream as Connection>::OwnedWriteHalf,
  inprogress_tx: async_channel::Sender<Event>,
  finish_rx: async_channel::Receiver<
    Result<PipelineAppendEntriesResponse<I, A::Address>, super::Error<I, A, W>>,
  >,
  shutdown_rx: async_channel::Receiver<()>,
  shutdown_tx: async_channel::Sender<()>,
}

impl<I, A, D, S, W> Drop for NetAppendEntriesPipeline<I, A, D, S, W>
where
  I: Id,
  A: AddressResolver,
  D: Data,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::Address, Data = D>,
{
  fn drop(&mut self) {
    self.shutdown_tx.close();
  }
}

impl<I, A, D, S, W> NetAppendEntriesPipeline<I, A, D, S, W>
where
  I: Id,
  A: AddressResolver + Send + Sync + 'static,
  A::Error: Send + Sync + 'static,
  D: Data,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::Address, Data = D>,
{
  pub(super) fn new(conn: S::Stream, max_inflight: usize, timeout: Duration) -> Self {
    if max_inflight < super::MIN_IN_FLIGHT_FOR_PIPELINING {
      panic!("pipelining makes no sense if max_inflight < 2");
    }
    if timeout > Duration::ZERO {
      conn.set_timeout(Some(timeout));
    }
    let (shutdown_tx, shutdown_rx) = async_channel::bounded(1);
    let (inprogress_tx, inprogress_rx) = async_channel::bounded(max_inflight - 2);
    let (finish_tx, finish_rx) = async_channel::bounded(max_inflight - 2);

    let (reader, writer) = conn.into_split();

    let tshutdown_rx = shutdown_rx.clone();
    <A::Runtime as Runtime>::spawn_detach(async move {
      Self::decode_responses(reader, finish_tx, inprogress_rx, tshutdown_rx).await
    });

    Self {
      conn: writer,
      inprogress_tx,
      finish_rx,
      shutdown_tx,
      shutdown_rx,
    }
  }

  async fn decode_responses(
    mut conn: <S::Stream as Connection>::OwnedReadHalf,
    finish_tx: async_channel::Sender<
      Result<PipelineAppendEntriesResponse<I, A::Address>, super::Error<I, A, W>>,
    >,
    inprogress_rx: async_channel::Receiver<Event>,
    shutdown_rx: async_channel::Receiver<()>,
  ) {
    loop {
      futures::select! {
        ev = inprogress_rx.recv().fuse() => {
          // No need to handle error here, because
          // if we fail to receive a tx, it means
          // that the pipeline has been closed.
          if let Ok(ev) = ev {
            tracing::error!("DEBUG: recv event");
            let resp = W::decode_response_from_reader(&mut conn)
              .await
              .map_err(|e| Error::wire(W::Error::io(e)))
              .and_then(|resp|
              {
                match resp {
                  Response::AppendEntries(resp) => Ok(PipelineAppendEntriesResponse {
                    term: ev.term,
                    highest_log_index: ev.highest_log_index,
                    num_entries: ev.num_entries,
                    start: ev.start,
                    resp
                  }),
                  Response::Error(resp) => Err(Error::Remote(resp.error)),
                  resp => Err(Error::UnexpectedResponse {
                    expected: "AppendEntries",
                    actual: resp.description(),
                  }),
                }
              });

            futures::select! {
              _ = finish_tx.send(resp).fuse() => {
                tracing::error!("DEBUG: send response");
                // no need to handle send error here
                // because if we fail to send, it means that the pipeline has been closed.
              },
              _ = shutdown_rx.recv().fuse() => return,
            }
          }
        }
        _ = shutdown_rx.recv().fuse() => return,
      }
    }
  }
}

impl<I, A, D, S, W> AppendEntriesPipeline for NetAppendEntriesPipeline<I, A, D, S, W>
where
  I: Id,

  A: AddressResolver,

  <<<A as AddressResolver>::Runtime as Runtime>::Sleep as Future>::Output: Send,
  D: Data,
  S: StreamLayer,
  W: Wire<Id = I, Address = <A as AddressResolver>::Address, Data = D>,
{
  type Error = super::Error<I, A, W>;

  type Id = I;

  type Address = A::Address;

  type Data = D;

  fn consumer(
    &self,
  ) -> impl futures::Stream<
    Item = Result<PipelineAppendEntriesResponse<Self::Id, Self::Address>, Self::Error>,
  > + Send
       + 'static {
    self.finish_rx.clone()
  }

  async fn append_entries(
    &mut self,
    req: AppendEntriesRequest<Self::Id, Self::Address, Self::Data>,
  ) -> Result<(), Self::Error> {
    let start = SystemTime::now();
    let ev = Event {
      term: req.term,
      num_entries: req.entries().len(),
      highest_log_index: req.entries().last().map(|e| e.index()),
      start,
    };

    // Send the RPC
    tracing::error!("DEBUG: send");
    {
      W::encode_request_to_writer(&Request::AppendEntries(req), &mut self.conn)
        .await
        .map_err(|e| {
          tracing::error!("DEBUG: wire encode error: {:?}", e);
          Error::wire(W::Error::io(e))
        })?;
      tracing::error!("DEBUG: after encode");
      self.conn.flush().await.map_err(|e| {
        tracing::error!("DEBUG: wire error: {:?}", e);
        Error::wire(W::Error::io(e))
      })?;
      tracing::error!("DEBUG: after flush");
    }

    // Hand-off for decoding, this can also cause back-pressure
    // to prevent too many inflight requests
    futures::select! {
      rst = self.inprogress_tx.send(ev).fuse() => {
        if rst.is_err() {
          return Err(Error::PipelineShutdown);
        }

        Ok(())
      },
      _ = self.shutdown_rx.recv().fuse() => Err(Error::PipelineShutdown),
    }
  }

  async fn close(self) -> Result<(), Self::Error> {
    drop(self);
    Ok(())
  }
}
