use std::{
  pin::Pin,
  task::{Context, Poll},
};

use futures::channel::oneshot;

use super::*;

pub struct NetAppendEntriesPipeline<I, A, D, S, W>
where
  I: Id + Send + Sync + 'static,
  I::Error: Send + Sync + 'static,
  A: AddressResolver + Send + Sync + 'static,
  A::Address: Send + Sync + 'static,
  A::Error: Send + Sync + 'static,
  D: Data,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::Address, Data = D>,
{
  conn: S::Stream,
  max_inflight: usize,
  target: Node<I, A::Address>,
  inprogress_tx: async_channel::Sender<
    oneshot::Sender<Result<PipelineAppendEntriesResponse<I, A::Address>, super::Error<I, A, W>>>,
  >,
  finish_rx: async_channel::Receiver<
    Result<PipelineAppendEntriesResponse<I, A::Address>, super::Error<I, A, W>>,
  >,
  shutdown: AtomicBool,
  shutdown_tx: async_channel::Sender<()>,
}

impl<I, A, D, S, W> NetAppendEntriesPipeline<I, A, D, S, W>
where
  I: Id + Send + Sync + 'static,
  I::Error: Send + Sync + 'static,
  A: AddressResolver + Send + Sync + 'static,
  A::Address: Send + Sync + 'static,
  A::Error: Send + Sync + 'static,
  D: Data,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::Address, Data = D>,
{
  pub(super) fn new(target: Node<I, A::Address>, conn: S::Stream, max_inflight: usize) -> Self {
    if max_inflight < super::MIN_IN_FLIGHT_FOR_PIPELINING {
      panic!("pipelining makes no sense if max_inflight < 2");
    }
    let (shutdown_tx, _shutdown_rx) = async_channel::bounded(1);
    let (inprogress_tx, _inprogress_rx) = async_channel::bounded(max_inflight - 2);
    let (_finish_tx, finish_rx) = async_channel::bounded(max_inflight - 2);

    Self {
      target,
      conn,
      max_inflight,
      inprogress_tx,
      finish_rx,
      shutdown: AtomicBool::new(false),
      shutdown_tx,
    }
  }
}

impl<I, A, D, S, W> AppendEntriesPipeline for NetAppendEntriesPipeline<I, A, D, S, W>
where
  I: Id + Send + Sync + 'static,
  <I as Transformable>::Error: Send + Sync + 'static,
  A: AddressResolver,
  A::Address: Send + Sync + 'static,
  <<A as AddressResolver>::Address as Transformable>::Error: Send + Sync + 'static,
  <<<A as AddressResolver>::Runtime as Runtime>::Sleep as Future>::Output: Send,
  D: Data,
  S: StreamLayer,
  W: Wire<Id = I, Address = <A as AddressResolver>::Address, Data = D>,
{
  type Error = super::Error<I, A, W>;

  type Id = I;

  type Address = A::Address;

  type Data = D;

  type Response = NetAppendEntriesPipelineFuture<Self::Id, A, Self::Data, S, W>;

  fn consumer(
    &self,
  ) -> impl futures::Stream<
    Item = Result<PipelineAppendEntriesResponse<Self::Id, Self::Address>, Self::Error>,
  > + Send
       + 'static {
    self.finish_rx.clone()
  }

  async fn append_entries(
    &self,
    _req: AppendEntriesRequest<Self::Id, Self::Address, Self::Data>,
  ) -> Result<Self::Response, Self::Error> {
    todo!()
  }

  async fn close(self) -> Result<(), Self::Error> {
    todo!()
  }
}

#[pin_project::pin_project]
pub struct NetAppendEntriesPipelineFuture<I, A, D, S, W>
where
  I: Id + Send + Sync + 'static,
  A: AddressResolver,
  D: Data,
  S: StreamLayer,
  W: Wire<Id = I, Address = <A as AddressResolver>::Address, Data = D>,
{
  start: Instant,
  #[pin]
  rx:
    oneshot::Receiver<Result<PipelineAppendEntriesResponse<I, A::Address>, super::Error<I, A, W>>>,
  _marker: std::marker::PhantomData<S>,
}

impl<I, A, D, S, W> Future for NetAppendEntriesPipelineFuture<I, A, D, S, W>
where
  I: Id + Send + Sync + 'static,
  A: AddressResolver,
  D: Data,
  S: StreamLayer,
  W: Wire<Id = I, Address = <A as AddressResolver>::Address, Data = D>,
{
  type Output = Result<PipelineAppendEntriesResponse<I, A::Address>, super::Error<I, A, W>>;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    match self.project().rx.poll(cx) {
      Poll::Ready(Ok(resp)) => Poll::Ready(resp),
      Poll::Ready(Err(_)) => Poll::Ready(Err(super::Error::PipelingClosed)),
      Poll::Pending => Poll::Pending,
    }
  }
}

impl<I, A, D, S, W> AppendEntriesPipelineFuture for NetAppendEntriesPipelineFuture<I, A, D, S, W>
where
  I: Id + Send + Sync + 'static,
  <I as Transformable>::Error: Send + Sync + 'static,
  A: AddressResolver,
  A::Address: Send + Sync + 'static,
  <<A as AddressResolver>::Address as Transformable>::Error: Send + Sync + 'static,
  <<<A as AddressResolver>::Runtime as Runtime>::Sleep as Future>::Output: Send,
  D: Data,
  S: StreamLayer,
  W: Wire<Id = I, Address = <A as AddressResolver>::Address, Data = D>,
{
  type Id = I;

  type Address = A::Address;

  type Pipeline = NetAppendEntriesPipeline<I, A, D, S, W>;

  fn start(&self) -> std::time::Instant {
    self.start
  }
}
