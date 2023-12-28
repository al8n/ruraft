use core::panic;
use std::{convert::Infallible, marker::PhantomData};

use futures::StreamExt;
use ruraft_core::{
  storage::{Log, LogKind},
  transport::{
    tests::{__make_append_req, __make_append_resp},
    *,
  },
};

use super::*;

struct TestAddressResolver<A: Address, R: Runtime>(PhantomData<(A, R)>);

impl<A: Address, R: Runtime> TestAddressResolver<A, R> {
  fn new() -> Self {
    Self(PhantomData)
  }
}

impl<A: Address, R: Runtime> AddressResolver for TestAddressResolver<A, R> {
  type Address = A;

  type ResolvedAddress = A;

  type Error = Infallible;

  type Runtime = R;

  async fn resolve(&self, address: &Self::Address) -> Result<Self::ResolvedAddress, Self::Error> {
    Ok(address.clone())
  }
}

/// Test [`NetTransport`]'s close_streams.
pub async fn close_streams<
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  D: Data,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::Address, Data = D>,
>(
  header1: Header<I, A::Address>,
  stream_layer1: S,
  resolver1: A,
  header2: Header<I, A::Address>,
  stream_layer2: S,
  resolver2: A,
) where
  <<A::Runtime as Runtime>::Sleep as Future>::Output: Send + 'static,
{
  // Transport 1 is consumer
  let trans1 = NetTransport::<I, A, D, S, W>::new(
    resolver1,
    stream_layer1,
    NetTransportOptions::new(header1)
      .with_max_pool(2)
      .with_timeout(Duration::from_secs(1)),
  )
  .await
  .expect("failed to create transport");
  let trans2 = Arc::new(
    NetTransport::<I, A, D, S, W>::new(
      resolver2,
      stream_layer2,
      NetTransportOptions::new(header2)
        .with_max_pool(3)
        .with_timeout(Duration::from_secs(3)),
    )
    .await
    .expect("failed to create transport"),
  );

  let trans1_consumer = trans1.consumer();
  let trans1_header = trans1.header().clone();

  // Make the RPC request
  let args = AppendEntriesRequest {
    term: 10,
    prev_log_entry: 100,
    prev_log_term: 4,
    entries: vec![Log::__crate_new(101, 4, LogKind::Noop)],
    leader_commit: 90,
    header: trans1.header().clone(),
  };

  let resp = AppendEntriesResponse {
    header: trans2.header().clone(),
    term: 4,
    last_log: 90,
    success: true,
    no_retry_backoff: false,
  };
  let resp1 = resp.clone();

  // errCh is used to report errors from any of the goroutines
  // created in this test.
  // It is buffered as to not block.
  let (err_tx, err_rx) = async_channel::bounded(100);

  // Listen for a request
  <A::Runtime as Runtime>::spawn_detach(async move {
    futures::pin_mut!(trans1_consumer);

    loop {
      futures::select! {
        req = trans1_consumer.next().fuse() => {
          let req = req.unwrap();
          let Ok(_) = req.respond(Response::append_entries(resp1.clone())) else {
            panic!("unexpected respond fail");
          };
        },
        _ = <A::Runtime as Runtime>::sleep(Duration::from_millis(200)).fuse() => {
          panic!("timeout");
        },
      }
    }
  });

  for i in 0..2 {
    // Create wait group
    let wg = AsyncWaitGroup::new();

    // Try to do parallel appends, should stress the conn pool
    for _ in 0..5 {
      let new_wg = wg.add(1);
      let trans = trans2.clone();
      let err_tx = err_tx.clone();
      let expected_resp = resp.clone();
      let target = trans1_header.from().clone();
      let req = args.clone();
      <A::Runtime as Runtime>::spawn_detach(async move {
        scopeguard::defer!(new_wg.done());

        match trans.append_entries(&target, req).await {
          Ok(res) => {
            // Verify the response
            assert_eq!(res, expected_resp);
          }
          Err(e) => {
            err_tx.send(e).await.expect("failed to send error");
          }
        }
      });
    }

    wg.wait().await;

    // Check if we received any errors from the above goroutines.
    if !err_rx.is_empty() {
      panic!("unexpected error");
    }

    // Check the conn pool size
    {
      let pool = trans2.conn_pool.lock().await;
      let conns = pool.get(trans1.local_addr()).expect("no conns in the pool");
      assert_eq!(conns.len(), 3, "Expected 3 pooled conns!");
    }

    if i == 0 {
      trans2.close_streams().await;
      assert!(
        trans2
          .conn_pool
          .lock()
          .await
          .get(trans1.local_addr())
          .is_none(),
        "Expected no pooled conns after closing streams!"
      );
    }
  }
}

/// Test [`NetTransport::new`] and [`NetTransport::shutdown`](Transport::shutdown) implementation.
pub async fn start_and_shutdown<S: StreamLayer, R: Runtime>(s: S)
where
  <R::Sleep as Future>::Output: Send + 'static,
{
  let addr = "127.0.0.1:8080".parse().unwrap();
  let trans = NetTransport::<_, _, Vec<u8>, _, ruraft_wire::LpeWire<_, _, _>>::new(
    TestAddressResolver::<_, R>::new(),
    s,
    NetTransportOptions::new(Header::new(
      ProtocolVersion::V1,
      smol_str::SmolStr::from("test-net-transport-new"),
      addr,
    )),
  )
  .await
  .unwrap();

  trans.shutdown().await.unwrap();
}

/// Test [`NetTransport::set_heartbeat_handler`](Transport::set_heartbeat_handler) implementation.
pub async fn heartbeat_fastpath<S: StreamLayer, W: Wire, R: Runtime>(_s1: S, _s2: S) {
  unimplemented!()
}

/// Test [`NetTransport::append_entries`](Transport::append_entries) implementation.
pub async fn append_entries<
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  D: Data,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::Address, Data = D>,
>(
  header1: Header<I, A::Address>,
  stream_layer1: S,
  resolver1: A,
  header2: Header<I, A::Address>,
  stream_layer2: S,
  resolver2: A,
) where
  <<A::Runtime as Runtime>::Sleep as Future>::Output: Send + 'static,
{
  let trans1 =
    NetTransport::<_, _, D, _, W>::new(resolver1, stream_layer1, NetTransportOptions::new(header1))
      .await
      .unwrap();
  let trans2 = NetTransport::new(resolver2, stream_layer2, NetTransportOptions::new(header2))
    .await
    .unwrap();

  ruraft_core::tests::transport::append_entries(trans1, trans2).await;
}

/// Test [`NetTransport::append_entries_pipeline`](Transport::append_entries_pipeline).
pub async fn append_entries_pipeline<
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  D: Data,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::Address, Data = D>,
>(
  header1: Header<I, A::Address>,
  stream_layer1: S,
  resolver1: A,
  header2: Header<I, A::Address>,
  stream_layer2: S,
  resolver2: A,
) where
  D: core::fmt::Debug + PartialEq,
  <<A::Runtime as Runtime>::Sleep as Future>::Output: Send + 'static,
{
  let trans1 =
    NetTransport::<_, _, D, _, W>::new(resolver1, stream_layer1, NetTransportOptions::new(header1))
      .await
      .unwrap();
  let trans2 = NetTransport::new(resolver2, stream_layer2, NetTransportOptions::new(header2))
    .await
    .unwrap();

  ruraft_core::tests::transport::append_entries_pipeline(trans1, trans2).await;
}

/// Test [`NetTransport::append_entries_pipeline`](Transport::append_entries_pipeline) and [`NetTransport::close_streams`].
pub async fn append_entries_pipeline_close_streams<
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  D: Data + PartialEq + core::fmt::Debug,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::Address, Data = D>,
>(
  header1: Header<I, A::Address>,
  stream_layer1: S,
  resolver1: A,
  header2: Header<I, A::Address>,
  stream_layer2: S,
  resolver2: A,
) where
  <<A::Runtime as Runtime>::Sleep as Future>::Output: Send + 'static,
{
  // Transport 1 is consumer
  let trans1 =
    NetTransport::<_, _, D, _, W>::new(resolver1, stream_layer1, NetTransportOptions::new(header1))
      .await
      .unwrap();
  let trans2 =
    NetTransport::<_, _, D, _, W>::new(resolver2, stream_layer2, NetTransportOptions::new(header2))
      .await
      .unwrap();

  let trans1_consumer = trans1.consumer();
  let trans1_header = trans1.header().clone();

  // Make the RPC request
  let args = __make_append_req(trans1.local_id().clone(), trans1.local_addr().clone());
  let args1 = args.clone();
  let resp = __make_append_resp(trans1.local_id().clone(), trans1.local_addr().clone());
  let resp1 = resp.clone();

  let (shutdown_tx, shutdown_rx) = async_channel::bounded::<()>(1);
  scopeguard::defer!(let _ = shutdown_tx.close(););

  // Listen for a request
  <A::Runtime as Runtime>::spawn_detach(async move {
    futures::pin_mut!(trans1_consumer);

    loop {
      futures::select! {
        req = trans1_consumer.next().fuse() => {
          let req = req.unwrap();
          if let Request::AppendEntries(req) = req.request() {
            assert_eq!(req, &args1, "request mismatch");
          } else {
            panic!("unexpected request");
          }

          let Ok(_) = req.respond(Response::append_entries(resp1.clone())) else {
            panic!("unexpected respond fail");
          };
        },
        _ = shutdown_rx.recv().fuse() => {
          break;
        },
      }
    }
  });

  for cancel_stream in [true, false] {
    let mut pipeline = trans2
      .append_entries_pipeline(trans1_header.from().clone())
      .await
      .expect("failed to create pipeline");

    for i in 0..100 {
      // On the last one, close the streams on the transport one.
      if cancel_stream && i == 10 {
        trans1.close_streams().await;
        <A::Runtime as Runtime>::sleep(Duration::from_millis(10)).await;
      }

      if pipeline.append_entries(args.clone()).await.is_err() {
        break;
      }
    }

    let consumer = pipeline.consumer();
    let mut future_err = None;
    futures::pin_mut!(consumer);
    for _ in 0..100 {
      futures::select! {
        res = consumer.next().fuse() => {
          let res = res.unwrap();
          match res {
            Ok(res) => {
              // Verify the response
              assert_eq!(res.response(), &resp, "response mismatch");
            },
            Err(e) => {
              future_err = Some(e);
              break;
            }
          }
        },
        _ = <A::Runtime as Runtime>::sleep(Duration::from_millis(200)).fuse() => {
          panic!("timeout when cancel streams is {}", cancel_stream);
        },
      }
    }

    if cancel_stream && future_err.is_none() {
      panic!("expected an error due to the streams being closed");
    } else if !cancel_stream && future_err.is_some() {
      panic!("unexpected error: {}", future_err.unwrap());
    }

    pipeline.close().await.expect("failed to close pipeline");
  }
}

/// Test [`NetTransport::append_entries_pipeline`](Transport::append_entries_pipeline) and [`NetTransport`]'s max rpc inflight special case.
pub async fn append_entries_pipeline_max_rpc_inflight_default<
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  D: Data,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::Address, Data = D>,
>(
  header1: Header<I, A::Address>,
  stream_layer1: S,
  resolver1: A,
  header2: Header<I, A::Address>,
  stream_layer2: S,
  resolver2: A,
) where
  <<A::Runtime as Runtime>::Sleep as Future>::Output: Send + 'static,
{
  append_entries_pipeline_max_rpc_inflight_runner::<I, A, D, S, W>(
    header1,
    stream_layer1,
    resolver1,
    header2,
    stream_layer2,
    resolver2,
    DEFAULT_MAX_INFLIGHT_REQUESTS,
  )
  .await;
}

/// Test [`NetTransport::append_entries_pipeline`](Transport::append_entries_pipeline) and [`NetTransport`]'s max rpc inflight special case.
pub async fn append_entries_pipeline_max_rpc_inflight_0<
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  D: Data,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::Address, Data = D>,
>(
  header1: Header<I, A::Address>,
  stream_layer1: S,
  resolver1: A,
  header2: Header<I, A::Address>,
  stream_layer2: S,
  resolver2: A,
) where
  <<A::Runtime as Runtime>::Sleep as Future>::Output: Send + 'static,
{
  append_entries_pipeline_max_rpc_inflight_runner::<I, A, D, S, W>(
    header1,
    stream_layer1,
    resolver1,
    header2,
    stream_layer2,
    resolver2,
    0,
  )
  .await;
}

/// Test [`NetTransport::append_entries_pipeline`](Transport::append_entries_pipeline) and [`NetTransport`]'s max rpc inflight special case.
pub async fn append_entries_pipeline_max_rpc_inflight_one<
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  D: Data,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::Address, Data = D>,
>(
  header1: Header<I, A::Address>,
  stream_layer1: S,
  resolver1: A,
  header2: Header<I, A::Address>,
  stream_layer2: S,
  resolver2: A,
) where
  <<A::Runtime as Runtime>::Sleep as Future>::Output: Send + 'static,
{
  append_entries_pipeline_max_rpc_inflight_runner::<I, A, D, S, W>(
    header1,
    stream_layer1,
    resolver1,
    header2,
    stream_layer2,
    resolver2,
    1,
  )
  .await;
}

/// Test [`NetTransport::append_entries_pipeline`](Transport::append_entries_pipeline) and [`NetTransport`]'s max rpc inflight special case.
pub async fn append_entries_pipeline_max_rpc_inflight_some<
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  D: Data,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::Address, Data = D>,
>(
  header1: Header<I, A::Address>,
  stream_layer1: S,
  resolver1: A,
  header2: Header<I, A::Address>,
  stream_layer2: S,
  resolver2: A,
) where
  <<A::Runtime as Runtime>::Sleep as Future>::Output: Send + 'static,
{
  append_entries_pipeline_max_rpc_inflight_runner::<I, A, D, S, W>(
    header1,
    stream_layer1,
    resolver1,
    header2,
    stream_layer2,
    resolver2,
    10,
  )
  .await;
}

async fn append_entries_pipeline_max_rpc_inflight_runner<
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  D: Data,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::Address, Data = D>,
>(
  header1: Header<I, A::Address>,
  stream_layer1: S,
  resolver1: A,
  header2: Header<I, A::Address>,
  stream_layer2: S,
  resolver2: A,
  max: usize,
) where
  <<A::Runtime as Runtime>::Sleep as Future>::Output: Send + 'static,
{
  let config1 = NetTransportOptions::new(header1)
    .with_max_pool(2)
    .with_max_inflight_requests(max)
    .with_timeout(Duration::from_secs(1));

  // Transport 1 is consumer
  let trans1 = Arc::new(
    NetTransport::<_, _, D, _, W>::new(resolver1, stream_layer1, config1)
      .await
      .expect("failed to create transport"),
  );

  // Make the RPC request
  let args = __make_append_req(trans1.local_id().clone(), trans1.local_addr().clone());
  let resp = __make_append_resp(trans1.local_id().clone(), trans1.local_addr().clone());
  let resp1 = resp.clone();

  // Transport 2 makes outbound request
  let config2 = NetTransportOptions::new(header2)
    .with_max_pool(2)
    .with_max_inflight_requests(max)
    .with_timeout(Duration::from_secs(1));

  // Transport 1 is consumer
  let trans2 = Arc::new(
    NetTransport::<_, _, D, _, W>::new(resolver2, stream_layer2, config2)
      .await
      .expect("failed to create transport"),
  );

  // Kill the transports on the timeout to unblock. That means things that
  // shouldn't have blocked did block.
  let t1 = trans1.clone();
  let t2 = trans2.clone();
  <A::Runtime as Runtime>::spawn_detach(async move {
    <A::Runtime as Runtime>::sleep(Duration::from_secs(5)).await;
    t1.shutdown().await.unwrap();
    t2.shutdown().await.unwrap();
  });

  // Attempt to pipeline
  let pipeline = trans2
    .append_entries_pipeline(trans1.header().from().clone())
    .await;

  if max == 1 {
    // Max == 1 implies no pipelining
    assert!(matches!(
      pipeline,
      Err(Error::PipelineReplicationNotSupported(_))
    ));
    return;
  }

  let mut pipeline = pipeline.expect("failed to create pipeline");

  let mut expected_max = max;
  if max == 0 {
    // Should have defaulted to 2
    expected_max = 3;
  }

  for _ in 0..expected_max - 1 {
    // We should be able to send `max - 1` rpcs before `AppendEntries`
    // blocks. It blocks on the `max` one because it it sends before pushing
    // to the chan. It will block forever when it does because nothing is
    // responding yet.
    pipeline.append_entries(args.clone()).await.unwrap();
  }

  let pc = pipeline.consumer();
  futures::pin_mut!(pc);
  // Verify the next send blocks without blocking test forever
  let (err_tx, err_rx) = async_channel::bounded(1);
  <A::Runtime as Runtime>::spawn_detach(async move {
    if let Err(e) = pipeline.append_entries(args.clone()).await {
      err_tx.send(e).await.expect("failed to send error");
    }
  });

  futures::select! {
    res = err_rx.recv().fuse() => {
      if res.is_ok() {
        panic!("unexpected error");
      }
    },
    _ = <A::Runtime as Runtime>::sleep(Duration::from_millis(50)).fuse() => {
      // OK it's probably blocked or we got _really_ unlucky with scheduling!
    },
  }

  // Verify that once we receive/respond another one can be sent.
  let rpc = trans1.consumer().recv().await.unwrap();
  rpc
    .respond(Response::append_entries(resp1.clone()))
    .unwrap();

  // We also need to consume the response from the pipeline in case chan is
  // unbuffered (inflight is 2 or 1)
  pc.next().await;

  // The last append should unblock once the response is received.
  futures::select! {
    _ = err_rx.recv().fuse() => {
      // Ok
    },
    _ = <A::Runtime as Runtime>::sleep(Duration::from_millis(50)).fuse() => {
      panic!("last append didn't unblock");
    },
  }
}

/// Test [`NetTransport::vote`](Transport::vote) implementation.
pub async fn vote<
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  D: Data,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::Address, Data = D>,
>(
  header1: Header<I, A::Address>,
  stream_layer1: S,
  resolver1: A,
  header2: Header<I, A::Address>,
  stream_layer2: S,
  resolver2: A,
  fake_target: Header<I, A::Address>,
) where
  <<A::Runtime as Runtime>::Sleep as Future>::Output: Send + 'static,
{
  let trans1 =
    NetTransport::<_, _, D, _, W>::new(resolver1, stream_layer1, NetTransportOptions::new(header1))
      .await
      .unwrap();
  let trans2 = NetTransport::new(resolver2, stream_layer2, NetTransportOptions::new(header2))
    .await
    .unwrap();

  ruraft_core::tests::transport::vote(trans1, trans2, fake_target).await;
}

/// Test [`NetTransport::install_snapshot`](Transport::install_snapshot) implementation.
pub async fn install_snapshot<
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  D: Data,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::Address, Data = D>,
>(
  header1: Header<I, A::Address>,
  stream_layer1: S,
  resolver1: A,
  header2: Header<I, A::Address>,
  stream_layer2: S,
  resolver2: A,
  fake_target: Header<I, A::Address>,
) where
  <<A::Runtime as Runtime>::Sleep as Future>::Output: Send + 'static,
{
  let trans1 =
    NetTransport::<_, _, D, _, W>::new(resolver1, stream_layer1, NetTransportOptions::new(header1))
      .await
      .unwrap();
  let trans2 = NetTransport::new(resolver2, stream_layer2, NetTransportOptions::new(header2))
    .await
    .unwrap();

  ruraft_core::tests::transport::install_snapshot(trans1, trans2, fake_target).await;
}

/// Test [`NetTransport`] pooled connection functionality.
pub async fn pooled_conn<
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  D: Data,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::Address, Data = D>,
>(
  header1: Header<I, A::Address>,
  stream_layer1: S,
  resolver1: A,
  header2: Header<I, A::Address>,
  stream_layer2: S,
  resolver2: A,
) where
  <<A::Runtime as Runtime>::Sleep as Future>::Output: Send + 'static,
{
  let trans1 =
    NetTransport::<_, _, D, _, W>::new(resolver1, stream_layer1, NetTransportOptions::new(header1))
      .await
      .unwrap();
  let trans2 = Arc::new(
    NetTransport::<_, _, D, _, W>::new(resolver2, stream_layer2, NetTransportOptions::new(header2))
      .await
      .unwrap(),
  );

  let trans1_consumer = trans1.consumer();
  let trans1_header = trans1.header().clone();

  // Make the RPC request
  let args = AppendEntriesRequest {
    header: trans2.header().clone(),
    term: 10,
    prev_log_entry: 100,
    prev_log_term: 4,
    entries: vec![Log::__crate_new(4, 101, LogKind::Noop)],
    leader_commit: 90,
  };

  let resp = AppendEntriesResponse {
    header: trans1.header().clone(),
    term: 4,
    last_log: 90,
    success: true,
    no_retry_backoff: false,
  };
  let resp1 = resp.cheap_clone();

  // errCh is used to report errors from any of the goroutines
  // created in this test.
  // It is buffered as to not block.
  let (err_tx, err_rx) = async_channel::bounded(100);

  // Listen for a request
  <A::Runtime as Runtime>::spawn_detach(async move {
    futures::pin_mut!(trans1_consumer);

    loop {
      futures::select! {
        req = trans1_consumer.next().fuse() => {
          let req = req.unwrap();
          let Ok(_) = req.respond(Response::append_entries(resp1.clone())) else {
            panic!("unexpected respond fail");
          };
        },
        _ = <A::Runtime as Runtime>::sleep(Duration::from_millis(200)).fuse() => {
          panic!("timeout");
        },
      }
    }
  });

  let wg = AsyncWaitGroup::new();

  for _ in 0..5 {
    let new_wg = wg.add(1);
    let trans = trans2.clone();
    let err_tx = err_tx.clone();
    let expected_resp = resp.clone();
    let target = trans1_header.from().clone();
    let req = args.clone();
    <A::Runtime as Runtime>::spawn_detach(async move {
      scopeguard::defer!(new_wg.done());

      match trans.append_entries(&target, req).await {
        Ok(res) => {
          // Verify the response
          assert_eq!(res, expected_resp);
        }
        Err(e) => {
          err_tx.send(e).await.expect("failed to send error");
        }
      }
    });
  }

  wg.wait().await;

  // Check if we received any errors from the above goroutines.
  if !err_rx.is_empty() {
    panic!("unexpected error");
  }

  // Check the conn pool size
  {
    let pool = trans2.conn_pool.lock().await;
    let conns = pool.get(trans1.local_addr()).expect("no conns in the pool");
    assert_eq!(conns.len(), 3, "Expected 3 pooled conns!");
  }
}

/// Tests that [`Listener::accept`] errors in `NetTransport`
/// do not result in a tight loop and spam the log. We verify this here by counting the number
/// of calls against [`Listener::accept`] and the logger
#[tokio::test]
#[cfg(test)]
async fn test_network_transport_listenbackoff() {
  use std::{
    pin::Pin,
    task::{Context, Poll},
  };

  use futures::AsyncWrite;

  #[derive(Clone)]
  struct TestCountingStreamLayer {
    num_calls: Arc<std::sync::atomic::AtomicUsize>,
  }

  impl TestCountingStreamLayer {
    fn new() -> Self {
      Self {
        num_calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
      }
    }
  }

  impl AsyncRead for TestCountingStreamLayer {
    fn poll_read(
      self: Pin<&mut Self>,
      _cx: &mut Context<'_>,
      _buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
      unreachable!()
    }
  }

  impl AsyncWrite for TestCountingStreamLayer {
    fn poll_write(
      self: Pin<&mut Self>,
      _cx: &mut Context<'_>,
      _buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
      unreachable!()
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
      unreachable!()
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
      unreachable!()
    }
  }

  impl Connection for TestCountingStreamLayer {
    type OwnedReadHalf = Self;

    type OwnedWriteHalf = Self;

    fn set_write_timeout(&self, _timeout: Option<Duration>) {}

    fn write_timeout(&self) -> Option<Duration> {
      None
    }

    fn set_read_timeout(&self, _timeout: Option<Duration>) {}

    fn read_timeout(&self) -> Option<Duration> {
      None
    }

    fn into_split(self) -> (Self::OwnedReadHalf, Self::OwnedWriteHalf) {
      (self.clone(), self.clone())
    }
  }

  impl Listener for TestCountingStreamLayer {
    type Stream = Self;

    async fn accept(&mut self) -> std::io::Result<(Self::Stream, SocketAddr)> {
      self.num_calls.fetch_add(1, Ordering::SeqCst);
      Err(std::io::Error::new(
        std::io::ErrorKind::Other,
        "intentional error in test",
      ))
    }

    fn local_addr(&self) -> std::io::Result<SocketAddr> {
      panic!("no needed")
    }
  }

  impl StreamLayer for TestCountingStreamLayer {
    type Listener = Self;

    type Stream = Self;

    async fn connect(&self, _addr: SocketAddr) -> std::io::Result<Self::Stream> {
      panic!("no needed")
    }

    async fn bind(&mut self, _addr: SocketAddr) -> std::io::Result<Self::Listener> {
      Ok(self.clone())
    }
  }

  // TEST_TIME is the amount of time we will allow NetworkTransport#listen() to run
  // This needs to be long enough that to verify that maxDelay is in force,
  // but not so long as to be obnoxious when running the test suite.
  const TEST_TIME: Duration = Duration::from_secs(4);

  let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
  let trans: NetTransport<_, _, Vec<_>, _, ruraft_wire::LpeWire<_, _, Vec<u8>>> =
    NetTransport::new(
      TestAddressResolver::<_, agnostic::tokio::TokioRuntime>::new(),
      TestCountingStreamLayer::new(),
      NetTransportOptions::new(Header::new(
        ProtocolVersion::V1,
        smol_str::SmolStr::from("test"),
        addr,
      )),
    )
    .await
    .unwrap();

  tokio::time::sleep(TEST_TIME).await;
  trans.shutdown().await.unwrap();

  // Verify that the method exited (but without block this test)
  // maxDelay == 1s, so we will give the routine 1.25s to loop around and shut down.
  tokio::select! {
    _ = trans.shutdown_rx.recv() => {},
    _ = tokio::time::sleep(Duration::from_millis(1250)) => {
      panic!("timed out waiting for NetworkTransport to shut down");
    },
  }

  assert!(trans.shutdown.load(Ordering::SeqCst));

  // In testTime==4s, we expect to loop approximately 12 times
  // with the following delays (in ms):
  //   0+5+10+20+40+80+160+320+640+1000+1000+1000 == 4275 ms
  // Too few calls suggests that the minDelay is not in force; too many calls suggests that the
  // maxDelay is not in force or that the back-off isn't working at all.
  // We'll leave a little flex; the important thing here is the asymptotic behavior.
  // If the minDelay or maxDelay in NetworkTransport are modified, this test may fail
  // and need to be adjusted.

  assert!(trans.stream_layer.num_calls.load(Ordering::SeqCst) > 10);
  assert!(trans.stream_layer.num_calls.load(Ordering::SeqCst) < 13);
}
