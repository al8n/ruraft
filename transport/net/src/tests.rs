use std::{
  convert::Infallible,
  marker::PhantomData,
  pin::Pin,
  task::{Context, Poll},
};

use futures::AsyncWrite;
use ruraft_core::transport::*;

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
pub async fn close_streams<S: StreamLayer, W: Wire, R: Runtime>(_s1: S, _s2: S) {
  unimplemented!()
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
pub async fn append_entries<S: StreamLayer, W: Wire, R: Runtime>(_s1: S, _s2: S) {
  unimplemented!()
}

/// Test [`NetTransport::append_entries_pipeline`](Transport::append_entries_pipeline).
pub async fn append_entries_pipeline<S: StreamLayer, W: Wire, R: Runtime>(_s1: S, _s2: S) {
  unimplemented!()
}

/// Test [`NetTransport::append_entries_pipeline`](Transport::append_entries_pipeline) and [`NetTransport::close_streams`].
pub async fn append_entries_pipeline_close_streams<S: StreamLayer, W: Wire, R: Runtime>(
  _s1: S,
  _s2: S,
) {
  unimplemented!()
}

/// Test [`NetTransport::append_entries_pipeline`](Transport::append_entries_pipeline) and [`NetTransport`]'s max rpc inflight special case.
pub async fn append_entries_pipeline_max_rpc_inflight<S: StreamLayer, W: Wire, R: Runtime>(
  _s1: S,
  _s2: S,
) {
  unimplemented!()
}

/// Test [`NetTransport::vote`](Transport::vote) implementation.
pub async fn vote<S: StreamLayer, W: Wire, R: Runtime>(_s1: S, _s2: S) {
  unimplemented!()
}

/// Test [`NetTransport::install_snapshot`](Transport::install_snapshot) implementation.
pub async fn install_snapshot<S: StreamLayer, W: Wire, R: Runtime>(_s1: S, _s2: S) {
  unimplemented!()
}

/// Test [`NetTransport`] pooled connection functionality.
pub async fn pooled_conn<S: StreamLayer, W: Wire, R: Runtime>(_s1: S, _s2: S) {
  unimplemented!()
}

#[derive(Clone)]
struct TestCountingStreamLayer {
  num_calls: Arc<AtomicUsize>,
}

impl TestCountingStreamLayer {
  fn new() -> Self {
    Self {
      num_calls: Arc::new(AtomicUsize::new(0)),
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

/// Tests that [`Listener::accept`] errors in `NetTransport`
/// do not result in a tight loop and spam the log. We verify this here by counting the number
/// of calls against [`Listener::accept`] and the logger
#[tokio::test]
async fn test_network_transport_listenbackoff() {
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
