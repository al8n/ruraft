pub(crate) fn invalid_data<E: std::error::Error + Send + Sync + 'static>(err: E) -> std::io::Error {
  std::io::Error::new(std::io::ErrorKind::InvalidData, err)
}

pub(crate) async fn override_notify_bool(
  tx: &async_channel::Sender<bool>,
  rx: &async_channel::Receiver<bool>,
  v: bool,
) {
  use futures::FutureExt;

  futures::select! {
    _ = tx.send(v).fuse() => {
      // value sent, all done
    }
    _ = rx.recv().fuse() => {
      // channel had an old value
      futures::select! {
        _ = tx.send(v).fuse() => {
          // value sent, all done
        }
        default => {
          panic!("race: channel was sent concurrently");
        }
      }
    }
  }
}
