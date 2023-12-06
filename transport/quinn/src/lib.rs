use ruraft_net::stream::*;

pub struct QuinnStream {}

impl Connection for QuinnStream {
  type OwnedReadHalf;

  type OwnedWriteHalf;

  fn set_write_timeout(&self, timeout: Option<std::time::Duration>) {
    todo!()
  }

  fn write_timeout(&self) -> Option<std::time::Duration> {
    todo!()
  }

  fn set_read_timeout(&self, timeout: Option<std::time::Duration>) {
    todo!()
  }

  fn read_timeout(&self) -> Option<std::time::Duration> {
    todo!()
  }

  fn into_split(self) -> (Self::OwnedReadHalf, Self::OwnedWriteHalf) {
    todo!()
  }
}
