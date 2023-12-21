use std::{
  future::Future,
  pin::Pin,
  task::{Context, Poll},
  time::Instant,
};

use futures::{channel::oneshot, Stream};
use nodecraft::CheapClone;

use crate::{
  membership::Membership,
  options::{ProtocolVersion, SnapshotVersion},
  storage::Log,
  Node,
};

pub use async_channel::{RecvError, TryRecvError};

/// A common sub-structure used to pass along protocol version and
/// other information about the cluster.
#[viewit::viewit(
  vis_all = "pub(crate)",
  getters(vis_all = "pub"),
  setters(vis_all = "pub")
)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Header<I, A> {
  /// The protocol version of the request or response
  #[viewit(
    getter(
      const,
      attrs(doc = "Get the protocol version of the request or response"),
    ),
    setter(attrs(doc = "Set the protocol version of the request or response"),)
  )]
  protocol_version: ProtocolVersion,

  /// The id of the node sending the RPC Request or Response
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Get the node of the request or response"),
    ),
    setter(attrs(doc = "Set the node of the request or response"),)
  )]
  from: Node<I, A>,
}

impl<I: CheapClone, A: CheapClone> CheapClone for Header<I, A> {
  fn cheap_clone(&self) -> Self {
    Self {
      protocol_version: self.protocol_version,
      from: self.from.cheap_clone(),
    }
  }
}

impl<I, A> Header<I, A> {
  /// Create a new [`Header`] with the given `id` and `addr`.
  #[inline]
  pub fn new(version: ProtocolVersion, id: I, addr: A) -> Self {
    Self {
      protocol_version: version,
      from: Node::new(id, addr),
    }
  }

  /// Create a new [`Header`] with the given [`ProtocolVersion`] and [`Node`].
  #[inline]
  pub fn from_node(version: ProtocolVersion, node: Node<I, A>) -> Self {
    Self {
      protocol_version: version,
      from: node,
    }
  }

  /// Returns the address of the header.
  #[inline]
  pub fn addr(&self) -> &A {
    self.from.addr()
  }

  /// Returns the id of the header.
  #[inline]
  pub fn id(&self) -> &I {
    self.from.id()
  }
}

impl<I, A> From<(ProtocolVersion, Node<I, A>)> for Header<I, A> {
  #[inline]
  fn from((version, from): (ProtocolVersion, Node<I, A>)) -> Self {
    Self {
      protocol_version: version,
      from,
    }
  }
}

// macro_rules! encode {
//   (v1::$ty:ident { $expr: expr }) => {{
//     let mut buf = Vec::with_capacity(128);
//     buf.push({ $expr });
//     buf.push(ProtocolVersion::V1 as u8);

//     // Reserve length for the message.
//     buf.extend_from_slice(&[0, 0, 0, 0]);

//     rmp_serde::encode::write(&mut buf, $ty)
//       .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
//     let encoded_len = buf.len() - OFFSET;
//     buf[2..OFFSET].copy_from_slice(&(encoded_len as u32).to_be_bytes());
//     Ok(buf)
//   }};
// }

/// Used to send a response back to the remote.
pub struct RpcResponseSender<I, A>(oneshot::Sender<Response<I, A>>);

impl<I, A> RpcResponseSender<I, A> {
  /// Respond to the heartbeat RPC, if the remote half is closed, then the response will be returned back as an error.
  #[inline]
  pub fn respond(self, resp: Response<I, A>) -> Result<(), Response<I, A>> {
    self.0.send(resp)
  }
}

impl<I, A> From<oneshot::Sender<Response<I, A>>> for RpcResponseSender<I, A> {
  fn from(tx: oneshot::Sender<Response<I, A>>) -> Self {
    Self(tx)
  }
}

impl<I, A> From<RpcResponseSender<I, A>> for oneshot::Sender<Response<I, A>> {
  fn from(sender: RpcResponseSender<I, A>) -> Self {
    sender.0
  }
}

/// Errors returned by the [`RpcHandle`].
#[derive(Debug, thiserror::Error)]
pub enum RpcHandleError {
  /// Returned when the command is cancelled
  #[error("{0}")]
  Canceled(#[from] oneshot::Canceled),
}

/// A future for getting the corresponding response from the Raft.
#[pin_project::pin_project]
#[repr(transparent)]
pub struct RpcHandle<I, A> {
  #[pin]
  rx: oneshot::Receiver<Response<I, A>>,
}

impl<I, A> RpcHandle<I, A> {
  fn new(rx: oneshot::Receiver<Response<I, A>>) -> Self {
    Self { rx }
  }
}

impl<I, A> Future for RpcHandle<I, A> {
  type Output = Result<Response<I, A>, RpcHandleError>;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    // Using Pin::as_mut to get a Pin<&mut Receiver>.
    let this = self.project();

    // Now, poll the receiver directly
    this.rx.poll(cx).map(|res| match res {
      Ok(res) => Ok(res),
      Err(e) => Err(From::from(e)),
    })
  }
}

/// The struct is used to interact with the Raft.
pub struct Rpc<I, A, D, R> {
  req: Request<I, A, D>,
  tx: oneshot::Sender<Response<I, A>>,
  reader: Option<R>,
}

impl<I, A, D, R> Rpc<I, A, D, R> {
  /// Create a new command from the given request and reader.
  /// This reader must be provided when sending in the
  /// install snapshot rpc.
  pub fn new(req: Request<I, A, D>, reader: Option<R>) -> (Self, RpcHandle<I, A>) {
    let (tx, rx) = oneshot::channel();
    (Self { req, tx, reader }, RpcHandle::new(rx))
  }

  /// Returns the header of the request.
  pub fn header(&self) -> &Header<I, A> {
    self.req.header()
  }

  /// Respond to the request, if the remote half is closed
  /// then the response will be returned back as an error.
  pub fn respond(self, resp: Response<I, A>) -> Result<(), Response<I, A>> {
    self.tx.send(resp)
  }

  /// Consumes the [`Rpc`] and returns the components.
  pub fn into_components(self) -> (RpcResponseSender<I, A>, Request<I, A, D>, Option<R>) {
    (RpcResponseSender(self.tx), self.req, self.reader)
  }
}

/// Command stream is a consumer for Raft node to consume requests
/// from remote nodes
#[pin_project::pin_project]
#[derive(Debug)]
pub struct RpcConsumer<I, A, D, R> {
  #[pin]
  rx: async_channel::Receiver<Rpc<I, A, D, R>>,
}

impl<I, A, D, R> Clone for RpcConsumer<I, A, D, R> {
  fn clone(&self) -> Self {
    Self {
      rx: self.rx.clone(),
    }
  }
}

impl<I, A, D, R> Stream for RpcConsumer<I, A, D, R> {
  type Item = <async_channel::Receiver<Rpc<I, A, D, R>> as Stream>::Item;

  fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    <async_channel::Receiver<Rpc<I, A, D, R>> as Stream>::poll_next(self.project().rx, cx)
  }
}

impl<I, A, D, R> RpcConsumer<I, A, D, R> {
  /// Receives a [`Rpc`] from the consumer.
  pub async fn recv(&self) -> Result<Rpc<I, A, D, R>, RecvError> {
    self.rx.recv().await
  }

  /// Attempts to receive a [`Rpc`] from the consumer.
  ///
  /// If the consumer is empty, or empty and closed, this method returns an error
  pub fn try_recv(&self) -> Result<Rpc<I, A, D, R>, TryRecvError> {
    self.rx.try_recv()
  }
}

/// A producer for [`Rpc`]s
pub struct RpcProducer<I, A, D, R> {
  tx: async_channel::Sender<Rpc<I, A, D, R>>,
}

impl<I, A, D, R> Clone for RpcProducer<I, A, D, R> {
  fn clone(&self) -> Self {
    Self {
      tx: self.tx.clone(),
    }
  }
}

impl<I, A, D, R> RpcProducer<I, A, D, R> {
  /// Produce a command for processing
  pub async fn send(
    &self,
    req: Rpc<I, A, D, R>,
  ) -> Result<(), async_channel::SendError<Rpc<I, A, D, R>>> {
    self.tx.send(req).await
  }
}

/// Returns unbounded command producer and command consumer.
pub fn rpc<I, A, D, R>() -> (RpcProducer<I, A, D, R>, RpcConsumer<I, A, D, R>) {
  let (tx, rx) = async_channel::unbounded();
  (RpcProducer { tx }, RpcConsumer { rx })
}

macro_rules! enum_wrapper {
  (
    $(#[$outer:meta])*
    $vis:vis enum $name:ident $(<$($generic:tt),+>)? {
      $(
        $(#[$variant_meta:meta])*
        $variant:ident($variant_ty: ty) = $variant_tag:literal => $variant_snake_case: ident
      ), +$(,)?
    }
  ) => {
    $(#[$outer])*
    $vis enum $name $(< $($generic),+ >)? {
      $(
        $(#[$variant_meta])*
        $variant($variant_ty),
      )*
    }

    impl $(< $($generic),+ >)? $name $(< $($generic),+ >)? {
      const fn type_name(&self) -> &'static str {
        match self {
          $(
            Self::$variant(_) => stringify!($variant_ty),
          )*
        }
      }

      /// Returns the tag of this request kind for encoding/decoding.
      #[inline]
      pub const fn tag(&self) -> u8 {
        match self {
          $(
            Self::$variant(_) => $variant_tag,
          )*
        }
      }

      /// Returns the variant name
      #[inline]
      pub const fn description(&self) -> &'static str {
        match self {
          $(
            Self::$variant(_) => stringify!($variant),
          )*
        }
      }

      /// Returns the header of the request
      pub const fn header(&self) -> &Header<I, A> {
        match self {
          $(
            Self::$variant(req) => req.header(),
          )*
        }
      }

      /// Returns the protocol version of the request.
      #[inline]
      pub const fn protocol_version(&self) -> ProtocolVersion {
        self.header().protocol_version
      }

      $(
        paste::paste! {
          #[doc = concat!("Returns the contained [`", stringify!($variant_ty), "`] request, consuming the self value. Panics if the value is not [`", stringify!($variant_ty), "`].")]
          $vis fn [< unwrap_ $variant_snake_case>] (self) -> $variant_ty {
            if let Self::$variant(val) = self {
              val
            } else {
              panic!(concat!("expect ", stringify!($variant), ", buf got {}"), self.type_name())
            }
          }
        }

        #[doc = concat!("Construct a [`", stringify!($name), "`] from [`", stringify!($variant_ty), "`].")]
        pub const fn $variant_snake_case(val: $variant_ty) -> Self {
          Self::$variant(val)
        }
      )*
    }
  };
}

mod requests;
pub use requests::*;
mod responses;
pub use responses::*;
