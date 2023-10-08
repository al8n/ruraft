// // use agnostic::Runtime;
// // use ruraft_core::transport::AppendPipeline;

// // pub struct NetAppendPipeline<R: Runtime> {
// //   _marker: std::marker::PhantomData<R>,
// // }

// // impl<R: Runtime> NetAppendPipeline<R> {
// //   pub fn new() -> Self {
// //     Self {
// //       _marker: std::marker::PhantomData,
// //     }
// //   }
// // }

// // impl<R: Runtime> AppendPipeline for NetAppendPipeline<R> {

// // }

// use std::{
//   pin::Pin,
//   task::{Context, Poll},
// };

// use super::*;

// pub struct NetAppendPipeline<I: Id, A: Address> {}

// pub struct NetAppendFuture<I: Id, A: Address> {
//   start: Instant,
//   req: AppendEntriesRequest<I, A>,
// }

// impl<I: Id, A: Address> Future for NetAppendFuture<I, A> {
//   type Output = std::io::Result<AppendEntriesResponse<I, A>>;

//   fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
//     todo!()
//   }
// }

// impl<I: Id, A: Address> AppendFuture for NetAppendFuture<I, A> {
//   type Id = I;

//   type Address = A;

//   fn start(&self) -> std::time::Instant {
//     self.start
//   }
// }
