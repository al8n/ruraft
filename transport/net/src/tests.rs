
// use nodecraft::{resolver::socket_addr::SocketAddrResolver, NodeId};
// use ruraft_core::storage::{Log, LogKind};

// use super::*;

// pub trait NodeIdExt {
//   fn random() -> Self;
// }

// impl NodeIdExt for NodeId {
//   fn random() -> Self {
//     use rand::distributions::Alphanumeric;
//     use rand::Rng;
//     let rand_string: String = rand::thread_rng()
//         .sample_iter(&Alphanumeric)
//         .take(32) // Adjust this to the desired length
//         .map(char::from)
//         .collect();

//     NodeId::new(rand_string)
//   }
// }


// async fn make_transport<R>() -> NetTransport<NodeId, SocketAddrResolver<Runtime = R>, SomeWire>
// where
//   R: Runtime,
//   <R::Sleep as Future>::Output: Send,
// {
//   let opts = NetTransportOptions::new(NodeId::random(), "127.0.0.1:0".parse().unwrap());
//   NetTransport::<R>::new(SocketAddrResolver::default(), opts)
//     .await
//     .unwrap()
// }

// fn make_append_req(id: ServerId, addr: SocketAddr) -> AppendEntriesRequest {
//   AppendEntriesRequest {
//     header: Header::new(ProtocolVersion::V1, id, addr),
//     term: 10,
//     prev_log_entry: 100,
//     prev_log_term: 4,
//     entries: vec![Log::new(101, 4, LogKind::Noop)],
//     leader_commit: 90,
//   }
// }

// fn make_append_resp(id: ServerId, addr: SocketAddr) -> AppendEntriesResponse {
//   AppendEntriesResponse {
//     header: Header::new(ProtocolVersion::V1, id, addr),
//     term: 4,
//     last_log: 90,
//     success: true,
//     no_retry_backoff: false,
//   }
// }

// pub async fn test_net_transport_start_stop<R: Runtime>()
// where
//   <R::Sleep as Future>::Output: Send,
// {
//   let trans = NetTransport::<R>::new(NetTransportOptions::new(
//     NodeId::random(),
//     "127.0.0.1:0".parse().unwrap(),
//   ))
//   .await
//   .unwrap();

//   trans.shutdown().await.unwrap();
// }

// pub async fn test_net_transport_append_entries<R>()
// where
//   R: Runtime,
//   <R::Sleep as Future>::Output: Send,
// {
//   let trans1 = NetTransport::<R>::new(NetTransportOptions::new(
//     NodeId::random(),
//     "127.0.0.1:0".parse().unwrap(),
//   ))
//   .await
//   .unwrap();

//   let trans1_addr = trans1.local_addr();
//   let args = make_append_req(trans1.local_id().clone(), trans1_addr);
//   let expected_resp = make_append_resp(trans1.local_id().clone(), trans1_addr);
//   let mut consumer = trans1.consumer();
//   let resp = expected_resp.clone();

//   R::spawn_detach(async move {
//     use futures::StreamExt;

//     futures::select! {
//       req = consumer.next().fuse() => {
//         let req = req.unwrap();
//         let Ok(_) = req.respond(Response::append_entries(ProtocolVersion::V1, resp)) else {
//           panic!("unexpected respond fail");
//         };
//       },
//       _ = R::sleep(Duration::from_millis(200)).fuse() => {
//         panic!("timeout");
//       },
//     }
//   });

//   let trans2 = NetTransport::<R>::new(NetTransportOptions::new(
//     NodeId::random(),
//     "127.0.0.1:0".parse().unwrap(),
//   ))
//   .await
//   .unwrap();

//   let res = trans2.append_entries(args).await.unwrap();
//   assert_eq!(res, expected_resp);
// }
