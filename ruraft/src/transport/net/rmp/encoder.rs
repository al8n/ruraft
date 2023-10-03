// use super::encode_message_header;
// use rmp_serde::encode::write;
// use ruraft_core::transport::{Encoder, NodeId, Request, Response};

// #[derive(Debug, thiserror::Error)]
// pub enum EncodeError {}

// pub struct RmpEncoder;

// impl Encoder for RmpEncoder {
//   type Error = EncodeError;

//   type NodeId;

//   type NodeAddress;

//   type Bytes;

//   fn encode_request(
//     req: &Request<Self::NodeId, Self::NodeAddress>,
//   ) -> Result<Self::Bytes, Self::Error> {
//     let header = req.header();
//     let id = header.id();
//     let address = header.address();
//     let kind = req.kind();
//     let mut encoded_header = encode_message_header(req.protocol_version(), kind.tag(), id, address);

//     todo!()
//   }

//   fn encode_response(
//     resp: &Response<Self::NodeId, Self::NodeAddress>,
//   ) -> Result<Self::Bytes, Self::Error> {
//     let header = resp.header();
//     let id = header.id();
//     let address = header.address();
//     let kind = resp.kind();
//     let mut encoded_header =
//       encode_message_header(resp.protocol_version(), kind.tag(), id, address);
//     todo!()
//   }
// }

// #[test]
// fn test_() {}
