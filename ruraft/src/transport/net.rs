use core::mem;
use ruraft_core::{
  options::ProtocolVersion,
  transport::{NodeAddress, NodeId},
};

/// Network encoder and decoder based on [`rmp_serde`].
pub mod rmp;

const ENCODED_HEADER_SIZE: usize = mem::size_of::<ProtocolVersion>()
  + 1 // kind tag
  + mem::size_of::<u32>() // header length
  + mem::size_of::<u32>(); // req/resp length

fn encode_message_header<Id: NodeId, Address: NodeAddress>(
  protocol_version: ProtocolVersion,
  tag: u8,
  id: &Id,
  address: &Address,
) -> [u8; ENCODED_HEADER_SIZE] {
  let id_encoded_len = id.encoded_len();
  let address_encoded_len = address.encoded_len();
  let header_len = ENCODED_HEADER_SIZE + id_encoded_len + address_encoded_len;

  let mut buf = [0u8; ENCODED_HEADER_SIZE];
  let mut cur = 0;
  buf[0] = protocol_version as u8;
  cur += 1;
  buf[1] = tag;
  cur += 1;
  buf[cur..cur + mem::size_of::<u32>()].copy_from_slice(&(header_len as u32).to_be_bytes());
  cur += mem::size_of::<u32>();
  // We do not add req/resp length here, because we do not know the length of req/resp yet.
  // req/resp length will be updated by the caller.
  buf
}
