use std::{convert::Infallible, net::SocketAddr};

use agnostic::Runtime;

use super::*;

/// The [`NodeAddressResolver::NodeAddress`] of the [`SocketAddrResolver`] is [`SocketAddr`].
/// So it just returns the given address and impossible to return an error.
///
/// If you want a more powerful [`NodeAddressResolver`] implementation,
/// please see [`DnsResolver`](crate::transport::resolver::dns::DnsResolver).
pub struct SocketAddrResolver<R: agnostic::Runtime>(std::marker::PhantomData<R>);

#[async_trait::async_trait]
impl<R: Runtime> NodeAddressResolver for SocketAddrResolver<R> {
  type NodeAddress = SocketAddr;
  type Error = Infallible;
  type Runtime = R;

  async fn resolve(&self, address: &Self::NodeAddress) -> Result<SocketAddr, Self::Error> {
    Ok(*address)
  }
}
