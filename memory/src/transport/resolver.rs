use std::{convert::Infallible, marker::PhantomData};

use agnostic_lite::RuntimeLite;
use ruraft_core::transport::{Address, AddressResolver};

/// Inmemory address resolver, doing nothing actually.
pub struct MemoryAddressResolver<A, R>(PhantomData<(A, R)>);

impl<A, R> Default for MemoryAddressResolver<A, R> {
  fn default() -> Self {
    Self::new()
  }
}

impl<A, R> Clone for MemoryAddressResolver<A, R> {
  fn clone(&self) -> Self {
    *self
  }
}

impl<A, R> Copy for MemoryAddressResolver<A, R> {}

impl<A, R> MemoryAddressResolver<A, R> {
  /// Creates a new `MemoryAddressResolver`.
  #[inline]
  pub const fn new() -> Self {
    Self(PhantomData)
  }
}

impl<A: Address, R: RuntimeLite> AddressResolver for MemoryAddressResolver<A, R> {
  type Address = A;
  type ResolvedAddress = A;
  type Error = Infallible;
  type Runtime = R;
  type Options = ();

  async fn new(_options: Self::Options) -> Result<Self, Self::Error>
  where
    Self: Sized,
  {
    Ok(Self::new())
  }

  async fn resolve(&self, address: &Self::Address) -> Result<Self::ResolvedAddress, Self::Error> {
    Ok(address.cheap_clone())
  }
}
