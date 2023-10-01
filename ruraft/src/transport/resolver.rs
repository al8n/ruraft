use ruraft_core::transport::NodeAddressResolver;

/// The naive [`NodeAddressResolver`](ruraft_core::transport::NodeAddressResolver) implementation.
pub mod default;

/// The DNS [`NodeAddressResolver`](ruraft_core::transport::NodeAddressResolver) implementation.
pub mod dns;
