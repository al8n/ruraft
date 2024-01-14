use pyo3::*;

#[cfg(feature = "tokio")]
create_exception!(tokio_raft, MembershipError, pyo3::exceptions::PyException, "Membership error");

#[cfg(feature = "async-std")]
create_exception!(async_std_raft, MembershipError, pyo3::exceptions::PyException, "Membership error");
