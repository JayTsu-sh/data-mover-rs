//! Shared storage contracts, canonical configuration, and recovery machinery.
pub use data_mover_model as model;
pub mod error;
pub mod qos;
#[doc(hidden)]
pub mod runtime;
pub mod storage;
pub mod time_util;
#[doc(hidden)]
pub mod transfer_concurrency;
pub mod url_redact;
pub use error::Result;
pub use transfer_concurrency::TransferConcurrency;
pub const KB: u64 = 1024;
pub const MB: u64 = 1024 * KB;

#[cfg(any(test, feature = "test-support"))]
pub mod testing;
#[cfg(test)]
use testing::{AssertTestError, AssertTestValue};
