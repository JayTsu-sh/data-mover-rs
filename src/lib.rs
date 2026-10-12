//! Role-based storage, traversal, metadata and resumable transfer.
pub mod error;
pub mod filter;
mod filter_traversal;
pub mod integrity;
pub mod metadata;
pub mod model;
pub mod qos;
pub(crate) mod runtime;
pub mod storage;
pub mod tar_pack;
pub mod time_util;
pub mod transfer;
mod transfer_concurrency;
pub mod traversal;
pub mod url_redact;
pub use error::{HdfsErrorKind, HdfsOperationError};
pub use filter::{
    FilterExpression, FilterFieldDef, FilterOperatorDef, get_filter_field_definitions,
};
pub use filter_traversal::DslTraversalFilter;
pub use qos::QosManager;
pub use tar_pack::calculate_tar_size;
pub use transfer_concurrency::TransferConcurrency;
pub use url_redact::redact_storage_url;
pub type Result<T> = std::result::Result<T, error::StorageError>;
/// One kibibyte, in bytes.
pub const KB: u64 = 1024;
/// One mebibyte, in bytes.
pub const MB: u64 = 1024 * KB;

#[cfg(test)]
trait AssertTestValue {
    type Value;

    fn assert_value(self, context: &str) -> Self::Value;
}

#[cfg(test)]
trait AssertTestError {
    type Error;

    fn assert_error(self, context: &str) -> Self::Error;
}

#[cfg(test)]
impl<T, E> AssertTestError for std::result::Result<T, E> {
    type Error = E;

    fn assert_error(self, context: &str) -> E {
        match self {
            Ok(_) => panic!("{context}"),
            Err(error) => error,
        }
    }
}

#[cfg(test)]
impl<T, E: std::fmt::Debug> AssertTestValue for std::result::Result<T, E> {
    type Value = T;

    fn assert_value(self, context: &str) -> T {
        match self {
            Ok(value) => value,
            Err(error) => panic!("{context}: {error:?}"),
        }
    }
}

#[cfg(test)]
impl<T> AssertTestValue for Option<T> {
    type Value = T;

    fn assert_value(self, context: &str) -> T {
        match self {
            Some(value) => value,
            None => panic!("{context}"),
        }
    }
}

/// Explicit connectivity and server-clock probes over the connected roles.
pub mod probes;
