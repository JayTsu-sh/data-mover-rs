//! Role-based storage, traversal, metadata and resumable transfer.
pub mod error;
#[cfg(feature = "traversal")]
pub mod filter;
#[cfg(feature = "traversal")]
mod filter_traversal;
#[cfg(feature = "integrity")]
pub mod integrity;
#[cfg(feature = "metadata")]
pub mod metadata;
pub mod model;
pub mod qos;
pub mod storage;
#[cfg(feature = "transfer")]
pub mod tar_pack;
pub mod time_util;
#[cfg(feature = "transfer")]
pub mod transfer;
mod transfer_concurrency;
#[cfg(feature = "traversal")]
pub mod traversal;
pub mod url_redact;
pub use error::{HdfsErrorKind, HdfsOperationError};
#[cfg(feature = "traversal")]
pub use filter::{
    FilterExpression, FilterFieldDef, FilterOperatorDef, get_filter_field_definitions,
};
#[cfg(feature = "traversal")]
pub use filter_traversal::DslTraversalFilter;
pub use qos::QosManager;
#[cfg(feature = "transfer")]
pub use tar_pack::calculate_tar_size;
pub use transfer_concurrency::TransferConcurrency;
pub use url_redact::redact_storage_url;
pub type Result<T> = std::result::Result<T, error::StorageError>;
/// One kibibyte, in bytes.
pub const KB: u64 = 1024;
/// One mebibyte, in bytes.
pub const MB: u64 = 1024 * KB;

/// Explicit connectivity and server-clock probes over the connected roles.
#[cfg(feature = "transfer")]
pub mod probes;
