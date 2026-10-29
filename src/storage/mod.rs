//! Connected storage, built-in connectors, and compatibility exports.
pub use data_mover_storage_core::storage::*;
pub(crate) mod backends;
mod factory;
#[cfg(feature = "backend-hdfs")]
pub use backends::hdfs::build_hdfs_client;
#[cfg(all(windows, feature = "backend-local"))]
pub use backends::local::windows_acl;
#[cfg(feature = "backend-nfs")]
pub use backends::nfs::list_exports;
#[cfg(feature = "backend-s3")]
pub use backends::s3::{S3BucketInfo, list_buckets};
pub use factory::connect_backend;
