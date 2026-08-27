//! HDFS storage backend.
//!
//! The public seam remains a single deep module. Its implementation is split by
//! responsibility without exposing internal coordination details to callers.

include!("hdfs/config.rs");
include!("hdfs/transfer.rs");
include!("hdfs/lifecycle.rs");
include!("hdfs/namespace.rs");
include!("hdfs/io.rs");
include!("hdfs/support.rs");
include!("hdfs/tests.rs");
