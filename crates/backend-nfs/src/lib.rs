//! NFS storage adapter.
#![cfg_attr(
    all(feature = "test-support", not(test)),
    allow(unused_imports, dead_code)
)]
#[doc(hidden)]
pub mod backend;
pub use backend::connect_backend;
pub use data_mover_storage_core::{
    KB, MB, Result, error, time_util, transfer_concurrency, url_redact,
};
#[allow(unused_imports)]
mod model {
    pub use data_mover_acl as acl;
    pub use data_mover_model::*;
}
#[allow(unused_imports)]
mod storage {
    pub use data_mover_storage_core::storage::*;
    pub mod backends {
        pub use crate::backend as nfs;
        pub use data_mover_storage_core::storage::defaults::*;
    }
}
#[cfg(any(test, feature = "test-support"))]
#[allow(unused_imports)]
use data_mover_storage_core::testing::{AssertTestError, AssertTestValue};

#[allow(unused_imports)]
#[allow(clippy::wildcard_imports)]
use data_mover_storage_core::storage::defaults::*;
