//! Independent integrity engine over shared storage roles.
pub mod integrity;
pub use integrity::*;
#[allow(unused_imports)]
mod model {
    pub use data_mover_acl as acl;
    pub use data_mover_model::*;
}
#[allow(unused_imports)]
mod storage {
    pub use data_mover_storage_core::storage::*;
    #[cfg(test)]
    pub mod backends {
        pub use data_mover_backend_cifs::backend as cifs;
        pub use data_mover_backend_hdfs::backend as hdfs;
        pub use data_mover_backend_local::backend as local;
        pub use data_mover_backend_nfs::backend as nfs;
        pub use data_mover_backend_s3::backend as s3;
    }
}
#[allow(unused_imports)]
use data_mover_storage_core::runtime;
#[cfg(test)]
#[allow(unused_imports)]
use data_mover_storage_core::testing::{AssertTestError, AssertTestValue};
pub use data_mover_storage_core::{KB, MB, Result, error, time_util};
