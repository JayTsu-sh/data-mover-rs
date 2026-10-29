//! Built-in adapter assembly.
#[cfg(feature = "backend-cifs")]
pub(crate) use data_mover_backend_cifs::backend as cifs;
#[cfg(feature = "backend-hdfs")]
pub(crate) use data_mover_backend_hdfs::backend as hdfs;
#[cfg(feature = "backend-local")]
pub(crate) use data_mover_backend_local::backend as local;
#[cfg(feature = "backend-nfs")]
pub(crate) use data_mover_backend_nfs::backend as nfs;
#[cfg(feature = "backend-s3")]
pub(crate) use data_mover_backend_s3::backend as s3;
