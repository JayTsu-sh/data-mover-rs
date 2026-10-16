#[cfg(unix)]
use std::ffi::OsStr;
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt as _;
#[cfg(unix)]
use std::path::Path;

use super::{LocalNamespace, StorageRoleFailure};
#[cfg(unix)]
use super::{checked, entry_failure, in_parent, io_failure};
#[cfg(unix)]
use crate::model::{FailureClass, Operation, SymlinkTargetEncoding};
use crate::storage::CreateSymlinkRequest;

#[cfg(unix)]
pub(super) async fn create(
    namespace: &LocalNamespace,
    request: CreateSymlinkRequest,
) -> Result<(), StorageRoleFailure> {
    let native = checked(&request.path, Operation::Namespace, false)?;
    let target = Path::new(OsStr::from_bytes(request.target.as_bytes())).to_path_buf();
    if request.target.encoding() != SymlinkTargetEncoding::UnixBytes
        || request.uid == Some(u32::MAX)
        || request.gid == Some(u32::MAX)
    {
        return Err(entry_failure(
            &request.path,
            Operation::Namespace,
            FailureClass::InvalidInput,
        ));
    }
    let times = rustix::fs::Timestamps {
        last_access: timestamp(request.accessed)
            .map_err(|error| io_failure(&request.path, Operation::Namespace, &error))?,
        last_modification: timestamp(request.modified)
            .map_err(|error| io_failure(&request.path, Operation::Namespace, &error))?,
    };
    let path = request.path.clone();
    namespace
        .blocking(&request.path, Operation::Namespace, move |root, _| {
            in_parent(root, &native, |parent, leaf| {
                // The target is opaque link contents; only the destination parent is confined.
                parent.symlink_contents(&target, leaf)?;
                if request.uid.is_some() || request.gid.is_some() {
                    rustix::fs::chownat(
                        parent,
                        leaf,
                        request.uid.map(rustix::fs::Uid::from_raw),
                        request.gid.map(rustix::fs::Gid::from_raw),
                        rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
                    )?;
                }
                if request.accessed.is_some() || request.modified.is_some() {
                    rustix::fs::utimensat(
                        parent,
                        leaf,
                        &times,
                        rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
                    )?;
                }
                super::sync_directory(parent)
            })
            .map_err(|error| io_failure(&path, Operation::Namespace, &error))
        })
        .await
}

#[cfg(unix)]
fn timestamp(
    value: Option<crate::model::StorageTimestamp>,
) -> std::io::Result<rustix::fs::Timespec> {
    match value {
        None => Ok(rustix::fs::Timespec {
            tv_sec: 0,
            tv_nsec: rustix::fs::UTIME_OMIT,
        }),
        Some(value) => Ok(rustix::fs::Timespec {
            tv_sec: value
                .unix_nanos()
                .div_euclid(1_000_000_000)
                .try_into()
                .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))?,
            tv_nsec: value
                .unix_nanos()
                .rem_euclid(1_000_000_000)
                .try_into()
                .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))?,
        }),
    }
}

#[cfg(not(unix))]
pub(super) async fn create(
    _namespace: &LocalNamespace,
    request: CreateSymlinkRequest,
) -> Result<(), StorageRoleFailure> {
    Err(super::entry_failure(
        &request.path,
        crate::model::Operation::Namespace,
        crate::model::FailureClass::Unsupported,
    ))
}
