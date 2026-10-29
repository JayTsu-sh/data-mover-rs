//! Lightweight rename evidence from metadata already obtained by the Local roles.

#[cfg(unix)]
use cap_std::fs::MetadataExt as _;
use cap_std::fs::{Dir, Metadata};

#[cfg(unix)]
use crate::time_util::checked_system_time_to_nanos;

use crate::model::RenameEvidence;

/// A connection-local device boundary, never a persistent dataset identifier.
#[cfg(unix)]
pub(super) fn root_device(root: &Dir) -> Option<u64> {
    root.dir_metadata().ok().map(|metadata| metadata.dev())
}

#[cfg(not(unix))]
pub(super) fn root_device(_root: &Dir) -> Option<u64> {
    None
}

/// Missing birth times, unsupported devices and nested mounts have no candidate.
#[cfg(unix)]
pub(super) fn evidence(metadata: &Metadata, root_device: Option<u64>) -> Option<RenameEvidence> {
    if root_device != Some(metadata.dev()) {
        return None;
    }
    let created = metadata.created().ok()?.into_std();
    let nanos = checked_system_time_to_nanos(created)?;
    Some(RenameEvidence::local_inode_birth_time(
        metadata.ino(),
        nanos,
    ))
}

#[cfg(not(unix))]
pub(super) fn evidence(_metadata: &Metadata, _root_device: Option<u64>) -> Option<RenameEvidence> {
    None
}

#[cfg(test)]
mod tests;
