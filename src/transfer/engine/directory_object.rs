//! Directory objects in transfers (P7 F5, §14.1 D2): a file an object store keeps under a key
//! ending in `/` (`p/d/`), which a traversal lists as a file entry of `p/d`.
//!
//! - Between object stores it is copied like any file, to the same key.
//! - A filesystem (Local, NFS, CIFS, HDFS) cannot store content on a directory. A consumer decides
//!   each entry with [`directory_object_action`] before it builds any request: a zero-byte one (a
//!   folder marker) or a delete marker is nothing to do — the directory entry already creates the
//!   directory — and one holding content is skipped with [`DIRECTORY_OBJECT_SKIP_REASON`].
//!   data-mover builds no sidecar file: any sidecar name can collide with a real key. A consumer
//!   that wants one maps `final_path` itself; the engine copies that like any file.
//! - The engine backstop stays strict whatever the size: a final path ending in `/` on a
//!   destination that holds no directory objects, or a source path ending in `/` on a source that
//!   holds none, is refused at `Preflight` with `Unsupported` at that path, before any I/O
//!   ([`refuse_unholdable`]).
//! - A destination that writes a directory object only in one request (S3: one `PutObject` or
//!   `CopyObject`, P7 D13) declares how much that request carries
//!   ([`StagedDestination::directory_object_limit`]); once the source is described, a larger one
//!   — or one of unknown size — is refused the same way, before any destination I/O, with no
//!   fallback ([`refuse_unwritable`], which also asks [`StagedDestination::final_refusal`] what
//!   else the destination cannot write: an ONTAP NAS bucket's size and key limits, P7 §14.2).

use crate::model::{
    EntryOperationFailure, EntryVersion, FailureClass, ObservedEntry, Operation, StoragePath,
    Transience,
};
use crate::storage::{NamespaceCapabilities, StagedDestination, Storage, StorageRoleFailure};

use super::{TransferFailure, TransferPhase, TransferSide};

/// Why a directory object holding content is not copied to a destination that cannot hold one.
pub const DIRECTORY_OBJECT_SKIP_REASON: &str =
    "the destination cannot store content on a directory (an object key ending in '/')";

/// What a consumer does with one listed entry when copying to a destination (P7 F5, D2).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DirectoryObjectAction {
    /// Request the transfer as usual: the entry is no directory object, or the destination holds
    /// directory objects (an object store: the key is recreated with its bytes).
    Copy,
    /// Nothing to copy: a zero-byte directory object (a folder marker) or a delete marker, bound
    /// for a destination that cannot hold directory objects. The directory entry already creates
    /// the directory; nothing is lost.
    NothingToDo,
    /// Do not copy; record the reason ([`DIRECTORY_OBJECT_SKIP_REASON`]): the entry holds content
    /// the destination cannot store on a directory. Not a failure of the transfer job.
    Skip(&'static str),
}

/// Decides one listed entry for `destination` (its namespace's capabilities). Only directory
/// objects are decided here; any other entry is [`DirectoryObjectAction::Copy`] (the ordinary
/// rules apply). An entry of a traversal of every version is one record: a consumer copying the
/// current state to a filesystem passes the key's latest record, and a latest delete marker means
/// the key holds nothing now. An entry without a size counts as holding nothing.
#[must_use]
pub fn directory_object_action(
    entry: &ObservedEntry,
    destination: &NamespaceCapabilities,
) -> DirectoryObjectAction {
    if !entry.is_directory_object() || destination.directory_objects {
        return DirectoryObjectAction::Copy;
    }
    let delete_marker = entry.version().is_some_and(EntryVersion::is_delete_marker);
    if delete_marker || entry.size().unwrap_or(0) == 0 {
        DirectoryObjectAction::NothingToDo
    } else {
        DirectoryObjectAction::Skip(DIRECTORY_OBJECT_SKIP_REASON)
    }
}

/// The engine's backstop: `path` ending in `/` on a `storage` that holds no directory objects is
/// that side's `Unsupported` failure at `Preflight`, before any I/O.
pub(super) fn refuse_unholdable(
    path: &StoragePath,
    storage: &Storage,
    side: TransferSide,
) -> Result<(), TransferFailure> {
    if !path.names_directory_object() || storage.holds_directory_objects() {
        return Ok(());
    }
    let (operation, diagnostic) = match side {
        TransferSide::Destination => (Operation::Prepare, DIRECTORY_OBJECT_SKIP_REASON),
        TransferSide::Source | TransferSide::Orchestration => (
            Operation::Observe,
            "the source holds no directory objects: a path ending in '/' names no file",
        ),
    };
    Err(unsupported_at_preflight(path, side, operation, diagnostic))
}

/// What the destination cannot write, decided from the final path and the described size alone,
/// as the destination's `Unsupported` at `Preflight`, before any destination I/O; no fallback:
///
/// - P7 D13: a directory object bound for a destination that writes one only in a single request,
///   when that request cannot carry it — more than [`StagedDestination::directory_object_limit`]
///   bytes, or of unknown `size`;
/// - anything [`StagedDestination::final_refusal`] refuses (an ONTAP NAS bucket: over 5 GiB, or a
///   key it would store under another name, P7 §14.2).
pub(super) fn refuse_unwritable(
    path: &StoragePath,
    size: Option<u64>,
    destination: &dyn StagedDestination,
) -> Result<(), TransferFailure> {
    if let Some(limit) = destination.directory_object_limit()
        && path.names_directory_object()
        && size.is_none_or(|size| size > limit)
    {
        return Err(unsupported_at_preflight(
            path,
            TransferSide::Destination,
            Operation::Prepare,
            "the destination writes a directory object (a key ending in '/') only in one request, \
             which needs its size up front and cannot carry this one",
        ));
    }
    match destination.final_refusal(path, size) {
        Some(reason) => Err(unsupported_at_preflight(
            path,
            TransferSide::Destination,
            Operation::Prepare,
            &reason,
        )),
        None => Ok(()),
    }
}

fn unsupported_at_preflight(
    path: &StoragePath,
    side: TransferSide,
    operation: Operation,
    diagnostic: &str,
) -> TransferFailure {
    match EntryOperationFailure::new(
        path.clone(),
        operation,
        FailureClass::Unsupported,
        Transience::Permanent,
        diagnostic,
    ) {
        Ok(failure) => TransferFailure::role(
            TransferPhase::Preflight,
            side,
            StorageRoleFailure::Entry(failure),
        ),
        Err(_) => TransferFailure::orchestration(
            TransferPhase::Preflight,
            "the destination cannot write this path",
        ),
    }
}
