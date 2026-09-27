//! Replaying a delete marker onto a destination (P7 H1, §14.1 D10): the primitive a caller uses
//! to rebuild a source key's history on a versioned destination.
//!
//! The caller (a sync engine, or `examples/s3_history_replay.rs`) first asks
//! [`destination_keeps_history`]. When it does, the caller walks the key's records —
//! `ObjectHistory::records()` of its entry, segment after segment — oldest to newest, strictly one
//! after another: each object version, zero-byte ones included, is a
//! [`transfer`](super::transfer) with `TransferRequest::with_source_version` to the final key;
//! each delete marker is one [`write_delete_marker`]. The destination's records then come in the
//! order they were written. A replay is not deduplicated: rerunning it adds the records again, so
//! a caller compares the destination's history first (D11). A destination that keeps no history
//! (unversioned or suspended) gets the latest state only, with the reason recorded (D12): a marker
//! there would remove data, so [`write_delete_marker`] refuses it.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use super::at_destination::acquire_for;
use super::directory_object::refuse_unholdable;
use super::{TransferFailure, TransferPhase, TransferSide};
use crate::model::StoragePath;
use crate::storage::{DeleteMarkerOutcome, PreflightPolicy, StagedDestination, Storage};

/// Writes a delete marker on `final_path` of `destination`, holding the same per-destination
/// lease a transfer of that file holds, so one key has one writer in this process.
///
/// Only a destination whose versioning is `Enabled` right now takes one (checked before every
/// call; a suspend landing between the check and the delete is a race the store's own answer
/// cannot reveal). Even there a store may remove data instead of covering it — DXN drops a
/// `"null"` version, `RustFS` keeps no versions of keys ending in `/` — which is reported as
/// [`DeleteMarkerOutcome::NullVersionRemoved`], never hidden. Once the delete was sent, a failure
/// to read its outcome leaves it unknown (`Transience::Unknown`): compare the key's history before
/// replaying again (D11).
///
/// # Errors
/// `Preflight` when the destination lends no staged destination or cannot hold the path (a
/// directory object on a filesystem); `Prepare` `Conflict` (transient) while a transfer in this
/// process writes the same file; `Publish` with the destination's failure otherwise — among them
/// `Unsupported` for a destination that keeps no delete markers or a bucket whose versioning is
/// not `Enabled`.
pub async fn write_delete_marker(
    destination: Storage,
    final_path: StoragePath,
    cancel: CancellationToken,
) -> Result<DeleteMarkerOutcome, TransferFailure> {
    refuse_unholdable(&final_path, &destination, TransferSide::Destination)?;
    let role = staged(&destination)?;
    let _lease = acquire_for(destination.identity(), &final_path)?;
    role.write_delete_marker(&final_path, cancel)
        .await
        .map_err(|error| {
            TransferFailure::role(TransferPhase::Publish, TransferSide::Destination, error)
        })
}

/// Whether `destination` keeps every record of a key right now — a bucket whose versioning is
/// `Enabled` — so a source history can be replayed onto it (D12: otherwise copy the latest state
/// only). Asked of the store now, not remembered.
///
/// # Errors
/// `Preflight` when the destination lends no staged destination; the destination's failure when
/// it cannot be asked.
pub async fn destination_keeps_history(destination: &Storage) -> Result<bool, TransferFailure> {
    staged(destination)?.keeps_history().await.map_err(|error| {
        TransferFailure::role(TransferPhase::Preflight, TransferSide::Destination, error)
    })
}

fn staged(destination: &Storage) -> Result<Arc<dyn StagedDestination>, TransferFailure> {
    destination
        .staged_destination(&PreflightPolicy::production())
        .map_err(|_| {
            TransferFailure::capability(
                TransferSide::Destination,
                "destination capability unavailable",
            )
        })
}
