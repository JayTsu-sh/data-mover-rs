//! Writing a delete marker on a final key (P7 H1): the step that replays a source history's
//! delete marker onto a versioned destination, between the versions copied before and after it.
//!
//! - **Only on a bucket whose versioning is `Enabled`, checked right before every call**
//!   (`GetBucketVersioning`, never the state at connection). On an unversioned bucket the same
//!   `DeleteObject` removes the object, and on a suspended one it replaces the `"null"` version:
//!   both remove data, so both are refused (`Unsupported`) before any request that changes
//!   anything. A suspend that lands between the check and the delete is a documented race.
//! - The outcome is read from the key's records before and after the delete (the versions of
//!   exactly that key, one bounded listing each), not from the reply alone, since stores differ:
//!   a new marker is [`DeleteMarkerOutcome::Written`]; `MinIO` and Ceph RGW / DXN add none when
//!   the newest record already is a marker ([`DeleteMarkerOutcome::Merged`]); DXN drops a
//!   `"null"` version instead of only covering it ([`DeleteMarkerOutcome::NullVersionRemoved`],
//!   also logged as a warning); `StorageGRID` adds a marker every time. Anything else (no new
//!   marker and none newest) is a `Protocol` failure, never reported as written.
//! - Cancellation is honoured before the delete is sent; once sent, its outcome is read.

use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::{S3StagedDestination, classified_entry, role_failure};
use crate::model::{FailureClass, Operation, StoragePath, Transience};
use crate::storage::artifacts::is_artifact_path;
use crate::storage::backends::s3::{S3BucketVersioning, S3DeleteReply, S3Protocol, S3VersionFacts};
use crate::storage::{DeleteMarkerOutcome, StorageRoleFailure};

/// Why a delete marker is refused on a bucket whose versioning is not `Enabled`.
const NOT_ENABLED: &str = "a DeleteObject on an unversioned or suspended bucket removes data; delete markers are \
     written only while the bucket's versioning is Enabled";

impl<P: S3Protocol + 'static> S3StagedDestination<P> {
    pub(super) async fn delete_marker(
        &self,
        path: &StoragePath,
        cancel: &CancellationToken,
    ) -> Result<DeleteMarkerOutcome, StorageRoleFailure> {
        let key = path.as_str();
        if key.is_empty() || is_artifact_path(key) {
            return Err(refused(
                path,
                FailureClass::InvalidInput,
                "a delete marker needs an object key outside transfer artifacts",
            ));
        }
        cancelled(path, cancel)?;
        if !self.versioning_enabled_now(path).await? {
            return Err(refused(path, FailureClass::Unsupported, NOT_ENABLED));
        }
        let before = self.records(path).await?;
        cancelled(path, cancel)?;
        // Once sent the server may have acted, whatever the reply: every failure from here on
        // leaves the outcome unknown (a retry could add a second marker).
        let reply = self
            .protocol
            .write_delete_marker(key)
            .await
            .map_err(|failure| unknown(path, &role_failure(path, Operation::Publish, failure)))?;
        // Sent: from here on a failure leaves the outcome unknown, whatever its cause.
        let after = self
            .records(path)
            .await
            .map_err(|failure| unknown(path, &failure))?;
        let outcome = outcome(&before, &after, &reply).ok_or_else(|| {
            refused(
                path,
                FailureClass::Protocol,
                "the S3 DeleteObject left no delete marker as the key's newest record",
            )
        })?;
        if let DeleteMarkerOutcome::NullVersionRemoved { version_id, .. } = &outcome {
            warn!("S3 DeleteObject of {path} removed version {version_id} instead of covering it");
        }
        Ok(outcome)
    }

    /// `GetBucketVersioning` now, for `path`'s failures: whether it answers `Enabled`.
    pub(super) async fn versioning_enabled_now(
        &self,
        path: &StoragePath,
    ) -> Result<bool, StorageRoleFailure> {
        // A bucket without versions (an ONTAP NAS bucket, P7 §14.2) keeps no records: nothing to
        // ask.
        if !self.protocol.bucket_capabilities().versions {
            return Ok(false);
        }
        let versioning = self
            .protocol
            .bucket_versioning()
            .await
            .map_err(|failure| role_failure(path, Operation::Publish, failure))?;
        Ok(versioning == S3BucketVersioning::Enabled)
    }

    async fn records(&self, path: &StoragePath) -> Result<Vec<S3VersionFacts>, StorageRoleFailure> {
        self.protocol
            .list_versions(path.as_str())
            .await
            .map_err(|failure| role_failure(path, Operation::Publish, failure))
    }
}

/// What the delete did, from the key's records before and after it; `None` when it neither left
/// a new marker nor found the newest record already a marker.
pub(super) fn outcome(
    before: &[S3VersionFacts],
    after: &[S3VersionFacts],
    reply: &S3DeleteReply,
) -> Option<DeleteMarkerOutcome> {
    // A record is the same record only with the same id and kind: a suspended bucket replaces its
    // `"null"` version with a `"null"` marker.
    let same = |left: &S3VersionFacts, right: &S3VersionFacts| {
        left.version_id == right.version_id && left.delete_marker == right.delete_marker
    };
    let had = |record: &S3VersionFacts| before.iter().any(|old| same(old, record));
    let has = |record: &S3VersionFacts| after.iter().any(|new| same(new, record));
    let new_markers: Vec<&S3VersionFacts> = after
        .iter()
        .filter(|record| record.delete_marker && !had(record))
        .collect();
    // The reply names the marker when it can; otherwise the one new marker listed.
    let marker = reply
        .version_id
        .as_deref()
        .filter(|id| reply.delete_marker && new_markers.iter().any(|m| m.version_id == *id))
        .or_else(|| new_markers.first().map(|m| m.version_id.as_str()))
        .map(str::to_string);
    if let Some(removed) = before.iter().find(|record| !has(record)) {
        return Some(DeleteMarkerOutcome::NullVersionRemoved {
            version_id: removed.version_id.clone(),
            marker,
        });
    }
    if let Some(version_id) = marker {
        if new_markers.len() > 1 {
            // A retried request the server had already applied (the SDK retries transient
            // failures): the history holds more markers than were replayed.
            warn!(
                "S3 DeleteObject left {} new delete markers; reporting {version_id}",
                new_markers.len()
            );
        }
        return Some(DeleteMarkerOutcome::Written { version_id });
    }
    let newest = |records: &[S3VersionFacts]| {
        records
            .iter()
            .find(|record| record.is_latest)
            .map(|record| (record.version_id.clone(), record.delete_marker))
    };
    match (newest(before), newest(after)) {
        (Some((was, true)), Some((is, true))) if was == is => {
            Some(DeleteMarkerOutcome::Merged { version_id: is })
        }
        _ => None,
    }
}

fn cancelled(path: &StoragePath, cancel: &CancellationToken) -> Result<(), StorageRoleFailure> {
    if cancel.is_cancelled() {
        Err(classified_entry(
            path,
            Operation::Publish,
            FailureClass::Cancelled,
            Transience::Transient,
            "the delete marker was cancelled before it was written",
        ))
    } else {
        Ok(())
    }
}

/// A failure once the delete was sent — of the delete itself (a timeout, a 5xx: the server may have
/// acted) or of reading its outcome: this key's failure with its class kept but
/// `Transience::Unknown`, so it is neither retried as transient nor taken as final (the caller
/// compares the key's history first, D11). A session-wide cause surfaces again on the session's
/// next request.
fn unknown(path: &StoragePath, failure: &StorageRoleFailure) -> StorageRoleFailure {
    let class = match failure {
        StorageRoleFailure::Entry(entry) => entry.class(),
        StorageRoleFailure::Session(session) => session.class(),
    };
    classified_entry(
        path,
        Operation::Publish,
        class,
        Transience::Unknown,
        "the S3 DeleteObject was sent but its outcome could not be read: it is unknown whether a \
         delete marker was written; compare the key's history before replaying again",
    )
}

fn refused(path: &StoragePath, class: FailureClass, diagnostic: &str) -> StorageRoleFailure {
    classified_entry(
        path,
        Operation::Publish,
        class,
        Transience::Permanent,
        diagnostic,
    )
}

#[cfg(test)]
#[path = "delete_marker_tests.rs"]
mod tests;
