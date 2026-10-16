//! Writing a delete marker on a final key (P7 H1), over the in-memory S3: only while versioning
//! is `Enabled`, and each store behaviour reported as what it is.

use std::slice;
use std::sync::Arc;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use super::super::S3StagedDestination;
use super::outcome;
use crate::model::{FailureClass, StoragePath};
use crate::storage::backends::s3::tests::{MemoryS3, Versioning, content_md5, identity};
use crate::storage::backends::s3::{S3DeleteReply, S3Protocol as _, S3VersionFacts};
use crate::storage::{DeleteMarkerOutcome, StagedDestination, StorageRoleFailure};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

async fn put(protocol: &MemoryS3, key: &str, body: &'static [u8]) -> TestResult {
    let bytes = Bytes::from_static(body);
    protocol
        .put_object(key, bytes.clone(), &content_md5(&bytes))
        .await
        .map_err(|e| format!("{e:?}"))?;
    Ok(())
}

async fn mark(
    protocol: &Arc<MemoryS3>,
    key: &str,
) -> Result<DeleteMarkerOutcome, StorageRoleFailure> {
    S3StagedDestination::new(protocol.clone(), identity())
        .write_delete_marker(
            &StoragePath::new(key).unwrap_or_else(|error| panic!("{error}")),
            CancellationToken::new(),
        )
        .await
}

/// `id kind size` of each record of `key`, oldest first.
async fn history(protocol: &MemoryS3, key: &str) -> TestResult<Vec<String>> {
    let mut records = protocol
        .list_versions(key)
        .await
        .map_err(|e| format!("{e:?}"))?;
    records.reverse();
    Ok(records
        .iter()
        .map(|record| {
            let kind = if record.delete_marker { "M" } else { "V" };
            format!("{} {kind} {}", record.version_id, record.size)
        })
        .collect())
}

fn class(result: &Result<DeleteMarkerOutcome, StorageRoleFailure>) -> Option<FailureClass> {
    match result {
        Err(StorageRoleFailure::Entry(failure)) => Some(failure.class()),
        _ => None,
    }
}

/// Unversioned or suspended, a `DeleteObject` would remove data: refused before anything is sent,
/// and the object is still there.
#[tokio::test]
async fn delete_marker_is_refused_on_unversioned_and_suspended_buckets() -> TestResult {
    for mode in [Versioning::Off, Versioning::Suspended] {
        let protocol = Arc::new(MemoryS3::default());
        protocol.set_versioning(mode);
        put(&protocol, "d/", b"hello").await?;
        let refused = mark(&protocol, "d/").await;
        assert_eq!(class(&refused), Some(FailureClass::Unsupported), "{mode:?}");
        assert_eq!(
            protocol.objects.lock().await.get("d/"),
            Some(&Bytes::from_static(b"hello")),
            "{mode:?}"
        );
    }
    Ok(())
}

/// Enabled: a new marker, the key's newest record, above every version it covers.
#[tokio::test]
async fn a_delete_marker_is_written_on_an_enabled_bucket() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    put(&protocol, "m/", b"fives").await?;
    let written = mark(&protocol, "m/").await?;
    assert_eq!(
        written,
        DeleteMarkerOutcome::Written {
            version_id: "v0002".into()
        }
    );
    assert_eq!(history(&protocol, "m/").await?, ["v0001 V 5", "v0002 M 0"]);
    // A key without any record gets a marker too.
    assert!(matches!(
        mark(&protocol, "never").await?,
        DeleteMarkerOutcome::Written { .. }
    ));
    Ok(())
}

/// `MinIO` / DXN: on a key whose newest record already is a marker the store adds none — merged,
/// and said so.
#[tokio::test]
async fn a_merged_marker_is_reported_merged() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    protocol.merge_delete_markers();
    put(&protocol, "m/", b"fives").await?;
    let first = mark(&protocol, "m/").await?;
    let second = mark(&protocol, "m/").await?;
    assert_eq!(
        (first, second),
        (
            DeleteMarkerOutcome::Written {
                version_id: "v0002".into()
            },
            DeleteMarkerOutcome::Merged {
                version_id: "v0002".into()
            }
        )
    );
    assert_eq!(history(&protocol, "m/").await?, ["v0001 V 5", "v0002 M 0"]);
    Ok(())
}

/// DXN: a delete that removes the key's `"null"` version instead of covering it is reported.
#[tokio::test]
async fn a_removed_null_version_is_reported() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Suspended);
    put(&protocol, "n/", b"null!").await?;
    protocol.set_versioning(Versioning::Enabled);
    protocol.drop_null_versions_on_delete();
    let removed = mark(&protocol, "n/").await?;
    assert_eq!(
        removed,
        DeleteMarkerOutcome::NullVersionRemoved {
            version_id: "null".into(),
            marker: None
        }
    );
    assert!(history(&protocol, "n/").await?.is_empty());
    Ok(())
}

/// Versioning is asked on every call, never remembered: the same destination writes a marker, and
/// once the bucket is suspended refuses the next one, leaving the object as it is.
#[tokio::test]
async fn versioning_is_checked_on_every_call() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    let destination = S3StagedDestination::new(protocol.clone(), identity());
    assert!(destination.keeps_history().await?);
    put(&protocol, "a", b"1").await?;
    put(&protocol, "b", b"2").await?;
    let path = |key: &str| StoragePath::new(key).unwrap_or_else(|error| panic!("{error}"));
    let first = destination
        .write_delete_marker(&path("a"), CancellationToken::new())
        .await;
    assert!(
        matches!(first, Ok(DeleteMarkerOutcome::Written { .. })),
        "{first:?}"
    );
    protocol.set_versioning(Versioning::Suspended);
    assert!(!destination.keeps_history().await?);
    let second = destination
        .write_delete_marker(&path("b"), CancellationToken::new())
        .await;
    assert_eq!(class(&second), Some(FailureClass::Unsupported));
    assert_eq!(
        protocol.objects.lock().await.get("b"),
        Some(&Bytes::from_static(b"2"))
    );
    Ok(())
}

/// DXN's probe sequence: a key holding the `"null"` version and a newer one loses the `"null"`
/// version and gets a marker — reported with both.
#[tokio::test]
async fn a_removed_null_version_with_a_marker_names_both() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Suspended);
    put(&protocol, "n", b"null!").await?;
    protocol.set_versioning(Versioning::Enabled);
    put(&protocol, "n", b"second").await?;
    protocol.drop_null_versions_on_delete();
    let outcome = mark(&protocol, "n").await?;
    let DeleteMarkerOutcome::NullVersionRemoved { version_id, marker } = outcome else {
        return Err(format!("{outcome:?}").into());
    };
    assert_eq!((version_id.as_str(), marker.is_some()), ("null", true));
    assert_eq!(
        history(&protocol, "n").await?.len(),
        2,
        "the version and the marker"
    );
    Ok(())
}

/// Artifacts and the root are refused; a cancelled request sends nothing.
#[tokio::test]
async fn artifacts_and_cancelled_requests_write_nothing() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    put(&protocol, "k", b"1").await?;
    for key in ["", "p/.data-mover-x.upload"] {
        assert_eq!(
            class(&mark(&protocol, key).await),
            Some(FailureClass::InvalidInput),
            "{key:?}"
        );
    }
    let cancel = CancellationToken::new();
    cancel.cancel();
    let cancelled = S3StagedDestination::new(protocol.clone(), identity())
        .write_delete_marker(&StoragePath::new("k")?, cancel)
        .await;
    assert_eq!(class(&cancelled), Some(FailureClass::Cancelled));
    assert_eq!(history(&protocol, "k").await?, ["v0001 V 1"]);
    Ok(())
}

fn record(id: &str, marker: bool, latest: bool) -> S3VersionFacts {
    S3VersionFacts {
        version_id: id.into(),
        is_latest: latest,
        delete_marker: marker,
        size: 0,
        etag: String::new(),
    }
}

/// The outcome comes from the records, not the reply alone: a reply naming an old marker is not a
/// new one, and nothing new over a newest version is no outcome at all.
#[test]
fn outcomes_are_read_from_the_records() {
    let version = record("v1", false, true);
    let old_marker = record("m1", true, true);
    let reply = |id: &str| S3DeleteReply {
        delete_marker: true,
        version_id: Some(id.into()),
    };
    // StorageGRID: a new marker on top of an old one.
    let after = [record("m2", true, true), record("m1", true, false)];
    assert_eq!(
        outcome(slice::from_ref(&old_marker), &after, &reply("m2")),
        Some(DeleteMarkerOutcome::Written {
            version_id: "m2".into()
        })
    );
    // The reply names the existing marker and nothing changed: merged, not written.
    assert_eq!(
        outcome(
            slice::from_ref(&old_marker),
            slice::from_ref(&old_marker),
            &reply("m1")
        ),
        Some(DeleteMarkerOutcome::Merged {
            version_id: "m1".into()
        })
    );
    // A reply claiming a marker that the records do not show is no outcome.
    assert_eq!(
        outcome(
            slice::from_ref(&version),
            slice::from_ref(&version),
            &reply("m9")
        ),
        None
    );
    // RustFS lists a page's markers before its versions: the newest record is found by
    // `is_latest`, not by position.
    let grouped_before = [record("m1", true, true), record("v1", false, false)];
    let grouped_after = [record("m1", true, true), record("v1", false, false)];
    assert_eq!(
        outcome(&grouped_before, &grouped_after, &reply("m1")),
        Some(DeleteMarkerOutcome::Merged {
            version_id: "m1".into()
        })
    );
    // A suspended bucket replaces its "null" version by a "null" marker: same id, another kind.
    let null_before = [record("v2", false, true), record("null", false, false)];
    let null_after = [record("null", true, true), record("v2", false, false)];
    assert_eq!(
        outcome(&null_before, &null_after, &reply("null")),
        Some(DeleteMarkerOutcome::NullVersionRemoved {
            version_id: "null".into(),
            marker: Some("null".into())
        })
    );
}
