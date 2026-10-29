//! Observation constraints remain independent of comparison timestamp options.

use futures::StreamExt as _;

use super::*;
use crate::integrity::{IntegrityFailure, IntegrityReport, MtimePrecision};

#[tokio::test]
async fn compare_accepts_observations_on_either_side_and_paths_still_work() -> Result {
    let fixture = Fixture::new(Some(b"payload"), Some(b"payload"))?;
    let source = storage(fixture.source_root.path())?;
    let destination = storage(fixture.destination_root.path())?;
    let path = StoragePath::new("object.bin")?;
    let source_observed = observed_file(&source, &path).await?;
    let destination_observed = observed_file(&destination, &path).await?;

    let both = IntegrityRequest {
        source: source.clone(),
        source_path: Box::new(source_observed.clone()),
        destination: destination.clone(),
        destination_path: destination_observed.clone(),
        plan: IntegrityPlan::default(),
        options: IntegrityOptions {
            mode: IntegrityMode::Content,
            ..IntegrityOptions::default()
        },
        cancel: tokio_util::sync::CancellationToken::new(),
    };
    assert!(compare(both).await?.matches());

    let source_observed_only = IntegrityRequest {
        source: source.clone(),
        source_path: source_observed,
        destination: destination.clone(),
        destination_path: path.clone(),
        plan: IntegrityPlan::default(),
        options: IntegrityOptions::default(),
        cancel: tokio_util::sync::CancellationToken::new(),
    };
    assert!(compare(source_observed_only).await?.matches());

    let destination_observed_only = IntegrityRequest {
        source,
        source_path: path,
        destination,
        destination_path: destination_observed,
        plan: IntegrityPlan::default(),
        options: IntegrityOptions::default(),
        cancel: tokio_util::sync::CancellationToken::new(),
    };
    assert!(compare(destination_observed_only).await?.matches());
    Ok(())
}

#[tokio::test]
async fn stale_or_foreign_observation_is_attributed_to_its_side() -> Result {
    let fixture = Fixture::new(Some(b"payload"), Some(b"payload"))?;
    let source = storage(fixture.source_root.path())?;
    let destination = storage(fixture.destination_root.path())?;
    let path = StoragePath::new("object.bin")?;
    let observed = observed_file(&source, &path).await?;

    let Err(foreign) = compare(IntegrityRequest {
        source: destination.clone(),
        source_path: observed.clone(),
        destination: destination.clone(),
        destination_path: path.clone(),
        plan: IntegrityPlan::default(),
        options: IntegrityOptions::default(),
        cancel: tokio_util::sync::CancellationToken::new(),
    })
    .await
    else {
        panic!("an observation from another storage must be rejected");
    };
    assert_eq!(foreign.side, IntegritySide::Source);
    assert!(matches!(
        foreign.failure,
        crate::storage::StorageRoleFailure::Entry(ref error)
            if error.class() == FailureClass::InvalidInput
    ));

    filetime::set_file_mtime(
        fixture.source_root.path().join("object.bin"),
        filetime::FileTime::from_unix_time(1_700_000_050, 0),
    )?;
    let Err(stale) = compare(IntegrityRequest {
        source,
        source_path: observed,
        destination,
        destination_path: path,
        plan: IntegrityPlan::default(),
        options: IntegrityOptions::default(),
        cancel: tokio_util::sync::CancellationToken::new(),
    })
    .await
    else {
        panic!("a changed entry must not pass an observation constraint");
    };
    assert_eq!(stale.side, IntegritySide::Source);
    assert!(matches!(
        stale.failure,
        crate::storage::StorageRoleFailure::Entry(ref error)
            if error.class() == FailureClass::Conflict
    ));
    Ok(())
}

#[tokio::test]
async fn observed_versions_are_compared_even_after_newer_versions_become_current() -> Result {
    use crate::model::{HistoryOrder, ObjectHistory, RecordKind, VersionRecord};
    use crate::storage::backends::s3::tests::{MemoryS3, Versioning};

    let source_protocol = std::sync::Arc::new(MemoryS3::default());
    let destination_protocol = std::sync::Arc::new(MemoryS3::default());
    source_protocol.set_versioning(Versioning::Enabled);
    destination_protocol.set_versioning(Versioning::Enabled);
    for protocol in [&source_protocol, &destination_protocol] {
        protocol
            .put_version("object", "v1", bytes::Bytes::from_static(b"same"))
            .await;
    }
    let source = crate::storage::backends::s3::connect(
        source_protocol.clone(),
        BackendIdentity::new(BackendKind::S3, "integrity-versioned-source")?,
        None,
    )?;
    let destination = crate::storage::backends::s3::connect(
        destination_protocol.clone(),
        BackendIdentity::new(BackendKind::S3, "integrity-versioned-destination")?,
        None,
    )?;
    let path = StoragePath::new("object")?;
    let versioned = |described: crate::storage::SourceDescriptor| -> Result<ObservedEntry> {
        let record = VersionRecord::new(
            Some("v1".into()),
            true,
            RecordKind::Object {
                size: described.size.ok_or("v1 must have a size")?,
                etag: String::new(),
                modified: None,
            },
            described.source_identity.clone(),
            None,
        )?;
        let history = ObjectHistory::whole(vec![record], HistoryOrder::Server)?;
        Ok(ObservedEntry::new(
            described.path,
            described.kind,
            described.size,
            None,
            described.source_identity,
        )?
        .with_history(Some(Box::new(history)))
        .unwrap_or_else(|error| panic!("{error}")))
    };
    let source_observed = versioned(source.read_source()?.describe(&path).await?)?;
    let destination_observed = versioned(destination.read_source()?.describe(&path).await?)?;

    source_protocol
        .put_version("object", "v2", bytes::Bytes::from_static(b"AAAA"))
        .await;
    destination_protocol
        .put_version("object", "v2", bytes::Bytes::from_static(b"BBBB"))
        .await;

    let observed_report = compare(IntegrityRequest {
        source: source.clone(),
        source_path: source_observed,
        destination: destination.clone(),
        destination_path: destination_observed,
        plan: IntegrityPlan::default(),
        options: IntegrityOptions {
            mode: IntegrityMode::Content,
            ..IntegrityOptions::default()
        },
        cancel: tokio_util::sync::CancellationToken::new(),
    })
    .await?;
    assert!(observed_report.matches());

    let current_report = compare(IntegrityRequest {
        source,
        source_path: path.clone(),
        destination,
        destination_path: path,
        plan: IntegrityPlan::default(),
        options: IntegrityOptions {
            mode: IntegrityMode::Content,
            ..IntegrityOptions::default()
        },
        cancel: tokio_util::sync::CancellationToken::new(),
    })
    .await?;
    assert!(matches!(
        current_report.differences(),
        [IntegrityDifference::Content { .. }]
    ));
    Ok(())
}

async fn scanned_s3_source(
    listed: StorageTimestamp,
    head: StorageTimestamp,
) -> Result<(Storage, ObservedEntry)> {
    use crate::storage::ListRequest;
    use crate::storage::backends::s3::tests::MemoryS3;

    let protocol = std::sync::Arc::new(MemoryS3::default());
    protocol
        .objects
        .lock()
        .await
        .insert("object".into(), bytes::Bytes::from_static(b"same"));
    *protocol.last_modified.lock().await = Some(listed);
    let storage = crate::storage::backends::s3::connect(
        protocol.clone(),
        BackendIdentity::new(BackendKind::S3, "observed-time-s3")?,
        None,
    )?;
    let root = StoragePath::root();
    let mut listing = storage.namespace()?.list(ListRequest::new(
        root.clone(),
        tokio_util::sync::CancellationToken::new(),
    ));
    let mut observed = None;
    while let Some(page) = listing.next().await {
        let page = page?;
        assert!(page.failures.is_empty(), "the S3 listing must succeed");
        for child in page.children {
            let described = child.into_descriptor(&root)?;
            let modified = described
                .inline_timestamps()
                .and_then(|timestamps| timestamps.modified);
            observed = Some(ObservedEntry::new(
                described.path,
                described.kind,
                described.size,
                modified,
                described.source_identity,
            )?);
        }
    }
    *protocol.last_modified.lock().await = Some(head);
    Ok((storage, observed.ok_or("S3 object was not observed")?))
}

fn observed_time_request(
    source: Storage,
    observed: ObservedEntry,
    destination: Storage,
) -> Result<IntegrityRequest<ObservedEntry>> {
    Ok(IntegrityRequest {
        source,
        source_path: observed,
        destination,
        destination_path: StoragePath::new("object")?,
        plan: IntegrityPlan::default(),
        options: IntegrityOptions {
            mode: IntegrityMode::Content,
            mtime_precision: MtimePrecision::Exact,
            mtime_tolerance: std::time::Duration::from_mins(1),
        },
        cancel: tokio_util::sync::CancellationToken::new(),
    })
}

fn fractional_listing_time() -> Result<StorageTimestamp> {
    Ok(StorageTimestamp::new(
        1_700_000_000_321_000_000,
        TimePrecision::Milliseconds,
    )?)
}

fn head_time(seconds: i128) -> Result<StorageTimestamp> {
    Ok(StorageTimestamp::new(
        seconds * 1_000_000_000,
        TimePrecision::Seconds,
    )?)
}

fn assert_stale_source(result: std::result::Result<IntegrityReport, IntegrityFailure>) {
    let Err(failure) = result else {
        panic!("a genuinely changed timestamp must reject the observation");
    };
    assert_eq!(failure.side, IntegritySide::Source);
    assert!(matches!(
        failure.failure,
        crate::storage::StorageRoleFailure::Entry(ref error)
            if error.class() == FailureClass::Conflict
    ));
}

#[tokio::test]
async fn observed_s3_listing_time_accepts_unchanged_second_precision_head() -> Result {
    let (storage, observed) =
        scanned_s3_source(fractional_listing_time()?, head_time(1_700_000_000)?).await?;
    assert_eq!(observed.modified(), Some(fractional_listing_time()?));
    let report = compare(observed_time_request(storage.clone(), observed, storage)?).await?;
    assert!(report.matches(), "unexpected {:?}", report.differences());
    assert_eq!(report.compared_bytes(), 4);
    Ok(())
}

#[tokio::test]
async fn observed_s3_listing_time_rejects_changed_head_despite_comparison_tolerance() -> Result {
    let (storage, observed) =
        scanned_s3_source(fractional_listing_time()?, head_time(1_700_000_002)?).await?;
    assert_stale_source(compare(observed_time_request(storage.clone(), observed, storage)?).await);
    Ok(())
}

async fn inline_time_request(seconds: i128) -> Result<IntegrityRequest<ObservedEntry>> {
    let source = described_storage("observed-time-inline", b"same", Some(head_time(seconds)?))?;
    let path = StoragePath::new("object")?;
    let described = source.read_source()?.describe(&path).await?;
    let observed = ObservedEntry::new(
        path,
        described.kind,
        described.size,
        Some(fractional_listing_time()?),
        described.source_identity,
    )?;
    observed_time_request(source.clone(), observed, source)
}

#[tokio::test]
async fn observed_time_accepts_unchanged_second_precision_inline_description() -> Result {
    let report = compare(inline_time_request(1_700_000_000).await?).await?;
    assert!(report.matches(), "unexpected {:?}", report.differences());
    assert_eq!(report.compared_bytes(), 4);
    Ok(())
}

#[tokio::test]
async fn observed_time_rejects_changed_inline_time_despite_comparison_tolerance() -> Result {
    assert_stale_source(compare(inline_time_request(1_700_000_002).await?).await);
    Ok(())
}
