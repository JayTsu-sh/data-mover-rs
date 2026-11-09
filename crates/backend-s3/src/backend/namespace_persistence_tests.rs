//! 目标持久化复核不能改写对象，也不能将缺失、替换或取消当作成功。

use super::*;
use crate::model::{
    BackendIdentity, BackendKind, IdentityStrength, SourceIdentity, SourceVersion,
    StorageTimestamp, TimePrecision,
};
use tokio_util::sync::CancellationToken;

fn assert_class(result: Result<(), StorageRoleFailure>, class: FailureClass) {
    match result {
        Err(StorageRoleFailure::Entry(error)) => assert_eq!(error.class(), class),
        other => panic!("expected entry failure {class:?}, got {other:?}"),
    }
}

#[tokio::test]
async fn persistence_confirms_current_empty_and_slash_objects_with_one_head() -> TestResult {
    let protocol = bucket(&[("key", b"bytes"), ("empty", b""), ("slash/", b"content")]).await?;
    let namespace = namespace(&protocol)?;
    for key in ["key", "empty", "slash/"] {
        let expected = namespace.stat(&path(key)).await?;
        let objects = protocol.objects.lock().await.clone();
        let before = protocol.head_versions.lock().await.len();
        namespace
            .persist_observed(&expected, CancellationToken::new())
            .await?;
        assert_eq!(protocol.head_versions.lock().await.len(), before + 1);
        assert_eq!(*protocol.objects.lock().await, objects);
    }
    assert!(protocol.versions.lock().await.is_empty());
    assert!(protocol.version_deletes().is_empty());
    assert_eq!(*protocol.tag_reads.lock().await, 0);
    Ok(())
}

#[tokio::test]
async fn persistence_refuses_replacement_size_time_and_missing_target() -> TestResult {
    let protocol = bucket(&[("key", b"before")]).await?;
    let namespace = namespace(&protocol)?;
    let expected = namespace.stat(&path("key")).await?;
    for replacement in [b"after!".as_slice(), b"longer replacement"] {
        protocol
            .objects
            .lock()
            .await
            .insert("key".into(), Bytes::copy_from_slice(replacement));
        assert_class(
            namespace
                .persist_observed(&expected, CancellationToken::new())
                .await,
            FailureClass::Conflict,
        );
    }
    protocol
        .objects
        .lock()
        .await
        .insert("key".into(), Bytes::from_static(b"before"));
    *protocol.last_modified.lock().await = Some(StorageTimestamp::new(
        1_000_000_000,
        TimePrecision::Seconds,
    )?);
    let timed = namespace.stat(&path("key")).await?;
    *protocol.last_modified.lock().await = Some(StorageTimestamp::new(
        2_000_000_000,
        TimePrecision::Seconds,
    )?);
    assert_class(
        namespace
            .persist_observed(&timed, CancellationToken::new())
            .await,
        FailureClass::Conflict,
    );
    protocol.objects.lock().await.remove("key");
    assert_class(
        namespace
            .persist_observed(&expected, CancellationToken::new())
            .await,
        FailureClass::NotFound,
    );
    Ok(())
}

#[tokio::test]
async fn persistence_rejects_incomplete_foreign_and_synthetic_evidence_without_io() -> TestResult {
    let protocol = bucket(&[("key", b"bytes"), ("dir/child", b"child")]).await?;
    let namespace = namespace(&protocol)?;
    let expected = namespace.stat(&path("key")).await?;
    let mut missing_etag = expected.clone();
    missing_etag.content_version = None;
    let mut missing_size = expected.clone();
    missing_size.size = None;
    let mut foreign = expected.clone();
    foreign.source_identity = SourceIdentity::new(
        BackendIdentity::new(BackendKind::S3, "foreign")?,
        IdentityStrength::PathScoped,
        b"identity",
    )?;
    let mut listed = expected.clone();
    listed.listing.listing_only = true;
    let directory = namespace.stat(&path("dir")).await?;
    let root = namespace.stat(&StoragePath::root()).await?;
    let before = protocol.head_versions.lock().await.len();
    for rejected in [missing_etag, missing_size, foreign, listed, directory, root] {
        assert_class(
            namespace
                .persist_observed(&rejected, CancellationToken::new())
                .await,
            FailureClass::Unsupported,
        );
    }
    assert_eq!(protocol.head_versions.lock().await.len(), before);
    Ok(())
}

#[tokio::test]
async fn persistence_preserves_typed_head_failures_and_pre_cancel_makes_no_request() -> TestResult {
    let protocol = bucket(&[("key", b"bytes")]).await?;
    let namespace = namespace(&protocol)?;
    let expected = namespace.stat(&path("key")).await?;
    *protocol.head_failure.lock().await = Some((
        "key".into(),
        S3ProtocolFailure::entry(
            FailureClass::PermissionDenied,
            Transience::Permanent,
            "permission refused",
        ),
    ));
    assert_class(
        namespace
            .persist_observed(&expected, CancellationToken::new())
            .await,
        FailureClass::PermissionDenied,
    );
    *protocol.head_failure.lock().await = Some((
        "key".into(),
        S3ProtocolFailure::session(
            FailureClass::Connectivity,
            Transience::Transient,
            "transport disconnected",
        ),
    ));
    let failure = namespace
        .persist_observed(&expected, CancellationToken::new())
        .await;
    assert!(
        matches!(failure, Err(StorageRoleFailure::Session(ref error)) if error.class() == FailureClass::Connectivity && error.transience() == Transience::Transient),
        "{failure:?}"
    );
    let before = protocol.head_versions.lock().await.len();
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert_class(
        namespace.persist_observed(&expected, cancel).await,
        FailureClass::Cancelled,
    );
    assert_eq!(protocol.head_versions.lock().await.len(), before);
    Ok(())
}

#[tokio::test]
async fn persistence_pins_historical_versions_and_rejects_ignored_version_selection() -> TestResult
{
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    put(&protocol, "key", b"old").await?;
    let storage = connect(protocol.clone(), identity(), None)?;
    let source = storage.read_source()?;
    let expected = source.describe(&path("key")).await?;
    assert!(matches!(expected.version, SourceVersion::Id(_)));
    put(&protocol, "key", b"new").await?;
    let namespace = storage.namespace()?;
    let before = protocol.head_versions.lock().await.len();
    namespace
        .persist_observed(&expected, CancellationToken::new())
        .await?;
    assert_eq!(protocol.head_versions.lock().await.len(), before + 1);
    assert_eq!(
        protocol.head_versions.lock().await.last(),
        Some(&expected.version.version_id().map(str::to_owned))
    );
    *protocol.ignores_version_id.lock().await = true;
    assert_class(
        namespace
            .persist_observed(&expected, CancellationToken::new())
            .await,
        FailureClass::Unsupported,
    );
    assert_eq!(protocol.versions.lock().await.len(), 2);
    assert!(protocol.version_deletes().is_empty());
    Ok(())
}

#[tokio::test]
async fn nas_publication_without_a_verified_persistence_contract_remains_unsupported() -> TestResult
{
    let protocol = Arc::new(MemoryS3::nas_bucket());
    put(&protocol, "key", b"bytes").await?;
    let namespace = namespace(&protocol)?;
    let expected = namespace.stat(&path("key")).await?;
    let before = protocol.head_versions.lock().await.len();
    assert_class(
        namespace
            .persist_observed(&expected, CancellationToken::new())
            .await,
        FailureClass::Unsupported,
    );
    assert_eq!(protocol.head_versions.lock().await.len(), before);
    Ok(())
}

#[tokio::test]
async fn selected_marker_can_be_described_and_persisted_without_becoming_data() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    put(&protocol, "key", b"old").await?;
    let marker = protocol
        .write_delete_marker("key")
        .await
        .map_err(|error| format!("{error:?}"))?
        .version_id
        .ok_or("marker receipt missing")?;
    put(&protocol, "key", b"current").await?;
    let storage = connect(protocol.clone(), identity(), None)?;
    let described = storage
        .read_source()?
        .describe_version(&path("key"), &SourceVersion::Id(marker.clone()))
        .await?;
    assert_eq!(described.size, None);
    assert!(described.listing.listing_only);
    assert!(described.listing.history.as_ref().is_some_and(
        |history| history.records().len() == 1 && history.records()[0].is_delete_marker()
    ));
    storage
        .namespace()?
        .persist_observed(&described, CancellationToken::new())
        .await?;
    let mut current = described;
    current.version = SourceVersion::Current;
    assert_class(
        storage
            .namespace()?
            .persist_observed(&current, CancellationToken::new())
            .await,
        FailureClass::Conflict,
    );
    assert!(protocol.version_deletes().is_empty());
    assert_eq!(
        protocol.objects.lock().await.get("key").map(AsRef::as_ref),
        Some(b"current".as_slice())
    );
    Ok(())
}

#[tokio::test]
async fn current_marker_confirmation_is_readonly_and_rejects_replacement_or_loss() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    put(&protocol, "key", b"old").await?;
    let marker = protocol
        .write_delete_marker("key")
        .await
        .map_err(|error| format!("{error:?}"))?
        .version_id
        .ok_or("marker receipt missing")?;
    let storage = connect(protocol.clone(), identity(), None)?;
    let source = storage.read_source()?;
    let mut expected = source
        .describe_version(&path("key"), &SourceVersion::Id(marker.clone()))
        .await?;
    expected.version = SourceVersion::Current;
    let namespace = storage.namespace()?;
    namespace
        .persist_observed(&expected, CancellationToken::new())
        .await?;
    assert!(matches!(source.describe(&path("key")).await,
        Err(StorageRoleFailure::Entry(ref error)) if error.class() == FailureClass::NotFound));
    for range in [None, Some(0..0)] {
        let result = source
            .read(ReadRequest {
                path: path("key"),
                range,
                expected_source: Some(expected.source_identity.clone()),
                maximum_chunk_bytes: 4,
                read_inflight: 1,
                read_budget: None,
                cancel: CancellationToken::new(),
                source_qos: None,
                version: SourceVersion::Id(marker.clone()),
            })
            .await;
        assert!(
            matches!(result, Err(StorageRoleFailure::Entry(ref error)) if error.class() == FailureClass::NotFound)
        );
    }
    protocol
        .write_delete_marker("key")
        .await
        .map_err(|error| format!("{error:?}"))?;
    assert_class(
        namespace
            .persist_observed(&expected, CancellationToken::new())
            .await,
        FailureClass::Conflict,
    );
    protocol
        .versions
        .lock()
        .await
        .remove(&("key".into(), marker));
    assert_class(
        namespace
            .persist_observed(&expected, CancellationToken::new())
            .await,
        FailureClass::NotFound,
    );
    assert!(protocol.version_deletes().is_empty());
    assert_eq!(*protocol.tag_reads.lock().await, 0);
    Ok(())
}

#[tokio::test]
async fn marker_persistence_rejects_foreign_or_unbound_evidence_and_precancel_without_io()
-> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    let marker = protocol
        .write_delete_marker("key")
        .await
        .map_err(|error| format!("{error:?}"))?
        .version_id
        .ok_or("marker receipt missing")?;
    let storage = connect(protocol.clone(), identity(), None)?;
    let expected = storage
        .read_source()?
        .describe_version(&path("key"), &SourceVersion::Id(marker))
        .await?;
    let mut foreign = expected.clone();
    foreign.source_identity = SourceIdentity::new(
        BackendIdentity::new(BackendKind::S3, "foreign")?,
        IdentityStrength::PathScoped,
        b"identity",
    )?;
    let mut wrong_version = expected.clone();
    wrong_version.version = SourceVersion::Id("other".into());
    let mut content = expected.clone();
    content.content_version = Some(Bytes::from_static(b"etag"));
    let mut sized = expected.clone();
    sized.size = Some(0);
    let mut data = expected.clone();
    data.listing.listing_only = false;
    let namespace = storage.namespace()?;
    let before = protocol.head_versions.lock().await.len();
    for rejected in [foreign, wrong_version, content, sized, data] {
        assert_class(
            namespace
                .persist_observed(&rejected, CancellationToken::new())
                .await,
            FailureClass::Unsupported,
        );
    }
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert_class(
        namespace.persist_observed(&expected, cancel).await,
        FailureClass::Cancelled,
    );
    assert_eq!(protocol.head_versions.lock().await.len(), before);
    assert!(protocol.version_deletes().is_empty());
    Ok(())
}

#[tokio::test]
async fn mutable_null_marker_requires_time_evidence_and_rechecks_it() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Suspended);
    protocol
        .write_delete_marker("key")
        .await
        .map_err(|error| format!("{error:?}"))?;
    let storage = connect(protocol.clone(), identity(), None)?;
    let source = storage.read_source()?;
    let namespace = storage.namespace()?;
    let missing = source
        .describe_version(&path("key"), &SourceVersion::Id("null".into()))
        .await?;
    let before = protocol.head_versions.lock().await.len();
    assert_class(
        namespace
            .persist_observed(&missing, CancellationToken::new())
            .await,
        FailureClass::Unsupported,
    );
    assert_eq!(protocol.head_versions.lock().await.len(), before);
    *protocol.last_modified.lock().await = Some(StorageTimestamp::new(
        1_000_000_000,
        TimePrecision::Seconds,
    )?);
    let expected = source
        .describe_version(&path("key"), &SourceVersion::Id("null".into()))
        .await?;
    namespace
        .persist_observed(&expected, CancellationToken::new())
        .await?;
    *protocol.last_modified.lock().await = Some(StorageTimestamp::new(
        2_000_000_000,
        TimePrecision::Seconds,
    )?);
    assert_class(
        namespace
            .persist_observed(&expected, CancellationToken::new())
            .await,
        FailureClass::Conflict,
    );
    assert!(protocol.version_deletes().is_empty());
    Ok(())
}
