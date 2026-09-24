//! Source versions on the S3 read source (ADR-0006 C6): what a describe pins, and that every read
//! afterwards reads the pinned version.

use futures::StreamExt as _;

use super::*;
use crate::model::{FailureClass, SourceVersion};
use crate::storage::{ReadSource, SourceDescriptor, StorageRoleFailure};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const KEY: &str = "object";

fn source(protocol: &Arc<MemoryS3>) -> TestResult<Arc<dyn ReadSource>> {
    let storage = connect(protocol.clone(), identity(), None)?;
    Ok(storage.read_source(&validation_policy())?)
}

async fn read_all(source: &dyn ReadSource, descriptor: &SourceDescriptor) -> TestResult<Vec<u8>> {
    let mut stream = source
        .read(ReadRequest {
            path: descriptor.path.clone(),
            range: None,
            expected_source: Some(descriptor.source_identity.clone()),
            maximum_chunk_bytes: 4,
            read_inflight: 1,
            read_budget: None,
            cancel: CancellationToken::new(),
            source_qos: None,
            version: descriptor.version().clone(),
        })
        .await?;
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        bytes.extend_from_slice(&chunk?);
    }
    Ok(bytes)
}

/// `Current` pins only a real version: an unversioned object, or one written before versioning
/// (`"null"`), stays `Current` and is guarded by its `ETag`. The `ETag` is the content version.
#[tokio::test]
async fn describing_current_pins_only_a_real_version() -> TestResult {
    for (reported, pinned) in [
        (None, SourceVersion::Current),
        (Some("null"), SourceVersion::Current),
        (Some(""), SourceVersion::Current),
        (Some("v1"), SourceVersion::Id("v1".into())),
    ] {
        let protocol = Arc::new(MemoryS3::default());
        protocol
            .objects
            .lock()
            .await
            .insert(KEY.into(), Bytes::from_static(b"payload"));
        *protocol.version.lock().await = reported.map(str::to_string);
        let descriptor = source(&protocol)?.describe(&StoragePath::new(KEY)?).await?;
        assert_eq!(*descriptor.version(), pinned, "{reported:?}");
        assert_eq!(
            descriptor.content_version.as_deref(),
            Some(etag_of(b"payload").as_bytes()),
            "{reported:?}"
        );
    }
    Ok(())
}

/// A version pinned at describe is what every later read returns, even after a newer version
/// became current — the transfer finishes with the object it started from.
#[tokio::test]
async fn a_pinned_read_survives_a_new_current_version() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol
        .put_version(KEY, "v1", Bytes::from_static(b"first"))
        .await;
    let source = source(&protocol)?;
    let descriptor = source.describe(&StoragePath::new(KEY)?).await?;
    assert_eq!(*descriptor.version(), SourceVersion::Id("v1".into()));

    protocol
        .put_version(KEY, "v2", Bytes::from_static(b"second, longer"))
        .await;

    assert_eq!(read_all(source.as_ref(), &descriptor).await?, b"first");
    Ok(())
}

/// `Id` describes and reads that version, not the current one.
#[tokio::test]
async fn a_named_version_is_described_and_read_as_it_is() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol
        .put_version(KEY, "v1", Bytes::from_static(b"first"))
        .await;
    protocol
        .put_version(KEY, "v2", Bytes::from_static(b"second, longer"))
        .await;
    let source = source(&protocol)?;
    assert!(source.supports_source_versions());
    let old = source
        .describe_version(&StoragePath::new(KEY)?, &SourceVersion::Id("v1".into()))
        .await?;
    let current = source.describe(&StoragePath::new(KEY)?).await?;
    assert_eq!(old.size, Some(5));
    assert_eq!(*old.version(), SourceVersion::Id("v1".into()));
    assert_ne!(old.source_identity, current.source_identity);
    assert_eq!(read_all(source.as_ref(), &old).await?, b"first");
    assert_eq!(
        read_all(source.as_ref(), &current).await?,
        b"second, longer"
    );
    Ok(())
}

/// A delete marker, or a version the bucket does not have, fails that entry — never the session.
#[tokio::test]
async fn a_deleted_or_missing_version_is_an_entry_not_found() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol
        .put_version(KEY, "v1", Bytes::from_static(b"first"))
        .await;
    protocol.put_delete_marker(KEY, "marker").await;
    let source = source(&protocol)?;
    for version in ["marker", "no-such-version"] {
        let result = source
            .describe_version(&StoragePath::new(KEY)?, &SourceVersion::Id(version.into()))
            .await;
        assert!(
            matches!(&result, Err(StorageRoleFailure::Entry(failure)) if failure.class() == FailureClass::NotFound),
            "{version}: {result:?}"
        );
    }
    // The version before the marker is still there to copy.
    let kept = source
        .describe_version(&StoragePath::new(KEY)?, &SourceVersion::Id("v1".into()))
        .await?;
    assert_eq!(read_all(source.as_ref(), &kept).await?, b"first");
    Ok(())
}

/// A store that ignores `?versionId=` answers with the current object; describing that as the
/// requested version would copy the wrong bytes under the right name.
#[tokio::test]
async fn a_store_ignoring_the_version_id_is_refused() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol
        .put_version(KEY, "v1", Bytes::from_static(b"first"))
        .await;
    protocol
        .put_version(KEY, "v2", Bytes::from_static(b"second"))
        .await;
    *protocol.ignores_version_id.lock().await = true;
    let result = source(&protocol)?
        .describe_version(&StoragePath::new(KEY)?, &SourceVersion::Id("v1".into()))
        .await;
    assert!(
        matches!(&result, Err(StorageRoleFailure::Entry(failure)) if failure.class() == FailureClass::Unsupported),
        "{result:?}"
    );
    Ok(())
}
