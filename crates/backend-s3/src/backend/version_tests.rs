//! Source versions on the S3 read source (ADR-0006 C6): what a describe pins, and that every read
//! afterwards reads the pinned version.

use futures::StreamExt as _;

use super::*;
use crate::model::{EntryKind, FailureClass, IdentityStrength, ObjectHistory, SourceVersion};
use crate::storage::backends::s3::source::path_scoped_bytes;
use crate::storage::listing::testing::listed as listed_current;
use crate::storage::{ReadSource, SourceDescriptor, StorageRoleFailure};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const KEY: &str = "object";

fn source(protocol: &Arc<MemoryS3>) -> TestResult<Arc<dyn ReadSource>> {
    let storage = connect(protocol.clone(), identity(), None)?;
    Ok(storage.read_source()?)
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

/// `PathScoped` identities include the path (P7 review, existing issue 7 / Q28): two keys holding
/// the same bytes — every empty object has the same `ETag` — never share an identity key, whether
/// listed or described, and the listing and a describe of one unchanged object still agree. The
/// `"null"` delete markers of a suspended bucket are one per key too.
#[tokio::test]
async fn the_same_content_at_two_keys_has_two_identities() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    for key in ["a", "b", "d/a"] {
        let empty = Bytes::new();
        protocol
            .put_object(key, empty.clone().into(), &content_md5(&empty))
            .await
            .map_err(|e| format!("{e:?}"))?;
    }
    let storage = connect(protocol.clone(), identity(), None)?;
    let source = storage.read_source()?;
    let mut keys = std::collections::HashSet::new();
    for key in ["a", "b", "d/a"] {
        let described = source.describe(&StoragePath::new(key)?).await?;
        assert_eq!(
            described.source_identity.strength(),
            IdentityStrength::PathScoped
        );
        assert!(
            keys.insert(described.source_identity.identity_key()),
            "{key}"
        );
    }
    let namespace = storage.namespace()?;
    let (entries, _) = listed_current(&*namespace, &StoragePath::root()).await?;
    for listed in entries.iter().filter(|entry| entry.kind == EntryKind::File) {
        let described = source.describe(&listed.path).await?;
        assert_eq!(listed.source_identity, described.source_identity);
    }

    protocol.set_versioning(Versioning::Suspended);
    for key in ["a", "b"] {
        protocol
            .delete_object(key)
            .await
            .map_err(|e| format!("{e:?}"))?;
    }
    // Suspended now: a connection made now lists histories (the answer is taken at connection).
    let namespace = connect(protocol.clone(), identity(), None)?.namespace()?;
    let deleted: Vec<SourceDescriptor> = listed_current(&*namespace, &StoragePath::root())
        .await?
        .0
        .into_iter()
        .filter(|entry| {
            entry
                .listing
                .history
                .as_ref()
                .is_some_and(|history| history.current().is_none())
        })
        .collect();
    assert_eq!(deleted.len(), 2);
    assert_ne!(
        deleted[0].source_identity.identity_key(),
        deleted[1].source_identity.identity_key()
    );
    let markers: Vec<_> = deleted
        .iter()
        .filter_map(|entry| entry.listing.history.as_deref())
        .flat_map(ObjectHistory::records)
        .filter(|record| record.is_delete_marker())
        .map(|record| record.identity().identity_key())
        .collect();
    assert_eq!(markers.len(), 2);
    assert_ne!(markers[0], markers[1]);
    Ok(())
}

/// A listed identity names its path: the expert revalidation never takes an object listed at one
/// key for a describe of another key with the same bytes (a versioned describe goes through the
/// `ETag` override, so that is where a path mix-up would slip in).
#[tokio::test]
async fn a_listed_identity_never_matches_another_keys_describe() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    for key in ["a", "b"] {
        let bytes = Bytes::from_static(b"same");
        protocol
            .put_object(key, bytes.clone().into(), &content_md5(&bytes))
            .await
            .map_err(|e| format!("{e:?}"))?;
    }
    let storage = connect(protocol.clone(), identity(), None)?;
    let source = storage.read_source()?;
    let namespace = storage.namespace()?;
    let (entries, _) = listed_current(&*namespace, &StoragePath::root()).await?;
    let listed_a = entries
        .iter()
        .find(|entry| entry.path.as_str() == "a")
        .ok_or("a not listed")?;
    let described_a = source.describe(&StoragePath::new("a")?).await?;
    let described_b = source.describe(&StoragePath::new("b")?).await?;
    assert_eq!(
        described_b.source_identity.strength(),
        IdentityStrength::VersionScoped
    );
    assert!(source.observation_matches(&listed_a.source_identity, &described_a));
    assert!(!source.observation_matches(&listed_a.source_identity, &described_b));
    Ok(())
}

/// The path is length-prefixed, so no path / `ETag` split can spell another's bytes.
#[test]
fn the_path_is_length_prefixed() -> TestResult {
    assert_ne!(
        path_scoped_bytes(&StoragePath::new("ab")?, b"c"),
        path_scoped_bytes(&StoragePath::new("a")?, b"bc")
    );
    Ok(())
}
