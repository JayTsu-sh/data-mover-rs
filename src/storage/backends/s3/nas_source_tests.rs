//! An ONTAP NAS bucket as a source (P7 §14.2, N1), over the in-memory bucket in NAS mode: no
//! conditional reads, so each range response is checked against the described object, a body
//! that broke off is classified by one `HEAD`, and one `HEAD` after the last range closes the
//! read; no versions; no directory objects.

use futures::StreamExt as _;

use super::memory_nas::BodyCut;
use super::*;
use crate::model::{FailureClass, IdentityStrength, SourceVersion, Transience};
use crate::storage::{ByteStream, StorageRoleFailure};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const CHUNK: usize = 4;

/// A NAS bucket holding `source` = `content`, written over S3 (so it has its opaque `ETag`).
async fn nas_with(content: &'static [u8]) -> TestResult<Arc<MemoryS3>> {
    let protocol = Arc::new(MemoryS3::nas_bucket());
    let body = Bytes::from_static(content);
    protocol
        .put_object("source", body.clone(), &content_md5(&body))
        .await
        .map_err(|failure| format!("{failure:?}"))?;
    Ok(protocol)
}

/// Opens a read of all of `source`, `CHUNK` bytes per range, bound to what `describe` found.
async fn open(protocol: &Arc<MemoryS3>) -> TestResult<ByteStream> {
    let storage = connect(protocol.clone(), identity(), None)?;
    let source = storage.read_source()?;
    let path = StoragePath::new("source")?;
    let described = source.describe(&path).await?;
    Ok(source
        .read(ReadRequest {
            path,
            range: None,
            expected_source: Some(described.source_identity),
            maximum_chunk_bytes: CHUNK,
            read_inflight: 1,
            read_budget: None,
            cancel: CancellationToken::new(),
            source_qos: None,
            version: SourceVersion::Current,
        })
        .await?)
}

/// Reads the stream to its end: the bytes, or the first failure.
async fn drain(mut stream: ByteStream) -> Result<Vec<u8>, StorageRoleFailure> {
    let mut read = Vec::new();
    while let Some(chunk) = stream.next().await {
        read.extend_from_slice(&chunk?);
    }
    Ok(read)
}

fn is_read_conflict(result: &Result<Vec<u8>, StorageRoleFailure>) -> bool {
    matches!(
        result,
        Err(StorageRoleFailure::Entry(failure))
            if failure.class() == FailureClass::Conflict
                && failure.transience() == Transience::Permanent
    )
}

/// The described object reads whole, identified by its path and opaque `ETag` (never an MD5),
/// with no range pinned by version, and one `HEAD` after the last range (the read's own `HEAD`
/// plus that one).
#[tokio::test]
async fn a_nas_object_reads_whole_and_is_confirmed_once_at_the_end() -> TestResult {
    let protocol = nas_with(b"0123456789").await?;
    let stream = open(&protocol).await?;
    let heads_before = protocol.nas().heads;
    assert_eq!(drain(stream).await?, b"0123456789");
    assert_eq!(protocol.nas().ranges, 3);
    assert_eq!(
        protocol.nas().heads - heads_before,
        1,
        "one HEAD after the last range"
    );
    let reads = protocol.range_observations.lock().await;
    assert!(reads.iter().all(|read| read.version_id.is_none()));
    assert!(reads.iter().all(|read| read.etag.starts_with("\"nas-")));
    assert_ne!(reads[0].etag, etag_of(b"0123456789"));
    drop(reads);
    // An object bucket's reads are pinned: no closing HEAD.
    let object = Arc::new(MemoryS3::default());
    object
        .objects
        .lock()
        .await
        .insert("source".into(), Bytes::from_static(b"0123456789"));
    let stream = open(&object).await?;
    let heads_before = object.nas().heads;
    assert_eq!(drain(stream).await?, b"0123456789");
    assert_eq!(object.nas().heads, heads_before, "no HEAD after the ranges");
    Ok(())
}

/// A replacement between two ranges, or after the last one, is a permanent `Conflict`: the next
/// range, or the closing `HEAD`, sees another `ETag`.
#[tokio::test]
async fn a_replacement_during_or_after_the_ranges_is_a_conflict() -> TestResult {
    for after in [1, 3] {
        let protocol = nas_with(b"0123456789").await?;
        let stream = open(&protocol).await?;
        protocol.nas().replace_after_ranges =
            Some(("source".into(), after, Bytes::from_static(b"ABCDEFGHIJ")));
        let read = drain(stream).await;
        assert!(is_read_conflict(&read), "after {after} ranges: {read:?}");
    }
    Ok(())
}

/// A body that broke off is followed by one `HEAD` (coordinator decision 2026-09-27): the object
/// replaced (the server cuts a read once a replacement commits, N0) is a permanent `Conflict`.
#[tokio::test]
async fn a_body_cut_by_a_replacement_is_a_conflict() -> TestResult {
    let protocol = nas_with(b"0123456789").await?;
    let stream = open(&protocol).await?;
    let heads_before = protocol.nas().heads;
    protocol.nas().cut_next_range = Some(BodyCut::Replaced(Bytes::from_static(b"ABCDEFGHIJ")));
    let read = drain(stream).await;
    assert!(is_read_conflict(&read), "{read:?}");
    assert_eq!(
        protocol.nas().heads - heads_before,
        1,
        "one HEAD after the cut"
    );
    // A new read (after a new describe) sees the replacement.
    assert_eq!(drain(open(&protocol).await?).await?, b"ABCDEFGHIJ");
    Ok(())
}

/// The object unchanged after the cut is a lost connection: transient, and the read retried from
/// the start reads it whole.
#[tokio::test]
async fn a_body_cut_by_a_lost_connection_is_retried() -> TestResult {
    let protocol = nas_with(b"0123456789").await?;
    let stream = open(&protocol).await?;
    let heads_before = protocol.nas().heads;
    protocol.nas().cut_next_range = Some(BodyCut::Dropped);
    let read = drain(stream).await;
    assert!(
        matches!(
            &read,
            Err(StorageRoleFailure::Session(failure))
                if failure.class() == FailureClass::Connectivity
                    && failure.transience() == Transience::Transient
        ),
        "{read:?}"
    );
    assert_eq!(
        protocol.nas().heads - heads_before,
        1,
        "one HEAD after the cut"
    );
    assert_eq!(drain(open(&protocol).await?).await?, b"0123456789");
    Ok(())
}

/// A `HEAD` after the cut that fails keeps its own class: the object gone is `NotFound`, a
/// connection still down a transient `Connectivity`.
#[tokio::test]
async fn a_failed_head_after_a_cut_keeps_its_own_class() -> TestResult {
    let cases = [
        (
            S3ProtocolFailure::entry(FailureClass::NotFound, Transience::Permanent, "not found"),
            FailureClass::NotFound,
            Transience::Permanent,
        ),
        (
            S3ProtocolFailure::session(
                FailureClass::Connectivity,
                Transience::Transient,
                "timed out",
            ),
            FailureClass::Connectivity,
            Transience::Transient,
        ),
    ];
    for (head, class, transience) in cases {
        let protocol = nas_with(b"0123456789").await?;
        let stream = open(&protocol).await?;
        protocol.nas().cut_next_range = Some(BodyCut::Dropped);
        *protocol.head_failure.lock().await = Some(("source".into(), head.clone()));
        let read = drain(stream).await;
        let classified = match &read {
            Err(StorageRoleFailure::Entry(failure)) => {
                Some((failure.class(), failure.transience()))
            }
            Err(StorageRoleFailure::Session(failure)) => {
                Some((failure.class(), failure.transience()))
            }
            _ => None,
        };
        assert_eq!(classified, Some((class, transience)), "{head:?}: {read:?}");
    }
    Ok(())
}

/// The residual risk the N0 measurements found: a rewrite over NFS that restores the mtime (and
/// keeps the size) keeps the opaque `ETag`, so nothing can see it.
#[tokio::test]
async fn a_rewrite_that_keeps_the_mtime_is_not_seen() -> TestResult {
    let protocol = nas_with(b"0123456789").await?;
    let stream = open(&protocol).await?;
    protocol
        .nas_rewrite_keeping_mtime("source", Bytes::from_static(b"ABCDEFGHIJ"))
        .await;
    assert_eq!(drain(stream).await?, b"ABCDEFGHIJ");
    Ok(())
}

/// A NAS bucket keeps no versions and no directory objects: a source does not offer versions,
/// the namespace lists no histories and holds no `d/`, and the destination keeps no history —
/// known without asking.
#[tokio::test]
async fn a_nas_bucket_keeps_no_versions_and_no_directory_objects() -> TestResult {
    let protocol = nas_with(b"x").await?;
    let storage = connect(protocol.clone(), identity(), None)?;
    let source = storage.read_source()?;
    assert!(!source.supports_source_versions());
    let described = source.describe(&StoragePath::new("source")?).await?;
    assert_eq!(
        described.source_identity.strength(),
        IdentityStrength::PathScoped
    );
    assert_eq!(described.version, SourceVersion::Current);
    let capabilities = storage.namespace()?.capabilities();
    assert!(!capabilities.histories);
    assert!(!capabilities.directory_objects);
    assert!(!storage.holds_directory_objects());
    let destination = storage.staged_destination()?;
    assert!(!destination.keeps_history().await?);
    assert_eq!(protocol.nas().versioning_requests, 0, "nothing to ask");
    // A version is never selected, and one a HEAD names (a versioned object bucket marked as a
    // NAS bucket) is not pinned: the `ETag` identifies the object.
    let path = StoragePath::new("source")?;
    let refused = source
        .describe_version(&path, &SourceVersion::Id("v1".into()))
        .await;
    assert!(
        matches!(&refused, Err(StorageRoleFailure::Entry(failure)) if failure.class() == FailureClass::Unsupported),
        "{refused:?}"
    );
    *protocol.version.lock().await = Some("v1".into());
    let described = source.describe(&path).await?;
    assert_eq!(described.version, SourceVersion::Current);
    assert_eq!(
        described.source_identity.strength(),
        IdentityStrength::PathScoped
    );
    // An object bucket keeps all three (histories: once it is versioned).
    let versioned = Arc::new(MemoryS3::default());
    versioned.set_versioning(Versioning::Enabled);
    let object = connect(versioned, identity(), None)?;
    assert!(object.read_source()?.supports_source_versions());
    assert!(object.namespace()?.capabilities().histories);
    assert!(object.holds_directory_objects());
    Ok(())
}
