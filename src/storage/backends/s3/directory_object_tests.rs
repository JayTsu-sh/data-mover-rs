//! Directory objects on the S3 namespace role (P7 F5): the key `<dir>/` itself, handed over by the
//! listing of `<dir>` as the file child with the empty name — its current object, or, on a bucket
//! that keeps histories, one child with its whole history — and `stat` of such a key.

use super::*;
use crate::model::{
    EntryKind, FailureClass, IdentityStrength, StorageTimestamp, TimePrecision, Transience,
};
use crate::storage::backends::s3::namespace::S3Namespace;
use crate::storage::listing::testing::{Described, listed as listed_current};
use crate::storage::{Namespace, SourceDescriptor, StorageRoleFailure};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn path(value: &str) -> StoragePath {
    StoragePath::new(value).unwrap_or_else(|error| panic!("{error}"))
}

fn namespace(protocol: &Arc<MemoryS3>) -> TestResult<Arc<dyn Namespace>> {
    let storage = connect(protocol.clone(), identity(), None)?;
    Ok(storage.namespace()?)
}

async fn put(protocol: &MemoryS3, key: &str, body: &'static [u8]) -> TestResult {
    let bytes = Bytes::from_static(body);
    protocol
        .put_object(key, bytes.clone().into(), &content_md5(&bytes))
        .await
        .map_err(|e| format!("{e:?}"))?;
    Ok(())
}

async fn delete(protocol: &MemoryS3, key: &str) -> TestResult {
    protocol
        .delete_object(key)
        .await
        .map_err(|e| format!("{e:?}"))?;
    Ok(())
}

async fn bucket(keys: &[(&str, &'static [u8])]) -> TestResult<Arc<MemoryS3>> {
    let protocol = Arc::new(MemoryS3::default());
    for (key, body) in keys {
        put(&protocol, key, body).await?;
    }
    Ok(protocol)
}

/// `path kind size` of a child, with ` [id size[ L][ M], …]` for a key with its history.
fn row(descriptor: &SourceDescriptor) -> String {
    let history = descriptor
        .listing
        .history
        .as_deref()
        .map_or(String::new(), |history| {
            let records: Vec<String> = history
                .records()
                .iter()
                .map(|record| {
                    format!(
                        "{} {:?}{}{}",
                        record.version_id().unwrap_or("null"),
                        record.size(),
                        if record.is_latest() { " L" } else { "" },
                        if record.is_delete_marker() { " M" } else { "" }
                    )
                })
                .collect();
            format!(" [{}]", records.join(", "))
        });
    format!(
        "{} {:?} {:?}{history}",
        descriptor.path, descriptor.kind, descriptor.size
    )
}

fn rows(described: &Described) -> (Vec<String>, Vec<String>) {
    (
        described.0.iter().map(row).collect(),
        described
            .1
            .iter()
            .map(|failure| format!("{} {:?}", failure.path(), failure.class()))
            .collect(),
    )
}

async fn current(namespace: &dyn Namespace, directory: &str) -> TestResult<Described> {
    Ok(listed_current(namespace, &path(directory)).await?)
}

/// Lists `directory` as a bucket that keeps histories (`histories`) or not, whatever its
/// versioning: what `protocol` answered when it was connected decides (P7 Q26).
async fn listed_as(
    protocol: &Arc<MemoryS3>,
    directory: &str,
    histories: bool,
) -> TestResult<Described> {
    protocol.answer_at_connect(if histories {
        S3VersioningAtConnect::Refused
    } else {
        S3VersioningAtConnect::Never
    });
    current(namespace(protocol)?.as_ref(), directory).await
}

/// The object `d/` is a file child of `d` at exactly its key, with its bytes, `ETag` and time; its
/// identity is path-scoped to `d/`, so it never collides with a file `d` holding the same bytes.
/// The parent sees only the directory `d`.
#[tokio::test]
async fn a_directory_object_is_listed_with_bytes_etag_and_time() -> TestResult {
    let protocol = bucket(&[("d/", b"hello"), ("d/x", b"1"), ("e", b"hello")]).await?;
    *protocol.last_modified.lock().await = Some(StorageTimestamp::new(
        7_000_000_000,
        TimePrecision::Milliseconds,
    )?);
    let namespace = namespace(&protocol)?;
    let listed = current(namespace.as_ref(), "d").await?;
    assert_eq!(
        rows(&listed),
        (
            vec![
                "d/ File Some(5)".to_string(),
                "d/x File Some(1)".to_string()
            ],
            vec![]
        )
    );
    let own = &listed.0[0];
    assert_eq!(
        own.content_version.as_deref(),
        Some(etag_of(b"hello").as_bytes())
    );
    assert!(
        own.inline_timestamps
            .and_then(|times| times.modified)
            .is_some()
    );
    assert_eq!(own.source_identity.strength(), IdentityStrength::PathScoped);
    let root = current(namespace.as_ref(), "").await?;
    let same_bytes = root
        .0
        .iter()
        .find(|descriptor| descriptor.path.as_str() == "e")
        .ok_or("the file e is listed")?;
    assert_ne!(own.source_identity, same_bytes.source_identity);
    assert_eq!(
        rows(&root).0,
        ["d Directory None", "e File Some(5)"],
        "the parent sees the directory only"
    );
    Ok(())
}

/// D8: a zero-byte folder marker is a zero-byte file entry, beside its children or alone (a
/// console-style empty folder).
#[tokio::test]
async fn a_zero_byte_marker_is_a_zero_size_file_entry() -> TestResult {
    let protocol = bucket(&[("b/", b""), ("b/c", b"1"), ("e/", b"")]).await?;
    let namespace = namespace(&protocol)?;
    assert_eq!(
        rows(&current(namespace.as_ref(), "b").await?).0,
        ["b/ File Some(0)", "b/c File Some(1)"]
    );
    assert_eq!(
        rows(&current(namespace.as_ref(), "e").await?),
        (vec!["e/ File Some(0)".to_string()], vec![])
    );
    Ok(())
}

/// The storage root's own object (the URL prefix's key) has no path: only a current-only
/// zero-byte marker is ignored. Content or history is an `Unsupported` failure at the root.
#[tokio::test]
async fn a_zero_byte_root_marker_is_ignored_and_a_content_one_reported() -> TestResult {
    let marker = bucket(&[("", b""), ("a", b"1")]).await?;
    for histories in [false, true] {
        let listed = listed_as(&marker, "", histories).await?;
        let paths: Vec<&str> = listed.0.iter().map(|d| d.path.as_str()).collect();
        assert_eq!(paths, ["a"]);
        let expected = if histories {
            vec![" Unsupported"]
        } else {
            vec![]
        };
        assert_eq!(rows(&listed).1, expected);
    }
    let content = bucket(&[("", b"x"), ("a", b"1")]).await?;
    for histories in [false, true] {
        let listed = listed_as(&content, "", histories).await?;
        assert_eq!(listed.0.len(), 1, "{:?}", rows(&listed));
        assert_eq!(rows(&listed).1, [" Unsupported"]);
    }
    Ok(())
}

/// However the store pages and orders the listing — one element a page, scrambled, repeating the
/// first object on every later page, or failing a page transiently — the own object is handed over
/// exactly once.
#[tokio::test]
async fn a_directory_object_on_a_later_shuffled_page_is_handed_over_once() -> TestResult {
    let protocol = bucket(&[
        ("d/a", b"1"),
        ("d/b", b"1"),
        ("d/", b"own"),
        ("d/c", b"1"),
        ("d/s/t", b"1"),
    ])
    .await?;
    let namespace = namespace(&protocol)?;
    for (unordered, repeat_first) in [(false, false), (true, false), (false, true), (true, true)] {
        {
            let mut knobs = protocol.listing();
            knobs.page_size = 1;
            knobs.unordered = unordered;
            knobs.repeat_first_object = repeat_first;
        }
        let (rows, failures) = rows(&current(namespace.as_ref(), "d").await?);
        let own = rows.iter().filter(|row| row.starts_with("d/ ")).count();
        assert_eq!(
            own, 1,
            "unordered {unordered} repeat {repeat_first}: {rows:?}"
        );
        assert!(failures.is_empty(), "{failures:?}");
    }
    Ok(())
}

fn connection_reset() -> S3ProtocolFailure {
    S3ProtocolFailure::session(
        FailureClass::Connectivity,
        Transience::Transient,
        "connection reset",
    )
}

/// `MinIO` lists a directory object whose newest record is a delete marker as a zero-byte own key
/// without an `ETag` (HEAD answers 404): no current object, so no child and no failure. An own
/// key with an `ETag` and no bytes is a folder marker as always.
#[tokio::test]
async fn a_deleted_directory_object_listed_without_an_etag_is_not_a_child() -> TestResult {
    let protocol = bucket(&[("m/x", b"1")]).await?;
    protocol.listing().stray = vec![(
        "m/".to_string(),
        S3ListedObject {
            key: "m/".to_string(),
            size: 0,
            etag: String::new(),
            last_modified: None,
        },
    )];
    let namespace = namespace(&protocol)?;
    assert_eq!(
        rows(&current(namespace.as_ref(), "m").await?),
        (vec!["m/x File Some(1)".to_string()], vec![])
    );
    Ok(())
}

/// A page that failed transiently is requested again with the same token; the own object on it
/// still comes out once.
#[tokio::test]
async fn a_retried_page_does_not_repeat_the_directory_object() -> TestResult {
    let protocol = bucket(&[("d/", b"own"), ("d/a", b"1"), ("d/b", b"1")]).await?;
    {
        let mut knobs = protocol.listing();
        knobs.page_size = 1;
        // The first request fails, and the one after the page holding `d/`.
        knobs.failing_calls = vec![(0, connection_reset()), (2, connection_reset())];
    }
    let namespace = namespace(&protocol)?;
    let (rows, failures) = rows(&current(namespace.as_ref(), "d").await?);
    assert_eq!(rows.iter().filter(|row| row.starts_with("d/ ")).count(), 1);
    assert_eq!(rows.len(), 3, "{rows:?}");
    assert!(failures.is_empty(), "{failures:?}");
    Ok(())
}

/// D5: the own key is one child with its whole history — content versions, zero-byte versions
/// and delete markers — in order, the latest last, beside its directory's other keys; its latest
/// record being a marker, it has no size of its own.
#[tokio::test]
async fn a_mixed_history_keeps_zero_byte_versions_and_markers_in_order() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    put(&protocol, "m/", b"fives").await?;
    put(&protocol, "m/", b"").await?;
    delete(&protocol, "m/").await?;
    put(&protocol, "m/", b"333").await?;
    delete(&protocol, "m/").await?;
    put(&protocol, "m/x", b"1").await?;
    let namespace = namespace(&protocol)?;
    for page_size in [0, 1, 2] {
        protocol.listing().page_size = page_size;
        assert_eq!(
            rows(&current(namespace.as_ref(), "m").await?),
            (
                vec![
                    "m/ File None [v0001 Some(5), v0002 Some(0), v0003 None M, v0004 Some(3), \
                     v0005 None L M]"
                        .to_string(),
                    "m/x File Some(1) [v0006 Some(1) L]".to_string(),
                ],
                vec![]
            ),
            "page size {page_size}"
        );
    }
    // Listed by current objects: the own key's latest record is a marker, so it has no current
    // object.
    assert_eq!(
        rows(&listed_as(&protocol, "m", false).await?).0,
        ["m/x File Some(1)"]
    );
    Ok(())
}

/// A history of nothing but zero-byte versions and markers is listed whole too (D8).
#[tokio::test]
async fn an_all_zero_history_is_listed_whole() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    put(&protocol, "z/", b"").await?;
    delete(&protocol, "z/").await?;
    put(&protocol, "z/", b"").await?;
    let namespace = namespace(&protocol)?;
    assert_eq!(
        rows(&current(namespace.as_ref(), "z").await?),
        (
            vec!["z/ File Some(0) [v0001 Some(0), v0002 None M, v0003 Some(0) L]".to_string()],
            vec![]
        )
    );
    Ok(())
}

/// `stat` of a key ending in `/` reads only that object: present, it is the file; gone, it is
/// `NotFound` — never the directory it names, never an invalid path.
#[tokio::test]
async fn stat_of_a_gone_directory_object_is_not_found() -> TestResult {
    let protocol = bucket(&[("d/", b"own"), ("d/x", b"1")]).await?;
    let namespace = namespace(&protocol)?;
    let found = namespace.stat(&path("d/")).await?;
    assert_eq!((found.kind, found.size), (EntryKind::File, Some(3)));
    delete(&protocol, "d/").await?;
    let gone = namespace.stat(&path("d/")).await;
    assert!(
        matches!(gone, Err(StorageRoleFailure::Entry(ref f)) if f.class() == FailureClass::NotFound),
        "{gone:?}"
    );
    assert_eq!(namespace.stat(&path("d")).await?.kind, EntryKind::Directory);
    Ok(())
}

/// S3 holds directory objects; listing a key ending in `/` stays refused: an object is not a
/// directory.
#[tokio::test]
async fn s3_holds_directory_objects_but_does_not_list_them_as_directories() -> TestResult {
    let protocol = bucket(&[("d/", b"own"), ("d/x", b"1")]).await?;
    let namespace = S3Namespace::new(Arc::clone(&protocol), identity());
    assert!(namespace.capabilities().directory_objects);
    let refused = listed_current(&namespace, &path("d/")).await;
    assert!(
        matches!(refused, Err(StorageRoleFailure::Entry(ref f)) if f.class() == FailureClass::InvalidInput),
        "{refused:?}"
    );
    Ok(())
}
