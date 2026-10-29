//! The S3 namespace role (ADR-0006 C22): delimiter listings of current objects and, on a bucket
//! that keeps histories, of one child per key with its history (P7 §10), `stat`, and the verbs S3
//! cannot serve.

use std::num::NonZeroUsize;

use tokio::time::timeout;

use super::*;
use crate::model::Transience;
use crate::model::{EntryKind, FailureClass, VersionRecord};
use crate::storage::backends::s3::namespace::S3Namespace;
use crate::storage::backends::s3::paging::ListingLimits;
use crate::storage::listing::testing::{Described, listed as listed_current};
use crate::storage::{
    DeleteTreeRequest, Namespace, SourceDescriptor, StorageRoleFailure, create_directory_all,
    delete_tree,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

/// One record of a history: id or `null`, latest, marker.
type Record = (String, bool, bool);

/// One listed child: path, kind, and for a key with its history its records, oldest first.
type Row = (String, EntryKind, Option<Vec<Record>>);

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

async fn bucket(keys: &[(&str, &'static [u8])]) -> TestResult<Arc<MemoryS3>> {
    let protocol = Arc::new(MemoryS3::default());
    for (key, body) in keys {
        put(&protocol, key, body).await?;
    }
    Ok(protocol)
}

fn record_row(record: &VersionRecord) -> Record {
    (
        record.version_id().unwrap_or("null").to_string(),
        record.is_latest(),
        record.is_delete_marker(),
    )
}

fn row(descriptor: &SourceDescriptor) -> Row {
    let history = descriptor
        .listing
        .history
        .as_deref()
        .map(|history| history.records().iter().map(record_row).collect());
    (descriptor.path.to_string(), descriptor.kind, history)
}

/// Makes `protocol`'s answer at connection list histories or not, whatever its versioning: a
/// never-versioned bucket that refused to say lists each object as its `"null"` history (Q26). A
/// namespace takes the answer when it is connected: connect it after.
fn lists_histories(protocol: &MemoryS3, histories: bool) {
    protocol.answer_at_connect(if histories {
        S3VersioningAtConnect::Refused
    } else {
        S3VersioningAtConnect::Never
    });
}

/// Lists `directory` whole.
async fn list_whole(
    namespace: &(impl Namespace + ?Sized),
    directory: &StoragePath,
) -> Result<Described, StorageRoleFailure> {
    listed_current(namespace, directory).await
}

/// Rows and failure paths of one listing.
async fn listed(namespace: &dyn Namespace, directory: &str) -> TestResult<(Vec<Row>, Vec<String>)> {
    let (entries, failures) = list_whole(namespace, &path(directory)).await?;
    Ok((
        entries.iter().map(row).collect(),
        failures.iter().map(|f| f.path().to_string()).collect(),
    ))
}

fn file(name: &str) -> Row {
    (name.to_string(), EntryKind::File, None)
}

fn dir(name: &str) -> Row {
    (name.to_string(), EntryKind::Directory, None)
}

/// A key with its history: (id, latest, marker) per record, oldest first.
fn key(name: &str, records: &[(&str, bool, bool)]) -> Row {
    (
        name.to_string(),
        EntryKind::File,
        Some(
            records
                .iter()
                .map(|(id, latest, marker)| ((*id).to_string(), *latest, *marker))
                .collect(),
        ),
    )
}

/// Children come out in S3 key order — the object `a` before the prefix `a/`, which sorts after
/// `a!`, `a-0` and `a.txt` — with artifacts hidden. A zero-byte directory marker `b/` is not a
/// child of the root (it is the prefix `b`) but the zero-byte file `b/` of `b` itself (P7 F5, D8).
#[tokio::test]
async fn current_children_are_in_key_order_without_artifacts() -> TestResult {
    let protocol = bucket(&[
        ("a", b"1"),
        ("a!", b"1"),
        ("a-0", b"1"),
        ("a.txt", b"1"),
        ("a/x", b"1"),
        ("b/", b""),
        ("d/e", b"1"),
        ("d/.data-mover-stage/y", b"1"),
        (".data-mover-x", b"1"),
    ])
    .await?;
    let namespace = namespace(&protocol)?;
    let (rows, failures) = listed(namespace.as_ref(), "").await?;
    assert_eq!(
        rows,
        [
            file("a"),
            file("a!"),
            file("a-0"),
            file("a.txt"),
            dir("a"),
            dir("b"),
            dir("d")
        ]
    );
    assert!(failures.is_empty());
    assert_eq!(listed(namespace.as_ref(), "d").await?.0, [file("d/e")]);
    assert_eq!(
        listed(namespace.as_ref(), "b").await?,
        (vec![file("b/")], vec![])
    );
    Ok(())
}

/// A prefix with an empty segment has no path in this tree: a per-child failure, and the siblings
/// are still listed. A marker object with content is no such key: it is the file `c/` of `c`
/// (P7 F5), never merged into the directory `c` nor into a file `c`.
#[tokio::test]
async fn unspellable_keys_are_failures_not_silent_merges() -> TestResult {
    let protocol = bucket(&[
        ("c/", b"x"),
        ("c/f", b"1"),
        ("e//x", b"1"),
        ("e/y", b"1"),
        ("/lead", b"1"),
    ])
    .await?;
    let namespace = namespace(&protocol)?;
    let (rows, failures) = listed(namespace.as_ref(), "").await?;
    assert_eq!(rows, [dir("c"), dir("e")]);
    assert_eq!(failures, ["/"]);
    let (rows, failures) = listed(namespace.as_ref(), "c").await?;
    assert_eq!((rows, failures), (vec![file("c/"), file("c/f")], vec![]));
    let (rows, failures) = listed(namespace.as_ref(), "e").await?;
    assert_eq!(
        (rows, failures),
        (vec![file("e/y")], vec!["e//".to_string()])
    );
    for bad in ["e/", "/e", "e//x"] {
        let refused = listed_current(&*namespace, &path(bad)).await;
        assert!(
            matches!(refused, Err(StorageRoleFailure::Entry(ref f)) if f.class() == FailureClass::InvalidInput),
            "{bad}: {refused:?}"
        );
    }
    Ok(())
}

/// A store that ignores the delimiter lists every key flat: keys below a deeper `/` roll up into
/// one prefix each, however many pages they span.
#[tokio::test]
async fn a_flat_listing_rolls_up_into_one_prefix_per_subdirectory() -> TestResult {
    let protocol = bucket(&[
        ("p/a/1", b"1"),
        ("p/a/2", b"1"),
        ("p/a/3/4", b"1"),
        ("p/b", b"1"),
    ])
    .await?;
    let namespace = namespace(&protocol)?;
    for page_size in [1, 2, 1000] {
        {
            let mut knobs = protocol.listing();
            knobs.ignore_delimiter = true;
            knobs.page_size = page_size;
        }
        let (rows, failures) = listed(namespace.as_ref(), "p").await?;
        assert_eq!(rows, [dir("p/a"), file("p/b")], "page size {page_size}");
        assert!(failures.is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn stat_tells_objects_prefixes_and_absence_apart() -> TestResult {
    let protocol = bucket(&[("a", b"12"), ("d/e", b"1")]).await?;
    let namespace = namespace(&protocol)?;
    let stat = |value: &'static str| {
        let namespace = &namespace;
        async move { namespace.stat(&path(value)).await }
    };
    let kind =
        |result: Result<SourceDescriptor, StorageRoleFailure>| result.map(|entry| Some(entry.kind));
    assert_eq!(kind(stat("a").await), Ok(Some(EntryKind::File)));
    assert_eq!(kind(stat("d").await), Ok(Some(EntryKind::Directory)));
    assert_eq!(kind(stat("").await), Ok(Some(EntryKind::Directory)));
    for (missing, class) in [
        ("x", FailureClass::NotFound),
        (".data-mover-x", FailureClass::InvalidInput),
    ] {
        assert!(
            matches!(kind(stat(missing).await), Err(StorageRoleFailure::Entry(ref f)) if f.class() == class),
            "{missing}"
        );
    }
    Ok(())
}

/// Current keys can be deleted; virtual directory creation is rejected before I/O.
#[tokio::test]
async fn namespace_mutations_are_checked_per_verb() -> TestResult {
    let protocol = bucket(&[("d/e", b"1")]).await?;
    let storage = connect(protocol.clone(), identity(), None)?;
    let namespace = storage.namespace()?;
    let capabilities = namespace.capabilities();
    assert!(capabilities.create_directory.is_err());
    assert!(capabilities.delete.is_ok());
    assert!(capabilities.rename.is_ok());
    assert!(create_directory_all(&storage, &path("n/m")).await.is_err());
    assert!(protocol.listing().calls.is_empty());
    assert!(matches!(namespace.delete(&path("d")).await,
        Err(StorageRoleFailure::Entry(e)) if e.class() == FailureClass::Conflict));
    let one = NonZeroUsize::MIN;
    let mut deletion = delete_tree(
        &storage,
        DeleteTreeRequest {
            root: path("d"),
            delete_root: true,
            max_inflight_operations: one,
            max_buffered_items: one,
            cancel: CancellationToken::new(),
        },
    )?;
    while deletion.next_item().await.is_some() {}
    let outcome = deletion.finish().await?;
    assert!(
        matches!(outcome, crate::storage::DeleteTreeOutcome::Completed(ref done)
        if done.entry_failures == 0)
    );
    assert!(!protocol.objects.lock().await.contains_key("d/e"));
    Ok(())
}

/// A listing refused for one prefix fails that directory only; a lost session fails it as a
/// session failure.
#[tokio::test]
async fn listing_failures_keep_their_scope() -> TestResult {
    let protocol = bucket(&[("d/e", b"1")]).await?;
    for (failure, session) in [
        (
            S3ProtocolFailure::entry(
                FailureClass::PermissionDenied,
                Transience::Permanent,
                "denied",
            ),
            false,
        ),
        (S3ProtocolFailure::protocol("gone"), true),
    ] {
        protocol.listing().failure = Some(("d/".to_string(), failure));
        for histories in [false, true] {
            lists_histories(&protocol, histories);
            let namespace = namespace(&protocol)?;
            let result = listed_current(&*namespace, &path("d")).await;
            assert_eq!(
                matches!(result, Err(StorageRoleFailure::Session(_))),
                session,
                "histories={histories}"
            );
            assert!(result.is_err());
        }
    }
    Ok(())
}

/// `.` and `..` segments would be folded into another path by a destination (`a/./b` is `a/b`),
/// so, like an empty segment, they are per-child failures and never listed paths.
#[tokio::test]
async fn dot_segments_are_failures_not_aliases() -> TestResult {
    let protocol = bucket(&[
        ("./x", b"1"),
        ("../y", b"1"),
        ("a/./b", b"1"),
        ("a/b", b"1"),
        ("a/.", b"1"),
        ("a/..", b"1"),
    ])
    .await?;
    for histories in [false, true] {
        lists_histories(&protocol, histories);
        let namespace = namespace(&protocol)?;
        let (rows, failures) = listed(namespace.as_ref(), "").await?;
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0].0, "a");
        assert_eq!(failures, ["../", "./"]);
        let (rows, failures) = listed(namespace.as_ref(), "a").await?;
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0].0, "a/b");
        assert_eq!(failures, ["a/.", "a/..", "a/./"]);
    }
    let namespace = namespace(&protocol)?;
    for bad in ["a/.", "./a", "a/../b"] {
        let refused = listed_current(&*namespace, &path(bad)).await;
        assert!(
            matches!(refused, Err(StorageRoleFailure::Entry(ref f)) if f.class() == FailureClass::InvalidInput),
            "{bad}: {refused:?}"
        );
    }
    Ok(())
}

/// A prefix under which nothing is stored does not exist: listing it is `NotFound`, not an empty
/// (and so exhaustive) directory. The storage root always exists.
#[tokio::test]
async fn an_absent_prefix_is_not_found() -> TestResult {
    let empty = Arc::new(MemoryS3::default());
    for histories in [false, true] {
        lists_histories(&empty, histories);
        let listing = listed(namespace(&empty)?.as_ref(), "").await?;
        assert_eq!(listing, (vec![], vec![]));
    }
    let protocol = bucket(&[("d/e", b"1")]).await?;
    for histories in [false, true] {
        lists_histories(&protocol, histories);
        let namespace = namespace(&protocol)?;
        let result = list_whole(&*namespace, &path("missing")).await;
        assert!(
            matches!(result, Err(StorageRoleFailure::Entry(ref f)) if f.class() == FailureClass::NotFound),
            "{result:?}"
        );
    }
    Ok(())
}

/// A server whose continuation comes back — on the very next page or later — would page forever;
/// that directory's listing fails instead (`Protocol`, `Permanent`, at the directory), and the
/// session goes on.
#[tokio::test]
async fn a_repeating_continuation_fails_the_directory() -> TestResult {
    let protocol = bucket(&[("a", b"1"), ("b", b"1")]).await?;
    for cycle in [1, 2, 3] {
        for histories in [false, true] {
            lists_histories(&protocol, histories);
            let namespace = namespace(&protocol)?;
            protocol.listing().cycle_tokens = cycle;
            let root = StoragePath::root();
            let listing = list_whole(&*namespace, &root);
            // Bounded, so a regression fails here instead of paging forever.
            let result = timeout(Duration::from_secs(5), listing).await?;
            assert!(
                matches!(
                    result,
                    Err(StorageRoleFailure::Entry(ref f))
                        if f.class() == FailureClass::Protocol
                            && f.transience() == Transience::Permanent
                            && f.path() == &StoragePath::root()
                ),
                "cycle {cycle}: {result:?}"
            );
        }
    }
    Ok(())
}

/// A server that ignores the continuation of one prefix serves its first page again, naming the
/// same continuation (for versions, `MinIO`'s `<key>[minio_cache:v2,return:]` marker): that
/// directory fails, the prefix beside it lists whole.
#[tokio::test]
async fn a_restarting_minio_listing_fails_only_that_directory() -> TestResult {
    let protocol = bucket(&[("a/x", b"1"), ("a/y", b"1"), ("b/z", b"1")]).await?;
    {
        let mut knobs = protocol.listing();
        knobs.page_size = 1;
        knobs.minio_markers = true;
        knobs.restart_prefix = Some("a/".to_string());
    }
    for histories in [false, true] {
        lists_histories(&protocol, histories);
        let namespace = namespace(&protocol)?;
        let list = |name: &'static str| {
            let namespace = &namespace;
            async move { list_whole(&**namespace, &path(name)).await }
        };
        let looping = timeout(Duration::from_secs(5), list("a")).await?;
        assert!(
            matches!(
                looping,
                Err(StorageRoleFailure::Entry(ref f))
                    if f.class() == FailureClass::Protocol && f.path() == &path("a")
            ),
            "histories={histories}: {looping:?}"
        );
        let sibling = timeout(Duration::from_secs(5), list("b")).await??;
        assert_eq!(sibling.0.len(), 1);
    }
    Ok(())
}

/// A store that ignores the delimiter lists versions and markers flat too: they roll up into one
/// prefix per subdirectory, markers included.
#[tokio::test]
async fn a_flat_version_listing_rolls_up_versions_and_markers() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    put(&protocol, "d/x", b"1").await?;
    put(&protocol, "d/x", b"22").await?;
    put(&protocol, "d/y/z", b"1").await?;
    put(&protocol, "gone/z", b"1").await?;
    protocol
        .delete_object("gone/z")
        .await
        .map_err(|e| format!("{e:?}"))?;
    protocol.listing().ignore_delimiter = true;
    let namespace = namespace(&protocol)?;
    for page_size in [1, 2, 1000] {
        protocol.listing().page_size = page_size;
        let (rows, _) = listed(namespace.as_ref(), "").await?;
        assert_eq!(rows, [dir("d"), dir("gone")], "page size {page_size}");
        let (rows, _) = listed(namespace.as_ref(), "d").await?;
        assert_eq!(
            rows,
            [
                key("d/x", &[("v0001", false, false), ("v0002", true, false)]),
                dir("d/y"),
            ]
        );
    }
    Ok(())
}

fn limited(protocol: &Arc<MemoryS3>, max_entries: usize) -> S3Namespace<MemoryS3> {
    S3Namespace::new(protocol.clone(), identity()).with_limits(ListingLimits {
        max_entries,
        max_empty_pages: 3,
        ..ListingLimits::PRODUCTION
    })
}

/// A directory whose listing would hold more entries than one listing may fails (`Capacity`, Q9)
/// instead of being cut short; one at the limit lists whole. Every record of a history is held (a
/// history listing counts versions, markers and prefixes); current objects stream, and only the
/// subdirectory names count (P7 §12.4): `a`, `b` and `c/` hold one name.
#[tokio::test]
async fn a_directory_past_the_entry_limit_fails_whole() -> TestResult {
    let protocol = bucket(&[("a", b"1"), ("b", b"1"), ("c/d", b"1")]).await?;
    protocol.listing().page_size = 1;
    for (histories, held) in [(false, 1), (true, 3)] {
        lists_histories(&protocol, histories);
        let at_limit = limited(&protocol, held);
        let listing = list_whole(&at_limit, &StoragePath::root()).await;
        assert_eq!(listing?.0.len(), 3);
        let over = limited(&protocol, held - 1);
        let listing = list_whole(&over, &StoragePath::root()).await;
        assert!(
            matches!(listing, Err(StorageRoleFailure::Entry(ref f)) if f.class() == FailureClass::Capacity),
            "{listing:?}"
        );
    }
    Ok(())
}

/// A server that answers every page empty yet names a new continuation fails that directory
/// after a bounded run, instead of paging forever.
#[tokio::test]
async fn endless_empty_pages_fail_the_directory() -> TestResult {
    let protocol = bucket(&[("a", b"1")]).await?;
    protocol.listing().endless_empty = true;
    for histories in [false, true] {
        lists_histories(&protocol, histories);
        let namespace = limited(&protocol, 1000);
        let root = StoragePath::root();
        let listing = list_whole(&namespace, &root);
        let result = timeout(Duration::from_secs(5), listing).await?;
        assert!(
            matches!(result, Err(StorageRoleFailure::Entry(ref f)) if f.class() == FailureClass::Protocol),
            "{result:?}"
        );
    }
    Ok(())
}

/// `stat` of a prefix pages past empty pages that name a continuation instead of calling the
/// prefix absent; a server that never stops doing that fails within the listing bounds.
#[tokio::test]
async fn stat_pages_past_empty_pages() -> TestResult {
    let protocol = bucket(&[("d/e", b"1")]).await?;
    protocol.listing().endless_empty = true;
    let namespace = limited(&protocol, 1000);
    let stat = timeout(Duration::from_secs(5), namespace.stat(&path("d"))).await?;
    assert!(
        matches!(stat, Err(StorageRoleFailure::Entry(ref f)) if f.class() == FailureClass::Protocol),
        "{stat:?}"
    );
    Ok(())
}

#[path = "namespace_history_tests.rs"]
mod histories;

/// Tree deletion removes the current key and never purges older object versions or markers.
#[tokio::test]
async fn tree_deletion_keeps_version_history() -> TestResult {
    let protocol = bucket(&[]).await?;
    protocol
        .put_version("d/e", "v1", Bytes::from_static(b"old"))
        .await;
    protocol
        .put_version("d/e", "v2", Bytes::from_static(b"new"))
        .await;
    lists_histories(&protocol, true);
    let storage = connect(protocol.clone(), identity(), None)?;
    let mut deletion = delete_tree(
        &storage,
        DeleteTreeRequest {
            root: path("d"),
            delete_root: true,
            max_inflight_operations: NonZeroUsize::MIN,
            max_buffered_items: NonZeroUsize::MIN,
            cancel: CancellationToken::new(),
        },
    )?;
    while deletion.next_item().await.is_some() {}
    assert!(
        matches!(deletion.finish().await?, crate::storage::DeleteTreeOutcome::Completed(done)
        if done.entry_failures == 0)
    );
    assert!(
        protocol
            .versions
            .lock()
            .await
            .contains_key(&("d/e".into(), "v1".into()))
    );
    assert!(
        protocol
            .versions
            .lock()
            .await
            .contains_key(&("d/e".into(), "v2".into()))
    );
    assert!(protocol.version_deletes().is_empty());
    Ok(())
}

#[tokio::test]
async fn deleting_virtual_directories_removes_markers_and_artifacts_but_keeps_visible_children()
-> TestResult {
    let artifact = crate::storage::artifacts::artifact_name(
        "file",
        crate::storage::artifacts::ArtifactKind::Pointer,
    );
    for marker in [false, true] {
        let protocol = bucket(&[]).await?;
        put(&protocol, &format!("d/{artifact}"), b"pointer").await?;
        if marker {
            put(&protocol, "d/", b"").await?;
        }
        let namespace = namespace(&protocol)?;
        put(&protocol, "d/visible", b"keep").await?;
        assert!(matches!(namespace.delete(&path("d")).await,
            Err(StorageRoleFailure::Entry(e)) if e.class() == FailureClass::Conflict));
        assert!(
            protocol
                .objects
                .lock()
                .await
                .contains_key(&format!("d/{artifact}"))
        );
        namespace.delete(&path("d/visible")).await?;
        namespace.delete(&path("d")).await?;
        assert!(protocol.objects.lock().await.is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn recursive_delete_cleans_artifacts_even_when_storage_root_is_retained() -> TestResult {
    let protocol = bucket(&[]).await?;
    let artifact = crate::storage::artifacts::artifact_name(
        "file",
        crate::storage::artifacts::ArtifactKind::Pointer,
    );
    put(&protocol, &artifact, b"pointer").await?;
    put(&protocol, &format!("{artifact}/private-child"), b"private").await?;
    let storage = connect(protocol.clone(), identity(), None)?;
    let mut deletion = delete_tree(
        &storage,
        DeleteTreeRequest {
            root: StoragePath::root(),
            delete_root: false,
            max_inflight_operations: NonZeroUsize::MIN,
            max_buffered_items: NonZeroUsize::MIN,
            cancel: CancellationToken::new(),
        },
    )?;
    while deletion.next_item().await.is_some() {}
    assert!(
        matches!(deletion.finish().await?, crate::storage::DeleteTreeOutcome::Completed(done)
        if done.entry_failures == 0)
    );
    assert!(protocol.objects.lock().await.is_empty());
    Ok(())
}

#[tokio::test]
async fn cleanup_refuses_outside_keys_and_prefixes_before_deleting_any_artifact() -> TestResult {
    let artifact = crate::storage::artifacts::artifact_name(
        "file",
        crate::storage::artifacts::ArtifactKind::Pointer,
    );
    for require_empty in [false, true] {
        for common_prefix in [false, true] {
            let protocol = bucket(&[]).await?;
            let own = format!("victim/{artifact}");
            let outside = format!("other/{artifact}");
            put(&protocol, &own, b"own").await?;
            put(&protocol, &outside, b"outside").await?;
            if common_prefix {
                protocol
                    .listing()
                    .stray_prefixes
                    .push(("victim/".into(), format!("{outside}/")));
            } else {
                protocol.listing().stray.push((
                    "victim/".into(),
                    S3ListedObject {
                        key: outside.clone(),
                        size: 7,
                        etag: "etag".into(),
                        last_modified: None,
                    },
                ));
            }
            assert!(
                super::namespace_cleanup::cleanup(
                    protocol.as_ref(),
                    &path("victim"),
                    ListingLimits::PRODUCTION,
                    require_empty,
                )
                .await
                .is_err()
            );
            let objects = protocol.objects.lock().await;
            assert!(objects.contains_key(&own));
            assert!(objects.contains_key(&outside));
            assert_eq!(objects.len(), 2);
            assert_eq!(protocol.listing().calls, ["victim/"]);
        }
    }
    Ok(())
}

#[tokio::test]
async fn deleting_virtual_directory_preserves_its_nonempty_directory_object() -> TestResult {
    let protocol = bucket(&[("d/", b"content")]).await?;
    let namespace = namespace(&protocol)?;
    assert!(matches!(namespace.delete(&path("d")).await,
        Err(StorageRoleFailure::Entry(e)) if e.class() == FailureClass::Conflict));
    assert_eq!(
        protocol.objects.lock().await.get("d/"),
        Some(&Bytes::from_static(b"content"))
    );
    Ok(())
}

#[tokio::test]
async fn rename_refuses_either_storage_root_before_calling_protocol() -> TestResult {
    let protocol = bucket(&[("file", b"content")]).await?;
    let namespace = namespace(&protocol)?;
    for (from, to) in [("", "file"), ("file", ""), ("", "")] {
        // This protocol's rename returns Unsupported if reached. The namespace must refuse
        // InvalidInput first, without reading or mutating any key.
        assert!(matches!(namespace.rename(&path(from), &path(to)).await,
            Err(StorageRoleFailure::Entry(e)) if e.class() == FailureClass::InvalidInput));
    }
    assert!(protocol.head_versions.lock().await.is_empty());
    assert_eq!(protocol.objects.lock().await.len(), 1);
    Ok(())
}
