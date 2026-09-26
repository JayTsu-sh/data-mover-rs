//! The S3 namespace role (ADR-0006 C22): delimiter listings of current objects and of every
//! version, `stat`, and the verbs S3 cannot serve.

use std::num::NonZeroUsize;

use tokio::time::timeout;

use super::*;
use crate::model::Transience;
use crate::model::{EntryKind, EntryVersion, FailureClass, HistoryOrder, SourceVersion};
use crate::storage::backends::s3::namespace::S3Namespace;
use crate::storage::backends::s3::paging::ListingLimits;
use crate::storage::listing::testing::{
    Described, Perform as _, Verb, listed as listed_current, listed_versions,
};
use crate::storage::{
    Capability, CapabilityAvailability, DeleteTreeRequest, Namespace, SourceDescriptor,
    StorageRoleFailure, create_directory_all, delete_tree,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

/// One listed child: path, kind, and for a version (id or `null`, latest, marker, rank).
type Row = (String, EntryKind, Option<(String, bool, bool, u32)>);

fn path(value: &str) -> StoragePath {
    StoragePath::new(value).unwrap_or_else(|error| panic!("{error}"))
}

fn namespace(protocol: &Arc<MemoryS3>) -> TestResult<Arc<dyn Namespace>> {
    let storage = connect(protocol.clone(), identity(), None)?;
    Ok(storage.namespace(&validation_policy())?)
}

async fn put(protocol: &MemoryS3, key: &str, body: &'static [u8]) -> TestResult {
    let bytes = Bytes::from_static(body);
    protocol
        .put_object(key, bytes.clone(), &content_md5(&bytes))
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

fn row(descriptor: &SourceDescriptor) -> Row {
    let version = descriptor.listing.version.as_deref().map(|version| {
        (
            version.id().unwrap_or("null").to_string(),
            version.is_latest(),
            version.is_delete_marker(),
            descriptor.listing.rank,
        )
    });
    (descriptor.path.to_string(), descriptor.kind, version)
}

/// Lists `directory` whole: every version when `all`, the current objects otherwise.
async fn list_whole(
    namespace: &(impl Namespace + ?Sized),
    directory: &StoragePath,
    all: bool,
) -> Result<Described, StorageRoleFailure> {
    if all {
        listed_versions(namespace, directory).await
    } else {
        listed_current(namespace, directory).await
    }
}

/// Rows and failure paths of one listing.
async fn listed(
    namespace: &dyn Namespace,
    directory: &str,
    all: bool,
) -> TestResult<(Vec<Row>, Vec<String>)> {
    let (entries, failures) = list_whole(namespace, &path(directory), all).await?;
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

fn version(name: &str, id: &str, latest: bool, marker: bool, rank: u32) -> Row {
    (
        name.to_string(),
        EntryKind::File,
        Some((id.to_string(), latest, marker, rank)),
    )
}

/// Children come out in S3 key order — the object `a` before the prefix `a/`, which sorts after
/// `a!`, `a-0` and `a.txt` — with artifacts hidden and a zero-byte directory marker skipped.
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
    let (rows, failures) = listed(namespace.as_ref(), "", false).await?;
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
    assert_eq!(
        listed(namespace.as_ref(), "d", false).await?.0,
        [file("d/e")]
    );
    assert_eq!(
        listed(namespace.as_ref(), "b", false).await?,
        (vec![], vec![])
    );
    Ok(())
}

/// A marker object with content and a prefix with an empty segment have no path in this tree:
/// each is a per-child failure, and the siblings are still listed.
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
    let (rows, failures) = listed(namespace.as_ref(), "", false).await?;
    assert_eq!(rows, [dir("c"), dir("e")]);
    assert_eq!(failures, ["/"]);
    let (rows, failures) = listed(namespace.as_ref(), "c", false).await?;
    assert_eq!((rows, failures), (vec![file("c/f")], vec!["c".to_string()]));
    let (rows, failures) = listed(namespace.as_ref(), "e", false).await?;
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
        let (rows, failures) = listed(namespace.as_ref(), "p", false).await?;
        assert_eq!(rows, [dir("p/a"), file("p/b")], "page size {page_size}");
        assert!(failures.is_empty());
    }
    Ok(())
}

/// The history of `k` (v1, v2, a delete marker, v3), the key `gone` (written, then deleted) and
/// `z` with three versions, in a versioned bucket.
async fn versioned_history() -> TestResult<Arc<MemoryS3>> {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    put(&protocol, "k", b"v1").await?;
    put(&protocol, "k", b"v2").await?;
    protocol
        .delete_object("k")
        .await
        .map_err(|e| format!("{e:?}"))?;
    put(&protocol, "k", b"v3").await?;
    put(&protocol, "gone", b"g").await?;
    protocol
        .delete_object("gone")
        .await
        .map_err(|e| format!("{e:?}"))?;
    for body in [b"z1", b"z2", b"z3"] {
        put(&protocol, "z", body).await?;
    }
    put(&protocol, ".data-mover-x", b"artifact").await?;
    Ok(protocol)
}

fn expected_history() -> Vec<Row> {
    vec![
        version("gone", "v0005", false, false, 0),
        version("gone", "v0006", true, true, 1),
        version("k", "v0001", false, false, 0),
        version("k", "v0002", false, false, 1),
        version("k", "v0003", false, true, 2),
        version("k", "v0004", true, false, 3),
        version("z", "v0007", false, false, 0),
        version("z", "v0008", false, false, 1),
        version("z", "v0009", true, false, 2),
    ]
}

/// Every version and marker, each key oldest first with the latest last, whatever the page size
/// and however the store spells its key marker; the current listing has v3 of `k` only and no
/// `gone`.
#[tokio::test]
async fn every_version_is_listed_oldest_first_across_pages() -> TestResult {
    let protocol = versioned_history().await?;
    let namespace = namespace(&protocol)?;
    for (page_size, minio) in [(0, false), (1, true), (2, true), (3, false), (4, true)] {
        {
            let mut knobs = protocol.listing();
            knobs.page_size = page_size;
            knobs.minio_markers = minio;
        }
        let (rows, failures) = listed(namespace.as_ref(), "", true).await?;
        assert_eq!(rows, expected_history(), "page size {page_size}");
        assert!(failures.is_empty());
    }
    let (rows, _) = listed(namespace.as_ref(), "", false).await?;
    assert_eq!(rows, [file("k"), file("z")]);
    Ok(())
}

/// Entries written in one instant keep the listing order reversed, the latest last.
#[tokio::test]
async fn one_instant_keeps_the_written_order() -> TestResult {
    let protocol = versioned_history().await?;
    protocol.listing().same_instant = true;
    let (rows, _) = listed(namespace(&protocol)?.as_ref(), "", true).await?;
    assert_eq!(rows, expected_history());
    Ok(())
}

/// The order every listed version reports for its key's history.
async fn history_orders(namespace: &dyn Namespace) -> TestResult<Vec<HistoryOrder>> {
    let (entries, _) = listed_versions(namespace, &StoragePath::root()).await?;
    Ok(entries
        .iter()
        .filter_map(|entry| entry.listing.version.as_deref().map(EntryVersion::order))
        .collect())
}

/// A store listing in S3's order gives server-ordered histories, even when every entry was written
/// in one instant and pages split the keys; a store that lists each key oldest first (a
/// non-conforming listing) is not trusted — its histories are inferred from `LastModified` (here
/// still right: every version has its own second) and marked so.
#[tokio::test]
async fn a_non_conforming_store_is_ordered_by_time_and_marked_inferred() -> TestResult {
    let protocol = versioned_history().await?;
    let namespace = namespace(&protocol)?;
    {
        let mut knobs = protocol.listing();
        knobs.same_instant = true;
        knobs.page_size = 2;
    }
    assert_eq!(
        listed(namespace.as_ref(), "", true).await?.0,
        expected_history()
    );
    let orders = history_orders(namespace.as_ref()).await?;
    assert!(orders.iter().all(|order| *order == HistoryOrder::Server));
    {
        let mut knobs = protocol.listing();
        knobs.same_instant = false;
        knobs.page_size = 0;
        knobs.oldest_first_versions = true;
    }
    assert_eq!(
        listed(namespace.as_ref(), "", true).await?.0,
        expected_history()
    );
    let orders = history_orders(namespace.as_ref()).await?;
    assert_eq!(orders.len(), 9);
    assert!(orders.iter().all(|order| *order == HistoryOrder::Inferred));
    Ok(())
}

/// What each listed version selects for a copy: a real version its id; a delete marker nothing
/// (it is listing-only, without a size).
#[tokio::test]
async fn listed_versions_select_themselves_for_a_copy() -> TestResult {
    let protocol = versioned_history().await?;
    let namespace = namespace(&protocol)?;
    let (entries, _) = listed_versions(&*namespace, &StoragePath::root()).await?;
    let k: Vec<&SourceDescriptor> = entries.iter().filter(|e| e.path == path("k")).collect();
    assert_eq!(k[0].version, SourceVersion::Id("v0001".into()));
    assert_eq!(k[0].size, Some(2));
    assert!(!k[0].listing.listing_only);
    assert!(k[2].listing.listing_only && k[2].size.is_none());
    assert_eq!(k[3].version, SourceVersion::Id("v0004".into()));
    Ok(())
}

/// The `"null"` version names no id: it selects `Current` while it is the latest, and
/// `Id("null")` once a newer version exists (`Current` would copy that one).
#[tokio::test]
async fn the_null_version_selects_itself_for_a_copy() -> TestResult {
    let unversioned = bucket(&[("n", b"1")]).await?;
    let (rows, _) = listed(namespace(&unversioned)?.as_ref(), "", true).await?;
    assert_eq!(rows, [version("n", "null", true, false, 0)]);

    let suspended = Arc::new(MemoryS3::default());
    suspended.set_versioning(Versioning::Enabled);
    put(&suspended, "s", b"real").await?;
    suspended.set_versioning(Versioning::Suspended);
    put(&suspended, "s", b"null").await?;
    let (entries, _) = listed_versions(&*namespace(&suspended)?, &StoragePath::root()).await?;
    let selected: Vec<(Option<&str>, &SourceVersion)> = entries
        .iter()
        .map(|e| {
            (
                e.listing.version.as_deref().and_then(|v| v.id()),
                &e.version,
            )
        })
        .collect();
    assert_eq!(
        selected,
        [
            (Some("v0001"), &SourceVersion::Id("v0001".into())),
            (None, &SourceVersion::Current)
        ]
    );
    suspended.set_versioning(Versioning::Enabled);
    put(&suspended, "s", b"newer").await?;
    let (entries, _) = listed_versions(&*namespace(&suspended)?, &StoragePath::root()).await?;
    assert_eq!(entries[1].version, SourceVersion::Id("null".into()));
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

/// S3 lends the namespace now, but not its mutating verbs: those are `Unsupported`, and recursive
/// delete and create refuse the storage before any listing, as they did when it lent none.
#[tokio::test]
async fn mutations_are_refused_before_any_io() -> TestResult {
    let protocol = bucket(&[("d/e", b"1")]).await?;
    let storage = connect(protocol.clone(), identity(), None)?;
    assert_eq!(
        storage.capabilities().availability(Capability::Namespace),
        &CapabilityAvailability::Supported
    );
    let namespace = storage.namespace(&validation_policy())?;
    assert!(namespace.capabilities().histories);
    assert!(namespace.capabilities().mutations.is_err());
    for request in [
        Verb::CreateDirectory(path("n")),
        Verb::Delete(path("d/e")),
        Verb::Rename {
            from: path("d/e"),
            to: path("f"),
        },
        Verb::ReadLink(path("d/e")),
    ] {
        let refused = namespace.perform(request).await;
        assert!(
            matches!(refused, Err(StorageRoleFailure::Entry(ref f)) if f.class() == FailureClass::Unsupported),
            "{refused:?}"
        );
    }
    let one = NonZeroUsize::MIN;
    let delete = DeleteTreeRequest {
        root: path("d"),
        delete_root: true,
        max_inflight_operations: one,
        max_buffered_items: one,
        cancel: CancellationToken::new(),
    };
    assert!(delete_tree(&storage, delete).is_err());
    assert!(create_directory_all(&storage, &path("n/m")).await.is_err());

    assert!(protocol.listing().calls.is_empty());
    assert!(protocol.objects.lock().await.contains_key("d/e"));
    Ok(())
}

/// A listing refused for one prefix fails that directory only; a lost session fails it as a
/// session failure.
#[tokio::test]
async fn listing_failures_keep_their_scope() -> TestResult {
    let protocol = bucket(&[("d/e", b"1")]).await?;
    let namespace = namespace(&protocol)?;
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
        let result = listed_current(&*namespace, &path("d")).await;
        assert_eq!(
            matches!(result, Err(StorageRoleFailure::Session(_))),
            session
        );
        assert!(result.is_err());
        assert!(listed_versions(&*namespace, &path("d")).await.is_err());
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
    let namespace = namespace(&protocol)?;
    for all in [false, true] {
        let (rows, failures) = listed(namespace.as_ref(), "", all).await?;
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0].0, "a");
        assert_eq!(failures, ["../", "./"]);
        let (rows, failures) = listed(namespace.as_ref(), "a", all).await?;
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0].0, "a/b");
        assert_eq!(failures, ["a/.", "a/..", "a/./"]);
    }
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
    let empty = namespace(&Arc::new(MemoryS3::default()))?;
    assert_eq!(listed(empty.as_ref(), "", true).await?, (vec![], vec![]));
    let protocol = bucket(&[("d/e", b"1")]).await?;
    let namespace = namespace(&protocol)?;
    for all in [false, true] {
        let result = list_whole(&*namespace, &path("missing"), all).await;
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
    let namespace = namespace(&protocol)?;
    for cycle in [1, 2, 3] {
        for all in [false, true] {
            protocol.listing().cycle_tokens = cycle;
            let root = StoragePath::root();
            let listing = list_whole(&*namespace, &root, all);
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
    let namespace = namespace(&protocol)?;
    for all in [false, true] {
        let list = |name: &'static str| {
            let namespace = &namespace;
            async move { list_whole(&**namespace, &path(name), all).await }
        };
        let looping = timeout(Duration::from_secs(5), list("a")).await?;
        assert!(
            matches!(
                looping,
                Err(StorageRoleFailure::Entry(ref f))
                    if f.class() == FailureClass::Protocol && f.path() == &path("a")
            ),
            "all={all}: {looping:?}"
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
        let (rows, _) = listed(namespace.as_ref(), "", true).await?;
        assert_eq!(rows, [dir("d"), dir("gone")], "page size {page_size}");
        let (rows, _) = listed(namespace.as_ref(), "d", true).await?;
        assert_eq!(
            rows,
            [
                version("d/x", "v0001", false, false, 0),
                version("d/x", "v0002", true, false, 1),
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

/// A directory that lists more entries than one listing may hold fails (`Capacity`) instead of
/// being cut short; one at the limit lists whole.
#[tokio::test]
async fn a_directory_past_the_entry_limit_fails_whole() -> TestResult {
    let protocol = bucket(&[("a", b"1"), ("b", b"1"), ("c/d", b"1")]).await?;
    protocol.listing().page_size = 1;
    for all in [false, true] {
        let at_limit = limited(&protocol, 3);
        let listing = list_whole(&at_limit, &StoragePath::root(), all).await;
        assert_eq!(listing?.0.len(), 3);
        let over = limited(&protocol, 2);
        let listing = list_whole(&over, &StoragePath::root(), all).await;
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
    let namespace = limited(&protocol, 1000);
    for all in [false, true] {
        let root = StoragePath::root();
        let listing = list_whole(&namespace, &root, all);
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
