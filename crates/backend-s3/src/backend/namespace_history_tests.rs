//! The S3 namespace role on a bucket that keeps histories (P7 §10, Q26): one child per key with
//! its whole history, oldest first, keys deleted now included; the entry-level facts are the
//! current object's; the answer at connection decides between histories and current objects.

use futures::StreamExt as _;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::model::{HistoryOrder, IdentityStrength, ObjectHistory, SourceVersion};
use crate::storage::ListRequest;

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
        key("gone", &[("v0005", false, false), ("v0006", true, true)]),
        key(
            "k",
            &[
                ("v0001", false, false),
                ("v0002", false, false),
                ("v0003", false, true),
                ("v0004", true, false),
            ],
        ),
        key(
            "z",
            &[
                ("v0007", false, false),
                ("v0008", false, false),
                ("v0009", true, false),
            ],
        ),
    ]
}

/// One child per key with its whole history, oldest first with the latest last — a key deleted
/// now (`gone`) included — whatever the page size and however the store spells its key marker. The
/// answer at connection, not the bucket's versioning, decides: a bucket that answered never
/// versioned lists its current objects only (`k` as v3, no `gone`), with `ListObjectsV2` alone.
#[tokio::test]
async fn every_key_is_one_child_with_its_history_across_pages() -> TestResult {
    let protocol = versioned_history().await?;
    let namespace = namespace(&protocol)?;
    for (page_size, minio) in [(0, false), (1, true), (2, true), (3, false), (4, true)] {
        {
            let mut knobs = protocol.listing();
            knobs.page_size = page_size;
            knobs.minio_markers = minio;
        }
        let (rows, failures) = listed(namespace.as_ref(), "").await?;
        assert_eq!(rows, expected_history(), "page size {page_size}");
        assert!(failures.is_empty());
    }
    let objects_before = protocol.listing().tokens.len();
    assert!(protocol.listing().version_pages > 0);
    assert_eq!(
        objects_before, 0,
        "a versioned bucket is listed with ListObjectVersions only"
    );
    lists_histories(&protocol, false);
    let versions_before = protocol.listing().version_pages;
    let (rows, _) = listed(super::namespace(&protocol)?.as_ref(), "").await?;
    assert_eq!(rows, [file("k"), file("z")]);
    assert_eq!(protocol.listing().version_pages, versions_before);
    Ok(())
}

/// A suspended bucket keeps the histories written while versioning was on, and its `"null"`
/// record: it lists histories as an enabled one does.
#[tokio::test]
async fn a_suspended_bucket_lists_histories() -> TestResult {
    let protocol = versioned_history().await?;
    protocol.set_versioning(Versioning::Suspended);
    put(&protocol, "z", b"z4").await?;
    let (rows, _) = listed(namespace(&protocol)?.as_ref(), "").await?;
    let mut expected = expected_history();
    expected[2] = key(
        "z",
        &[
            ("v0007", false, false),
            ("v0008", false, false),
            ("v0009", false, false),
            ("null", true, false),
        ],
    );
    assert_eq!(rows, expected);
    Ok(())
}

/// Never versioned, a store without versioning, or a NAS bucket: `ListObjectsV2` only, no
/// history on any child.
#[tokio::test]
async fn a_bucket_without_histories_is_listed_by_current_objects_only() -> TestResult {
    for answer in [
        S3VersioningAtConnect::Never,
        S3VersioningAtConnect::NoVersioning,
    ] {
        let protocol = bucket(&[("a", b"1"), ("d/e", b"1")]).await?;
        protocol.answer_at_connect(answer);
        let (rows, _) = listed(namespace(&protocol)?.as_ref(), "").await?;
        assert_eq!(rows, [file("a"), dir("d")], "{answer:?}");
        assert_eq!(protocol.listing().version_pages, 0, "{answer:?}");
    }
    let nas = Arc::new(MemoryS3::nas_bucket());
    nas.answer_at_connect(S3VersioningAtConnect::Refused);
    put(&nas, "a", b"1").await?;
    let (rows, _) = listed(namespace(&nas)?.as_ref(), "").await?;
    assert_eq!(rows, [file("a")]);
    assert_eq!(nas.listing().version_pages, 0);
    Ok(())
}

/// Entries written in one instant keep the listing order reversed, the latest last.
#[tokio::test]
async fn one_instant_keeps_the_written_order() -> TestResult {
    let protocol = versioned_history().await?;
    protocol.listing().same_instant = true;
    let (rows, _) = listed(namespace(&protocol)?.as_ref(), "").await?;
    assert_eq!(rows, expected_history());
    Ok(())
}

/// The order each listed key's history reports.
async fn history_orders(namespace: &dyn Namespace) -> TestResult<Vec<HistoryOrder>> {
    let (entries, _) = listed_current(namespace, &StoragePath::root()).await?;
    Ok(entries
        .iter()
        .filter_map(|entry| entry.listing.history.as_deref().map(ObjectHistory::order))
        .collect())
}

/// A store listing in S3's order gives server-ordered histories, even when every entry was written
/// in one instant and pages split the keys; a store that lists each key oldest first, or a key's
/// records apart (a non-conforming listing), is not trusted — its histories are inferred from
/// `LastModified` (here still right: every version has its own second) and marked so, each key
/// still one child.
#[tokio::test]
async fn a_non_conforming_store_is_ordered_by_time_and_marked_inferred() -> TestResult {
    let protocol = versioned_history().await?;
    let namespace = namespace(&protocol)?;
    {
        let mut knobs = protocol.listing();
        knobs.same_instant = true;
        knobs.page_size = 2;
    }
    assert_eq!(listed(namespace.as_ref(), "").await?.0, expected_history());
    let orders = history_orders(namespace.as_ref()).await?;
    assert!(orders.iter().all(|order| *order == HistoryOrder::Server));
    {
        let mut knobs = protocol.listing();
        knobs.same_instant = false;
        knobs.page_size = 0;
        knobs.oldest_first_versions = true;
    }
    assert_eq!(listed(namespace.as_ref(), "").await?.0, expected_history());
    let orders = history_orders(namespace.as_ref()).await?;
    assert_eq!(orders, [HistoryOrder::Inferred; 3]);
    {
        let mut knobs = protocol.listing();
        knobs.oldest_first_versions = false;
        knobs.split_keys = true;
        knobs.page_size = 2;
    }
    assert_eq!(listed(namespace.as_ref(), "").await?.0, expected_history());
    let orders = history_orders(namespace.as_ref()).await?;
    assert_eq!(orders, [HistoryOrder::Inferred; 3]);
    Ok(())
}

/// A key's entry-level facts are its current object's, exactly as `ListObjectsV2` gives them — the
/// same identity (so a bucket turning versioning on or off does not re-identify its objects), size
/// and `ETag`; each record selects itself for a copy. A key deleted now has no size, nothing behind
/// it to observe, and an identity of its own.
#[tokio::test]
async fn a_key_is_its_current_object_and_its_records_select_themselves() -> TestResult {
    let protocol = versioned_history().await?;
    let namespace = namespace(&protocol)?;
    let (entries, _) = listed_current(&*namespace, &StoragePath::root()).await?;
    let find = |name: &str| {
        entries
            .iter()
            .find(|entry| entry.path == path(name))
            .cloned()
            .ok_or(format!("{name} listed"))
    };
    let k = find("k")?;
    let history = k.listing.history.as_deref().ok_or("k has a history")?;
    let records = history.records();
    assert_eq!(records[0].source(), Some(SourceVersion::Id("v0001".into())));
    assert_eq!(records[0].size(), Some(2));
    assert!(records[2].is_delete_marker() && records[2].size().is_none());
    assert_eq!(records[2].source(), None);
    assert_eq!(records[3].source(), Some(SourceVersion::Id("v0004".into())));
    assert_eq!(
        history.current().and_then(VersionRecord::version_id),
        Some("v0004")
    );
    assert_eq!(k.size, Some(2));
    assert!(!k.listing.listing_only);
    assert_eq!(k.source_identity.strength(), IdentityStrength::PathScoped);
    let gone = find("gone")?;
    assert!(gone.listing.listing_only && gone.size.is_none());
    assert_ne!(gone.source_identity, k.source_identity);

    lists_histories(&protocol, false);
    let (current, _) = listed_current(&*super::namespace(&protocol)?, &StoragePath::root()).await?;
    let listed_k = current
        .iter()
        .find(|entry| entry.path == path("k"))
        .ok_or("k is current")?;
    assert_eq!(listed_k.source_identity, k.source_identity);
    assert_eq!(listed_k.size, k.size);
    assert_eq!(listed_k.content_version, k.content_version);
    Ok(())
}

/// The `"null"` version names no id: it selects `Current` while it is the latest, and
/// `Id("null")` once a newer version exists (`Current` would copy that one).
#[tokio::test]
async fn the_null_version_selects_itself_for_a_copy() -> TestResult {
    let unversioned = bucket(&[("n", b"1")]).await?;
    lists_histories(&unversioned, true);
    let (rows, _) = listed(namespace(&unversioned)?.as_ref(), "").await?;
    assert_eq!(rows, [key("n", &[("null", true, false)])]);

    let suspended = Arc::new(MemoryS3::default());
    suspended.set_versioning(Versioning::Enabled);
    put(&suspended, "s", b"real").await?;
    suspended.set_versioning(Versioning::Suspended);
    put(&suspended, "s", b"null").await?;
    let selected = |entries: &Described| -> Vec<(Option<String>, Option<SourceVersion>)> {
        entries
            .0
            .iter()
            .filter_map(|entry| entry.listing.history.as_deref())
            .flat_map(ObjectHistory::records)
            .map(|record| (record.version_id().map(str::to_string), record.source()))
            .collect()
    };
    let listing = listed_current(&*namespace(&suspended)?, &StoragePath::root()).await?;
    assert_eq!(
        selected(&listing),
        [
            (
                Some("v0001".to_string()),
                Some(SourceVersion::Id("v0001".into()))
            ),
            (None, Some(SourceVersion::Current))
        ]
    );
    suspended.set_versioning(Versioning::Enabled);
    put(&suspended, "s", b"newer").await?;
    let listing = listed_current(&*namespace(&suspended)?, &StoragePath::root()).await?;
    assert_eq!(
        selected(&listing)[1],
        (None, Some(SourceVersion::Id("null".into())))
    );
    Ok(())
}

/// A directory gathered whole is handed over a page at a time: `page_hint` keys built at a time,
/// the directory's own key (and the failures) on the first page, every key once, in key order.
#[tokio::test]
async fn a_gathered_directory_is_handed_over_page_by_page() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    for key in ["d/", "d/.data-mover-x", "d/a", "d/b", "d/c", "d/d", "d/e"] {
        put(&protocol, key, b"1").await?;
    }
    let namespace = namespace(&protocol)?;
    let request = ListRequest::new(path("d"), CancellationToken::new())
        .with_page_hint(NonZeroUsize::new(2).ok_or("nonzero")?);
    let mut stream = namespace.list(request);
    let mut pages = Vec::new();
    while let Some(page) = stream.next().await {
        let page = page?;
        let names: Vec<String> = page.children.iter().map(|c| c.name().to_owned()).collect();
        pages.push((names, page.internal_hidden));
    }
    assert_eq!(
        pages,
        [
            // The artifact is one of the first two keys built, and hidden.
            (vec![String::new(), "a".into()], 1),
            (vec!["b".into(), "c".into()], 0),
            (vec!["d".into(), "e".into()], 0),
        ]
    );
    Ok(())
}
