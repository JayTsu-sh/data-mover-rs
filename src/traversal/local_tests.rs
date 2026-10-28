//! Traversal over the Local backend, through the same `StorageTraversalSource` every other
//! namespace-lending backend uses. These run against a real filesystem, so CI exercises the
//! generic scheduler end to end without any external service.

use std::io;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use super::{
    FailureScope, ListingOutcome, StorageTraversalSource, TraversalCompletion, TraversalItem,
    TraversalOrder, TraversalOutcome, TraversalRequest, TraversalSession, TraversalSource,
    TraversalTerminalFailure,
};
#[cfg(unix)]
use crate::model::ObservationMode;
use crate::model::{
    EntryKind, FailureClass, ObservationPlan, Operation, SourceVersion, StoragePath,
};
#[cfg(unix)]
use crate::storage::Metadata as MetadataRole;
use crate::storage::backends::local::namespace::LocalNamespace;
use crate::storage::backends::local::observation::LocalObservationAdapter;
use crate::storage::backends::local::test_traversal_storage;
use crate::storage::listing::DEFAULT_PAGE_HINT;
#[cfg(unix)]
use crate::storage::listing::testing::listed;

struct Fixture {
    source: StorageTraversalSource,
    namespace: Arc<LocalNamespace>,
    adapter: Arc<LocalObservationAdapter>,
}

fn fixture(root: &Path) -> io::Result<Fixture> {
    let (storage, namespace, adapter) = test_traversal_storage(root, "local-traversal-test")
        .map_err(|error| io::Error::other(error.to_string()))?;
    let source = StorageTraversalSource::new(&storage).map_err(io::Error::other)?;
    Ok(Fixture {
        source,
        namespace,
        adapter,
    })
}

fn request(cancel: CancellationToken, inflight: usize) -> TraversalRequest {
    TraversalRequest::new(StoragePath::root())
        .order(TraversalOrder::Blocks)
        .max_inflight_operations(
            NonZeroUsize::new(inflight).unwrap_or_else(|| unreachable!("test limit is nonzero")),
        )
        .cancel(cancel)
}

async fn drain(session: &mut TraversalSession) -> Vec<TraversalItem> {
    let mut items = Vec::new();
    while let Some(item) = session.next_item().await {
        items.push(item);
    }
    items
}

/// Everything but the directory events, for tests that are about entry order.
async fn drain_entries(session: &mut TraversalSession) -> Vec<TraversalItem> {
    drain(session)
        .await
        .into_iter()
        .filter(|item| !crate::traversal::is_marker(item))
        .collect()
}

fn completed(outcome: TraversalOutcome) -> io::Result<TraversalCompletion> {
    match outcome {
        TraversalOutcome::Completed(completion) => Ok(completion),
        TraversalOutcome::Cancelled => Err(io::Error::other("unexpected cancellation")),
    }
}

fn entry_paths(items: &[TraversalItem]) -> Vec<String> {
    items
        .iter()
        .filter_map(|item| match item {
            TraversalItem::Entry(entry) => Some(entry.path().as_str().to_owned()),
            TraversalItem::Failure(_)
            | TraversalItem::ObservationFailure(_)
            | TraversalItem::DirectoryListed(_)
            | TraversalItem::Checkpoint(_) => None,
        })
        .collect()
}

#[tokio::test]
async fn traverses_recursively_and_returns_positive_completion() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("dir"))?;
    std::fs::write(root.path().join("first"), b"1")?;
    std::fs::write(root.path().join("dir/second"), b"22")?;
    let mut session = fixture(root.path())?
        .source
        .traverse(request(CancellationToken::new(), 2));

    let items = drain(&mut session).await;
    let completion = completed(session.finish().await.map_err(io::Error::other)?)?;
    assert_eq!(completion.totals.emitted, 3);
    assert_eq!(completion.totals.failed_children, 0);
    assert_eq!(completion.totals.listing_failures, 0);
    let mut paths = entry_paths(&items);
    paths.sort();
    // Root-relative paths carry no `./` prefix.
    assert_eq!(paths, ["dir", "dir/second", "first"]);
    assert!(items.iter().any(
        |item| matches!(item, TraversalItem::Entry(entry) if entry.kind() == EntryKind::Directory)
    ));
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn literal_backslash_is_distinct_from_nested_path() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("a"))?;
    std::fs::write(root.path().join("a/b"), b"nested")?;
    std::fs::write(root.path().join(r"a\b"), b"literal")?;
    let mut session = fixture(root.path())?
        .source
        .traverse(request(CancellationToken::new(), 2));
    let items = drain(&mut session).await;
    let completion = completed(session.finish().await.map_err(io::Error::other)?)?;
    assert_eq!(completion.totals.failed_children, 0);
    assert_eq!(completion.totals.listing_failures, 0);
    let paths = entry_paths(&items);
    for name in [r"a\b", "a/b"] {
        assert!(paths.iter().any(|path| path == name), "{paths:?}");
    }
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn traversal_applies_one_optional_metadata_plan_to_each_entry() -> io::Result<()> {
    use crate::model::{MetadataObservation, MetadataProvenance};

    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("file"), b"value")?;
    let mut request = request(CancellationToken::new(), 1);
    request.observation_plan =
        ObservationPlan::default().with_ownership_mode(ObservationMode::InlineOnly);
    let mut session = fixture(root.path())?.source.traverse(request);

    let items = drain_entries(&mut session).await;
    let _ = completed(session.finish().await.map_err(io::Error::other)?)?;
    assert!(matches!(
        items.as_slice(),
        [TraversalItem::Entry(entry)] if matches!(
            entry.metadata().ownership_mode(),
            MetadataObservation::Value { provenance: MetadataProvenance::Inline, .. }
        )
    ));
    Ok(())
}

#[tokio::test]
async fn cancellation_is_a_terminal_not_an_entry_failure() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    for index in 0..32 {
        std::fs::write(root.path().join(format!("file-{index}")), b"x")?;
    }
    let cancel = CancellationToken::new();
    let mut session = fixture(root.path())?
        .source
        .output_capacity(NonZeroUsize::MIN)
        .traverse(request(cancel.clone(), 1));
    assert!(session.next_item().await.is_some());
    cancel.cancel();
    let items = drain(&mut session).await;
    assert!(items.is_empty());
    assert_eq!(session.finish().await, Ok(TraversalOutcome::Cancelled));
    Ok(())
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_preempts_optional_metadata_observation() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("file"), b"value")?;
    let fixture = fixture(root.path())?;
    fixture
        .adapter
        .delay_optional_calls(std::time::Duration::from_millis(200));
    let cancel = CancellationToken::new();
    let mut request = request(cancel.clone(), 1);
    request.observation_plan =
        ObservationPlan::default().with_acl(crate::model::ObservationMode::Required);
    let mut session = fixture
        .source
        .output_capacity(NonZeroUsize::MIN)
        .traverse(request);
    // Wait for the slow ACL read to start rather than assuming it has after a fixed delay: the
    // listing runs on the blocking pool first, which a loaded test run can hold up.
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while fixture.adapter.optional_call_count() == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    })
    .await
    .map_err(io::Error::other)?;
    assert_eq!(fixture.adapter.optional_call_count(), 1);
    cancel.cancel();

    let outcome = tokio::time::timeout(std::time::Duration::from_millis(100), async {
        let _ = drain(&mut session).await;
        session.finish().await
    })
    .await
    .map_err(io::Error::other)?;
    assert_eq!(outcome, Ok(TraversalOutcome::Cancelled));
    Ok(())
}

#[tokio::test]
async fn an_invalid_root_is_an_ordered_listing_failure_with_completion() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let mut request = request(CancellationToken::new(), 1);
    request.root = StoragePath::new("../escape").map_err(io::Error::other)?;
    let mut session = fixture(root.path())?.source.traverse(request);
    let items = drain(&mut session).await;
    let completion = completed(session.finish().await.map_err(io::Error::other)?)?;

    assert_eq!(completion.totals.emitted, 0);
    assert_eq!(completion.totals.listed, 0);
    assert_eq!(completion.totals.listing_failures, 1);
    // The root could not be listed, so its block holds only that failure: the listing failure
    // first, then the `DirectoryListed` that says so. A consumer that ignores the directory event
    // still sees a listing failure and cannot read this as an empty directory.
    let [
        TraversalItem::Failure(failure),
        TraversalItem::DirectoryListed(listed),
    ] = items.as_slice()
    else {
        panic!("unexpected item sequence: {items:?}")
    };
    assert_eq!(failure.scope, FailureScope::Listing);
    assert_eq!(failure.path, listed.path);
    assert_eq!(failure.failure.operation(), Operation::Traverse);
    assert_eq!(listed.outcome, ListingOutcome::Unreadable);
    Ok(())
}

#[tokio::test]
async fn a_real_tree_is_counted_exactly() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("sub"))?;
    std::fs::write(root.path().join("top.txt"), b"value")?;
    std::fs::write(root.path().join("sub/inner.txt"), b"value")?;
    let mut session = fixture(root.path())?
        .source
        .traverse(request(CancellationToken::new(), 4));

    let items = drain(&mut session).await;
    let completion = completed(session.finish().await.map_err(io::Error::other)?)?;

    let listed: Vec<_> = items
        .iter()
        .filter_map(|item| match item {
            TraversalItem::DirectoryListed(listed) => Some(listed),
            _ => None,
        })
        .collect();
    let [root_block, sub_block] = listed.as_slice() else {
        panic!("two directories were listed: {items:?}")
    };
    assert_eq!(root_block.path, StoragePath::root());
    assert_eq!(sub_block.path.as_str(), "sub");
    for block in [root_block, sub_block] {
        assert_eq!(block.outcome, ListingOutcome::Complete);
        assert!(block.counts.is_balanced(), "{block:?}");
    }
    assert_eq!((root_block.counts.listed, sub_block.counts.listed), (2, 1));
    let totals = completion.totals;
    assert!(totals.is_balanced(), "{totals:?}");
    assert_eq!((totals.listed, totals.emitted), (3, 3));
    assert_eq!((totals.files, totals.directories), (2, 1));
    assert_eq!(totals.bytes, 10);
    assert_eq!(
        (
            totals.filtered,
            totals.failed_children,
            totals.listing_failures
        ),
        (0, 0, 0)
    );
    Ok(())
}

#[tokio::test]
async fn directory_read_failure_is_ordered_and_siblings_continue() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("a"))?;
    std::fs::create_dir(root.path().join("b"))?;
    std::fs::write(root.path().join("a/child"), b"a")?;
    std::fs::write(root.path().join("b/child"), b"b")?;
    let fixture = fixture(root.path())?;
    // Fail `a` by name: with concurrent listing, "the second List call" could be either child.
    fixture
        .namespace
        .fail_list_path(StoragePath::new("a").map_err(io::Error::other)?);
    let mut session = fixture
        .source
        .traverse(request(CancellationToken::new(), 2));

    let items = drain(&mut session).await;
    let completion = completed(session.finish().await.map_err(io::Error::other)?)?;
    let failed_path = fixture
        .namespace
        .failed_list_path()
        .ok_or_else(|| io::Error::other("failure probe was not exercised"))?;
    let failure_index = items
        .iter()
        .position(
            |item| matches!(item, TraversalItem::Failure(failure) if failure.path == failed_path),
        )
        .ok_or_else(|| io::Error::other("missing injected listing failure"))?;

    assert_eq!(completion.totals.listing_failures, 1);
    assert_eq!(completion.totals.failed_children, 0);
    assert_eq!(completion.totals.emitted, 3);
    // The sibling's subtree is still traversed. Where it lands relative to the failure depends
    // on the order the filesystem returns `a` and `b` in, so only its presence is asserted.
    assert!(entry_paths(&items).iter().any(|path| path == "b/child"));
    assert!(matches!(
        &items[failure_index],
        TraversalItem::Failure(failure)
            if failure.scope == FailureScope::Listing
                && failure.failure.operation() == Operation::Traverse
                && failure.failure.class() == FailureClass::PermissionDenied
    ));
    assert!(matches!(
        items.get(failure_index + 1),
        Some(TraversalItem::DirectoryListed(listed))
            if listed.path == failed_path && listed.outcome == ListingOutcome::Unreadable
    ));
    Ok(())
}

#[tokio::test]
async fn finish_before_eof_cannot_claim_completeness() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("file"), b"x")?;
    let session = fixture(root.path())?
        .source
        .traverse(request(CancellationToken::new(), 1));
    assert_eq!(
        session.finish().await,
        Err(TraversalTerminalFailure::Internal)
    );
    Ok(())
}

/// The default plan is served from the listing alone, and so is ownership (the listing's own
/// `lstat` carries it), so asking for xattrs is what sends every entry through the Metadata
/// role, whose probe delays the first observation.
#[cfg(unix)]
#[tokio::test]
async fn out_of_order_completion_is_reordered_and_backpressured() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    for index in 0..12 {
        std::fs::write(root.path().join(format!("file-{index:02}")), b"x")?;
    }
    let fixture = fixture(root.path())?;
    let mut request = request(CancellationToken::new(), 3);
    request.observation_plan = ObservationPlan::default().with_xattrs(ObservationMode::BestEffort);
    let mut session = fixture
        .source
        .output_capacity(NonZeroUsize::MIN)
        .traverse(request);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let (started_before_drain, _) = fixture.adapter.probe_orders();
    assert!(started_before_drain.len() <= 3);

    let items = drain(&mut session).await;
    let completion = completed(session.finish().await.map_err(io::Error::other)?)?;
    let (started, finished) = fixture.adapter.probe_orders();
    assert_eq!(completion.totals.emitted, 12);
    assert_eq!(entry_paths(&items), started);
    assert_ne!(finished, started);
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn non_utf8_entry_failures_keep_distinct_lossless_identities() -> io::Result<()> {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt as _;

    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join(OsString::from_vec(vec![0xff])), b"a")?;
    std::fs::write(root.path().join(OsString::from_vec(vec![0xfe])), b"b")?;
    let mut session = fixture(root.path())?
        .source
        .traverse(request(CancellationToken::new(), 1));
    let items = drain(&mut session).await;
    let completion = completed(session.finish().await.map_err(io::Error::other)?)?;
    let failures = items
        .iter()
        .filter_map(|item| match item {
            TraversalItem::Failure(failure) if failure.scope == FailureScope::Child => {
                Some(&failure.failure)
            }
            _ => None,
        })
        .collect::<Vec<_>>();

    assert_eq!(completion.totals.failed_children, 2);
    assert_eq!(completion.totals.listed, 2);
    assert_eq!(failures.len(), 2);
    // Both are failures at the listed directory (P7 C3: no stand-in path), told apart by their
    // raw names in the diagnostic and by their identities.
    assert!(
        failures
            .iter()
            .all(|error| error.path().as_str().is_empty())
    );
    let mut raw: Vec<bool> = ["\\xff", "\\xfe"]
        .iter()
        .map(|name| {
            failures
                .iter()
                .any(|error| error.diagnostic().contains(name))
        })
        .collect();
    raw.dedup();
    assert_eq!(raw, [true]);
    let identities = failures
        .iter()
        .filter_map(|error| error.identity())
        .collect::<Vec<_>>();
    assert_eq!(identities.len(), 2);
    assert_ne!(identities[0], identities[1]);
    Ok(())
}

#[tokio::test]
async fn traversal_omits_reserved_local_transfer_artifacts() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("dir"))?;
    for name in [
        ".data-mover-owned.stage",
        ".data-mover-owned.stage.checkpoint",
        ".data-mover-owned.stage.claim",
    ] {
        std::fs::write(root.path().join("dir").join(name), b"internal")?;
    }
    std::fs::write(root.path().join("dir/final.bin"), b"visible")?;
    let mut session = fixture(root.path())?
        .source
        .traverse(request(CancellationToken::new(), 2));
    let items = drain(&mut session).await;
    let completion = completed(session.finish().await.map_err(io::Error::other)?)?;
    assert_eq!(entry_paths(&items), ["dir", "dir/final.bin"]);
    assert_eq!(completion.totals.emitted, 2);
    assert_eq!(completion.totals.failed_children, 0);
    // Hidden, and counted as hidden rather than as children.
    assert_eq!(completion.totals.internal_hidden, 3);
    assert_eq!(completion.totals.listed, 2);
    Ok(())
}

/// A source without versions lists no histories: every entry of a Local traversal has none, and
/// it selects its current state for a copy.
#[tokio::test]
async fn a_source_without_versions_lists_no_histories() -> io::Result<()> {
    let temp = tempfile::tempdir()?;
    std::fs::write(temp.path().join("file"), b"x")?;
    std::fs::create_dir(temp.path().join("dir"))?;
    let fixture = fixture(temp.path())?;
    let mut session = fixture
        .source
        .traverse(request(CancellationToken::new(), 4));
    let items = drain(&mut session).await;
    let entries: Vec<_> = items
        .iter()
        .filter_map(|item| match item {
            TraversalItem::Entry(entry) => Some(entry),
            _ => None,
        })
        .collect();
    assert_eq!(entries.len(), 2);
    assert!(entries.iter().all(|entry| entry.history().is_none()));
    assert!(
        entries
            .iter()
            .all(|entry| entry.source_version() == Some(SourceVersion::Current))
    );
    assert!(matches!(
        session.finish().await,
        Ok(TraversalOutcome::Completed(_))
    ));
    Ok(())
}

/// Every plan, answered from what a listing's `lstat` carried, is exactly what the observation
/// role's own `lstat` gives for a file, a directory and a symlink; it declines only when an ACL
/// or xattrs are really wanted, which take a call.
#[cfg(unix)]
#[tokio::test]
async fn a_listed_child_is_observed_exactly_as_its_own_lstat_would() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("file"), b"x")?;
    std::fs::create_dir(root.path().join("dir"))?;
    std::os::unix::fs::symlink("file", root.path().join("link"))?;
    let fixture = fixture(root.path())?;
    let (listed, failures) = listed(fixture.namespace.as_ref(), &StoragePath::root())
        .await
        .map_err(io::Error::other)?;
    assert!(failures.is_empty(), "unexpected failures {failures:?}");
    assert_eq!(listed.len(), 3);
    let mut answered = 0;
    for (descriptor, plan) in listed
        .iter()
        .flat_map(|descriptor| ObservationPlan::every().map(move |plan| (descriptor, plan)))
    {
        let calls = |mode| {
            descriptor.kind != EntryKind::Symlink
                && matches!(
                    mode,
                    ObservationMode::BestEffort | ObservationMode::Required
                )
        };
        let needs_call = calls(plan.acl()) || calls(plan.xattrs());
        let Some(from_listing) = fixture.adapter.observe_listed(descriptor, plan) else {
            assert!(needs_call, "{plan:?} declined for {}", descriptor.path);
            continue;
        };
        assert!(!needs_call, "{plan:?} answered without its call");
        let from_lstat =
            MetadataRole::observe(fixture.adapter.as_ref(), &descriptor.path, plan).await;
        assert_eq!(
            format!("{from_listing:?}"),
            format!("{from_lstat:?}"),
            "{plan:?} for {}",
            descriptor.path
        );
        answered += 1;
    }
    assert!(answered > 300, "{answered}");
    Ok(())
}

/// Ownership is carried by the listing's `lstat`, so a traversal that asks for it observes
/// nothing through the metadata role and still reports it on every entry.
#[cfg(unix)]
#[tokio::test]
async fn an_ownership_plan_costs_no_observation_per_entry() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("dir"))?;
    for name in ["a", "dir/b", "dir/c"] {
        std::fs::write(root.path().join(name), b"x")?;
    }
    let fixture = fixture(root.path())?;
    let mut request = request(CancellationToken::new(), 4);
    request.observation_plan = ObservationPlan::default()
        .with_ownership_mode(ObservationMode::Required)
        .with_timestamps(ObservationMode::InlineOnly);
    let mut session = fixture.source.traverse(request);
    let items = drain(&mut session).await;
    let completion = completed(session.finish().await.map_err(io::Error::other)?)?;
    assert_eq!(completion.totals.emitted, 4);
    for item in &items {
        if let TraversalItem::Entry(entry) = item {
            assert!(
                entry.metadata().ownership_mode().value().is_some(),
                "{}",
                entry.path()
            );
        }
    }
    let (started, _) = fixture.adapter.probe_orders();
    assert!(started.is_empty(), "observed through the role: {started:?}");
    Ok(())
}

/// Removes `name` (a file or a link, relative to `root`) when a hook reaches it.
fn remove_when(
    root: &Path,
    name: &'static str,
) -> crate::storage::backends::local::namespace::Hook {
    let root = root.to_path_buf();
    Arc::new(move |native: &Path| {
        if native == Path::new(name) {
            let _ = std::fs::remove_file(root.join(native));
        }
    })
}

/// A real filesystem race, made deterministic by the namespace's test seams: one file deleted
/// between `readdir` and its `lstat`, and one symlink deleted before its target is read. Both
/// were listed and are gone: counted as vanished, not failed, and nothing is emitted for them
/// (P7 §8, Q12). Fault injection: report the `lstat`'s `NotFound` as a child failure, or settle
/// the link's `NotFound` as one, and `vanished` / `failed_children` go red.
#[cfg(unix)]
#[tokio::test]
async fn a_child_deleted_between_listing_and_observation_is_vanished() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("kept"), b"1")?;
    std::fs::write(root.path().join("raced"), b"2")?;
    std::os::unix::fs::symlink("kept", root.path().join("link"))?;
    let fixture = fixture(root.path())?;
    fixture
        .namespace
        .before_describe(remove_when(root.path(), "raced"));
    fixture
        .namespace
        .before_read_link(remove_when(root.path(), "link"));
    let mut session = fixture
        .source
        .traverse(request(CancellationToken::new(), 2));
    let items = drain(&mut session).await;
    let completion = completed(session.finish().await.map_err(io::Error::other)?)?;
    assert_eq!(entry_paths(&items), ["kept"]);
    assert!(
        !items
            .iter()
            .any(|item| matches!(item, TraversalItem::Failure(_))),
        "a vanished child is no failure"
    );
    let totals = completion.totals;
    assert_eq!(
        (
            totals.listed,
            totals.emitted,
            totals.vanished,
            totals.failed_children
        ),
        (3, 1, 2, 0)
    );
    assert!(totals.is_balanced());
    assert!(!root.path().join("raced").exists() && !root.path().join("link").exists());
    Ok(())
}

/// A subdirectory removed after its parent listed it and before its own listing: its listing is
/// complete and empty, no failure, counted in `vanished_listings`. The root itself missing is a
/// listing failure.
#[tokio::test]
async fn a_subdirectory_removed_before_its_listing_is_a_vanished_listing() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("doomed"))?;
    std::fs::write(root.path().join("doomed/inner"), b"1")?;
    let fixture = fixture(root.path())?;
    let doomed = root.path().join("doomed");
    // `doomed` was listed and emitted as the root's child; it is removed, with its contents, just
    // before its own listing opens it.
    fixture
        .namespace
        .before_list(Arc::new(move |native: &Path| {
            if native == Path::new("doomed") {
                let _ = std::fs::remove_dir_all(&doomed);
            }
        }));
    let mut session = fixture
        .source
        .traverse(request(CancellationToken::new(), 1));
    let items = drain(&mut session).await;
    let completion = completed(session.finish().await.map_err(io::Error::other)?)?;
    assert_eq!(entry_paths(&items), ["doomed"]);
    assert!(
        !items
            .iter()
            .any(|item| matches!(item, TraversalItem::Failure(_)))
    );
    let doomed_listed = items.iter().find_map(|item| match item {
        TraversalItem::DirectoryListed(listed) if listed.path.as_str() == "doomed" => {
            Some((listed.outcome, listed.counts.listed))
        }
        _ => None,
    });
    assert_eq!(doomed_listed, Some((ListingOutcome::Complete, 0)));
    let totals = completion.totals;
    assert_eq!(
        (
            totals.listing_failures,
            totals.vanished_listings,
            totals.emitted
        ),
        (0, 1, 1)
    );
    assert!(totals.is_balanced());

    let missing = TraversalRequest::new(StoragePath::new("absent").map_err(io::Error::other)?)
        .order(TraversalOrder::Blocks)
        .max_inflight_operations(NonZeroUsize::MIN);
    let mut session = fixture.source.traverse(missing);
    let items = drain(&mut session).await;
    let completion = completed(session.finish().await.map_err(io::Error::other)?)?;
    assert_eq!(completion.totals.listing_failures, 1);
    assert!(items.iter().any(|item| matches!(
        item,
        TraversalItem::Failure(failure)
            if failure.scope == FailureScope::Listing
                && failure.failure.class() == FailureClass::NotFound
    )));
    Ok(())
}

/// A directory read that fails after the first page (P7 C5, §8): the children read before it are
/// emitted, then one listing failure carrying how many were delivered, and the block closes
/// `Interrupted`. Fault injection: turn the failed read into a child failure (the stream reports
/// no more entries after an error, so the listing would end "complete") and this goes red.
#[tokio::test]
async fn a_directory_read_failing_after_the_first_page_is_interrupted() -> io::Result<()> {
    // The read fails in the second page of the traversal's listings.
    const FAIL_AT: usize = DEFAULT_PAGE_HINT.get() + 26;
    const FILES: usize = FAIL_AT + 50;
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("wide"))?;
    for index in 0..FILES {
        std::fs::write(root.path().join(format!("wide/f{index:05}")), b"")?;
    }
    let fixture = fixture(root.path())?;
    fixture
        .namespace
        .failing_directory_reads(Arc::new(|native: &Path, read: usize| {
            (native == Path::new("wide") && read == FAIL_AT)
                .then(|| io::Error::other("injected EIO"))
        }));
    let mut session = fixture
        .source
        .traverse(request(CancellationToken::new(), 4));
    let items = drain(&mut session).await;
    let completion = completed(session.finish().await.map_err(io::Error::other)?)?;

    let wide = StoragePath::new("wide").map_err(io::Error::other)?;
    let emitted_below = entry_paths(&items)
        .iter()
        .filter(|path| path.starts_with("wide/"))
        .count();
    assert_eq!(emitted_below, FAIL_AT);
    let failure = items
        .iter()
        .position(|item| matches!(item, TraversalItem::Failure(failure) if failure.path == wide))
        .ok_or_else(|| io::Error::other("missing listing failure"))?;
    assert!(matches!(
        &items[failure],
        TraversalItem::Failure(failure)
            if failure.scope == FailureScope::Listing
                && failure.delivered == Some(FAIL_AT as u64)
    ));
    assert!(matches!(
        items.get(failure + 1),
        Some(TraversalItem::DirectoryListed(listed))
            if listed.path == wide && listed.outcome == ListingOutcome::Interrupted
    ));
    let totals = completion.totals;
    assert_eq!(
        (
            totals.interrupted_listings,
            totals.emitted,
            totals.failed_children
        ),
        (1, FAIL_AT as u64 + 1, 0)
    );
    assert!(totals.is_balanced());
    Ok(())
}
