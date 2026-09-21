//! Traversal over the Local backend, through the same `StorageTraversalSource` every other
//! namespace-lending backend uses. These run against a real filesystem, so CI exercises the
//! generic scheduler end to end without any external service.

use std::io;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use super::{
    StorageTraversalSource, TraversalCompletion, TraversalItem, TraversalOrder, TraversalOutcome,
    TraversalRequest, TraversalSession, TraversalSource, TraversalTerminalFailure,
};
use crate::model::{EntryKind, FailureClass, ObservationPlan, Operation, StoragePath};
use crate::storage::backends::local::namespace::LocalNamespace;
use crate::storage::backends::local::observation::LocalObservationAdapter;
use crate::storage::backends::local::test_traversal_storage;

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

fn request(cancel: CancellationToken, inflight: usize, buffered: usize) -> TraversalRequest {
    TraversalRequest {
        root: StoragePath::root(),
        order: TraversalOrder::Admission,
        max_inflight_operations: NonZeroUsize::new(inflight)
            .unwrap_or_else(|| unreachable!("test limit is nonzero")),
        max_buffered_items: NonZeroUsize::new(buffered)
            .unwrap_or_else(|| unreachable!("test limit is nonzero")),
        observation_plan: ObservationPlan::default(),
        cancel,
        filter: None,
        max_depth: None,
    }
}

async fn drain(session: &mut TraversalSession) -> Vec<TraversalItem> {
    let mut items = Vec::new();
    while let Some(item) = session.next_item().await {
        items.push(item);
    }
    items
}

/// Everything but the directory completion items, for tests that are about entry order.
async fn drain_entries(session: &mut TraversalSession) -> Vec<TraversalItem> {
    drain(session)
        .await
        .into_iter()
        .filter(|item| !crate::traversal::is_completion(item))
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
            TraversalItem::EntryFailure(_)
            | TraversalItem::DirectoryListed(_)
            | TraversalItem::SubtreeComplete(_) => None,
        })
        .collect()
}

#[tokio::test]
async fn traverses_recursively_and_returns_positive_completion() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("dir"))?;
    std::fs::write(root.path().join("first"), b"1")?;
    std::fs::write(root.path().join("dir/second"), b"22")?;
    let mut session =
        fixture(root.path())?
            .source
            .traverse(request(CancellationToken::new(), 2, 1));

    let items = drain(&mut session).await;
    let completion = completed(session.finish().await.map_err(io::Error::other)?)?;
    assert_eq!(completion.observed_entries, 3);
    assert_eq!(completion.entry_failures, 0);
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
    let mut session =
        fixture(root.path())?
            .source
            .traverse(request(CancellationToken::new(), 2, 1));
    let items = drain(&mut session).await;
    let completion = completed(session.finish().await.map_err(io::Error::other)?)?;
    assert_eq!(completion.entry_failures, 0);
    let paths = entry_paths(&items);
    for name in [r"a\b", "a/b"] {
        assert!(paths.iter().any(|path| path == name), "{paths:?}");
    }
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn traversal_applies_one_optional_metadata_plan_to_each_entry() -> io::Result<()> {
    use crate::model::{MetadataObservation, MetadataProvenance, ObservationMode};

    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("file"), b"value")?;
    let mut request = request(CancellationToken::new(), 1, 1);
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
        .traverse(request(cancel.clone(), 1, 1));
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
    let mut request = request(cancel.clone(), 1, 1);
    request.observation_plan =
        ObservationPlan::default().with_acl(crate::model::ObservationMode::Required);
    let mut session = fixture.source.traverse(request);
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
async fn invalid_subtree_is_an_ordered_entry_failure_with_completion() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let mut request = request(CancellationToken::new(), 1, 1);
    request.root = StoragePath::new("../escape").map_err(io::Error::other)?;
    let mut session = fixture(root.path())?.source.traverse(request);
    let items = drain_entries(&mut session).await;
    let completion = completed(session.finish().await.map_err(io::Error::other)?)?;

    assert_eq!(completion.observed_entries, 0);
    assert_eq!(completion.entry_failures, 1);
    assert!(
        matches!(items.as_slice(), [TraversalItem::EntryFailure(error)] if error.operation() == Operation::Traverse)
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
        .traverse(request(CancellationToken::new(), 2, 2));

    let items = drain(&mut session).await;
    let completion = completed(session.finish().await.map_err(io::Error::other)?)?;
    let failed_path = fixture
        .namespace
        .failed_list_path()
        .ok_or_else(|| io::Error::other("failure probe was not exercised"))?;
    let failure_index = items
        .iter()
        .position(|item| matches!(item, TraversalItem::EntryFailure(error) if error.path() == &failed_path))
        .ok_or_else(|| io::Error::other("missing injected entry failure"))?;

    assert_eq!(completion.entry_failures, 1);
    assert_eq!(completion.observed_entries, 3);
    // The sibling's subtree is still traversed. Where it lands relative to the failure depends
    // on the order the filesystem returns `a` and `b` in, so only its presence is asserted.
    assert!(entry_paths(&items).iter().any(|path| path == "b/child"));
    assert!(matches!(
        &items[failure_index],
        TraversalItem::EntryFailure(error)
            if error.operation() == Operation::Traverse
                && error.class() == FailureClass::PermissionDenied
    ));
    Ok(())
}

#[tokio::test]
async fn finish_before_eof_cannot_claim_completeness() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("file"), b"x")?;
    let session = fixture(root.path())?
        .source
        .traverse(request(CancellationToken::new(), 1, 1));
    assert_eq!(
        session.finish().await,
        Err(TraversalTerminalFailure::Internal)
    );
    Ok(())
}

/// The default plan is served from the listing alone, so an explicit ownership plan is what
/// sends every entry through the Metadata role, whose probe delays the first observation.
#[cfg(unix)]
#[tokio::test]
async fn out_of_order_completion_is_reordered_and_backpressured() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    for index in 0..12 {
        std::fs::write(root.path().join(format!("file-{index:02}")), b"x")?;
    }
    let fixture = fixture(root.path())?;
    let mut request = request(CancellationToken::new(), 3, 1);
    request.observation_plan =
        ObservationPlan::default().with_ownership_mode(crate::model::ObservationMode::InlineOnly);
    let mut session = fixture.source.traverse(request);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let (started_before_drain, _) = fixture.adapter.probe_orders();
    assert!(started_before_drain.len() <= 3);

    let items = drain(&mut session).await;
    let completion = completed(session.finish().await.map_err(io::Error::other)?)?;
    let (started, finished) = fixture.adapter.probe_orders();
    assert_eq!(completion.observed_entries, 12);
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
    let mut session =
        fixture(root.path())?
            .source
            .traverse(request(CancellationToken::new(), 1, 2));
    let items = drain(&mut session).await;
    let completion = completed(session.finish().await.map_err(io::Error::other)?)?;
    let failures = items
        .iter()
        .filter_map(|item| match item {
            TraversalItem::EntryFailure(error) => Some(error),
            TraversalItem::Entry(_)
            | TraversalItem::DirectoryListed(_)
            | TraversalItem::SubtreeComplete(_) => None,
        })
        .collect::<Vec<_>>();

    assert_eq!(completion.entry_failures, 2);
    assert_eq!(failures.len(), 2);
    assert_ne!(failures[0].path(), failures[1].path());
    assert!(
        failures
            .iter()
            .all(|error| error.path().as_str().starts_with("@local-unix-hex:"))
    );
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
    let mut session =
        fixture(root.path())?
            .source
            .traverse(request(CancellationToken::new(), 2, 1));
    let items = drain(&mut session).await;
    let completion = completed(session.finish().await.map_err(io::Error::other)?)?;
    assert_eq!(entry_paths(&items), ["dir", "dir/final.bin"]);
    assert_eq!(completion.observed_entries, 2);
    assert_eq!(completion.entry_failures, 0);
    Ok(())
}
