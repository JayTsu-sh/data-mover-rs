use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};

use super::*;
use crate::model::EntryKind;
use crate::traversal::{TraversalOrder, TraversalTerminalFailure};

static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(1);

struct TestRoot(PathBuf);

impl TestRoot {
    fn new() -> io::Result<Self> {
        let sequence = TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "data-mover-local-traversal-{}-{sequence}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path)?;
        Ok(Self(path))
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn source(root: &Path) -> LocalTraversalSource {
    let identity = crate::storage::backends::local::test_identity("local-traversal-test");
    LocalTraversalSource::new(root, identity).unwrap_or_else(|error| panic!("{error}"))
}

fn request(cancel: CancellationToken, inflight: usize, buffered: usize) -> TraversalRequest {
    TraversalRequest {
        root: StoragePath::root(),
        order: TraversalOrder::Admission,
        max_inflight_operations: NonZeroUsize::new(inflight)
            .unwrap_or_else(|| unreachable!("test limit is nonzero")),
        max_buffered_items: NonZeroUsize::new(buffered)
            .unwrap_or_else(|| unreachable!("test limit is nonzero")),
        observation_plan: crate::model::ObservationPlan::default(),
        cancel,
    }
}

async fn drain(session: &mut TraversalSession) -> Vec<TraversalItem> {
    let mut items = Vec::new();
    while let Some(item) = session.next_item().await {
        items.push(item);
    }
    items
}

fn completed(outcome: TraversalOutcome) -> io::Result<TraversalCompletion> {
    match outcome {
        TraversalOutcome::Completed(completion) => Ok(completion),
        TraversalOutcome::Cancelled => Err(io::Error::other("unexpected cancellation")),
    }
}

#[tokio::test]
async fn traverses_recursively_and_returns_positive_completion() -> io::Result<()> {
    let root = TestRoot::new()?;
    std::fs::create_dir(root.0.join("dir"))?;
    std::fs::write(root.0.join("first"), b"1")?;
    std::fs::write(root.0.join("dir/second"), b"22")?;
    let mut session = source(&root.0).traverse(request(CancellationToken::new(), 2, 1));

    let items = drain(&mut session).await;
    let completion = completed(session.finish().await.map_err(io::Error::other)?)?;
    assert_eq!(completion.observed_entries, 3);
    assert_eq!(completion.entry_failures, 0);
    assert_eq!(items.len(), 3);
    assert!(items.iter().any(
        |item| matches!(item, TraversalItem::Entry(entry) if entry.kind() == EntryKind::Directory)
    ));
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn traversal_applies_one_optional_metadata_plan_to_each_entry() -> io::Result<()> {
    use crate::model::{MetadataObservation, MetadataProvenance, ObservationMode};

    let root = TestRoot::new()?;
    std::fs::write(root.0.join("file"), b"value")?;
    let source = source(&root.0);
    let mut request = request(CancellationToken::new(), 1, 1);
    request.observation_plan =
        crate::model::ObservationPlan::default().with_ownership_mode(ObservationMode::InlineOnly);
    let mut session = source.traverse(request);

    let items = drain(&mut session).await;
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
    let root = TestRoot::new()?;
    for index in 0..32 {
        std::fs::write(root.0.join(format!("file-{index}")), b"x")?;
    }
    let cancel = CancellationToken::new();
    let mut session = source(&root.0).traverse(request(cancel.clone(), 1, 1));
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
    let root = TestRoot::new()?;
    std::fs::write(root.0.join("file"), b"value")?;
    let source = source(&root.0);
    source
        .observer
        .delay_optional_calls(std::time::Duration::from_millis(200));
    let cancel = CancellationToken::new();
    let mut request = request(cancel.clone(), 1, 1);
    request.observation_plan =
        crate::model::ObservationPlan::default().with_acl(crate::model::ObservationMode::Required);
    let mut session = source.traverse(request);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert_eq!(source.observer.optional_call_count(), 1);
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
    let root = TestRoot::new()?;
    let mut request = request(CancellationToken::new(), 1, 1);
    request.root = StoragePath::new("../escape").map_err(io::Error::other)?;
    let mut session = source(&root.0).traverse(request);
    let items = drain(&mut session).await;
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
    let root = TestRoot::new()?;
    std::fs::create_dir(root.0.join("a"))?;
    std::fs::create_dir(root.0.join("b"))?;
    std::fs::write(root.0.join("a/child"), b"a")?;
    std::fs::write(root.0.join("b/child"), b"b")?;
    let source = source(&root.0);
    source.fail_enumeration_read(2);
    let mut session = source.traverse(request(CancellationToken::new(), 2, 2));

    let items = drain(&mut session).await;
    let completion = completed(session.finish().await.map_err(io::Error::other)?)?;
    let failed_path = source
        .failed_enumeration_path()
        .ok_or_else(|| io::Error::other("failure probe was not exercised"))?;
    let failure_index = items
        .iter()
        .position(|item| matches!(item, TraversalItem::EntryFailure(error) if error.path() == &failed_path))
        .ok_or_else(|| io::Error::other("missing injected entry failure"))?;

    assert_eq!(completion.entry_failures, 1);
    assert_eq!(completion.observed_entries, 3);
    assert!(
        items[failure_index + 1..]
            .iter()
            .any(|item| matches!(item, TraversalItem::Entry(_)))
    );
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
    let root = TestRoot::new()?;
    std::fs::write(root.0.join("file"), b"x")?;
    let session = source(&root.0).traverse(request(CancellationToken::new(), 1, 1));
    assert_eq!(
        session.finish().await,
        Err(TraversalTerminalFailure::Internal)
    );
    Ok(())
}

#[tokio::test]
async fn out_of_order_completion_is_reordered_and_backpressured() -> io::Result<()> {
    let root = TestRoot::new()?;
    for index in 0..12 {
        std::fs::write(root.0.join(format!("file-{index:02}")), b"x")?;
    }
    let source = source(&root.0);
    let mut session = source.traverse(request(CancellationToken::new(), 3, 1));
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let (started_before_drain, _) = source.observation_orders();
    assert!(started_before_drain.len() <= 3);

    let items = drain(&mut session).await;
    let completion = completed(session.finish().await.map_err(io::Error::other)?)?;
    let (started, completed) = source.observation_orders();
    let emitted = items
        .iter()
        .filter_map(|item| match item {
            TraversalItem::Entry(entry) => Some(entry.path().as_str().to_owned()),
            TraversalItem::EntryFailure(_) => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(completion.observed_entries, 12);
    assert_eq!(emitted, started);
    assert_ne!(completed, started);
    Ok(())
}

#[tokio::test]
async fn backend_session_failure_terminates_without_becoming_an_item() -> io::Result<()> {
    let root = TestRoot::new()?;
    let source = source(&root.0);
    let request = request(CancellationToken::new(), 2, 1);
    let (candidate_tx, candidate_rx) = mpsc::channel(1);
    let (item_tx, item_rx) = mpsc::channel(1);
    let (completion_tx, completion_rx) = oneshot::channel();
    tokio::spawn(run_observers(
        Arc::clone(&source.observer),
        request,
        candidate_rx,
        item_tx,
        completion_tx,
    ));
    let failure = BackendSessionFailure::new(
        Operation::Traverse,
        FailureClass::Protocol,
        Transience::Transient,
        "test backend session failed",
    )
    .map_err(io::Error::other)?;
    let pending = EntryOperationFailure::new(
        StoragePath::new("later").map_err(io::Error::other)?,
        Operation::Traverse,
        FailureClass::NotFound,
        Transience::Permanent,
        "test entry failed",
    )
    .map_err(io::Error::other)?;
    candidate_tx
        .send(Candidate::EntryFailure {
            sequence: 1,
            error: pending,
        })
        .await
        .map_err(io::Error::other)?;
    candidate_tx
        .send(Candidate::SessionFailure(failure.clone()))
        .await
        .map_err(io::Error::other)?;
    drop(candidate_tx);
    let mut session = TraversalSession::new(item_rx, completion_rx, CancellationToken::new());

    assert!(session.next_item().await.is_none());
    assert_eq!(
        session.finish().await,
        Err(TraversalTerminalFailure::Session(failure))
    );
    Ok(())
}

#[tokio::test]
async fn producer_loss_is_internal_not_successful_completion() -> io::Result<()> {
    let root = TestRoot::new()?;
    let source = source(&root.0);
    let request = request(CancellationToken::new(), 1, 1);
    let (candidate_tx, candidate_rx) = mpsc::channel(1);
    let (item_tx, item_rx) = mpsc::channel(1);
    let (completion_tx, completion_rx) = oneshot::channel();
    tokio::spawn(run_observers(
        Arc::clone(&source.observer),
        request,
        candidate_rx,
        item_tx,
        completion_tx,
    ));
    drop(candidate_tx);
    let mut session = TraversalSession::new(item_rx, completion_rx, CancellationToken::new());

    assert!(session.next_item().await.is_none());
    assert_eq!(
        session.finish().await,
        Err(TraversalTerminalFailure::Internal)
    );
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn non_utf8_entry_failures_keep_distinct_lossless_identities() -> io::Result<()> {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt as _;

    let root = TestRoot::new()?;
    std::fs::write(root.0.join(OsString::from_vec(vec![0xff])), b"a")?;
    std::fs::write(root.0.join(OsString::from_vec(vec![0xfe])), b"b")?;
    let mut session = source(&root.0).traverse(request(CancellationToken::new(), 1, 2));
    let items = drain(&mut session).await;
    let completion = completed(session.finish().await.map_err(io::Error::other)?)?;
    let paths = items
        .iter()
        .filter_map(|item| match item {
            TraversalItem::EntryFailure(error) => Some(error.path().as_str()),
            TraversalItem::Entry(_) => None,
        })
        .collect::<Vec<_>>();
    let identities = items
        .iter()
        .filter_map(|item| match item {
            TraversalItem::EntryFailure(error) => error.identity(),
            TraversalItem::Entry(_) => None,
        })
        .collect::<Vec<_>>();

    assert_eq!(completion.entry_failures, 2);
    assert_eq!(paths.len(), 2);
    assert_ne!(paths[0], paths[1]);
    assert_eq!(identities.len(), 2);
    assert_ne!(identities[0], identities[1]);
    assert!(
        paths
            .iter()
            .all(|path| path.starts_with("@local-unix-hex:"))
    );
    Ok(())
}
