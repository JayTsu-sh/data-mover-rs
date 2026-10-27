//! DSL filtering through the real Local backend and both traversal schedulers.

use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use filetime::{FileTime, set_file_mtime};

use crate::filter::parse_filter_expression;
use crate::model::{
    EntryKind, MetadataRequirements, ObservationMode, ObservationPlan, StoragePath,
};
use crate::storage::backends::local::test_traversal_storage;
use crate::time_util::now_secs;
use crate::traversal::{
    Checkpoint, Resumable, ResumeRefusal, StorageTraversalSource, TraversalCandidate,
    TraversalDecision, TraversalFilter, TraversalItem, TraversalOrder, TraversalOutcome,
    TraversalRequest, TraversalSource, TraversalTerminalFailure,
};
use crate::{AssertTestValue, DslTraversalFilter};

struct Scan {
    paths: Vec<String>,
    saved: Vec<(Vec<String>, Checkpoint)>,
    outcome: Result<TraversalOutcome, TraversalTerminalFailure>,
}

async fn scan(root: &Path, request: TraversalRequest) -> io::Result<Scan> {
    let (storage, _, _) = test_traversal_storage(root, "local-filter-test")
        .map_err(|error| io::Error::other(error.to_string()))?;
    let source = StorageTraversalSource::new(&storage).map_err(io::Error::other)?;
    let mut session = source.traverse(request);
    let mut paths = Vec::new();
    let mut saved = Vec::new();
    while let Some(item) = session.next_item().await {
        match item {
            TraversalItem::Entry(entry) => paths.push(entry.path().as_str().to_owned()),
            TraversalItem::Checkpoint(checkpoint) => saved.push((paths.clone(), *checkpoint)),
            TraversalItem::DirectoryListed(_) => {}
            TraversalItem::Failure(failure) => panic!("unexpected failure: {failure:?}"),
        }
    }
    paths.sort();
    Ok(Scan {
        paths,
        saved,
        outcome: session.finish().await,
    })
}

fn request(order: TraversalOrder, include: &str, exclude: Option<&str>) -> TraversalRequest {
    TraversalRequest::new(StoragePath::root())
        .order(order)
        .filter(Arc::new(
            DslTraversalFilter::parse(Some(include), exclude).assert_value("filter"),
        ))
}

fn assert_completed(scan: &Scan) {
    assert!(
        matches!(scan.outcome, Ok(TraversalOutcome::Completed(_))),
        "{:?}",
        scan.outcome
    );
}

#[tokio::test]
async fn lisa3849_excluded_files_stay_excluded_under_a_matched_directory() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir_all(root.path().join("k/sub"))?;
    for name in ["k/123.txt", "k/sub/123.txt", "k/sub/top.txt", "outside.txt"] {
        std::fs::write(root.path().join(name), b"value")?;
    }
    for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
        let scan = scan(
            root.path(),
            request(order, "path == k", Some("name == 123.txt")),
        )
        .await?;
        assert_completed(&scan);
        assert_eq!(scan.paths, ["k", "k/sub", "k/sub/top.txt"], "{order:?}");
    }
    Ok(())
}

#[tokio::test]
async fn globstar_ancestor_matching_keeps_allowed_descendants_in_both_orders() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir_all(root.path().join("k/a/sub"))?;
    for name in [
        "k/a/allowed",
        "k/a/blocked",
        "k/a/sub/allowed",
        "k/a/sub/blocked",
    ] {
        std::fs::write(root.path().join(name), b"value")?;
    }
    for include in ["path == 'k/**/a'", "path == 'k/**/**/a'", "path == '**/a'"] {
        for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
            let scan = scan(
                root.path(),
                request(order, include, Some("name == blocked")),
            )
            .await?;
            assert_completed(&scan);
            assert_eq!(
                scan.paths,
                ["k/a", "k/a/allowed", "k/a/sub", "k/a/sub/allowed"],
                "{order:?}: {include}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn negative_path_rechecks_the_wildcard_suffix_in_both_orders() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir_all(root.path().join("k/a"))?;
    for name in ["k/a/blocked", "k/a/allowed"] {
        std::fs::write(root.path().join(name), b"value")?;
    }
    for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
        let scan = scan(root.path(), request(order, "path != 'k/**/blocked'", None)).await?;
        assert_completed(&scan);
        assert_eq!(scan.paths, ["k/a", "k/a/allowed"], "{order:?}");
    }
    Ok(())
}

#[tokio::test]
async fn mixed_or_preserves_path_inheritance_and_the_other_branch() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("k"))?;
    for name in ["k/plain.txt", "outside.pdf", "outside.txt"] {
        std::fs::write(root.path().join(name), b"value")?;
    }
    for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
        let scan = scan(
            root.path(),
            request(order, "path == k or name == '*.pdf'", None),
        )
        .await?;
        assert_completed(&scan);
        assert_eq!(scan.paths, ["k", "k/plain.txt", "outside.pdf"], "{order:?}");
    }
    Ok(())
}

#[tokio::test]
async fn nested_date_directories_are_filtered_by_their_own_dates() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir_all(root.path().join("20240101/20240501"))?;
    std::fs::create_dir(root.path().join("20240101/misc"))?;
    for name in [
        "20240101/20240501/blocked",
        "20240101/misc/keep",
        "20240101/readme",
    ] {
        std::fs::write(root.path().join(name), b"value")?;
    }
    for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
        let scan = scan(root.path(), request(order, "dir_date <= 20240301", None)).await?;
        assert_completed(&scan);
        assert_eq!(
            scan.paths,
            [
                "20240101",
                "20240101/misc",
                "20240101/misc/keep",
                "20240101/readme"
            ]
        );
    }
    Ok(())
}

#[tokio::test]
async fn modified_conditions_are_applied_to_each_descendant() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("k"))?;
    std::fs::write(root.path().join("k/old"), b"old")?;
    std::fs::write(root.path().join("k/new"), b"new")?;
    let old = FileTime::from_unix_time(now_secs() - 30 * 86_400, 0);
    set_file_mtime(root.path().join("k/old"), old)?;
    set_file_mtime(root.path().join("k"), old)?;
    for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
        let request = request(order, "path == k and modified > 7d", None).observation_plan(
            ObservationPlan::default().with_timestamps(ObservationMode::InlineOnly),
        );
        let scan = scan(root.path(), request).await?;
        assert_completed(&scan);
        assert_eq!(scan.paths, ["k", "k/old"], "{order:?}");
    }
    Ok(())
}

/// Replays the old adapter's key and path-admission behavior to create pre-fix checkpoints.
#[derive(Debug)]
struct LegacyDslFilter {
    filter: DslTraversalFilter,
    key: Vec<u8>,
}

impl LegacyDslFilter {
    fn new() -> Self {
        let include = Some(parse_filter_expression("path == k").assert_value("include"));
        let exclude = Some(parse_filter_expression("name == 123.txt").assert_value("exclude"));
        let mut hasher = blake3::Hasher::new_derive_key("data-mover DSL traversal filter");
        hasher.update(format!("{include:?}").as_bytes());
        hasher.update(&[0]);
        hasher.update(format!("{exclude:?}").as_bytes());
        Self {
            filter: DslTraversalFilter::new(include, exclude),
            key: hasher.finalize().as_bytes().to_vec(),
        }
    }
}

impl TraversalFilter for LegacyDslFilter {
    fn required_metadata(&self) -> MetadataRequirements {
        self.filter.required_metadata()
    }

    fn decide(&self, candidate: &TraversalCandidate<'_>) -> TraversalDecision {
        let mut decision = self.filter.decide(candidate);
        if candidate.kind == EntryKind::Directory && decision.emit {
            decision.filter_children = false;
        }
        decision
    }

    fn resume_key(&self) -> Option<Vec<u8>> {
        Some(self.key.clone())
    }
}

#[tokio::test]
async fn old_dsl_checkpoints_are_refused_before_their_unfiltered_frontier_is_used() -> io::Result<()>
{
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("k"))?;
    std::fs::write(root.path().join("k/123.txt"), b"excluded")?;
    for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
        let legacy = request(order, "path == k", Some("name == 123.txt"))
            .filter(Arc::new(LegacyDslFilter::new()))
            .resumable(Resumable::On {
                every: Duration::ZERO,
            });
        let old = scan(root.path(), legacy).await?;
        assert_completed(&old);
        assert!(old.paths.iter().any(|path| path == "k/123.txt"));
        assert!(!old.saved.is_empty());
        for (_, checkpoint) in old.saved {
            let resumed = scan(
                root.path(),
                request(order, "path == k", Some("name == 123.txt")).resume_from(checkpoint),
            )
            .await?;
            assert!(resumed.paths.is_empty());
            assert_eq!(
                resumed.outcome,
                Err(TraversalTerminalFailure::Resume(ResumeRefusal::Mismatch))
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn current_dsl_checkpoints_resume_without_losing_entries_or_bypassing_excludes()
-> io::Result<()> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir_all(root.path().join("k/sub"))?;
    for name in ["k/123.txt", "k/sub/123.txt", "k/sub/top.txt"] {
        std::fs::write(root.path().join(name), b"value")?;
    }
    for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
        let make = || request(order, "path == k", Some("name == 123.txt"));
        let full = scan(
            root.path(),
            make().resumable(Resumable::On {
                every: Duration::ZERO,
            }),
        )
        .await?;
        assert_completed(&full);
        assert_eq!(full.paths, ["k", "k/sub", "k/sub/top.txt"]);
        assert!(!full.saved.is_empty());
        for (mut delivered, checkpoint) in full.saved {
            let resumed = scan(root.path(), make().resume_from(checkpoint)).await?;
            assert_completed(&resumed);
            assert!(resumed.paths.iter().all(|path| !path.ends_with("/123.txt")));
            delivered.extend(resumed.paths);
            delivered.sort();
            delivered.dedup();
            assert_eq!(delivered, full.paths, "{order:?}");
        }
    }
    Ok(())
}
