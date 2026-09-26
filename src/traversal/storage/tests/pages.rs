//! Children observed inside their listing's task and children observed through the metadata
//! role must be indistinguishable in the output, and the first kind must cost no task of its own.

use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;
use crate::model::{MAX_MODEL_FIELD_BYTES, ObservationMode, StorageTimestamp, TimePrecision};
use crate::traversal::{TraversalCandidate, TraversalDecision, TraversalFilter, TraversalOrder};

/// The one timestamp every child has, derived from its path so both observation paths agree.
fn time_of(value: &str) -> TimestampMetadata {
    let nanos = value
        .bytes()
        .fold(1_700_000_000_i128, |sum, byte| sum * 31 + i128::from(byte))
        % 1_000_000_000_000;
    let time = StorageTimestamp::new(nanos * 1_000_000_000, TimePrecision::Nanoseconds)
        .unwrap_or_else(|error| panic!("{error}"));
    TimestampMetadata {
        accessed: None,
        modified: Some(time),
        created: None,
    }
}

/// Gives way `turns` times: enough to reorder completions, without timers slowing the test.
async fn linger(turns: usize) {
    for _ in 0..turns * 3 {
        tokio::task::yield_now().await;
    }
}

/// Files, symlinks and directories, several levels deep; every child's timestamp is either
/// carried by the listing (`inline`) or only known to [`TimesMetadata`].
struct MixedNamespace {
    inline: bool,
    /// Keep every `d09` directory's timestamps out of the listing even when `inline`.
    holdout: bool,
}

impl MixedNamespace {
    /// With `holdout`, every `d09` directory is held out of the listing's timestamps even when
    /// the rest are carried, so one block mixes directories decided in the listing's task with
    /// one whose decision waits for the metadata role: the descent must still follow the block's
    /// order.
    fn child(&self, value: &str, kind: EntryKind) -> SourceDescriptor {
        let described = descriptor(value, kind);
        if self.inline && !(self.holdout && value.ends_with("d09")) {
            described.with_inline_timestamps(time_of(value))
        } else {
            described
        }
    }

    fn children(&self, directory: &str) -> Vec<SourceDescriptor> {
        let join = |name: &str| {
            if directory.is_empty() {
                name.to_owned()
            } else {
                format!("{directory}/{name}")
            }
        };
        let depth = directory.matches('/').count() + usize::from(!directory.is_empty());
        let mut children = Vec::new();
        for index in (0..12).rev() {
            children.push(self.child(&join(&format!("f{index:02}")), EntryKind::File));
            if index % 5 == 0 {
                children.push(self.child(&join(&format!("l{index:02}")), EntryKind::Symlink));
            }
            if depth < 3 && index % 4 == 1 {
                children.push(self.child(&join(&format!("d{index:02}")), EntryKind::Directory));
            }
        }
        children
    }
}

#[async_trait]
impl Namespace for MixedNamespace {
    fn list(&self, request: ListRequest) -> ListingStream {
        entries(&request.path, self.children(request.path.as_str()))
    }

    async fn read_link(&self, link: &StoragePath) -> Result<SymlinkTarget, StorageRoleFailure> {
        // A read that takes a while, so role observations and link reads settle out of order
        // against the children observed in the listing's task.
        linger(link.as_str().len() % 3).await;
        Ok(
            SymlinkTarget::new(SymlinkTargetEncoding::UnixBytes, b"target".to_vec())
                .unwrap_or_else(|error| panic!("{error}")),
        )
    }
}

/// Answers every child's timestamps with what [`MixedNamespace`] would have carried inline.
#[derive(Default)]
struct TimesMetadata {
    calls: AtomicUsize,
}

#[async_trait]
impl Metadata for TimesMetadata {
    async fn observe(
        &self,
        observed: &StoragePath,
        plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        linger(observed.as_str().len() % 4).await;
        let timestamps = if plan.timestamps() == ObservationMode::Omit {
            MetadataObservation::NotRequested
        } else {
            MetadataObservation::Value {
                value: time_of(observed.as_str()),
                provenance: MetadataProvenance::Inline,
            }
        };
        MetadataObservations::new(
            MetadataObservation::NotRequested,
            MetadataObservation::NotRequested,
            MetadataObservation::NotRequested,
            MetadataObservation::NotRequested,
            timestamps,
        )
        .map_err(|_| StorageRoleFailure::Entry(entry_failure(observed, FailureClass::Protocol)))
    }

    async fn apply(
        &self,
        _path: &StoragePath,
        _mutation: MetadataMutation,
        _cancel: tokio_util::sync::CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        unreachable!("traversal never applies metadata")
    }
}

/// Needs `modified`: hides some entries by it, prunes one directory name and stops filtering
/// below another, so deferred decisions shape both the output and the descent.
#[derive(Debug)]
struct ByTime;

impl TraversalFilter for ByTime {
    fn needs_modified(&self) -> bool {
        true
    }

    fn decide(&self, candidate: &TraversalCandidate<'_>) -> TraversalDecision {
        let modified = candidate
            .modified
            .unwrap_or_else(|| panic!("{} decided without modified", candidate.path));
        TraversalDecision {
            emit: modified.unix_nanos() / 1_000_000_000 % 3 != 0,
            descend: candidate.name != "d05",
            filter_children: candidate.name != "d09",
        }
    }
}

fn mixed_request(order: TraversalOrder, window: usize, filtered: bool) -> TraversalRequest {
    TraversalRequest {
        root: StoragePath::root(),
        order,
        max_inflight_operations: NonZeroUsize::new(window)
            .unwrap_or_else(|| unreachable!("callers pass a nonzero window")),
        max_buffered_items: NonZeroUsize::new(2)
            .unwrap_or_else(|| unreachable!("constant is nonzero")),
        observation_plan: ObservationPlan::default().with_timestamps(ObservationMode::Required),
        cancel: tokio_util::sync::CancellationToken::new(),
        filter: filtered.then(|| Arc::new(ByTime) as Arc<dyn TraversalFilter>),
        max_depth: None,
        versions: TraversalVersions::Current,
    }
}

async fn every_item(
    inline: bool,
    request: TraversalRequest,
) -> (Vec<TraversalItem>, TraversalOutcome, usize) {
    let (source, observers) = StorageTraversalSource::with_roles_counting(
        Arc::new(MixedNamespace {
            inline,
            holdout: true,
        }),
        Arc::new(TimesMetadata::default()),
    );
    let mut session = source.traverse(request);
    let mut items = Vec::new();
    while let Some(item) = session.next_item().await {
        items.push(item);
    }
    let outcome = session
        .finish()
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    (items, outcome, observers.started.load(Ordering::SeqCst))
}

/// The same tree, once with every timestamp in the listing (files and directories observed in
/// the listing's task, symlinks still reading their target) and once with none (every child
/// observed through the metadata role), must give the same items, entry for entry and marker
/// for marker, in both orders, under a filter that defers on `modified` and without one, and
/// whatever the admission window.
#[tokio::test]
async fn children_observed_in_the_listing_task_are_indistinguishable_from_role_observed_ones() {
    for order in [TraversalOrder::Admission, TraversalOrder::NameBytes] {
        for filtered in [false, true] {
            for window in [1, 3, 64] {
                let request = mixed_request(order, window, filtered);
                let (listed, listed_outcome, _) = every_item(true, request.clone()).await;
                let (observed, observed_outcome, _) = every_item(false, request).await;
                let context = format!("{order:?} filtered={filtered} window={window}");
                assert!(listed.len() > 100, "{context}: {}", listed.len());
                assert_eq!(listed, observed, "{context}");
                assert_eq!(listed_outcome, observed_outcome, "{context}");
            }
        }
    }
}

/// Under the default plan nothing but listings reaches the backend, and no child costs a task.
#[tokio::test]
async fn children_needing_no_io_start_no_observation_task() {
    let mut request = mixed_request(TraversalOrder::Admission, 8, false);
    request.observation_plan = ObservationPlan::default();
    let (source, observers) = StorageTraversalSource::with_roles_counting(
        // No symlinks in this tree: a symlink's target is always a read.
        Arc::new(super::filter::TreeNamespace::new()),
        Arc::new(TimesMetadata::default()),
    );
    let mut session = source.traverse(request);
    let mut entries = 0;
    while let Some(item) = session.next_item().await {
        entries += usize::from(matches!(item, TraversalItem::Entry(_)));
    }
    assert!(matches!(
        session.finish().await,
        Ok(TraversalOutcome::Completed(_))
    ));
    assert!(entries > 0);
    assert_eq!(observers.started.load(Ordering::SeqCst), 0);
}

/// Children that do need the metadata role are observed by one task per block, however many
/// children the block has — never one task per child.
#[tokio::test]
async fn children_needing_io_cost_one_task_per_block() {
    let (items, outcome, observers) =
        every_item(false, mixed_request(TraversalOrder::Admission, 64, false)).await;
    let TraversalOutcome::Completed(completion) = outcome else {
        panic!("{outcome:?}")
    };
    let entries = items
        .iter()
        .filter(|item| matches!(item, TraversalItem::Entry(_)))
        .count();
    let blocks = items
        .iter()
        .filter(|item| matches!(item, TraversalItem::DirectoryListed(_)))
        .count();
    assert!(completion.totals.is_balanced(), "{:?}", completion.totals);
    assert!(
        entries > 10 * blocks,
        "{entries} entries in {blocks} blocks"
    );
    assert!(observers >= 1, "nothing was observed through the role");
    assert!(
        observers <= blocks,
        "{observers} observation tasks for {blocks} blocks"
    );
}

/// Many small blocks, each with a few children that need I/O: every block starts a task, and a
/// finished task must not stay behind for the rest of the traversal. What is held at once is
/// bounded by what is still being observed, not by how many blocks the tree has.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn finished_observation_tasks_do_not_accumulate() {
    const DIRECTORIES: usize = 300;
    let (source, observers) = StorageTraversalSource::with_roles_counting(
        Arc::new(SymlinkPerDirectory {
            directories: DIRECTORIES,
        }),
        Arc::new(TimesMetadata::default()),
    );
    let mut request = mixed_request(TraversalOrder::Admission, 64, false);
    request.observation_plan = ObservationPlan::default();
    let mut session = source.traverse(request);
    let mut links = 0;
    while let Some(item) = session.next_item().await {
        links += usize::from(
            matches!(&item, TraversalItem::Entry(entry) if entry.kind() == EntryKind::Symlink),
        );
    }
    assert!(matches!(
        session.finish().await,
        Ok(TraversalOutcome::Completed(_))
    ));
    assert_eq!(links, 3 * DIRECTORIES);
    assert_eq!(observers.started.load(Ordering::SeqCst), DIRECTORIES);
    let held = observers.peak_held.load(Ordering::SeqCst);
    // A window of 64 over three observed children per block keeps at most about 22 blocks
    // observing at once; without reaping this reaches the hundreds.
    assert!(held <= 32, "{held} observation tasks held at once");
}

/// `directories` directories under the root, each holding one file and three symlinks.
struct SymlinkPerDirectory {
    directories: usize,
}

#[async_trait]
impl Namespace for SymlinkPerDirectory {
    fn list(&self, request: ListRequest) -> ListingStream {
        let directory = request.path;
        if directory.as_str().is_empty() {
            return entries(
                &directory,
                (0..self.directories)
                    .map(|index| descriptor(&format!("d{index:04}"), EntryKind::Directory))
                    .collect(),
            );
        }
        let children = ["file", "link-a", "link-b", "link-c"]
            .into_iter()
            .map(|name| {
                let kind = if name == "file" {
                    EntryKind::File
                } else {
                    EntryKind::Symlink
                };
                descriptor(&format!("{}/{name}", directory.as_str()), kind)
            })
            .collect();
        entries(&directory, children)
    }

    async fn read_link(&self, _link: &StoragePath) -> Result<SymlinkTarget, StorageRoleFailure> {
        Ok(
            SymlinkTarget::new(SymlinkTargetEncoding::UnixBytes, b"file".to_vec())
                .unwrap_or_else(|error| panic!("{error}")),
        )
    }
}

/// A filter that needs `modified` decides in the listing's task whenever the listing carried
/// it, so no decision waits for an observation at all; without the listing's timestamps every
/// subdirectory's decision waits. The first test proves both give the same items.
#[tokio::test]
async fn a_filter_needing_modified_decides_at_once_when_the_listing_carries_it() {
    let mut waited = Vec::new();
    for inline in [true, false] {
        let metadata = Arc::new(TimesMetadata::default());
        let (source, peak) = StorageTraversalSource::with_roles_probed(
            Arc::new(MixedNamespace {
                inline,
                holdout: false,
            }),
            Arc::clone(&metadata) as Arc<dyn Metadata>,
        );
        let mut session = source.traverse(mixed_request(TraversalOrder::Admission, 8, true));
        while session.next_item().await.is_some() {}
        assert!(matches!(
            session.finish().await,
            Ok(TraversalOutcome::Completed(_))
        ));
        let calls = metadata.calls.load(Ordering::SeqCst);
        waited.push((peak.peak().deferred_decisions, calls));
    }
    // Only the symlinks' targets were read; nothing asked the metadata role.
    assert_eq!(waited[0], (0, 0), "with the timestamps listed");
    assert!(
        waited[1].0 > 0 && waited[1].1 > 0,
        "without: {:?}",
        waited[1]
    );
}

/// Under a filter that needs `modified`, a child whose observation fails inside the listing's
/// task keeps the deferred path's meaning: a failure item, and a directory still listed, with
/// the filter still applying below it — in its place among siblings decided at once.
#[tokio::test]
async fn an_observation_failing_in_the_listing_task_still_lists_the_directory_in_order() {
    let source = StorageTraversalSource::with_roles(
        Arc::new(OversizedFact),
        Arc::new(TimesMetadata::default()),
    );
    let mut request = mixed_request(TraversalOrder::Admission, 4, true);
    request.filter = Some(Arc::new(EveryModified));
    let mut session = source.traverse(request);
    let (mut listed, mut failures, mut entries) = (Vec::new(), Vec::new(), Vec::new());
    while let Some(item) = session.next_item().await {
        match item {
            TraversalItem::Entry(entry) => entries.push(entry.path().as_str().to_owned()),
            TraversalItem::Failure(failure) => failures.push(failure.path.as_str().to_owned()),
            TraversalItem::DirectoryListed(done) => listed.push(done.path.as_str().to_owned()),
            TraversalItem::Checkpoint(_) => {}
        }
    }
    assert!(matches!(
        session.finish().await,
        Ok(TraversalOutcome::Completed(_))
    ));
    assert_eq!(failures, ["b"]);
    assert_eq!(listed, ["", "a", "b", "c"]);
    assert_eq!(entries, ["a", "c", "a/f", "b/f", "c/f"]);
}

/// Needs `modified`, keeps everything, and asserts it was given one.
#[derive(Debug)]
struct EveryModified;

impl TraversalFilter for EveryModified {
    fn needs_modified(&self) -> bool {
        true
    }

    fn decide(&self, candidate: &TraversalCandidate<'_>) -> TraversalDecision {
        assert!(
            candidate.modified.is_some(),
            "{} undecidable",
            candidate.path
        );
        TraversalDecision {
            emit: true,
            descend: candidate.kind == EntryKind::Directory,
            filter_children: true,
        }
    }
}

/// Root directories `a`, `b`, `c`, each holding a file `f`, every timestamp listed; `b` carries
/// a backend fact over the model limit, so assembling its entry fails.
struct OversizedFact;

#[async_trait]
impl Namespace for OversizedFact {
    fn list(&self, request: ListRequest) -> ListingStream {
        let directory = request.path;
        let listed =
            |value: &str, kind| descriptor(value, kind).with_inline_timestamps(time_of(value));
        entries(
            &directory,
            if directory.as_str().is_empty() {
                let oversized = bytes::Bytes::from(vec![0_u8; MAX_MODEL_FIELD_BYTES + 1]);
                vec![
                    listed("a", EntryKind::Directory),
                    listed("b", EntryKind::Directory).with_backend_fact(oversized),
                    listed("c", EntryKind::Directory),
                ]
            } else {
                vec![listed(
                    &format!("{}/f", directory.as_str()),
                    EntryKind::File,
                )]
            },
        )
    }
}
