//! 绝对目录贡献与调度无关：过滤、分页和重放都经同一输出合同。
#![allow(clippy::unwrap_used)]
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt as _;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::model::SpecialFileKind;
use crate::traversal::{
    Checkpoint, DirectoryStatistics, ListingOutcome, Resumable, ScanTotals, TraversalFilter,
};

struct PagedNamespace {
    reverse: AtomicBool,
}

#[async_trait]
impl Namespace for PagedNamespace {
    fn list(&self, request: ListRequest) -> ListingStream {
        let root = request.path;
        if !root.as_str().is_empty() {
            assert_eq!(root.as_str(), "kept");
            return entries(&root, vec![descriptor("kept/child", EntryKind::File)]);
        }
        let mut children = [
            descriptor("first", EntryKind::File),
            descriptor("drop", EntryKind::File),
            descriptor("excluded", EntryKind::Directory),
            descriptor("kept", EntryKind::Directory),
            descriptor("link", EntryKind::Symlink),
            descriptor("pipe", EntryKind::Special(SpecialFileKind::Fifo)),
        ];
        if self.reverse.load(Ordering::SeqCst) {
            children.reverse();
        }
        let pages = children
            .chunks(2)
            .map(|chunk| {
                Ok(ListingPage::from_descriptors(
                    &root,
                    chunk.to_vec(),
                    Vec::new(),
                    2,
                ))
            })
            .collect::<Vec<_>>();
        ListingStream::from_pages(root, pages)
    }
    async fn read_link(&self, _: &StoragePath) -> Result<SymlinkTarget, StorageRoleFailure> {
        Ok(SymlinkTarget::new(SymlinkTargetEncoding::UnixBytes, b"first".to_vec()).unwrap())
    }
}

#[derive(Debug)]
struct Filter {
    all: bool,
    deferred: bool,
}
impl TraversalFilter for Filter {
    fn required_metadata(&self) -> MetadataRequirements {
        if self.deferred {
            MetadataRequirements::NONE.with(MetadataFamily::Timestamps)
        } else {
            MetadataRequirements::NONE
        }
    }
    fn decide(&self, candidate: &TraversalCandidate<'_>) -> TraversalDecision {
        if self.all || matches!(candidate.path, "drop" | "excluded") {
            TraversalDecision {
                emit: false,
                descend: false,
                filter_children: true,
            }
        } else if candidate.path == "kept" {
            TraversalDecision {
                emit: true,
                descend: false,
                filter_children: true,
            }
        } else {
            TraversalDecision::unfiltered(candidate.kind)
        }
    }
    fn resume_key(&self) -> Option<Vec<u8>> {
        Some(vec![u8::from(self.all), u8::from(self.deferred)])
    }
}

fn snapshots(items: &[TraversalItem]) -> Vec<&DirectoryStatistics> {
    items
        .iter()
        .filter_map(|item| match item {
            TraversalItem::DirectoryStatistics(stats) => Some(stats.as_ref()),
            _ => None,
        })
        .collect()
}

async fn collect(source: &StorageTraversalSource, request: TraversalRequest) -> Vec<TraversalItem> {
    let mut session = source.traverse(request);
    let mut items = Vec::new();
    while let Some(item) = session.next_item().await {
        items.push(item);
    }
    assert!(matches!(
        session.finish().await,
        Ok(TraversalOutcome::Completed(_))
    ));
    items
}

#[tokio::test]
async fn directory_statistics_exclude_pruned_hidden_entries_and_preserve_typed_kinds() {
    for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
        for deferred in [false, true] {
            let source = StorageTraversalSource::with_roles(
                Arc::new(PagedNamespace {
                    reverse: AtomicBool::new(false),
                }),
                Arc::new(FakeMetadata),
            );
            let items = collect(
                &source,
                request(CancellationToken::new())
                    .order(order)
                    .directory_statistics(true)
                    .filter(Arc::new(Filter {
                        all: false,
                        deferred,
                    })),
            )
            .await;
            let stats = snapshots(&items);
            assert_eq!(stats.len(), 4);
            assert!(stats[0].pass_start);
            assert!(stats.iter().skip(1).all(|stat| !stat.pass_start));
            assert_eq!(
                stats.iter().map(|s| s.revision).collect::<Vec<_>>(),
                [1, 2, 3, 4]
            );
            let final_stats = stats.last().unwrap();
            assert_eq!(final_stats.outcome, Some(ListingOutcome::Complete));
            assert_eq!(final_stats.scanned.total(), 5);
            assert_eq!(final_stats.filtered.regular_files, 1);
            assert_eq!(final_stats.retained.total(), 4);
            assert_eq!(final_stats.retained.directories, 1);
            assert_eq!(final_stats.retained.regular_files, 1);
            assert_eq!(final_stats.retained.symlinks, 1);
            assert_eq!(final_stats.retained.fifos, 1);
            assert_eq!(final_stats.regular_file_bytes, 3);
            assert_eq!(final_stats.pruned_dirs, 2);
            assert!(stats.iter().all(|s| {
                s.scanned
                    == s.retained
                        .saturating_add(s.filtered)
                        .saturating_add(s.failed)
                        .saturating_add(s.vanished)
            }));
        }
    }
}

#[tokio::test]
async fn fully_filtered_pages_still_deliver_partial_statistics() {
    for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
        let source = StorageTraversalSource::with_roles(
            Arc::new(PagedNamespace {
                reverse: AtomicBool::new(false),
            }),
            Arc::new(FakeMetadata),
        );
        let items = collect(
            &source,
            request(CancellationToken::new())
                .order(order)
                .directory_statistics(true)
                .filter(Arc::new(Filter {
                    all: true,
                    deferred: false,
                })),
        )
        .await;
        assert!(
            !items
                .iter()
                .any(|item| matches!(item, TraversalItem::Entry(_)))
        );
        let stats = snapshots(&items);
        assert_eq!(stats.len(), 4);
        assert!(stats[..3].iter().all(|stat| stat.outcome.is_none()));
        assert_eq!(stats[0].filtered.regular_files, 2);
        assert_eq!(stats.last().unwrap().filtered.total(), 4);
        assert_eq!(stats.last().unwrap().scanned.directories, 0);
    }
}

struct ReplayNamespace {
    inner: PagedNamespace,
    pause: AtomicBool,
}
#[async_trait]
impl Namespace for ReplayNamespace {
    fn list(&self, request: ListRequest) -> ListingStream {
        let root = request.path.clone();
        let cancel = request.cancel.clone();
        let mut stream = self.inner.list(request);
        if !root.as_str().is_empty() || !self.pause.load(Ordering::SeqCst) {
            return stream;
        }
        ListingStream::from_producer(root, move |sink| async move {
            if let Some(Ok(page)) = stream.next().await
                && sink.send(page).await.is_err()
            {
                return;
            }
            cancel.cancelled().await;
        })
    }
    async fn read_link(&self, path: &StoragePath) -> Result<SymlinkTarget, StorageRoleFailure> {
        self.inner.read_link(path).await
    }
}

async fn checkpoint_partial(
    source: &StorageTraversalSource,
    request: TraversalRequest,
) -> (Checkpoint, DirectoryStatistics) {
    let cancel = request.cancel.clone();
    let mut session = source.traverse(request);
    let mut partial = None;
    let mut checkpoint = None;
    while let Some(item) = session.next_item().await {
        match item {
            TraversalItem::DirectoryStatistics(counts) => {
                partial = Some(*counts);
                cancel.cancel();
            }
            TraversalItem::Checkpoint(saved) => checkpoint = Some(*saved),
            _ => {}
        }
    }
    assert!(matches!(
        session.finish().await,
        Ok(TraversalOutcome::Cancelled)
    ));
    (checkpoint.unwrap(), partial.unwrap())
}

#[tokio::test]
async fn a_reordered_replay_opens_a_new_absolute_pass_in_both_orders() {
    for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
        let namespace = Arc::new(ReplayNamespace {
            inner: PagedNamespace {
                reverse: AtomicBool::new(false),
            },
            pause: AtomicBool::new(true),
        });
        let source = StorageTraversalSource::with_roles(namespace.clone(), Arc::new(FakeMetadata));
        let original = request(CancellationToken::new())
            .order(order)
            .directory_statistics(true)
            .filter(Arc::new(Filter {
                all: false,
                deferred: false,
            }))
            .resumable(Resumable::On {
                every: Duration::ZERO,
            });
        let (checkpoint, old) = checkpoint_partial(&source, original.clone()).await;
        assert_eq!(old.scanned.total(), 2);
        assert!(old.outcome.is_none());
        namespace.pause.store(false, Ordering::SeqCst);
        namespace.inner.reverse.store(true, Ordering::SeqCst);
        let resumed = collect(
            &source,
            original
                .cancel(CancellationToken::new())
                .resume_from(checkpoint),
        )
        .await;
        let stats = snapshots(&resumed);
        assert_eq!(stats.len(), 4);
        assert!(stats[0].pass_start);
        assert_eq!(stats[0].revision, 1);
        let complete = stats.last().unwrap();
        assert_eq!(complete.outcome, Some(ListingOutcome::Complete));
        assert_eq!(complete.scanned.total(), 5);
        assert_eq!(complete.retained.total(), 4);
        // 目录贡献替换旧 2，不把新的完整 5 加成 7；变序不要求拒绝 checkpoint。
        let replaced = old
            .scanned
            .saturating_sub(old.scanned)
            .saturating_add(complete.scanned);
        assert_eq!(replaced.total(), 5);
    }
}

async fn raw_totals(source: &StorageTraversalSource, enabled: bool) -> (ScanTotals, usize) {
    let mut session = source.traverse(
        request(CancellationToken::new())
            .directory_statistics(enabled)
            .filter(Arc::new(Filter {
                all: false,
                deferred: false,
            })),
    );
    let mut extra = 0;
    while let Some(item) = session.next_item().await {
        extra += usize::from(matches!(item, TraversalItem::DirectoryStatistics(_)));
    }
    let TraversalOutcome::Completed(completion) = session.finish().await.unwrap() else {
        panic!("not completed")
    };
    (completion.totals, extra)
}

#[tokio::test]
async fn directory_statistics_opt_in_preserves_raw_totals_and_default_stream() {
    let source = StorageTraversalSource::with_roles(
        Arc::new(PagedNamespace {
            reverse: AtomicBool::new(false),
        }),
        Arc::new(FakeMetadata),
    );
    let (before, no_events) = raw_totals(&source, false).await;
    let (after, events) = raw_totals(&source, true).await;
    assert_eq!(before, after);
    assert_eq!(no_events, 0);
    assert_eq!(events, 4);
    assert_eq!(after.listed, 6);
    assert_eq!(after.filtered, 2);
    assert_eq!(after.internal_hidden, 6);
}
