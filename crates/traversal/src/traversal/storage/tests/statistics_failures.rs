//! 子项失败、消失和终态失败不能伪装成 retained 或完整目录。
#![allow(clippy::unwrap_used)]
use super::*;
use crate::model::{BackendSessionFailure, SpecialFileKind};
use crate::traversal::DirectoryStatistics;
use tokio::sync::Semaphore;

struct ErrorNamespace;
#[async_trait]
impl Namespace for ErrorNamespace {
    fn list(&self, request: ListRequest) -> ListingStream {
        let page = ListingPage::from_descriptors(
            &request.path,
            vec![
                descriptor("bad", EntryKind::File),
                descriptor("gone", EntryKind::Symlink),
            ],
            vec![
                entry_failure(&path("unknown"), FailureClass::Unsupported),
                entry_failure(&path("unknown_gone"), FailureClass::NotFound),
            ],
            1,
        );
        ListingStream::from_pages(request.path, [Ok(page)])
    }
    async fn stat(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        Err(StorageRoleFailure::Entry(entry_failure(
            path,
            FailureClass::NotFound,
        )))
    }
}
struct ErrorMetadata;
#[async_trait]
impl Metadata for ErrorMetadata {
    async fn observe(
        &self,
        observed: &StoragePath,
        _: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        let class = if observed.as_str() == "gone" {
            FailureClass::NotFound
        } else {
            FailureClass::PermissionDenied
        };
        Err(StorageRoleFailure::Entry(entry_failure(observed, class)))
    }
    async fn apply(
        &self,
        _: &StoragePath,
        _: MetadataMutation,
        _: tokio_util::sync::CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        unreachable!("read-only traversal")
    }
}

#[tokio::test]
async fn directory_statistics_separate_known_failures_unknown_failures_and_vanished() {
    for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
        let source =
            StorageTraversalSource::with_roles(Arc::new(ErrorNamespace), Arc::new(ErrorMetadata));
        let mut session = source.traverse(
            request(tokio_util::sync::CancellationToken::new())
                .order(order)
                .directory_statistics(true),
        );
        let mut completed = None;
        while let Some(item) = session.next_item().await {
            if let TraversalItem::DirectoryStatistics(stats) = item
                && stats.outcome.is_some()
            {
                completed = Some(stats);
            }
        }
        assert!(matches!(
            session.finish().await,
            Ok(TraversalOutcome::Completed(_))
        ));
        let stats = completed.unwrap();
        assert_eq!(stats.scanned.total(), 4);
        assert_eq!(stats.failed.regular_files, 1);
        assert_eq!(stats.failed.unknown, 1);
        assert_eq!(stats.vanished.symlinks, 1);
        assert_eq!(stats.vanished.unknown, 1);
        assert_eq!(stats.retained.total(), 0);
        assert_eq!(stats.filtered.total(), 0);
    }
}

struct TerminalNamespace(Arc<Semaphore>);
#[async_trait]
impl Namespace for TerminalNamespace {
    fn list(&self, request: ListRequest) -> ListingStream {
        let gate = self.0.clone();
        let root = request.path;
        ListingStream::from_producer(root.clone(), move |sink| async move {
            let page = ListingPage::from_descriptors(
                &root,
                vec![descriptor("file", EntryKind::File)],
                Vec::new(),
                0,
            );
            if sink.send(page).await.is_err() {
                return;
            }
            let permit = gate.acquire().await.unwrap();
            permit.forget();
            let failure = BackendSessionFailure::new(
                Operation::Traverse,
                FailureClass::Connectivity,
                Transience::Transient,
                "disconnected",
            )
            .unwrap();
            sink.fail(StorageRoleFailure::Session(failure)).await;
        })
    }
}

#[tokio::test]
async fn directory_statistics_never_publish_complete_after_a_terminal_listing_fault() {
    for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
        let gate = Arc::new(Semaphore::new(0));
        let source = StorageTraversalSource::with_roles(
            Arc::new(TerminalNamespace(gate.clone())),
            Arc::new(FakeMetadata),
        );
        let mut session = source.traverse(
            request(tokio_util::sync::CancellationToken::new())
                .order(order)
                .directory_statistics(true),
        );
        let mut snapshots: Vec<Box<DirectoryStatistics>> = Vec::new();
        while let Some(item) = session.next_item().await {
            if let TraversalItem::DirectoryStatistics(stats) = item {
                assert!(stats.outcome.is_none());
                gate.add_permits(1);
                snapshots.push(stats);
            }
        }
        assert!(session.finish().await.is_err());
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].retained.regular_files, 1);
    }
}

struct SpecialNamespace;
#[async_trait]
impl Namespace for SpecialNamespace {
    fn list(&self, request: ListRequest) -> ListingStream {
        let special = [
            SpecialFileKind::BlockDevice,
            SpecialFileKind::CharacterDevice,
            SpecialFileKind::Fifo,
            SpecialFileKind::Socket,
        ];
        let mut values = special
            .into_iter()
            .enumerate()
            .map(|(index, kind)| descriptor(&format!("special{index}"), EntryKind::Special(kind)))
            .collect::<Vec<_>>();
        let mut unknown_size = descriptor("unknown_size", EntryKind::File);
        unknown_size.size = None;
        values.push(unknown_size);
        entries(&request.path, values)
    }
}

#[tokio::test]
async fn directory_statistics_keep_special_kinds_unknown_size_and_json_losslessly() {
    for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
        let source =
            StorageTraversalSource::with_roles(Arc::new(SpecialNamespace), Arc::new(FakeMetadata));
        let mut session = source.traverse(
            request(tokio_util::sync::CancellationToken::new())
                .order(order)
                .directory_statistics(true),
        );
        let mut completed = None;
        while let Some(item) = session.next_item().await {
            if let TraversalItem::DirectoryStatistics(stats) = item
                && stats.outcome.is_some()
            {
                completed = Some(*stats);
            }
        }
        session.finish().await.unwrap();
        let counts = completed.unwrap();
        assert_eq!(counts.retained.total(), 5);
        assert_eq!(counts.retained.regular_files, 1);
        assert_eq!(counts.retained.block_devices, 1);
        assert_eq!(counts.retained.character_devices, 1);
        assert_eq!(counts.retained.fifos, 1);
        assert_eq!(counts.retained.sockets, 1);
        assert_eq!(counts.unknown_file_sizes, 1);
        let json = serde_json::to_vec(&counts).unwrap();
        assert_eq!(
            serde_json::from_slice::<DirectoryStatistics>(&json).unwrap(),
            counts
        );
        assert_eq!(
            counts
                .retained
                .saturating_add(counts.retained)
                .saturating_sub(counts.retained),
            counts.retained
        );
    }
}
