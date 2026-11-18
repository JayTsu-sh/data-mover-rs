//! 不迁移的内容对象只省略输出属性，过滤依赖、空标记及子项保持原有观察契约。
use super::*;
use crate::storage::NamespaceCapabilities;

struct SlashListing {
    history: bool,
    size: Option<u64>,
}

#[async_trait]
impl Namespace for SlashListing {
    fn capabilities(&self) -> NamespaceCapabilities {
        NamespaceCapabilities {
            directory_objects: true,
            ..NamespaceCapabilities::default()
        }
    }
    fn list(&self, request: ListRequest) -> ListingStream {
        let children = match request.path.as_str() {
            "" => ["a", "empty"]
                .map(|name| {
                    let mut directory = descriptor(name, EntryKind::Directory);
                    directory.listing.listing_only = true;
                    directory
                })
                .to_vec(),
            "a" => vec![self.content(), descriptor("a/file", EntryKind::File)],
            "empty" => {
                let mut marker = descriptor("empty/", EntryKind::File);
                marker.size = Some(0);
                vec![marker]
            }
            _ => Vec::new(),
        };
        entries(&request.path, children)
    }
}

impl SlashListing {
    fn content(&self) -> SourceDescriptor {
        let mut content = descriptor("a/", EntryKind::File);
        content.size = self.size;
        if self.history {
            let records = (0..20)
                .map(|index| {
                    VersionRecord::new(
                        Some(format!("v{index}")),
                        index == 19,
                        RecordKind::Object {
                            size: 3,
                            etag: format!("etag-{index}"),
                            modified: times().modified,
                        },
                        content.source_identity.clone(),
                        None,
                    )
                    .unwrap_or_else(|error| panic!("{error}"))
                })
                .collect();
            content.listing.history = Some(Box::new(
                ObjectHistory::whole(records, HistoryOrder::Server)
                    .unwrap_or_else(|error| panic!("{error}")),
            ));
        }
        content
    }
}

#[derive(Debug)]
struct TagsFilter;

impl TraversalFilter for TagsFilter {
    fn required_metadata(&self) -> MetadataRequirements {
        MetadataRequirements::NONE.with(F::Tags)
    }
    fn decide(&self, candidate: &TraversalCandidate<'_>) -> TraversalDecision {
        if candidate.kind == EntryKind::File {
            assert!(candidate.metadata.tags().value().is_some());
        }
        TraversalDecision::unfiltered(candidate.kind)
    }
}

fn verify_content_entries(items: &[TraversalItem], history: bool, observed: bool) {
    assert!(
        !items
            .iter()
            .any(|item| matches!(item, TraversalItem::Failure(_))),
        "{items:?}"
    );
    let found: Vec<_> = items
        .iter()
        .filter_map(|item| match item {
            TraversalItem::Entry(entry) => Some(entry),
            _ => None,
        })
        .collect();
    assert!(found.iter().any(|entry| entry.path().as_str() == "a/file"));
    assert!(found.iter().any(|entry| entry.path().as_str() == "empty/"));
    let content = found
        .iter()
        .find(|entry| entry.path().as_str() == "a/")
        .unwrap_or_else(|| panic!("content entry must still be delivered"));
    assert_eq!(
        content
            .history()
            .map_or(1, |history| history.records().len()),
        if history { 20 } else { 1 }
    );
    assert_eq!(content.metadata().tags().value().is_some(), observed);
}

#[derive(Clone, Copy)]
enum MetadataPurpose {
    Unused,
    Output,
    Filter,
    OutputAndFilter,
}

async fn check_case(order: TraversalOrder, history: bool, latest: bool, purpose: MetadataPurpose) {
    let output = matches!(
        purpose,
        MetadataPurpose::Output | MetadataPurpose::OutputAndFilter
    );
    let filter = matches!(
        purpose,
        MetadataPurpose::Filter | MetadataPurpose::OutputAndFilter
    );
    let fixture = fixture(false, S::Supported, Reply::Value, None);
    let source = StorageTraversalSource::with_roles(
        Arc::new(SlashListing {
            history,
            size: Some(3),
        }),
        fixture.source.metadata.clone(),
    );
    let mut request = TraversalRequest::new(StoragePath::root())
        .order(order)
        .observation_plan(ObservationPlan::default().with_tags(M::Required))
        .latest_metadata_only(latest)
        .directory_object_metadata(output);
    if filter {
        request = request.filter(Arc::new(TagsFilter));
    }
    let (items, outcome) = run(&source, request).await;
    assert!(matches!(outcome, Ok(TraversalOutcome::Completed(_))));
    verify_content_entries(&items, history, output || filter);
    let calls = fixture
        .observations
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .len();
    let expected = 2 + if output || filter {
        if history && !latest { 20 } else { 1 }
    } else {
        0
    };
    assert_eq!(
        calls, expected,
        "order={order:?} history={history} latest={latest} output={output} filter={filter}"
    );
}

#[tokio::test]
async fn omitted_directory_object_output_metadata_preserves_filter_reads_markers_and_children() {
    for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
        for history in [false, true] {
            for latest in [false, true] {
                for purpose in [
                    MetadataPurpose::Unused,
                    MetadataPurpose::Output,
                    MetadataPurpose::Filter,
                    MetadataPurpose::OutputAndFilter,
                ] {
                    check_case(order, history, latest, purpose).await;
                }
            }
        }
    }
}

#[tokio::test]
async fn unknown_directory_object_size_does_not_authorize_omitting_metadata() {
    let fixture = fixture(false, S::Supported, Reply::Value, None);
    let source = StorageTraversalSource::with_roles(
        Arc::new(SlashListing {
            history: false,
            size: None,
        }),
        fixture.source.metadata.clone(),
    );
    let (_, outcome) = run(
        &source,
        TraversalRequest::new(StoragePath::root())
            .observation_plan(ObservationPlan::default().with_tags(M::Required))
            .directory_object_metadata(false),
    )
    .await;
    assert!(matches!(outcome, Ok(TraversalOutcome::Completed(_))));
    assert_eq!(
        fixture
            .observations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len(),
        3
    );
}
