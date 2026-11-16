//! Whole-role denial is distinct from an unreadable or replaced data object.
use std::collections::{BTreeMap, BTreeSet, HashMap};

use super::*;
use crate::model::{EntryOperationFailure, MetadataFamily, ObservationPlan, SourceVersion};
use crate::storage::backends::local::connect_backend;
use crate::storage::{LocalBackendConfig, ReadRequest, Storage};

const BODY: &[u8] = b"readable source bytes";

struct WholeRole {
    class: FailureClass,
    session: bool,
    calls: Mutex<Vec<(StoragePath, ObservationPlan)>>,
}

impl WholeRole {
    fn denied() -> Arc<Self> {
        Arc::new(Self {
            class: FailureClass::PermissionDenied,
            session: false,
            calls: Mutex::new(Vec::new()),
        })
    }
}

#[async_trait]
impl Metadata for WholeRole {
    async fn observe(
        &self,
        path: &StoragePath,
        plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        assert!(
            !path.as_str().is_empty(),
            "copy-root ACL observation remains caller-owned"
        );
        self.calls.lock().push((path.clone(), plan));
        let diagnostic = "redacted whole-role source observation failure";
        if self.session {
            Err(StorageRoleFailure::Session(
                crate::model::BackendSessionFailure::new(
                    Operation::Observe,
                    self.class,
                    Transience::Permanent,
                    diagnostic,
                )
                .assert_value("whole-role fixture"),
            ))
        } else {
            Err(StorageRoleFailure::Entry(
                EntryOperationFailure::new(
                    path.clone(),
                    Operation::Observe,
                    self.class,
                    Transience::Permanent,
                    diagnostic,
                )
                .assert_value("whole-role fixture"),
            ))
        }
    }
    async fn apply(
        &self,
        _path: &StoragePath,
        _mutation: MetadataMutation,
        _cancel: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        panic!("source metadata is never mutated")
    }
}

fn all_best_effort() -> TraversalRequest {
    let mut request = configured();
    request.observation_plan = MetadataFamily::ALL
        .into_iter()
        .fold(ObservationPlan::default(), |plan, family| {
            plan.with(family, ObservationMode::BestEffort)
        });
    request.consumer_max_inflight = NonZeroUsize::MIN;
    request.max_inflight_operations = NonZeroUsize::MIN;
    request
}

fn local(root: &std::path::Path) -> Storage {
    connect_backend(LocalBackendConfig {
        root: root.to_path_buf(),
        read_concurrency: NonZeroUsize::MIN,
        write_concurrency: NonZeroUsize::MIN,
        read_chunk_bytes: None,
        write_chunk_bytes: None,
        rename_context: None,
    })
    .assert_value("whole-role connected Local source")
}

async fn pinned_read(
    storage: &Storage,
    entry: &ObservedEntry,
) -> Result<Vec<u8>, StorageRoleFailure> {
    let source = storage
        .read_source()
        .assert_value("whole-role source read role");
    let mut stream = source
        .read(ReadRequest {
            path: entry.path().clone(),
            range: None,
            expected_source: Some(entry.source_identity().clone()),
            maximum_chunk_bytes: 4096,
            read_inflight: 1,
            read_budget: None,
            cancel: CancellationToken::new(),
            source_qos: None,
            version: SourceVersion::Current,
        })
        .await?;
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        bytes.extend_from_slice(&chunk?);
    }
    Ok(bytes)
}

#[tokio::test]
async fn whole_role_permission_denied_retains_file_body_and_directory_children() {
    let root = tempfile::tempdir().assert_value("whole-role fixture");
    std::fs::create_dir(root.path().join("dir")).assert_value("whole-role fixture");
    for path in ["top", "dir/kept"] {
        std::fs::write(root.path().join(path), BODY).assert_value("whole-role fixture");
    }
    let storage = local(root.path());
    let namespace = storage.namespace().assert_value("whole-role namespace");
    let metadata = WholeRole::denied();
    let source = StorageTraversalSource::with_roles(namespace, metadata.clone())
        .identified(storage.identity().clone());
    let mut session = source.traverse(all_best_effort());
    let mut feedback = Vec::new();
    let mut entries = BTreeMap::new();
    let mut finalized = BTreeMap::new();
    let mut diagnostics = HashMap::<(String, MetadataFamily), usize>::new();
    let mut bodies = BTreeSet::new();
    while let Some(item) = session.next_item(&mut feedback).await {
        match item {
            TraversalItem::ObservationFailure(error) => {
                assert_eq!(error.class, FailureClass::PermissionDenied);
                *diagnostics
                    .entry((error.path.as_str().to_owned(), error.family))
                    .or_default() += 1;
            }
            TraversalItem::ConsumerEntry(work) => {
                let (id, entry) = *work;
                assert!(
                    entry.metadata().ownership_mode().value().is_some(),
                    "listed ownership must survive whole-role failure"
                );
                assert!(
                    entry.metadata().timestamps().value().is_some(),
                    "listed timestamps must survive whole-role failure"
                );
                if entry.kind() == EntryKind::File {
                    assert_eq!(
                        pinned_read(&storage, &entry)
                            .await
                            .assert_value("readable after metadata denial"),
                        BODY
                    );
                    bodies.insert(entry.path().as_str().to_owned());
                }
                entries.insert(
                    entry.path().as_str().to_owned(),
                    entry
                        .try_encode_snapshot()
                        .assert_value("whole-role snapshot"),
                );
                feedback.push((id, true));
            }
            TraversalItem::SubtreeReady(work) => {
                let (id, entry) = *work;
                finalized.insert(
                    entry.path().as_str().to_owned(),
                    entry
                        .try_encode_snapshot()
                        .assert_value("whole-role snapshot"),
                );
                feedback.push((id, true));
            }
            other => panic!(
                "whole metadata-role denial must retain known descriptor for data attempt: {other:?}"
            ),
        }
    }
    assert_eq!(
        bodies,
        BTreeSet::from(["top".to_owned(), "dir/kept".to_owned()])
    );
    assert_eq!(entries.len(), 3);
    assert_eq!(finalized.get("dir"), entries.get("dir"));
    for path in ["top", "dir", "dir/kept"] {
        for family in [
            MetadataFamily::Acl,
            MetadataFamily::Xattrs,
            MetadataFamily::Tags,
        ] {
            assert_eq!(
                diagnostics.get(&(path.to_owned(), family)),
                Some(&1),
                "each requested missing family once"
            );
        }
    }
    assert_eq!(diagnostics.len(), 9);
    {
        let calls = metadata.calls.lock();
        assert_eq!(calls.len(), 3, "one metadata request per selected entry");
        for (_, plan) in calls.iter() {
            assert_eq!(plan.ownership_mode(), ObservationMode::Omit);
            assert_eq!(plan.timestamps(), ObservationMode::Omit);
        }
    }
    assert!(matches!(
        session.finish().await,
        Ok(TraversalOutcome::Completed(_))
    ));
}

struct NoInline(Arc<dyn Namespace>);
#[async_trait]
impl Namespace for NoInline {
    async fn stat(
        &self,
        path: &StoragePath,
    ) -> Result<crate::storage::SourceDescriptor, StorageRoleFailure> {
        self.0.stat(path).await
    }
    fn list(&self, request: ListRequest) -> ListingStream {
        let path = request.path.clone();
        let mut stream = self.0.list(request);
        ListingStream::from_producer(path, move |sink| async move {
            while let Some(result) = stream.next().await {
                match result {
                    Ok(mut page) => {
                        for child in &mut page.children {
                            child.inline_ownership = None;
                            child.inline_timestamps = None;
                        }
                        if sink.send(page).await.is_err() {
                            break;
                        }
                    }
                    Err(error) => {
                        sink.fail(error).await;
                        break;
                    }
                }
            }
        })
    }
}

#[tokio::test]
async fn whole_role_denial_marks_all_five_actually_requested_unknown_families_once() {
    let root = tempfile::tempdir().assert_value("whole-role fixture");
    std::fs::write(root.path().join("kept"), BODY).assert_value("whole-role fixture");
    let storage = local(root.path());
    let metadata = WholeRole::denied();
    let namespace = Arc::new(NoInline(
        storage.namespace().assert_value("whole-role namespace"),
    ));
    let source = StorageTraversalSource::with_roles(namespace, metadata.clone())
        .identified(storage.identity().clone());
    let (items, terminal) = acknowledge(source.traverse(all_best_effort())).await;
    let failures: Vec<_> = items
        .iter()
        .filter_map(|item| {
            if let TraversalItem::ObservationFailure(f) = item {
                Some(f)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(failures.len(), 5);
    for family in MetadataFamily::ALL {
        assert_eq!(failures.iter().filter(|f| f.family == family).count(), 1);
    }
    let entry = items
        .iter()
        .find_map(|item| {
            if let TraversalItem::ConsumerEntry(work) = item {
                Some(&work.1)
            } else {
                None
            }
        })
        .assert_value("denied but known descriptor");
    assert_eq!(entry.metadata().failures().count(), 5);
    assert_eq!(
        pinned_read(&storage, entry)
            .await
            .assert_value("whole-role readable body"),
        BODY
    );
    assert!(matches!(terminal, Ok(TraversalOutcome::Completed(_))));
    assert_eq!(metadata.calls.lock().len(), 1);
}

#[tokio::test]
async fn other_role_failures_required_and_ordinary_none_never_gain_a_consumer_entry() {
    for (class, session, consumer, required) in [
        (FailureClass::NotFound, false, true, false),
        (FailureClass::Conflict, false, true, false),
        (FailureClass::Protocol, false, true, false),
        (FailureClass::Cancelled, false, true, false),
        (FailureClass::PermissionDenied, true, true, false),
        (FailureClass::PermissionDenied, false, true, true),
        (FailureClass::PermissionDenied, false, false, false),
    ] {
        let root = tempfile::tempdir().assert_value("whole-role fixture");
        std::fs::write(root.path().join("kept"), BODY).assert_value("whole-role fixture");
        let storage = local(root.path());
        let metadata = Arc::new(WholeRole {
            class,
            session,
            calls: Mutex::new(Vec::new()),
        });
        let source = StorageTraversalSource::with_roles(
            storage.namespace().assert_value("whole-role namespace"),
            metadata.clone(),
        )
        .identified(storage.identity().clone());
        let mut request = all_best_effort();
        if !consumer {
            request.consumer_completion_budget = None;
        }
        if required {
            request.observation_plan = request.observation_plan.with_acl(ObservationMode::Required);
        }
        let (items, terminal) = acknowledge(source.traverse(request)).await;
        assert!(
            !items.iter().any(|item| matches!(
                item,
                TraversalItem::Entry(_)
                    | TraversalItem::ConsumerEntry(_)
                    | TraversalItem::SubtreeReady(_)
            )),
            "unexpected data work for {class:?}/{session}/{consumer}/{required}"
        );
        assert_eq!(metadata.calls.lock().len(), 1);
        let expected = if class == FailureClass::NotFound {
            FailureClass::Conflict
        } else {
            class
        };
        if session {
            assert!(
                matches!(terminal,Err(TraversalTerminalFailure::Session(error)) if error.class()==expected)
            );
        } else {
            assert!(items.iter().any(|item| matches!(item,TraversalItem::Failure(error) if error.failure.class()==expected)),"original child failure must remain: {items:?}");
            if consumer {
                assert!(
                    matches!(terminal,Err(TraversalTerminalFailure::ConsumerSource(error)) if error.class()==expected)
                );
            } else {
                assert!(
                    matches!(terminal, Ok(TraversalOutcome::Completed(_))),
                    "ordinary scanning keeps its original child-failure outcome"
                );
            }
        }
    }
}

#[derive(Debug)]
struct NeedsFamily {
    family: MetadataFamily,
    calls: AtomicUsize,
}
impl TraversalFilter for NeedsFamily {
    fn required_metadata(&self) -> MetadataRequirements {
        MetadataRequirements::NONE.with(self.family)
    }
    fn decide(&self, candidate: &TraversalCandidate<'_>) -> TraversalDecision {
        self.calls.fetch_add(1, Ordering::SeqCst);
        TraversalDecision::unfiltered(candidate.kind)
    }
}

#[tokio::test]
async fn unknown_filter_dependency_keeps_original_failure_while_known_inline_dependency_can_proceed()
 {
    for (family, expected_entries, expected_decisions) in [
        (MetadataFamily::Acl, 0, 0),
        (MetadataFamily::OwnershipMode, 1, 1),
    ] {
        let root = tempfile::tempdir().assert_value("whole-role fixture");
        std::fs::write(root.path().join("kept"), BODY).assert_value("whole-role fixture");
        let storage = local(root.path());
        let metadata = WholeRole::denied();
        let filter = Arc::new(NeedsFamily {
            family,
            calls: AtomicUsize::new(0),
        });
        let source = StorageTraversalSource::with_roles(
            storage.namespace().assert_value("whole-role namespace"),
            metadata.clone(),
        )
        .identified(storage.identity().clone());
        let (items, terminal) =
            acknowledge(source.traverse(all_best_effort().filter(filter.clone()))).await;
        assert_eq!(
            items
                .iter()
                .filter(|item| matches!(item, TraversalItem::ConsumerEntry(_)))
                .count(),
            expected_entries
        );
        assert_eq!(filter.calls.load(Ordering::SeqCst), expected_decisions);
        assert_eq!(metadata.calls.lock().len(), 1);
        if expected_entries == 0 {
            assert!(matches!(
                terminal,
                Err(TraversalTerminalFailure::ConsumerSource(_))
            ));
        } else {
            assert!(matches!(terminal, Ok(TraversalOutcome::Completed(_))));
        }
    }
}

#[tokio::test]
async fn retained_metadata_failure_cannot_hide_a_real_missing_or_replaced_source_read() {
    for replaced in [false, true] {
        let root = tempfile::tempdir().assert_value("whole-role fixture");
        std::fs::write(root.path().join("kept"), BODY).assert_value("whole-role fixture");
        let storage = local(root.path());
        let metadata = WholeRole::denied();
        let source = StorageTraversalSource::with_roles(
            storage.namespace().assert_value("whole-role namespace"),
            metadata.clone(),
        )
        .identified(storage.identity().clone());
        let mut session = source.traverse(all_best_effort());
        let mut feedback = Vec::new();
        let mut diagnostics = 0;
        let mut attempted = false;
        while let Some(item) = session.next_item(&mut feedback).await {
            match item {
                TraversalItem::ObservationFailure(_) => diagnostics += 1,
                TraversalItem::ConsumerEntry(work) => {
                    let (id, entry) = *work;
                    // Keep the old inode alive, so the replacement cannot reuse its identity.
                    std::fs::rename(root.path().join("kept"), root.path().join("old-inode"))
                        .assert_value("read identity fixture");
                    if replaced {
                        std::fs::write(root.path().join("kept"), vec![b'x'; BODY.len()])
                            .assert_value("read identity fixture");
                    }
                    let result = pinned_read(&storage, &entry).await;
                    assert!(
                        matches!(result,Err(StorageRoleFailure::Entry(ref error)) if error.class()==if replaced {FailureClass::Conflict} else {FailureClass::NotFound}),
                        "bound read must reject the actual source change: {result:?}"
                    );
                    attempted = true;
                    feedback.push((id, false));
                }
                other => panic!("unexpected work after failed data attempt: {other:?}"),
            }
        }
        assert!(attempted);
        assert_eq!(diagnostics, 3);
        assert_eq!(
            session.finish().await,
            Err(TraversalTerminalFailure::ConsumerFailed)
        );
        assert_eq!(
            metadata.calls.lock().len(),
            1,
            "failed data read does not re-observe metadata"
        );
        assert_eq!(
            std::fs::read(root.path().join("old-inode")).assert_value("source inode retained"),
            BODY
        );
    }
}

struct RefusedLink(Arc<dyn Namespace>);
#[async_trait]
impl Namespace for RefusedLink {
    async fn stat(
        &self,
        path: &StoragePath,
    ) -> Result<crate::storage::SourceDescriptor, StorageRoleFailure> {
        self.0.stat(path).await
    }
    fn list(&self, request: ListRequest) -> ListingStream {
        self.0.list(request)
    }
    async fn read_link(&self, path: &StoragePath) -> Result<SymlinkTarget, StorageRoleFailure> {
        Err(StorageRoleFailure::Entry(
            EntryOperationFailure::new(
                path.clone(),
                Operation::Observe,
                FailureClass::PermissionDenied,
                Transience::Permanent,
                "redacted link read refused",
            )
            .assert_value("whole-role link fixture"),
        ))
    }
}

#[cfg(unix)]
#[tokio::test]
async fn metadata_fallback_never_turns_a_refused_link_read_into_an_entry() {
    let root = tempfile::tempdir().assert_value("whole-role fixture");
    std::os::unix::fs::symlink("outside", root.path().join("link"))
        .assert_value("whole-role link fixture");
    let storage = local(root.path());
    let metadata = WholeRole::denied();
    let namespace = Arc::new(RefusedLink(
        storage.namespace().assert_value("whole-role namespace"),
    ));
    let source = StorageTraversalSource::with_roles(namespace, metadata.clone())
        .identified(storage.identity().clone());
    let (items, terminal) = acknowledge(source.traverse(all_best_effort())).await;
    assert!(
        !items
            .iter()
            .any(|item| matches!(item, TraversalItem::ConsumerEntry(_)))
    );
    assert!(
        matches!(terminal,Err(TraversalTerminalFailure::ConsumerSource(error)) if error.class()==FailureClass::PermissionDenied)
    );
    let facts: Vec<_> = items
        .iter()
        .filter_map(|item| {
            if let TraversalItem::ObservationFailure(f) = item {
                Some(f.family)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(
        facts,
        [MetadataFamily::Tags],
        "ACL/xattrs are already NotApplicable for this listed symlink"
    );
    assert_eq!(metadata.calls.lock().len(), 1);
}

#[tokio::test]
async fn whole_role_denial_does_not_relabel_inline_only_unknown_family() {
    let root = tempfile::tempdir().assert_value("whole-role fixture");
    std::fs::write(root.path().join("kept"), BODY).assert_value("whole-role fixture");
    let storage = local(root.path());
    let metadata = WholeRole::denied();
    let source = StorageTraversalSource::with_roles(
        storage.namespace().assert_value("whole-role namespace"),
        metadata.clone(),
    )
    .identified(storage.identity().clone());
    let mut request = all_best_effort();
    request.observation_plan = request
        .observation_plan
        .with_tags(ObservationMode::InlineOnly);
    let (items, terminal) = acknowledge(source.traverse(request)).await;
    let entry = items
        .iter()
        .find_map(|item| {
            if let TraversalItem::ConsumerEntry(work) = item {
                Some(&work.1)
            } else {
                None
            }
        })
        .assert_value("known source remains available");
    assert!(matches!(
        entry.metadata().tags(),
        MetadataObservation::Unsupported
    ));
    assert_eq!(
        items
            .iter()
            .filter(|item| matches!(item, TraversalItem::ObservationFailure(_)))
            .count(),
        2
    );
    assert_eq!(metadata.calls.lock()[0].1.tags(), ObservationMode::Omit);
    assert!(matches!(terminal, Ok(TraversalOutcome::Completed(_))));
}
