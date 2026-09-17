use std::sync::Mutex;

use async_trait::async_trait;

use super::*;
use crate::model::{BackendIdentity, BackendKind, IdentityStrength, SourceIdentity};
use crate::storage::SourceDescriptor;

/// In-memory namespace: a flat map of path → kind with recorded deletes.
struct MemoryTree {
    entries: Mutex<BTreeMap<String, EntryKind>>,
    deletes: Mutex<Vec<String>>,
    /// Paths whose Delete fails with the given class.
    failing: BTreeMap<String, FailureClass>,
    /// Paths that vanish before Delete (server answers `NotFound`).
    vanished: Vec<String>,
    list_failure: Option<String>,
    block_deletes: Option<CancellationToken>,
}

impl MemoryTree {
    fn new(paths: &[(&str, EntryKind)]) -> Self {
        Self {
            entries: Mutex::new(
                paths
                    .iter()
                    .map(|(path, kind)| ((*path).to_owned(), *kind))
                    .collect(),
            ),
            deletes: Mutex::new(Vec::new()),
            failing: BTreeMap::new(),
            vanished: Vec::new(),
            list_failure: None,
            block_deletes: None,
        }
    }

    fn deletes(&self) -> Vec<String> {
        self.deletes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn remaining(&self) -> Vec<String> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .cloned()
            .collect()
    }

    fn descriptor(path: &str, kind: EntryKind) -> SourceDescriptor {
        SourceDescriptor::new(
            StoragePath::new(path).unwrap_or_else(|error| panic!("{error}")),
            kind,
            (kind == EntryKind::File).then_some(1),
            SourceIdentity::new(
                BackendIdentity::new(BackendKind::Nfs, "delete-tree-test")
                    .unwrap_or_else(|error| panic!("{error}")),
                IdentityStrength::PathScoped,
                path.as_bytes(),
            )
            .unwrap_or_else(|error| panic!("{error}")),
        )
    }
}

fn failure(path: &StoragePath, class: FailureClass) -> StorageRoleFailure {
    StorageRoleFailure::Entry(entry_failure(path, Operation::Namespace, class))
}

#[async_trait]
impl Namespace for MemoryTree {
    async fn execute(
        &self,
        request: NamespaceRequest,
    ) -> Result<NamespaceResult, StorageRoleFailure> {
        match request {
            NamespaceRequest::List(directory) => {
                if self.list_failure.as_deref() == Some(directory.as_str()) {
                    return Err(failure(&directory, FailureClass::PermissionDenied));
                }
                let prefix = if directory.as_str().is_empty() {
                    String::new()
                } else {
                    format!("{}/", directory.as_str())
                };
                let entries = self
                    .entries
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .iter()
                    .filter(|(path, _)| {
                        path.starts_with(&prefix)
                            && !path[prefix.len()..].contains('/')
                            && path.len() > prefix.len()
                    })
                    .map(|(path, kind)| Self::descriptor(path, *kind))
                    .collect();
                Ok(NamespaceResult::Entries(entries))
            }
            NamespaceRequest::Delete(path) => {
                if let Some(token) = &self.block_deletes {
                    token.cancelled().await;
                }
                self.deletes
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(path.as_str().to_owned());
                if let Some(class) = self.failing.get(path.as_str()) {
                    return Err(failure(&path, *class));
                }
                let mut entries = self
                    .entries
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if self.vanished.iter().any(|value| value == path.as_str()) {
                    // Removed by someone else between listing and delete.
                    entries.remove(path.as_str());
                    return Err(failure(&path, FailureClass::NotFound));
                }
                let has_children = entries
                    .keys()
                    .any(|value| value.starts_with(&format!("{}/", path.as_str())));
                if has_children {
                    return Err(failure(&path, FailureClass::Conflict));
                }
                if entries.remove(path.as_str()).is_none() {
                    return Err(failure(&path, FailureClass::NotFound));
                }
                Ok(NamespaceResult::Completed)
            }
            _ => Err(failure(&StoragePath::root(), FailureClass::Unsupported)),
        }
    }
}

fn request(root: &str, delete_root: bool) -> DeleteTreeRequest {
    DeleteTreeRequest {
        root: StoragePath::new(root).unwrap_or_else(|error| panic!("{error}")),
        delete_root,
        max_inflight_operations: NonZeroUsize::new(2)
            .unwrap_or_else(|| unreachable!("constant is nonzero")),
        max_buffered_items: NonZeroUsize::new(1)
            .unwrap_or_else(|| unreachable!("constant is nonzero")),
        cancel: CancellationToken::new(),
    }
}

async fn collect(
    mut session: DeleteTreeSession,
) -> (
    Vec<DeleteTreeItem>,
    Result<DeleteTreeOutcome, DeleteTreeTerminalFailure>,
) {
    let mut items = Vec::new();
    while let Some(item) = session.next_item().await {
        items.push(item);
    }
    let outcome = session.finish().await;
    (items, outcome)
}

fn tree() -> MemoryTree {
    MemoryTree::new(&[
        ("top", EntryKind::Directory),
        ("top/a.bin", EntryKind::File),
        ("top/link", EntryKind::Symlink),
        ("top/sub", EntryKind::Directory),
        ("top/sub/b.bin", EntryKind::File),
        ("top/sub/deep", EntryKind::Directory),
        ("top/sub/deep/c.bin", EntryKind::File),
        ("top/empty", EntryKind::Directory),
        ("other", EntryKind::File),
    ])
}

#[tokio::test]
async fn deletes_entries_first_then_directories_deepest_first_and_the_root() {
    let namespace = Arc::new(tree());
    let session = delete_tree_with_namespace(
        Arc::clone(&namespace) as Arc<dyn Namespace>,
        request("top", true),
    );
    let (items, outcome) = collect(session).await;
    assert_eq!(
        outcome,
        Ok(DeleteTreeOutcome::Completed(DeleteTreeCompletion {
            deleted_entries: 4,
            deleted_directories: 4,
            entry_failures: 0,
        }))
    );
    assert_eq!(items.len(), 8);
    assert_eq!(namespace.remaining(), ["other"]);
    let deletes = namespace.deletes();
    let position = |path: &str| {
        deletes
            .iter()
            .position(|value| value == path)
            .unwrap_or_else(|| panic!("{path} was never deleted"))
    };
    assert!(position("top/sub/deep/c.bin") < position("top/sub/deep"));
    assert!(position("top/sub/deep") < position("top/sub"));
    assert!(position("top/sub") < position("top"));
    assert!(position("top/empty") < position("top"));
    assert!(position("top/link") < position("top"));
}

#[tokio::test]
async fn keeps_the_root_when_not_requested_and_never_deletes_the_backend_root() {
    let namespace = Arc::new(tree());
    let (_, outcome) = collect(delete_tree_with_namespace(
        Arc::clone(&namespace) as Arc<dyn Namespace>,
        request("top", false),
    ))
    .await;
    assert!(matches!(outcome, Ok(DeleteTreeOutcome::Completed(_))));
    assert_eq!(namespace.remaining(), ["other", "top"]);

    let namespace = Arc::new(tree());
    let (_, outcome) = collect(delete_tree_with_namespace(
        Arc::clone(&namespace) as Arc<dyn Namespace>,
        request("", true),
    ))
    .await;
    assert!(matches!(outcome, Ok(DeleteTreeOutcome::Completed(_))));
    assert!(namespace.remaining().is_empty());
    assert!(!namespace.deletes().iter().any(String::is_empty));
}

#[tokio::test]
async fn a_failed_entry_keeps_its_ancestors_and_reports_each_once() {
    let mut namespace = tree();
    namespace.failing.insert(
        "top/sub/deep/c.bin".to_owned(),
        FailureClass::PermissionDenied,
    );
    let namespace = Arc::new(namespace);
    let (items, outcome) = collect(delete_tree_with_namespace(
        Arc::clone(&namespace) as Arc<dyn Namespace>,
        request("top", true),
    ))
    .await;
    let failures: Vec<(String, FailureClass)> = items
        .iter()
        .filter_map(|item| match item {
            DeleteTreeItem::EntryFailure(error) => {
                Some((error.path().as_str().to_owned(), error.class()))
            }
            DeleteTreeItem::Deleted { .. } => None,
        })
        .collect();
    assert_eq!(
        failures,
        [
            (
                "top/sub/deep/c.bin".to_owned(),
                FailureClass::PermissionDenied
            ),
            ("top/sub/deep".to_owned(), FailureClass::Conflict),
            ("top/sub".to_owned(), FailureClass::Conflict),
            ("top".to_owned(), FailureClass::Conflict),
        ]
    );
    assert_eq!(
        outcome,
        Ok(DeleteTreeOutcome::Completed(DeleteTreeCompletion {
            deleted_entries: 3,
            deleted_directories: 1,
            entry_failures: 4,
        }))
    );
    assert!(
        !namespace.deletes().iter().any(|path| path == "top/sub"),
        "incomplete directories are not sent to the backend"
    );
    assert_eq!(
        namespace.remaining(),
        [
            "other",
            "top",
            "top/sub",
            "top/sub/deep",
            "top/sub/deep/c.bin"
        ]
    );
}

#[tokio::test]
async fn not_found_on_delete_counts_as_deleted() {
    let mut namespace = tree();
    namespace.vanished.push("top/a.bin".to_owned());
    let namespace = Arc::new(namespace);
    let (items, outcome) = collect(delete_tree_with_namespace(
        Arc::clone(&namespace) as Arc<dyn Namespace>,
        request("top", true),
    ))
    .await;
    assert!(
        items
            .iter()
            .all(|item| matches!(item, DeleteTreeItem::Deleted { .. }))
    );
    assert!(matches!(
        outcome,
        Ok(DeleteTreeOutcome::Completed(DeleteTreeCompletion {
            entry_failures: 0,
            ..
        }))
    ));
}

#[tokio::test]
async fn a_listing_failure_isolates_that_subtree() {
    let mut namespace = tree();
    namespace.list_failure = Some("top/sub".to_owned());
    let namespace = Arc::new(namespace);
    let (items, outcome) = collect(delete_tree_with_namespace(
        Arc::clone(&namespace) as Arc<dyn Namespace>,
        request("top", true),
    ))
    .await;
    assert!(matches!(
        outcome,
        Ok(DeleteTreeOutcome::Completed(DeleteTreeCompletion {
            entry_failures: 3,
            deleted_entries: 2,
            deleted_directories: 1,
        }))
    ));
    assert!(items.iter().any(|item| matches!(
        item,
        DeleteTreeItem::EntryFailure(error)
            if error.path().as_str() == "top/sub" && error.class() == FailureClass::PermissionDenied
    )));
    assert!(namespace.remaining().contains(&"top/sub/b.bin".to_owned()));
}

#[tokio::test]
async fn cancellation_awaits_inflight_deletes_and_reports_cancelled() {
    let gate = CancellationToken::new();
    let mut namespace = tree();
    namespace.block_deletes = Some(gate.clone());
    let namespace = Arc::new(namespace);
    let request = request("top", true);
    let cancel = request.cancel.clone();
    let mut session =
        delete_tree_with_namespace(Arc::clone(&namespace) as Arc<dyn Namespace>, request);
    tokio::task::yield_now().await;
    cancel.cancel();
    assert!(session.next_item().await.is_none());
    // The blocked deletes must be released before the driver can finish.
    gate.cancel();
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(2), session.finish())
            .await
            .unwrap_or_else(|_| panic!("cancelled delete did not terminate")),
        Ok(DeleteTreeOutcome::Cancelled)
    );
    assert!(
        namespace.remaining().contains(&"top".to_owned()),
        "no directory deletion after cancel"
    );
}
