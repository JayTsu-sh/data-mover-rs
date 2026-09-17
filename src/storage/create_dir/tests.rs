use std::collections::HashSet;
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;

use super::{CreateDirectoryAllFailure, create_directory_all_with_namespace};
use crate::model::{
    BackendIdentity, BackendKind, EntryKind, EntryOperationFailure, FailureClass, IdentityStrength,
    Operation, SourceIdentity, StoragePath, Transience,
};
use crate::storage::{
    Namespace, NamespaceRequest, NamespaceResult, SourceDescriptor, StorageRoleFailure,
};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

/// How a backend answers `CreateDirectory` for a path that already exists.
#[derive(Clone, Copy, Eq, PartialEq)]
enum ExistingBehaviour {
    /// CIFS: `STATUS_OBJECT_NAME_COLLISION`.
    Conflict,
    /// NFS: `create_directory` is itself recursive and idempotent.
    Completed,
}

struct FakeNamespace {
    /// Paths that already exist as directories.
    directories: Mutex<HashSet<String>>,
    /// Paths that already exist as files.
    files: HashSet<String>,
    existing: ExistingBehaviour,
    session_failure_at: Option<String>,
    calls: Mutex<Vec<String>>,
}

impl FakeNamespace {
    fn new(existing: ExistingBehaviour) -> Arc<Self> {
        Arc::new(Self {
            directories: Mutex::new(HashSet::new()),
            files: HashSet::new(),
            existing,
            session_failure_at: None,
            calls: Mutex::new(Vec::new()),
        })
    }

    fn with_existing_directories(existing: ExistingBehaviour, paths: &[&str]) -> Arc<Self> {
        let namespace = Self::new(existing);
        let mut directories = namespace
            .directories
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        for path in paths {
            directories.insert((*path).to_owned());
        }
        drop(directories);
        namespace
    }

    fn with_existing_file(existing: ExistingBehaviour, file: &str) -> Arc<Self> {
        Arc::new(Self {
            directories: Mutex::new(HashSet::new()),
            files: std::iter::once(file.to_owned()).collect(),
            existing,
            session_failure_at: None,
            calls: Mutex::new(Vec::new()),
        })
    }

    fn failing_session_at(path: &str) -> Arc<Self> {
        Arc::new(Self {
            directories: Mutex::new(HashSet::new()),
            files: HashSet::new(),
            existing: ExistingBehaviour::Conflict,
            session_failure_at: Some(path.to_owned()),
            calls: Mutex::new(Vec::new()),
        })
    }

    fn calls(&self) -> Vec<String> {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn record(&self, call: String) {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(call);
    }
}

fn entry_failure(path: &StoragePath, class: FailureClass) -> StorageRoleFailure {
    StorageRoleFailure::Entry(
        EntryOperationFailure::new(
            path.clone(),
            Operation::Namespace,
            class,
            Transience::Permanent,
            "create_dir test namespace",
        )
        .unwrap_or_else(|error| panic!("{error}")),
    )
}

fn session_failure() -> StorageRoleFailure {
    StorageRoleFailure::Session(
        crate::model::BackendSessionFailure::new(
            Operation::Namespace,
            FailureClass::Connectivity,
            Transience::Transient,
            "create_dir test session",
        )
        .unwrap_or_else(|error| panic!("{error}")),
    )
}

fn descriptor(path: &StoragePath, kind: EntryKind) -> Result<SourceDescriptor> {
    Ok(SourceDescriptor::new(
        path.clone(),
        kind,
        None,
        SourceIdentity::new(
            BackendIdentity::new(BackendKind::Cifs, "create-dir-test")?,
            IdentityStrength::PathScoped,
            path.as_str().as_bytes(),
        )?,
    ))
}

#[async_trait]
impl Namespace for FakeNamespace {
    async fn execute(
        &self,
        request: NamespaceRequest,
    ) -> std::result::Result<NamespaceResult, StorageRoleFailure> {
        match request {
            NamespaceRequest::CreateDirectory(path) => {
                self.record(format!("mkdir {}", path.as_str()));
                if self.session_failure_at.as_deref() == Some(path.as_str()) {
                    return Err(session_failure());
                }
                if self.files.contains(path.as_str()) {
                    // A file is in the way: backends report the same collision as for a
                    // directory, which is exactly why the leaf gets verified.
                    return Err(entry_failure(&path, FailureClass::Conflict));
                }
                let fresh = self
                    .directories
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(path.as_str().to_owned());
                if fresh || self.existing == ExistingBehaviour::Completed {
                    Ok(NamespaceResult::Completed)
                } else {
                    Err(entry_failure(&path, FailureClass::Conflict))
                }
            }
            NamespaceRequest::Stat(path) => {
                self.record(format!("stat {}", path.as_str()));
                let kind = if self.files.contains(path.as_str()) {
                    EntryKind::File
                } else if self
                    .directories
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .contains(path.as_str())
                {
                    EntryKind::Directory
                } else {
                    return Err(entry_failure(&path, FailureClass::NotFound));
                };
                let entry = descriptor(&path, kind)
                    .map_err(|_| entry_failure(&path, FailureClass::Protocol))?;
                Ok(NamespaceResult::Entries(vec![entry]))
            }
            other => {
                self.record(format!("{other:?}"));
                Err(entry_failure(
                    &StoragePath::root(),
                    FailureClass::Unsupported,
                ))
            }
        }
    }
}

fn path(value: &str) -> Result<StoragePath> {
    Ok(StoragePath::new(value)?)
}

#[tokio::test]
async fn every_missing_component_is_created_in_order() -> Result {
    let namespace = FakeNamespace::new(ExistingBehaviour::Conflict);
    create_directory_all_with_namespace(namespace.as_ref(), &path("a/b/c")?).await?;
    assert_eq!(
        namespace.calls(),
        vec!["mkdir a", "mkdir a/b", "mkdir a/b/c"]
    );
    Ok(())
}

#[tokio::test]
async fn an_existing_tree_is_a_success_whichever_way_the_backend_reports_it() -> Result {
    for existing in [ExistingBehaviour::Conflict, ExistingBehaviour::Completed] {
        let namespace = FakeNamespace::with_existing_directories(existing, &["a", "a/b", "a/b/c"]);
        create_directory_all_with_namespace(namespace.as_ref(), &path("a/b/c")?).await?;
        assert!(
            namespace.calls().contains(&"mkdir a/b/c".to_owned()),
            "every level is still attempted"
        );
    }
    Ok(())
}

#[tokio::test]
async fn an_existing_intermediate_level_does_not_stop_the_deeper_ones() -> Result {
    let namespace = FakeNamespace::with_existing_directories(ExistingBehaviour::Conflict, &["a"]);
    create_directory_all_with_namespace(namespace.as_ref(), &path("a/b")?).await?;
    assert_eq!(
        namespace.calls(),
        vec!["mkdir a", "mkdir a/b"],
        "an intermediate conflict costs no extra Stat"
    );
    Ok(())
}

#[tokio::test]
async fn a_file_at_the_leaf_is_reported_as_a_conflict_not_a_success() -> Result {
    let namespace = FakeNamespace::with_existing_file(ExistingBehaviour::Conflict, "a/b");
    let outcome = create_directory_all_with_namespace(namespace.as_ref(), &path("a/b")?).await;
    let Err(CreateDirectoryAllFailure::Role(failure)) = outcome else {
        return Err(format!("expected a role failure, got {outcome:?}").into());
    };
    assert_eq!(super::class_of(&failure), FailureClass::Conflict);
    assert_eq!(
        namespace.calls(),
        vec!["mkdir a", "mkdir a/b", "stat a/b"],
        "the leaf conflict is verified with exactly one Stat"
    );
    Ok(())
}

#[tokio::test]
async fn an_existing_directory_at_the_leaf_costs_one_stat_and_succeeds() -> Result {
    let namespace = FakeNamespace::with_existing_directories(ExistingBehaviour::Conflict, &["a"]);
    create_directory_all_with_namespace(namespace.as_ref(), &path("a")?).await?;
    assert_eq!(namespace.calls(), vec!["mkdir a", "stat a"]);
    Ok(())
}

#[tokio::test]
async fn the_backend_root_is_never_requested() -> Result {
    let namespace = FakeNamespace::new(ExistingBehaviour::Conflict);
    create_directory_all_with_namespace(namespace.as_ref(), &StoragePath::root()).await?;
    assert!(
        namespace.calls().is_empty(),
        "the root always exists, and HDFS refuses a root-targeted CreateDirectory"
    );
    Ok(())
}

#[tokio::test]
async fn repeated_separators_collapse_instead_of_creating_empty_levels() -> Result {
    let namespace = FakeNamespace::new(ExistingBehaviour::Conflict);
    // `StoragePath` may normalise this itself; either way no empty component may be requested.
    let target = StoragePath::new("a//b").or_else(|_| StoragePath::new("a/b"))?;
    create_directory_all_with_namespace(namespace.as_ref(), &target).await?;
    assert_eq!(namespace.calls(), vec!["mkdir a", "mkdir a/b"]);
    Ok(())
}

#[tokio::test]
async fn a_session_failure_stops_immediately_without_touching_deeper_levels() -> Result {
    let namespace = FakeNamespace::failing_session_at("a/b");
    let outcome = create_directory_all_with_namespace(namespace.as_ref(), &path("a/b/c")?).await;
    let Err(CreateDirectoryAllFailure::Role(StorageRoleFailure::Session(_))) = outcome else {
        return Err(format!("expected a session failure, got {outcome:?}").into());
    };
    assert_eq!(
        namespace.calls(),
        vec!["mkdir a", "mkdir a/b"],
        "the level below the failure is never attempted"
    );
    Ok(())
}

#[tokio::test]
async fn a_permission_failure_propagates_rather_than_being_treated_as_existing() -> Result {
    struct DeniedNamespace;
    #[async_trait]
    impl Namespace for DeniedNamespace {
        async fn execute(
            &self,
            request: NamespaceRequest,
        ) -> std::result::Result<NamespaceResult, StorageRoleFailure> {
            let NamespaceRequest::CreateDirectory(path) = request else {
                return Err(entry_failure(
                    &StoragePath::root(),
                    FailureClass::Unsupported,
                ));
            };
            Err(entry_failure(&path, FailureClass::PermissionDenied))
        }
    }

    let outcome = create_directory_all_with_namespace(&DeniedNamespace, &path("a/b")?).await;
    let Err(CreateDirectoryAllFailure::Role(failure)) = outcome else {
        return Err(format!("expected a role failure, got {outcome:?}").into());
    };
    assert_eq!(super::class_of(&failure), FailureClass::PermissionDenied);
    Ok(())
}
