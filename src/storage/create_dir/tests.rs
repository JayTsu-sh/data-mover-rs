use std::collections::HashSet;
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;

use super::{CreateDirectoryAllFailure, create_directory_all_with_namespace};
use crate::model::{
    BackendIdentity, BackendKind, EntryKind, EntryOperationFailure, FailureClass, IdentityStrength,
    Operation, SourceIdentity, StoragePath, SymlinkTarget, Transience,
};
use crate::storage::listing::testing::answer;
use crate::storage::{ListRequest, ListingStream, Namespace, SourceDescriptor, StorageRoleFailure};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

/// How a backend answers `CreateDirectory` for a path that already exists.
#[derive(Clone, Copy, Eq, PartialEq)]
enum ExistingBehaviour {
    /// CIFS: `STATUS_OBJECT_NAME_COLLISION`.
    Conflict,
    /// NFS: `create_directory` is itself recursive and idempotent.
    Completed,
}

/// How every `stat` of a [`FakeNamespace`] fails, given the probed path.
type StatFailure = fn(&StoragePath) -> StorageRoleFailure;

struct FakeNamespace {
    /// Paths that already exist as directories.
    directories: Mutex<HashSet<String>>,
    /// Paths that already exist as files.
    files: HashSet<String>,
    existing: ExistingBehaviour,
    session_failure_at: Option<String>,
    /// Paths whose `CreateDirectory` is refused as `PermissionDenied` before the server looks
    /// at the name, as ONTAP does on a share the account may only read.
    denied: HashSet<String>,
    /// Every `stat` fails with this failure.
    stat_failure: Option<StatFailure>,
    calls: Mutex<Vec<String>>,
}

impl FakeNamespace {
    fn new(existing: ExistingBehaviour) -> Arc<Self> {
        Arc::new(Self {
            directories: Mutex::new(HashSet::new()),
            files: HashSet::new(),
            existing,
            session_failure_at: None,
            denied: HashSet::new(),
            stat_failure: None,
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
            denied: HashSet::new(),
            stat_failure: None,
            calls: Mutex::new(Vec::new()),
        })
    }

    fn failing_session_at(path: &str) -> Arc<Self> {
        Arc::new(Self {
            directories: Mutex::new(HashSet::new()),
            files: HashSet::new(),
            existing: ExistingBehaviour::Conflict,
            session_failure_at: Some(path.to_owned()),
            denied: HashSet::new(),
            stat_failure: None,
            calls: Mutex::new(Vec::new()),
        })
    }

    /// A namespace that refuses `CreateDirectory` on every `denied` path, whether or not the
    /// name exists, holding `directories` and `files`.
    fn denying(
        directories: &[&str],
        files: &[&str],
        denied: &[&str],
        stat_failure: Option<StatFailure>,
    ) -> Arc<Self> {
        let owned = |paths: &[&str]| paths.iter().map(|path| (*path).to_owned()).collect();
        Arc::new(Self {
            directories: Mutex::new(owned(directories)),
            files: owned(files),
            existing: ExistingBehaviour::Conflict,
            session_failure_at: None,
            denied: owned(denied),
            stat_failure,
            calls: Mutex::new(Vec::new()),
        })
    }

    fn calls(&self) -> Vec<String> {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Whether some strict ancestor of `path` exists as a file.
    fn blocked_by_file_ancestor(&self, path: &str) -> bool {
        let mut prefix = String::new();
        for component in path.split('/') {
            if !prefix.is_empty() {
                prefix.push('/');
            }
            prefix.push_str(component);
            if prefix.len() < path.len() && self.files.contains(&prefix) {
                return true;
            }
        }
        false
    }

    fn record(&self, call: String) {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(call);
    }
}

fn entry_failure(path: &StoragePath, class: FailureClass) -> StorageRoleFailure {
    transient_or_not(path, class, Transience::Permanent)
}

fn transient_or_not(
    path: &StoragePath,
    class: FailureClass,
    transience: Transience,
) -> StorageRoleFailure {
    StorageRoleFailure::Entry(
        EntryOperationFailure::new(
            path.clone(),
            Operation::Namespace,
            class,
            transience,
            "create_dir test namespace",
        )
        .unwrap_or_else(|error| panic!("{error}")),
    )
}

fn session_failure() -> StorageRoleFailure {
    session_failure_of(FailureClass::Connectivity)
}

fn session_failure_of(class: FailureClass) -> StorageRoleFailure {
    StorageRoleFailure::Session(
        crate::model::BackendSessionFailure::new(
            Operation::Namespace,
            class,
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
    async fn create_directory(
        &self,
        path: &StoragePath,
    ) -> std::result::Result<(), StorageRoleFailure> {
        self.record(format!("mkdir {}", path.as_str()));
        if self.session_failure_at.as_deref() == Some(path.as_str()) {
            return Err(session_failure());
        }
        if self.denied.contains(path.as_str()) {
            return Err(entry_failure(path, FailureClass::PermissionDenied));
        }
        if self.files.contains(path.as_str()) {
            // A file is in the way: backends report the same collision as for a directory,
            // which is exactly why the leaf gets verified.
            return Err(entry_failure(path, FailureClass::Conflict));
        }
        if self.blocked_by_file_ancestor(path.as_str()) {
            // CIFS / HDFS behaviour: a file at an ancestor level makes the level below it fail
            // on its own, which is the argument for not verifying every level.
            return Err(entry_failure(path, FailureClass::NotFound));
        }
        let fresh = self
            .directories
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(path.as_str().to_owned());
        if fresh || self.existing == ExistingBehaviour::Completed {
            Ok(())
        } else {
            Err(entry_failure(path, FailureClass::Conflict))
        }
    }

    async fn stat(
        &self,
        path: &StoragePath,
    ) -> std::result::Result<SourceDescriptor, StorageRoleFailure> {
        self.record(format!("stat {}", path.as_str()));
        if let Some(failure) = self.stat_failure {
            return Err(failure(path));
        }
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
            return Err(entry_failure(path, FailureClass::NotFound));
        };
        descriptor(path, kind).map_err(|_| entry_failure(path, FailureClass::Protocol))
    }

    fn list(&self, request: ListRequest) -> ListingStream {
        self.record(format!("list {}", request.path.as_str()));
        answer(&request.path, Err(unsupported()))
    }

    async fn read_link(
        &self,
        path: &StoragePath,
    ) -> std::result::Result<SymlinkTarget, StorageRoleFailure> {
        self.record(format!("read_link {}", path.as_str()));
        Err(unsupported())
    }

    async fn delete(&self, path: &StoragePath) -> std::result::Result<(), StorageRoleFailure> {
        self.record(format!("delete {}", path.as_str()));
        Err(unsupported())
    }

    async fn rename(
        &self,
        from: &StoragePath,
        to: &StoragePath,
    ) -> std::result::Result<(), StorageRoleFailure> {
        self.record(format!("rename {} {}", from.as_str(), to.as_str()));
        Err(unsupported())
    }
}

fn unsupported() -> StorageRoleFailure {
    entry_failure(&StoragePath::root(), FailureClass::Unsupported)
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
        let expected = match existing {
            // A Conflict at the leaf buys one Stat to prove it is a directory.
            ExistingBehaviour::Conflict => {
                vec!["mkdir a", "mkdir a/b", "mkdir a/b/c", "stat a/b/c"]
            }
            ExistingBehaviour::Completed => vec!["mkdir a", "mkdir a/b", "mkdir a/b/c"],
        };
        assert_eq!(namespace.calls(), expected);
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
    // `StoragePath` accepts this spelling, so the helper is what must not request an empty
    // level between the two separators.
    let target = StoragePath::new("a//b")?;
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
        async fn create_directory(
            &self,
            path: &StoragePath,
        ) -> std::result::Result<(), StorageRoleFailure> {
            Err(entry_failure(path, FailureClass::PermissionDenied))
        }

        fn list(&self, request: ListRequest) -> ListingStream {
            answer(&request.path, Err(unsupported()))
        }
    }

    let outcome = create_directory_all_with_namespace(&DeniedNamespace, &path("a/b")?).await;
    let Err(CreateDirectoryAllFailure::Role(failure)) = outcome else {
        return Err(format!("expected a role failure, got {outcome:?}").into());
    };
    assert_eq!(super::class_of(&failure), FailureClass::PermissionDenied);
    Ok(())
}

#[tokio::test]
async fn a_file_at_an_intermediate_level_surfaces_through_the_level_below_it() -> Result {
    let namespace = FakeNamespace::with_existing_file(ExistingBehaviour::Conflict, "a");
    let outcome = create_directory_all_with_namespace(namespace.as_ref(), &path("a/b")?).await;
    let Err(CreateDirectoryAllFailure::Role(failure)) = outcome else {
        return Err(format!("expected a role failure, got {outcome:?}").into());
    };
    assert_eq!(
        super::class_of(&failure),
        FailureClass::NotFound,
        "the deeper level fails on its own, which is why intermediate levels are not verified"
    );
    assert_eq!(
        namespace.calls(),
        vec!["mkdir a", "mkdir a/b"],
        "no Stat is spent on the intermediate level"
    );
    Ok(())
}

/// The failure `create_directory_all` ended with, required to be an entry failure.
async fn denied_outcome(namespace: &FakeNamespace, target: &str) -> Result<EntryOperationFailure> {
    match create_directory_all_with_namespace(namespace, &path(target)?).await {
        Err(CreateDirectoryAllFailure::Role(StorageRoleFailure::Entry(failure))) => Ok(failure),
        outcome => Err(format!("{target}: expected an entry failure, got {outcome:?}").into()),
    }
}

#[tokio::test]
async fn an_existing_intermediate_level_refused_as_denied_is_proved_and_passed() -> Result {
    // ONTAP on a read-only root: `CREATE` of the existing `a` is `ACCESS_DENIED`, not a
    // collision; `a` itself is writable.
    let namespace = FakeNamespace::denying(&["a"], &[], &["a"], None);
    create_directory_all_with_namespace(namespace.as_ref(), &path("a/b")?).await?;
    assert_eq!(namespace.calls(), vec!["mkdir a", "stat a", "mkdir a/b"]);
    Ok(())
}

#[tokio::test]
async fn an_existing_leaf_refused_as_denied_is_a_success() -> Result {
    let namespace = FakeNamespace::denying(&["a", "a/b"], &[], &["a", "a/b"], None);
    create_directory_all_with_namespace(namespace.as_ref(), &path("a/b")?).await?;
    assert_eq!(
        namespace.calls(),
        vec!["mkdir a", "stat a", "mkdir a/b", "stat a/b"]
    );
    Ok(())
}

#[tokio::test]
async fn a_missing_level_refused_as_denied_keeps_the_refusal() -> Result {
    // A missing intermediate level (`a`) and a missing leaf (`a/new`), each refused.
    for (directories, denied, target, refused, calls) in [
        (&[][..], &["a"][..], "a/b", "a", &["mkdir a", "stat a"][..]),
        (
            &["a"][..],
            &["a", "a/new"][..],
            "a/new",
            "a/new",
            &["mkdir a", "stat a", "mkdir a/new", "stat a/new"][..],
        ),
    ] {
        let namespace = FakeNamespace::denying(directories, &[], denied, None);
        let failure = denied_outcome(&namespace, target).await?;
        assert_eq!(failure.class(), FailureClass::PermissionDenied, "{target}");
        assert_eq!(failure.path().as_str(), refused, "{target}");
        assert_eq!(namespace.calls(), calls, "{target}");
    }
    Ok(())
}

#[tokio::test]
async fn a_file_at_a_level_refused_as_denied_keeps_the_refusal() -> Result {
    // A file at the intermediate level `a`, then at the leaf `a/b`.
    for (directories, files, refused, calls) in [
        (&[][..], &["a"][..], "a", &["mkdir a", "stat a"][..]),
        (
            &["a"][..],
            &["a/b"][..],
            "a/b",
            &["mkdir a", "stat a", "mkdir a/b", "stat a/b"][..],
        ),
    ] {
        let namespace = FakeNamespace::denying(directories, files, &["a", "a/b"], None);
        let failure = denied_outcome(&namespace, "a/b").await?;
        assert_eq!(failure.class(), FailureClass::PermissionDenied, "{files:?}");
        assert_eq!(failure.path().as_str(), refused, "{files:?}");
        assert_eq!(namespace.calls(), calls, "{files:?}");
    }
    Ok(())
}

#[tokio::test]
async fn a_probe_failing_for_the_entry_keeps_the_original_refusal() -> Result {
    let probes: [StatFailure; 5] = [
        |path| entry_failure(path, FailureClass::PermissionDenied),
        |path| entry_failure(path, FailureClass::NotFound),
        |path| entry_failure(path, FailureClass::Protocol),
        |path| entry_failure(path, FailureClass::Internal),
        // Transient but still about the entry (a CIFS sharing violation).
        |path| transient_or_not(path, FailureClass::Conflict, Transience::Transient),
    ];
    for probe in probes {
        let namespace = FakeNamespace::denying(&["a"], &[], &["a"], Some(probe));
        let failure = denied_outcome(&namespace, "a/b").await?;
        assert_eq!(failure.class(), FailureClass::PermissionDenied);
        assert_eq!(failure.path().as_str(), "a");
        assert_eq!(namespace.calls(), vec!["mkdir a", "stat a"]);
    }
    // The leaf conflict probe too: the file-or-directory question stays unanswered.
    for probe in probes {
        let namespace = FakeNamespace::denying(&[], &["a"], &[], Some(probe));
        let failure = denied_outcome(&namespace, "a").await?;
        assert_eq!(failure.class(), FailureClass::Conflict);
        assert_eq!(failure.transience(), Transience::Permanent);
    }
    Ok(())
}

/// A probe failure that is not about the probed entry: cancellation (R10) or a session failure,
/// reported either as a `Session` failure or at entry scope with a session class.
const WIDER_PROBES: [(&str, StatFailure); 6] = [
    ("session", |_| session_failure()),
    ("session refusal", |_| {
        session_failure_of(FailureClass::PermissionDenied)
    }),
    ("cancelled", |path| {
        entry_failure(path, FailureClass::Cancelled)
    }),
    ("connectivity", |path| {
        entry_failure(path, FailureClass::Connectivity)
    }),
    ("authentication", |path| {
        entry_failure(path, FailureClass::Authentication)
    }),
    ("throttled", |path| {
        entry_failure(path, FailureClass::Throttled)
    }),
];

#[tokio::test]
async fn a_cancelled_or_session_probe_reports_itself_not_the_refusal() -> Result {
    for (name, probe) in WIDER_PROBES {
        // The refused intermediate level `a`, then a conflict at the leaf `a` (a file there).
        for (directories, files, denied, target) in [
            (&["a"][..], &[][..], &["a"][..], "a/b"),
            (&[][..], &["a"][..], &[][..], "a"),
        ] {
            let namespace = FakeNamespace::denying(directories, files, denied, Some(probe));
            let outcome =
                create_directory_all_with_namespace(namespace.as_ref(), &path(target)?).await;
            let Err(CreateDirectoryAllFailure::Role(failure)) = outcome else {
                return Err(format!("{name} {target}: expected a failure, got {outcome:?}").into());
            };
            assert_eq!(failure, probe(&path("a")?), "{name} {target}");
            assert_eq!(
                namespace.calls(),
                vec!["mkdir a", "stat a"],
                "{name} {target}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn a_session_scoped_refusal_is_not_probed() -> Result {
    struct SessionDenied;
    #[async_trait]
    impl Namespace for SessionDenied {
        async fn create_directory(
            &self,
            _path: &StoragePath,
        ) -> std::result::Result<(), StorageRoleFailure> {
            Err(StorageRoleFailure::Session(
                crate::model::BackendSessionFailure::new(
                    Operation::Namespace,
                    FailureClass::PermissionDenied,
                    Transience::Permanent,
                    "create_dir test session",
                )
                .unwrap_or_else(|error| panic!("{error}")),
            ))
        }

        async fn stat(
            &self,
            path: &StoragePath,
        ) -> std::result::Result<SourceDescriptor, StorageRoleFailure> {
            // Were it probed, the level would pass as an existing directory.
            descriptor(path, EntryKind::Directory).map_err(|_| unsupported())
        }

        fn list(&self, request: ListRequest) -> ListingStream {
            answer(&request.path, Err(unsupported()))
        }
    }

    let outcome = create_directory_all_with_namespace(&SessionDenied, &path("a")?).await;
    let Err(CreateDirectoryAllFailure::Role(StorageRoleFailure::Session(failure))) = outcome else {
        return Err(format!("expected the session refusal, got {outcome:?}").into());
    };
    assert_eq!(failure.class(), FailureClass::PermissionDenied);
    Ok(())
}
