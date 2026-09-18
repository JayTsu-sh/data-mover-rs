use std::sync::Arc;

use cap_std::ambient_authority;
use cap_std::fs::Dir;

use super::*;
use crate::storage::backends::local::observation::LocalObservationAdapter;
use crate::storage::backends::local::test_identity;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn roles(root: &Path) -> Result<(LocalNamespace, LocalObservationAdapter)> {
    let dir = Arc::new(Dir::open_ambient_dir(
        std::fs::canonicalize(root)?,
        ambient_authority(),
    )?);
    let identity = test_identity("local-namespace-test");
    Ok((
        LocalNamespace::from_root(Arc::clone(&dir), identity.clone()),
        LocalObservationAdapter::from_root(dir, identity),
    ))
}

fn path(value: &str) -> StoragePath {
    StoragePath::new(value).unwrap_or_else(|error| panic!("{error}"))
}

async fn list(namespace: &LocalNamespace, directory: &str) -> Vec<SourceDescriptor> {
    let result = namespace
        .execute(NamespaceRequest::List(path(directory)))
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    let Some((mut entries, failures)) = result.into_listing() else {
        panic!("List did not return a listing")
    };
    assert!(failures.is_empty(), "{failures:?}");
    entries.sort_by(|left, right| left.path.as_str().cmp(right.path.as_str()));
    entries
}

fn failure_class(result: std::result::Result<NamespaceResult, StorageRoleFailure>) -> FailureClass {
    match result {
        Err(StorageRoleFailure::Entry(error)) => error.class(),
        other => panic!("expected an entry failure, got {other:?}"),
    }
}

#[tokio::test]
async fn list_describes_children_without_a_dot_prefix_and_with_inline_timestamps() -> Result {
    let temp = tempfile::tempdir()?;
    std::fs::create_dir(temp.path().join("dir"))?;
    std::fs::write(temp.path().join("file"), b"abc")?;
    std::fs::write(temp.path().join("dir/child"), b"x")?;
    let (namespace, _) = roles(temp.path())?;

    let root = list(&namespace, "").await;
    let kinds = root
        .iter()
        .map(|entry| (entry.path.as_str(), entry.kind, entry.size))
        .collect::<Vec<_>>();
    assert_eq!(
        kinds,
        [
            ("dir", EntryKind::Directory, None),
            ("file", EntryKind::File, Some(3))
        ]
    );
    assert!(root.iter().all(|entry| {
        entry
            .inline_timestamps()
            .and_then(|value| value.modified)
            .is_some()
    }));
    let nested = list(&namespace, "dir").await;
    assert_eq!(nested[0].path.as_str(), "dir/child");
    Ok(())
}

#[tokio::test]
async fn listed_identity_and_facts_match_the_observation_role() -> Result {
    let temp = tempfile::tempdir()?;
    std::fs::write(temp.path().join("file"), b"abc")?;
    let (namespace, adapter) = roles(temp.path())?;
    let listed = list(&namespace, "").await;
    let (observed, version) = adapter
        .observe_versioned(path("file"), crate::model::ObservationPlan::default())
        .await
        .map_err(|error| format!("{error:?}"))?;
    assert_eq!(&listed[0].source_identity, observed.source_identity());
    let crate::model::observation::PrivateBackendEntryFacts::Local(observed_fact) =
        observed.backend_facts()
    else {
        panic!("a Local observation carries Local facts")
    };
    assert_eq!(
        listed[0].backend_fact.as_deref(),
        Some(observed_fact.as_slice())
    );
    assert_eq!(listed[0].content_version.as_ref(), Some(&version));
    Ok(())
}

#[tokio::test]
async fn transfer_artifacts_are_neither_listed_nor_addressable() -> Result {
    let temp = tempfile::tempdir()?;
    std::fs::write(temp.path().join(".data-mover-owned.stage"), b"internal")?;
    std::fs::write(temp.path().join("visible"), b"x")?;
    let (namespace, _) = roles(temp.path())?;
    let listed = list(&namespace, "").await;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].path.as_str(), "visible");
    assert_eq!(
        failure_class(
            namespace
                .execute(NamespaceRequest::Delete(path(".data-mover-owned.stage")))
                .await
        ),
        FailureClass::InvalidInput
    );
    Ok(())
}

#[tokio::test]
async fn paths_are_confined_and_the_root_cannot_be_mutated() -> Result {
    let temp = tempfile::tempdir()?;
    let (namespace, _) = roles(temp.path())?;
    for request in [
        NamespaceRequest::List(path("../escape")),
        NamespaceRequest::Stat(path("a/../../escape")),
        NamespaceRequest::Delete(StoragePath::root()),
        NamespaceRequest::CreateDirectory(StoragePath::root()),
        // Only `.` components name the root too.
        NamespaceRequest::Delete(path(".")),
        NamespaceRequest::Delete(path("./.")),
    ] {
        assert_eq!(
            failure_class(namespace.execute(request).await),
            FailureClass::InvalidInput
        );
    }
    match namespace
        .execute(NamespaceRequest::List(path("../escape")))
        .await
    {
        Err(StorageRoleFailure::Entry(error)) => assert_eq!(error.operation(), Operation::Traverse),
        other => panic!("expected an entry failure, got {other:?}"),
    }
    Ok(())
}

#[tokio::test]
async fn create_directory_creates_one_level_and_conflicts_when_present() -> Result {
    let temp = tempfile::tempdir()?;
    let (namespace, _) = roles(temp.path())?;
    namespace
        .execute(NamespaceRequest::CreateDirectory(path("made")))
        .await
        .map_err(|error| format!("{error:?}"))?;
    assert!(temp.path().join("made").is_dir());
    assert_eq!(
        failure_class(
            namespace
                .execute(NamespaceRequest::CreateDirectory(path("made")))
                .await
        ),
        FailureClass::Conflict
    );
    assert_eq!(
        failure_class(
            namespace
                .execute(NamespaceRequest::CreateDirectory(path("missing/child")))
                .await
        ),
        FailureClass::NotFound
    );
    Ok(())
}

#[tokio::test]
async fn delete_removes_files_and_empty_directories_but_never_recurses() -> Result {
    let temp = tempfile::tempdir()?;
    std::fs::create_dir(temp.path().join("full"))?;
    std::fs::write(temp.path().join("full/keep"), b"x")?;
    std::fs::create_dir(temp.path().join("empty"))?;
    std::fs::write(temp.path().join("file"), b"x")?;
    let (namespace, _) = roles(temp.path())?;
    for target in ["file", "empty"] {
        namespace
            .execute(NamespaceRequest::Delete(path(target)))
            .await
            .map_err(|error| format!("{error:?}"))?;
        assert!(!temp.path().join(target).exists());
    }
    assert_eq!(
        failure_class(
            namespace
                .execute(NamespaceRequest::Delete(path("full")))
                .await
        ),
        FailureClass::Conflict
    );
    assert!(temp.path().join("full/keep").exists());
    Ok(())
}

#[tokio::test]
async fn rename_replaces_a_file_and_moves_a_directory() -> Result {
    let temp = tempfile::tempdir()?;
    std::fs::write(temp.path().join("new"), b"new")?;
    std::fs::write(temp.path().join("old"), b"old")?;
    std::fs::create_dir(temp.path().join("dir"))?;
    std::fs::write(temp.path().join("dir/inner"), b"x")?;
    std::fs::create_dir(temp.path().join("elsewhere"))?;
    let (namespace, _) = roles(temp.path())?;
    namespace
        .execute(NamespaceRequest::Rename {
            from: path("new"),
            to: path("old"),
        })
        .await
        .map_err(|error| format!("{error:?}"))?;
    assert_eq!(std::fs::read(temp.path().join("old"))?, b"new");
    assert!(!temp.path().join("new").exists());
    namespace
        .execute(NamespaceRequest::Rename {
            from: path("dir"),
            to: path("elsewhere/dir"),
        })
        .await
        .map_err(|error| format!("{error:?}"))?;
    assert!(temp.path().join("elsewhere/dir/inner").exists());
    Ok(())
}

#[tokio::test]
async fn stat_describes_one_entry_and_reports_missing_ones() -> Result {
    let temp = tempfile::tempdir()?;
    std::fs::write(temp.path().join("file"), b"abcd")?;
    let (namespace, _) = roles(temp.path())?;
    let result = namespace
        .execute(NamespaceRequest::Stat(path("file")))
        .await
        .map_err(|error| format!("{error:?}"))?;
    let NamespaceResult::Entries(entries) = result else {
        panic!("Stat did not return entries")
    };
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].size, Some(4));
    assert_eq!(
        failure_class(
            namespace
                .execute(NamespaceRequest::Stat(path("absent")))
                .await
        ),
        FailureClass::NotFound
    );
    Ok(())
}

#[tokio::test]
async fn the_list_probe_fails_exactly_the_chosen_call() -> Result {
    let temp = tempfile::tempdir()?;
    std::fs::create_dir(temp.path().join("dir"))?;
    let (namespace, _) = roles(temp.path())?;
    namespace.fail_list_call(2);
    assert!(
        namespace
            .execute(NamespaceRequest::List(StoragePath::root()))
            .await
            .is_ok()
    );
    assert_eq!(
        failure_class(namespace.execute(NamespaceRequest::List(path("dir"))).await),
        FailureClass::PermissionDenied
    );
    assert_eq!(namespace.failed_list_path(), Some(path("dir")));
    Ok(())
}

#[cfg(unix)]
mod unix {
    use std::os::unix::ffi::OsStringExt as _;

    use super::*;

    #[tokio::test]
    async fn a_directory_symlink_is_listed_as_a_link_and_deleting_it_keeps_the_target() -> Result {
        let temp = tempfile::tempdir()?;
        std::fs::create_dir(temp.path().join("target"))?;
        std::fs::write(temp.path().join("target/precious"), b"x")?;
        std::os::unix::fs::symlink("target", temp.path().join("link"))?;
        let (namespace, _) = roles(temp.path())?;
        let listed = list(&namespace, "").await;
        let link = listed
            .iter()
            .find(|entry| entry.path.as_str() == "link")
            .ok_or("link was not listed")?;
        assert_eq!(link.kind, EntryKind::Symlink);
        let result = namespace
            .execute(NamespaceRequest::ReadLink(path("link")))
            .await
            .map_err(|error| format!("{error:?}"))?;
        let NamespaceResult::LinkTarget(target) = result else {
            panic!("ReadLink did not return a target")
        };
        assert_eq!(target.as_bytes(), b"target");
        namespace
            .execute(NamespaceRequest::Delete(path("link")))
            .await
            .map_err(|error| format!("{error:?}"))?;
        assert!(temp.path().join("target/precious").exists());
        assert!(std::fs::symlink_metadata(temp.path().join("link")).is_err());
        Ok(())
    }

    /// `precious/victim` plus `link -> precious`: every verb that would reach `victim`
    /// through `link` must refuse instead.
    #[tokio::test]
    async fn no_verb_follows_a_symlink_in_any_component() -> Result {
        let temp = tempfile::tempdir()?;
        std::fs::create_dir(temp.path().join("precious"))?;
        std::fs::write(temp.path().join("precious/victim"), b"x")?;
        std::fs::create_dir(temp.path().join("precious/inner"))?;
        std::os::unix::fs::symlink("precious", temp.path().join("link"))?;
        let (namespace, _) = roles(temp.path())?;
        for request in [
            NamespaceRequest::List(path("link")),
            NamespaceRequest::Stat(path("link/victim")),
            NamespaceRequest::Delete(path("link/victim")),
            NamespaceRequest::Delete(path("link/inner")),
            NamespaceRequest::CreateDirectory(path("link/new")),
            NamespaceRequest::Rename {
                from: path("link/victim"),
                to: path("moved"),
            },
            NamespaceRequest::Rename {
                from: path("precious/victim"),
                to: path("link/moved"),
            },
        ] {
            let description = format!("{request:?}");
            let outcome = namespace.execute(request).await;
            assert!(
                matches!(outcome, Err(StorageRoleFailure::Entry(_))),
                "{description} was not refused: {outcome:?}"
            );
        }
        assert!(temp.path().join("precious/victim").exists());
        assert!(temp.path().join("precious/inner").is_dir());
        assert!(!temp.path().join("precious/new").exists());
        assert!(!temp.path().join("moved").exists());
        Ok(())
    }

    #[tokio::test]
    async fn listing_a_symlink_or_a_file_is_invalid_input() -> Result {
        let temp = tempfile::tempdir()?;
        std::fs::create_dir(temp.path().join("dir"))?;
        std::os::unix::fs::symlink("dir", temp.path().join("link"))?;
        std::fs::write(temp.path().join("file"), b"x")?;
        let (namespace, _) = roles(temp.path())?;
        for target in ["link", "file"] {
            assert_eq!(
                failure_class(
                    namespace
                        .execute(NamespaceRequest::List(path(target)))
                        .await
                ),
                FailureClass::InvalidInput,
                "{target}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn read_link_returns_an_absolute_target_verbatim() -> Result {
        let temp = tempfile::tempdir()?;
        std::os::unix::fs::symlink("/etc/hostname", temp.path().join("abs"))?;
        let (namespace, _) = roles(temp.path())?;
        let result = namespace
            .execute(NamespaceRequest::ReadLink(path("abs")))
            .await
            .map_err(|error| format!("{error:?}"))?;
        let NamespaceResult::LinkTarget(target) = result else {
            panic!("ReadLink did not return a target")
        };
        assert_eq!(target.as_bytes(), b"/etc/hostname");
        Ok(())
    }

    #[tokio::test]
    async fn an_unspellable_name_is_a_per_child_failure_beside_its_siblings() -> Result {
        let temp = tempfile::tempdir()?;
        std::fs::write(
            temp.path().join(std::ffi::OsString::from_vec(vec![0xff])),
            b"a",
        )?;
        std::fs::write(temp.path().join("fine"), b"b")?;
        let (namespace, _) = roles(temp.path())?;
        let result = namespace
            .execute(NamespaceRequest::List(StoragePath::root()))
            .await
            .map_err(|error| format!("{error:?}"))?;
        let NamespaceResult::Listing { entries, failures } = result else {
            panic!("expected a partial listing, got {result:?}")
        };
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path.as_str(), "fine");
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].path().as_str(), "@local-unix-hex:ff");
        assert_eq!(failures[0].class(), FailureClass::Unsupported);
        Ok(())
    }

    #[tokio::test]
    async fn a_literal_backslash_is_an_ordinary_name() -> Result {
        let temp = tempfile::tempdir()?;
        std::fs::create_dir(temp.path().join("a"))?;
        std::fs::write(temp.path().join(r"a\b"), b"literal")?;
        let (namespace, _) = roles(temp.path())?;
        let names = list(&namespace, "")
            .await
            .into_iter()
            .map(|entry| entry.path.as_str().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(names, ["a", r"a\b"]);
        Ok(())
    }

    #[tokio::test]
    async fn listed_mode_is_the_real_permission_bits() -> Result {
        use std::os::unix::fs::PermissionsExt as _;
        let temp = tempfile::tempdir()?;
        let file = temp.path().join("file");
        std::fs::write(&file, b"x")?;
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o640))?;
        let (namespace, _) = roles(temp.path())?;
        assert_eq!(list(&namespace, "").await[0].inline_mode(), Some(0o640));
        Ok(())
    }
}
