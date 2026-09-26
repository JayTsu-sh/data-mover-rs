use std::sync::Arc;

use cap_std::ambient_authority;
use cap_std::fs::Dir;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::storage::backends::local::observation::LocalObservationAdapter;
use crate::storage::backends::local::test_identity;
use crate::storage::listing::testing::{Perform as _, Verb, listed};
use crate::storage::{CollectLimits, collect_listing};

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
    let (mut entries, failures) = listed(namespace, &path(directory))
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert!(failures.is_empty(), "{failures:?}");
    entries.sort_by(|left, right| left.path.as_str().cmp(right.path.as_str()));
    entries
}

fn failure_class<T: std::fmt::Debug>(
    result: std::result::Result<T, StorageRoleFailure>,
) -> FailureClass {
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
                .perform(Verb::Delete(path(".data-mover-owned.stage")))
                .await
        ),
        FailureClass::InvalidInput
    );
    Ok(())
}

/// Hidden artifacts are counted on the page, never listed.
#[tokio::test]
async fn a_listing_page_counts_the_artifacts_it_hides() -> Result {
    let temp = tempfile::tempdir()?;
    std::fs::write(temp.path().join(".data-mover-a.stage"), b"internal")?;
    std::fs::create_dir(temp.path().join(".data-mover-b.part"))?;
    std::fs::write(temp.path().join("visible"), b"x")?;
    let (namespace, _) = roles(temp.path())?;
    let request = ListRequest::new(StoragePath::root(), CancellationToken::new());
    let page = collect_listing(namespace.list(request), CollectLimits::UNBOUNDED)
        .await
        .map_err(|error| format!("{error:?}"))?;
    assert_eq!(page.internal_hidden, 2);
    assert_eq!(page.children.len(), 1);
    assert_eq!(page.children[0].name(), "visible");
    Ok(())
}

/// Children are spelled as the requested directory and their name, the listing contract,
/// however the request spelled the directory: `./dir` lists `./dir/child`, not `dir/child`.
#[tokio::test]
async fn children_keep_the_requested_spelling_of_their_directory() -> Result {
    let temp = tempfile::tempdir()?;
    std::fs::create_dir(temp.path().join("dir"))?;
    std::fs::write(temp.path().join("dir/child"), b"x")?;
    let (namespace, _) = roles(temp.path())?;
    for (directory, expected) in [("./dir", "./dir/child"), ("dir", "dir/child")] {
        let listed = list(&namespace, directory).await;
        let paths: Vec<&str> = listed.iter().map(|entry| entry.path.as_str()).collect();
        assert_eq!(paths, [expected]);
    }
    Ok(())
}

#[tokio::test]
async fn paths_are_confined_and_the_root_cannot_be_mutated() -> Result {
    let temp = tempfile::tempdir()?;
    let (namespace, _) = roles(temp.path())?;
    for request in [
        Verb::List(path("../escape")),
        Verb::Stat(path("a/../../escape")),
        Verb::Delete(StoragePath::root()),
        Verb::CreateDirectory(StoragePath::root()),
        // Only `.` components name the root too.
        Verb::Delete(path(".")),
        Verb::Delete(path("./.")),
    ] {
        assert_eq!(
            failure_class(namespace.perform(request).await),
            FailureClass::InvalidInput
        );
    }
    match namespace.perform(Verb::List(path("../escape"))).await {
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
        .perform(Verb::CreateDirectory(path("made")))
        .await
        .map_err(|error| format!("{error:?}"))?;
    assert!(temp.path().join("made").is_dir());
    assert_eq!(
        failure_class(namespace.perform(Verb::CreateDirectory(path("made"))).await),
        FailureClass::Conflict
    );
    assert_eq!(
        failure_class(
            namespace
                .perform(Verb::CreateDirectory(path("missing/child")))
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
            .perform(Verb::Delete(path(target)))
            .await
            .map_err(|error| format!("{error:?}"))?;
        assert!(!temp.path().join(target).exists());
    }
    assert_eq!(
        failure_class(namespace.perform(Verb::Delete(path("full"))).await),
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
        .perform(Verb::Rename {
            from: path("new"),
            to: path("old"),
        })
        .await
        .map_err(|error| format!("{error:?}"))?;
    assert_eq!(std::fs::read(temp.path().join("old"))?, b"new");
    assert!(!temp.path().join("new").exists());
    namespace
        .perform(Verb::Rename {
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
    let entry = namespace
        .stat(&path("file"))
        .await
        .map_err(|error| format!("{error:?}"))?;
    assert_eq!(entry.size, Some(4));
    assert_eq!(
        failure_class(namespace.perform(Verb::Stat(path("absent"))).await),
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
            .perform(Verb::List(StoragePath::root()))
            .await
            .is_ok()
    );
    assert_eq!(
        failure_class(namespace.perform(Verb::List(path("dir"))).await),
        FailureClass::PermissionDenied
    );
    assert_eq!(namespace.failed_list_path(), Some(path("dir")));
    Ok(())
}

#[tokio::test]
async fn the_list_probe_fails_a_chosen_path_on_every_call() -> Result {
    let temp = tempfile::tempdir()?;
    std::fs::create_dir(temp.path().join("a"))?;
    std::fs::create_dir(temp.path().join("b"))?;
    let (namespace, _) = roles(temp.path())?;
    namespace.fail_list_path(path("a"));
    assert!(namespace.perform(Verb::List(path("b"))).await.is_ok());
    for _ in 0..2 {
        assert_eq!(
            failure_class(namespace.perform(Verb::List(path("a"))).await),
            FailureClass::PermissionDenied
        );
    }
    assert_eq!(namespace.failed_list_path(), Some(path("a")));
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
        let target = namespace
            .read_link(&path("link"))
            .await
            .map_err(|error| format!("{error:?}"))?;
        assert_eq!(target.as_bytes(), b"target");
        namespace
            .perform(Verb::Delete(path("link")))
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
            Verb::List(path("link")),
            Verb::Stat(path("link/victim")),
            Verb::Delete(path("link/victim")),
            Verb::Delete(path("link/inner")),
            Verb::CreateDirectory(path("link/new")),
            Verb::Rename {
                from: path("link/victim"),
                to: path("moved"),
            },
            Verb::Rename {
                from: path("precious/victim"),
                to: path("link/moved"),
            },
        ] {
            let description = format!("{request:?}");
            let outcome = namespace.perform(request).await;
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
                failure_class(namespace.perform(Verb::List(path(target))).await),
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
        let target = namespace
            .read_link(&path("abs"))
            .await
            .map_err(|error| format!("{error:?}"))?;
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
        let (entries, failures) = listed(&namespace, &StoragePath::root())
            .await
            .map_err(|error| format!("{error:?}"))?;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path.as_str(), "fine");
        assert_eq!(failures.len(), 1);
        // No stand-in path that names nothing (P7 C3): the failure is at the listed directory,
        // and the raw name is in its diagnostic and in the page's `raw_name`.
        assert_eq!(failures[0].path(), &StoragePath::root());
        assert_eq!(failures[0].class(), FailureClass::Unsupported);
        assert!(
            failures[0].diagnostic().contains("\\xff"),
            "{}",
            failures[0].diagnostic()
        );
        assert!(failures[0].identity().is_some());
        let request = ListRequest::new(StoragePath::root(), CancellationToken::new());
        let page = collect_listing(namespace.list(request), CollectLimits::UNBOUNDED)
            .await
            .map_err(|error| format!("{error:?}"))?;
        assert_eq!(page.failures.len(), 1);
        assert_eq!(page.failures[0].name, None);
        assert_eq!(page.failures[0].raw_name.as_deref(), Some(&[0xff_u8][..]));
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
