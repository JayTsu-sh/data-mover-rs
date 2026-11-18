//! Regression tests for metadata written through a bound directory handle.
use std::fs::{self, FileTimes, Permissions};
use std::io;
use std::os::unix::fs::{PermissionsExt as _, symlink};
use std::time::{Duration, UNIX_EPOCH};

use tokio_util::sync::CancellationToken;

use super::super::{LocalObservationAdapter, system_time_to_timestamp};
use super::{TestRoot, adapter};
use crate::backend::namespace::LocalNamespace;
use crate::model::{
    EntryKind, ExtendedAttribute, FailureClass, Operation, StoragePath, TimestampMetadata,
    Transience,
};
use crate::storage::{Metadata, MetadataBatchFailure, MetadataMutation, StorageRoleFailure};

#[tokio::test]
async fn bound_configured_root_removal_or_replacement_is_conflict_for_write_and_fresh_stat()
-> Result<(), Box<dyn std::error::Error>> {
    use std::num::NonZeroUsize;

    use crate::backend::connect_backend;
    use crate::storage::LocalBackendConfig;

    for replacement in ["missing", "directory", "file", "symlink"] {
        let temporary = tempfile::tempdir()?;
        let configured = temporary.path().join("target");
        let held = temporary.path().join("held");
        let unrelated = temporary.path().join("unrelated");
        for directory in [&configured, &unrelated] {
            fs::create_dir(directory)?;
            fs::set_permissions(directory, Permissions::from_mode(0o711))?;
        }
        let storage = connect_backend(LocalBackendConfig {
            root: configured.clone(),
            rename_context: None,
            read_concurrency: NonZeroUsize::MIN,
            write_concurrency: NonZeroUsize::MIN,
            read_chunk_bytes: None,
            write_chunk_bytes: None,
        })?;
        let namespace = storage.namespace()?;
        let metadata = storage.metadata()?;
        let root = StoragePath::root();
        let key = namespace
            .stat(&root, true)
            .await?
            .source_identity
            .identity_key();
        let cancel = CancellationToken::new();
        assert!(
            metadata
                .apply_bound_directory(
                    &root,
                    key,
                    vec![MetadataMutation::Mode(0o751)],
                    cancel.clone()
                )
                .await
                .is_ok()
        );
        assert_eq!(
            namespace
                .stat(&root, true)
                .await?
                .source_identity
                .identity_key(),
            key
        );
        fs::rename(&configured, &held)?;
        match replacement {
            "directory" => {
                fs::create_dir(&configured)?;
                fs::set_permissions(&configured, Permissions::from_mode(0o713))?;
            }
            "file" => fs::write(&configured, b"keep")?,
            "symlink" => symlink(&unrelated, &configured)?,
            _ => {}
        }
        for path in [root, StoragePath::new(".")?] {
            let write = metadata
                .apply_bound_directory(
                    &path,
                    key,
                    vec![MetadataMutation::Mode(0o755)],
                    cancel.clone(),
                )
                .await;
            let read = namespace.stat(&path, true).await;
            assert!(
                matches!(&write, Err(batch) if batch.completed == 0 && matches!(
                    batch.error.as_ref(), Some(StorageRoleFailure::Entry(error))
                        if error.class() == FailureClass::Conflict && error.operation() == Operation::Metadata
                )),
                "{replacement} {write:?}"
            );
            assert!(
                matches!(&read, Err(StorageRoleFailure::Entry(error))
                    if error.class() == FailureClass::Conflict && error.operation() == Operation::Observe
                ),
                "{replacement} {read:?}"
            );
        }
        assert_eq!(fs::metadata(&held)?.permissions().mode() & 0o777, 0o751);
        assert_eq!(
            fs::metadata(&unrelated)?.permissions().mode() & 0o777,
            0o711
        );
        if replacement == "directory" {
            assert_eq!(
                fs::metadata(&configured)?.permissions().mode() & 0o777,
                0o713
            );
        }
        if replacement == "file" {
            assert_eq!(fs::read(&configured)?, b"keep");
        }
    }
    Ok(())
}

#[tokio::test]
async fn a_handle_only_root_cannot_attest_current_name() -> Result<(), Box<dyn std::error::Error>> {
    use crate::storage::Namespace;

    let root = tempfile::tempdir()?;
    fs::create_dir(root.path().join("child"))?;
    let named = adapter(root.path())?;
    let namespace = LocalNamespace::from_root(named.root.clone(), named.identity.clone());
    let handle_only =
        LocalObservationAdapter::from_root(named.root.clone(), named.identity.clone());

    for path in [StoragePath::root(), StoragePath::new(".")?] {
        let key = namespace
            .stat(&path, false)
            .await?
            .source_identity
            .identity_key();
        assert!(matches!(
            namespace.stat(&path, true).await,
            Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Unsupported
        ));
        assert!(matches!(
            handle_only.apply_bound_directory(
                &path, key, vec![MetadataMutation::Mode(0o755)], CancellationToken::new()
            ).await,
            Err(failure) if matches!(failure.error.as_ref(), Some(StorageRoleFailure::Entry(error))
                if error.class() == FailureClass::Unsupported)
        ));
    }
    assert_eq!(
        namespace
            .stat(&StoragePath::new("child")?, true)
            .await?
            .kind,
        EntryKind::Directory
    );
    Ok(())
}

#[test]
fn dropping_a_queued_bound_write_prevents_late_mutation() -> io::Result<()> {
    tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .enable_all()
        .build()?
        .block_on(async {
            let root = TestRoot::new()?;
            let directory = root.0.join("directory");
            fs::create_dir(&directory)?;
            fs::set_permissions(&directory, Permissions::from_mode(0o711))?;
            let adapter = adapter(&root.0).map_err(io::Error::other)?;
            let path = StoragePath::new("directory").map_err(io::Error::other)?;
            let expected = adapter
                .observe(path.clone())
                .await
                .map_err(io::Error::other)?
                .identity_key();
            let (ready, occupied) = tokio::sync::oneshot::channel();
            let (release, wait) = std::sync::mpsc::channel();
            let occupant = tokio::task::spawn_blocking(move || {
                let _ = ready.send(());
                let _ = wait.recv();
            });
            occupied.await.map_err(io::Error::other)?;
            let mut write = Box::pin(adapter.apply_bound_directory(
                &path,
                expected,
                vec![MetadataMutation::Mode(0o750)],
                CancellationToken::new(),
            ));
            tokio::select! {
                biased;
                result = &mut write => panic!("write completed before worker release: {result:?}"),
                () = std::future::ready(()) => {}
            }
            drop(write);
            release.send(()).map_err(io::Error::other)?;
            occupant.await.map_err(io::Error::other)?;
            tokio::task::spawn_blocking(|| ())
                .await
                .map_err(io::Error::other)?;
            assert_eq!(
                fs::metadata(directory)?.permissions().mode() & 0o7777,
                0o711
            );
            Ok(())
        })
}

#[tokio::test]
async fn bound_directory_metadata_updates_mode_and_time() -> io::Result<()> {
    let root = TestRoot::new()?;
    let directory = root.0.join("parent/directory");
    fs::create_dir_all(&directory)?;
    let adapter = adapter(&root.0).map_err(io::Error::other)?;
    let path = StoragePath::new("parent/directory").map_err(io::Error::other)?;
    let expected = adapter
        .observe(path.clone())
        .await
        .map_err(io::Error::other)?
        .identity_key();
    let modified = UNIX_EPOCH + Duration::from_secs(1_700_000_123);
    let stamp = system_time_to_timestamp(modified).ok_or_else(|| io::Error::other("timestamp"))?;
    assert!(adapter.supports_bound_directory_metadata());
    adapter
        .apply_bound_directory(
            &path,
            expected,
            vec![
                MetadataMutation::Mode(0o750),
                MetadataMutation::Timestamps(TimestampMetadata {
                    accessed: None,
                    modified: Some(stamp),
                    created: None,
                }),
            ],
            CancellationToken::new(),
        )
        .await
        .map_err(|failure| io::Error::other(format!("{failure:?}")))?;
    let after = fs::metadata(directory)?;
    assert_eq!(after.permissions().mode() & 0o7777, 0o750);
    assert_eq!(after.modified()?, modified);
    Ok(())
}

#[tokio::test]
async fn bound_directory_metadata_reports_partial_xattrs_and_stops_batch() -> io::Result<()> {
    let root = TestRoot::new()?;
    let directory = root.0.join("directory");
    fs::create_dir(&directory)?;
    fs::set_permissions(&directory, Permissions::from_mode(0o711))?;
    let adapter = adapter(&root.0).map_err(io::Error::other)?;
    let path = StoragePath::new("directory").map_err(io::Error::other)?;
    let expected = adapter
        .observe(path.clone())
        .await
        .map_err(io::Error::other)?
        .identity_key();
    let name = "user.bound-metadata-partial";
    let value = b"first attribute";
    let valid = ExtendedAttribute::new(name.as_bytes().to_vec(), value.to_vec())
        .map_err(io::Error::other)?;
    let invalid = ExtendedAttribute::new(b"user.invalid\0name".to_vec(), value.to_vec())
        .map_err(io::Error::other)?;
    for (attributes, transience) in [
        (vec![invalid.clone()], Transience::Permanent),
        (vec![valid, invalid], Transience::Unknown),
    ] {
        let result = adapter
            .apply_bound_directory(
                &path,
                expected,
                vec![
                    MetadataMutation::Xattrs(attributes),
                    MetadataMutation::Mode(0o750),
                ],
                CancellationToken::new(),
            )
            .await;
        assert!(
            matches!(
                result,
                Err(MetadataBatchFailure {
                    failed_index: 0,
                    completed: 0,
                    error: Some(StorageRoleFailure::Entry(ref error)),
                }) if error.class() == FailureClass::InvalidInput && error.transience() == transience
            ),
            "expected InvalidInput/{transience:?}, got {result:?}"
        );
    }
    assert_eq!(xattr::get(&directory, name)?.as_deref(), Some(&value[..]));
    assert_eq!(
        fs::metadata(&directory)?.permissions().mode() & 0o7777,
        0o711
    );
    Ok(())
}

#[tokio::test]
async fn bound_directory_metadata_refuses_replacement_and_symlink() -> io::Result<()> {
    let root = TestRoot::new()?;
    let directory = root.0.join("directory");
    let held = root.0.join("held");
    fs::create_dir(&directory)?;
    fs::set_permissions(&directory, Permissions::from_mode(0o711))?;
    let adapter = adapter(&root.0).map_err(io::Error::other)?;
    let path = StoragePath::new("directory").map_err(io::Error::other)?;
    let expected = adapter
        .observe(path.clone())
        .await
        .map_err(io::Error::other)?
        .identity_key();
    fs::rename(&directory, &held)?;
    fs::create_dir(&directory)?;
    fs::set_permissions(&directory, Permissions::from_mode(0o735))?;
    let modified = UNIX_EPOCH + Duration::from_secs(1_600_000_000);
    let file = fs::File::open(&directory)?;
    file.set_times(FileTimes::new().set_modified(modified))?;
    let attempted_time = system_time_to_timestamp(UNIX_EPOCH + Duration::from_secs(1_700_000_000))
        .ok_or_else(|| io::Error::other("timestamp"))?;
    let mutations = vec![
        MetadataMutation::Mode(0o751),
        MetadataMutation::Timestamps(TimestampMetadata {
            accessed: None,
            modified: Some(attempted_time),
            created: None,
        }),
    ];
    for replace_with_link in [false, true] {
        if replace_with_link {
            fs::remove_dir(&directory)?;
            symlink("held", &directory)?;
        }
        let result = adapter
            .apply_bound_directory(&path, expected, mutations.clone(), CancellationToken::new())
            .await;
        assert!(matches!(
            result,
            Err(MetadataBatchFailure {
                failed_index: 2,
                completed: 0,
                error: Some(StorageRoleFailure::Entry(error)),
            }) if error.class() == FailureClass::Conflict
        ));
        if !replace_with_link {
            let unchanged = fs::metadata(&directory)?;
            assert_eq!(unchanged.permissions().mode() & 0o7777, 0o735);
            assert_eq!(unchanged.modified()?, modified);
        }
        assert_eq!(fs::metadata(&held)?.permissions().mode() & 0o7777, 0o711);
    }
    Ok(())
}

#[tokio::test]
async fn bound_directory_metadata_validates_empty_root_and_reports_prefix() -> io::Result<()> {
    let root = TestRoot::new()?;
    fs::write(root.0.join("child"), b"original")?;
    let adapter = adapter(&root.0).map_err(io::Error::other)?;
    let path = StoragePath::root();
    let namespace = LocalNamespace::from_root(adapter.root.clone(), adapter.identity.clone())
        .with_persistence_root(fs::canonicalize(&root.0)?);
    let expected = crate::storage::Namespace::stat(&namespace, &path, true)
        .await
        .map_err(io::Error::other)?
        .source_identity
        .identity_key();
    assert!(
        adapter
            .apply_bound_directory(&path, expected, vec![], CancellationToken::new())
            .await
            .is_ok()
    );
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(matches!(
        adapter
            .apply_bound_directory(
                &path,
                expected,
                vec![MetadataMutation::Mode(0o777)],
                cancelled
            )
            .await,
        Err(MetadataBatchFailure {
            failed_index: 1,
            completed: 0,
            error: None
        })
    ));
    let result = adapter
        .apply_bound_directory(
            &path,
            expected,
            vec![
                MetadataMutation::Mode(0o750),
                MetadataMutation::Tags(Vec::new()),
                MetadataMutation::Mode(0o777),
            ],
            CancellationToken::new(),
        )
        .await;
    assert!(matches!(
        result,
        Err(MetadataBatchFailure {
            failed_index: 1,
            completed: 1,
            error: Some(StorageRoleFailure::Entry(error)),
        }) if error.class() == FailureClass::Unsupported
    ));
    assert_eq!(fs::metadata(&root.0)?.permissions().mode() & 0o7777, 0o750);
    assert_eq!(fs::read(root.0.join("child"))?, b"original");
    Ok(())
}
