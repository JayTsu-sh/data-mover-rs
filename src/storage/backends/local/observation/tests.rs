use std::sync::atomic::{AtomicU64, Ordering};

use super::*;
use crate::model::observation::PrivateBackendEntryFacts;
use crate::model::{
    BackendKind, MetadataObservation, MetadataProvenance, ObservationMode, ObservationPlan,
};

static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(1);

struct TestRoot(PathBuf);

impl TestRoot {
    fn new() -> io::Result<Self> {
        let sequence = TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "data-mover-local-observation-{}-{sequence}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path)?;
        Ok(Self(path))
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn adapter(root: &Path) -> Result<LocalObservationAdapter, StorageRoleFailure> {
    let identity = BackendIdentity::new(BackendKind::Local, "local-observation-test")
        .unwrap_or_else(|_| unreachable!("static identity is valid"));
    LocalObservationAdapter::new(root, identity)
}

#[tokio::test]
async fn observes_file_directory_and_stable_identity() -> io::Result<()> {
    let root = TestRoot::new()?;
    std::fs::write(root.0.join("file.bin"), b"payload")?;
    std::fs::create_dir(root.0.join("directory"))?;
    let adapter = adapter(&root.0).map_err(io::Error::other)?;

    let first = adapter
        .observe(StoragePath::new("file.bin").map_err(io::Error::other)?)
        .await
        .map_err(io::Error::other)?;
    let second = adapter
        .observe(StoragePath::new("file.bin").map_err(io::Error::other)?)
        .await
        .map_err(io::Error::other)?;
    let directory = adapter
        .observe(StoragePath::new("directory").map_err(io::Error::other)?)
        .await
        .map_err(io::Error::other)?;

    assert_eq!(first.kind(), EntryKind::File);
    assert_eq!(first.size(), Some(7));
    assert!(first.modified().is_some());
    assert_eq!(first.identity_key(), second.identity_key());
    assert_eq!(directory.kind(), EntryKind::Directory);
    assert_eq!(directory.size(), None);
    assert!(matches!(
        first.metadata().acl(),
        MetadataObservation::NotRequested
    ));
    assert!(matches!(
        first.metadata().xattrs(),
        MetadataObservation::NotRequested
    ));
    assert_eq!(adapter.optional_call_count(), 0);
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn inline_plan_uses_stat_facts_without_optional_storage_calls() -> io::Result<()> {
    let root = TestRoot::new()?;
    std::fs::write(root.0.join("file"), b"value")?;
    let adapter = adapter(&root.0).map_err(io::Error::other)?;
    let plan = ObservationPlan::default()
        .with_acl(ObservationMode::InlineOnly)
        .with_xattrs(ObservationMode::InlineOnly)
        .with_ownership_mode(ObservationMode::InlineOnly)
        .with_timestamps(ObservationMode::InlineOnly);

    let observed = adapter
        .observe_with_plan(StoragePath::new("file").map_err(io::Error::other)?, plan)
        .await
        .map_err(io::Error::other)?;

    assert!(matches!(
        observed.metadata().acl(),
        MetadataObservation::Unsupported
    ));
    assert!(matches!(
        observed.metadata().xattrs(),
        MetadataObservation::Unsupported
    ));
    assert!(matches!(
        observed.metadata().ownership_mode(),
        MetadataObservation::Value {
            provenance: MetadataProvenance::Inline,
            ..
        }
    ));
    assert!(matches!(
        observed.metadata().timestamps(),
        MetadataObservation::Value {
            provenance: MetadataProvenance::Inline,
            ..
        }
    ));
    assert_eq!(adapter.optional_call_count(), 0);
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn best_effort_records_optional_failure_but_required_fails_entry() -> io::Result<()> {
    let root = TestRoot::new()?;
    std::fs::write(root.0.join("file"), b"value")?;
    let adapter = adapter(&root.0).map_err(io::Error::other)?;
    adapter.fail_optional_calls();
    let path = StoragePath::new("file").map_err(io::Error::other)?;
    let best_effort = ObservationPlan::default()
        .with_acl(ObservationMode::BestEffort)
        .with_xattrs(ObservationMode::BestEffort);

    let observed = adapter
        .observe_with_plan(path.clone(), best_effort)
        .await
        .map_err(io::Error::other)?;
    assert!(matches!(
        observed.metadata().acl(),
        MetadataObservation::Failed {
            class: FailureClass::PermissionDenied,
            ..
        }
    ));
    assert!(matches!(
        observed.metadata().xattrs(),
        MetadataObservation::Failed {
            class: FailureClass::PermissionDenied,
            ..
        }
    ));
    assert_eq!(adapter.optional_call_count(), 2);

    let required = ObservationPlan::default().with_acl(ObservationMode::Required);
    assert!(matches!(
        adapter.observe_with_plan(path, required).await,
        Err(StorageRoleFailure::Entry(error))
            if error.operation() == Operation::Observe
                && error.class() == FailureClass::PermissionDenied
    ));
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn best_effort_does_not_demote_backend_session_failure() -> io::Result<()> {
    let root = TestRoot::new()?;
    std::fs::write(root.0.join("file"), b"value")?;
    let adapter = adapter(&root.0).map_err(io::Error::other)?;
    adapter.fail_optional_calls_with(io::ErrorKind::NotConnected);
    let plan = ObservationPlan::default().with_acl(ObservationMode::BestEffort);

    assert!(matches!(
        adapter
            .observe_with_plan(StoragePath::new("file").map_err(io::Error::other)?, plan)
            .await,
        Err(StorageRoleFailure::Session(error)) if error.operation() == Operation::Observe
    ));
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn required_xattrs_capture_empty_and_nonempty_values() -> io::Result<()> {
    let root = TestRoot::new()?;
    let file = root.0.join("file");
    std::fs::write(&file, b"value")?;
    xattr::set(&file, "user.empty", b"")?;
    xattr::set(&file, "user.value", b"metadata")?;
    let adapter = adapter(&root.0).map_err(io::Error::other)?;
    let plan = ObservationPlan::default().with_xattrs(ObservationMode::Required);

    let observed = adapter
        .observe_with_plan(StoragePath::new("file").map_err(io::Error::other)?, plan)
        .await
        .map_err(io::Error::other)?;
    let attributes = observed
        .metadata()
        .xattrs()
        .value()
        .ok_or_else(|| io::Error::other("xattrs were not observed"))?;

    assert_eq!(attributes.len(), 2);
    assert_eq!(attributes[0].name(), b"user.empty");
    assert_eq!(attributes[0].value(), b"");
    assert_eq!(attributes[1].name(), b"user.value");
    assert_eq!(attributes[1].value(), b"metadata");
    let snapshot = observed.encode_snapshot();
    std::fs::remove_file(file)?;
    let rebuilt = ObservedEntry::decode_snapshot(snapshot.as_bytes()).map_err(io::Error::other)?;
    assert_eq!(rebuilt.metadata(), observed.metadata());
    Ok(())
}

#[cfg(unix)]
fn posix_acl_blob() -> Vec<u8> {
    let mut bytes = 2_u32.to_le_bytes().to_vec();
    for (tag, permissions, id) in [
        (0x01_u16, 7_u16, u32::MAX),
        (0x02, 4, 0),
        (0x04, 5, u32::MAX),
        (0x10, 5, u32::MAX),
        (0x20, 0, u32::MAX),
    ] {
        bytes.extend_from_slice(&tag.to_le_bytes());
        bytes.extend_from_slice(&permissions.to_le_bytes());
        bytes.extend_from_slice(&id.to_le_bytes());
    }
    bytes
}

#[cfg(unix)]
#[tokio::test]
async fn required_acl_captures_access_and_directory_default_losslessly() -> io::Result<()> {
    let root = TestRoot::new()?;
    let directory = root.0.join("directory");
    std::fs::create_dir(&directory)?;
    let acl = posix_acl_blob();
    xattr::set(&directory, "system.posix_acl_access", &acl)?;
    xattr::set(&directory, "system.posix_acl_default", &acl)?;
    let adapter = adapter(&root.0).map_err(io::Error::other)?;
    let plan = ObservationPlan::default().with_acl(ObservationMode::Required);

    let observed = adapter
        .observe_with_plan(
            StoragePath::new("directory").map_err(io::Error::other)?,
            plan,
        )
        .await
        .map_err(io::Error::other)?;
    let value = observed
        .metadata()
        .acl()
        .value()
        .ok_or_else(|| io::Error::other("ACL was not observed"))?;

    assert_eq!(value.access(), Some(acl.as_slice()));
    assert_eq!(value.default_acl(), Some(acl.as_slice()));
    let snapshot = observed.encode_snapshot();
    std::fs::remove_dir(directory)?;
    let rebuilt = ObservedEntry::decode_snapshot(snapshot.as_bytes()).map_err(io::Error::other)?;
    assert_eq!(rebuilt.metadata().acl(), observed.metadata().acl());
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn observes_symlink_without_following_target() -> io::Result<()> {
    use std::os::unix::fs::symlink;

    let root = TestRoot::new()?;
    std::fs::write(root.0.join("target"), vec![0_u8; 64])?;
    symlink("target", root.0.join("link"))?;
    symlink("missing", root.0.join("dangling"))?;
    let adapter = adapter(&root.0).map_err(io::Error::other)?;

    for name in ["link", "dangling"] {
        let observed = adapter
            .observe(StoragePath::new(name).map_err(io::Error::other)?)
            .await
            .map_err(io::Error::other)?;
        assert_eq!(observed.kind(), EntryKind::Symlink);
        assert_eq!(observed.size(), None);
        let expected: &[u8] = if name == "link" {
            b"target"
        } else {
            b"missing"
        };
        assert_eq!(
            observed.symlink_target().map(SymlinkTarget::as_bytes),
            Some(expected)
        );
        if name == "link" {
            let snapshot = observed.encode_snapshot();
            std::fs::remove_file(root.0.join(name))?;
            let rebuilt =
                ObservedEntry::decode_snapshot(snapshot.as_bytes()).map_err(io::Error::other)?;
            assert_eq!(rebuilt.symlink_target(), observed.symlink_target());
        }
    }
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn optional_metadata_marks_symlinks_not_applicable_without_calls() -> io::Result<()> {
    use std::os::unix::fs::symlink;

    let root = TestRoot::new()?;
    symlink("missing", root.0.join("dangling"))?;
    let adapter = adapter(&root.0).map_err(io::Error::other)?;
    let plan = ObservationPlan::default()
        .with_acl(ObservationMode::Required)
        .with_xattrs(ObservationMode::Required);
    let observed = adapter
        .observe_with_plan(
            StoragePath::new("dangling").map_err(io::Error::other)?,
            plan,
        )
        .await
        .map_err(io::Error::other)?;

    assert!(matches!(
        observed.metadata().acl(),
        MetadataObservation::NotApplicable
    ));
    assert!(matches!(
        observed.metadata().xattrs(),
        MetadataObservation::NotApplicable
    ));
    assert_eq!(adapter.optional_call_count(), 0);
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn optional_metadata_never_follows_a_symlink_target() -> io::Result<()> {
    use std::os::unix::fs::symlink;

    let root = TestRoot::new()?;
    let outside = TestRoot::new()?;
    std::fs::write(outside.0.join("secret"), b"secret")?;
    symlink(outside.0.join("secret"), root.0.join("link"))?;
    let adapter = adapter(&root.0).map_err(io::Error::other)?;
    let plan = ObservationPlan::default()
        .with_acl(ObservationMode::Required)
        .with_xattrs(ObservationMode::Required);

    let result = adapter
        .observe_with_plan(StoragePath::new("link").map_err(io::Error::other)?, plan)
        .await;
    assert!(matches!(
        result,
        Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::PermissionDenied
    ));
    assert_eq!(adapter.optional_call_count(), 0);
    Ok(())
}

#[tokio::test]
async fn failures_are_entry_scoped_and_paths_remain_confined() -> io::Result<()> {
    let root = TestRoot::new()?;
    let adapter = adapter(&root.0).map_err(io::Error::other)?;
    let missing = adapter
        .observe(StoragePath::new("missing").map_err(io::Error::other)?)
        .await;
    let escaped = adapter
        .observe(StoragePath::new("../escape").map_err(io::Error::other)?)
        .await;

    assert!(matches!(
        missing,
        Err(StorageRoleFailure::Entry(error))
            if error.path().as_str() == "missing"
                && error.operation() == Operation::Observe
                && error.class() == FailureClass::NotFound
    ));
    assert!(matches!(
        escaped,
        Err(StorageRoleFailure::Entry(error))
            if error.path().as_str() == "../escape"
                && error.operation() == Operation::Observe
                && error.class() == FailureClass::InvalidInput
    ));
    Ok(())
}

#[test]
fn root_open_failure_is_session_scoped_and_io_transience_is_truthful() -> io::Result<()> {
    let missing = std::env::temp_dir().join("data-mover-definitely-missing-observation-root");
    let identity =
        BackendIdentity::new(BackendKind::Local, "missing-root").map_err(io::Error::other)?;
    assert!(matches!(
        LocalObservationAdapter::new(missing, identity),
        Err(StorageRoleFailure::Session(_))
    ));

    let path = StoragePath::new("entry").map_err(io::Error::other)?;
    let transient = io_failure(&path, &io::Error::from(io::ErrorKind::TimedOut));
    let unknown = io_failure(&path, &io::Error::other("uncertified"));
    let session = observation_io_failure(&path, &io::Error::from(io::ErrorKind::NotConnected));
    let other = observation_io_failure(&path, &io::Error::other("uncertified entry error"));
    assert!(
        matches!(transient, StorageRoleFailure::Entry(error) if error.transience() == Transience::Transient)
    );
    assert!(
        matches!(unknown, StorageRoleFailure::Entry(error) if error.transience() == Transience::Unknown)
    );
    assert!(
        matches!(session, StorageRoleFailure::Session(error) if error.operation() == Operation::Observe)
    );
    assert!(
        matches!(other, StorageRoleFailure::Entry(error) if error.transience() == Transience::Unknown)
    );
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn snapshot_rebuild_preserves_private_local_facts_without_requery() -> io::Result<()> {
    use std::os::unix::fs::MetadataExt as _;

    let root = TestRoot::new()?;
    std::fs::write(root.0.join("file"), b"value")?;
    let adapter = adapter(&root.0).map_err(io::Error::other)?;
    let observed = adapter
        .observe(StoragePath::new("file").map_err(io::Error::other)?)
        .await
        .map_err(io::Error::other)?;
    let metadata = std::fs::symlink_metadata(root.0.join("file"))?;
    let expected = [
        metadata.dev(),
        metadata.ino(),
        metadata.mode().into(),
        metadata.uid().into(),
        metadata.gid().into(),
        metadata.nlink(),
        metadata.rdev(),
    ];
    let snapshot = observed.encode_snapshot();
    std::fs::remove_file(root.0.join("file"))?;

    let rebuilt = ObservedEntry::decode_snapshot(snapshot.as_bytes()).map_err(io::Error::other)?;
    assert_eq!(rebuilt, observed);
    let PrivateBackendEntryFacts::Local(facts) = rebuilt.backend_facts() else {
        panic!("expected local backend facts");
    };
    assert_eq!(facts.first(), Some(&1));
    for (encoded, expected) in facts[1..].chunks_exact(8).zip(expected) {
        assert_eq!(encoded, expected.to_le_bytes());
    }
    Ok(())
}
