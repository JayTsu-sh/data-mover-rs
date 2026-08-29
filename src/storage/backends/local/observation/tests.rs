use std::sync::atomic::{AtomicU64, Ordering};

use super::*;
use crate::model::BackendKind;
use crate::model::observation::PrivateBackendEntryFacts;

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
