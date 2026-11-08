use std::os::unix::fs::symlink;

use bytes::Bytes;
use futures::stream;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::model::{EntryKind, IdentityStrength, SourceIdentity};
use crate::storage::backends::local::test_identity;
use crate::storage::{
    FinalDestination, PositionedChunk, PrepareRequest, SourceDescriptor, StagedDestination,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

struct Fixture {
    temporary: tempfile::TempDir,
    adapter: LocalStagedDestination,
}

impl Fixture {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        std::fs::create_dir_all(temporary.path().join("endpoint/tree"))?;
        let adapter = LocalStagedDestination::new(
            temporary.path().join("endpoint"),
            test_identity("failed-direct-cleanup"),
            2,
        )?;
        Ok(Self { temporary, adapter })
    }

    fn target(&self) -> std::path::PathBuf {
        self.temporary.path().join("endpoint/tree/file")
    }

    async fn prepare(&self) -> Result<PreparedStage, Box<dyn std::error::Error>> {
        let source = SourceDescriptor::new(
            StoragePath::new("source")?,
            EntryKind::File,
            Some(4),
            SourceIdentity::new(
                self.adapter.identity.clone(),
                IdentityStrength::StableWithinBackend,
                b"independent-source",
            )?,
        );
        Ok(self
            .adapter
            .prepare_direct(
                PrepareRequest {
                    final_destination: FinalDestination::new(StoragePath::new("tree/file")?),
                    source,
                    recovery_binding: [0; 32],
                },
                CancellationToken::new(),
            )
            .await?)
    }
}

#[tokio::test]
async fn explicit_cleanup_removes_created_files_but_default_discard_keeps_them() -> TestResult {
    let fixture = Fixture::new()?;
    let stage = fixture.prepare().await?;
    fixture.adapter.discard(stage).await?;
    assert!(fixture.target().is_file());
    std::fs::remove_file(fixture.target())?;
    let stage = fixture.prepare().await?;
    fixture.adapter.remove_failed_direct(stage).await?;
    assert!(!fixture.target().exists());
    assert!(fixture.target().parent().is_some_and(Path::is_dir));
    Ok(())
}

#[tokio::test]
async fn explicit_cleanup_removes_existing_outputs_changed_by_each_writer() -> TestResult {
    for mode in 0..3 {
        let fixture = Fixture::new()?;
        std::fs::write(fixture.target(), b"original-long-target")?;
        let stage = fixture.prepare().await?;
        let payload = Bytes::from_static(b"bad!");
        match mode {
            0 => {
                fixture.adapter.write_single(&stage, payload).await?;
            }
            1 => {
                let error = failure(
                    &StoragePath::new("source")?,
                    Operation::Read,
                    FailureClass::Cancelled,
                );
                assert!(
                    fixture
                        .adapter
                        .write(&stage, Box::pin(stream::iter([Ok(payload), Err(error),])))
                        .await
                        .is_err()
                );
            }
            _ => {
                fixture
                    .adapter
                    .write_positioned(
                        &stage,
                        Box::pin(stream::iter([Ok(PositionedChunk {
                            offset: 0,
                            data: payload,
                        })])),
                    )
                    .await?;
            }
        }
        fixture.adapter.remove_failed_direct(stage).await?;
        assert!(!fixture.target().exists());
    }
    Ok(())
}

#[tokio::test]
async fn unchanged_existing_targets_and_unattributed_changes_are_preserved() -> TestResult {
    for intended in [false, true] {
        let fixture = Fixture::new()?;
        std::fs::write(fixture.target(), b"original")?;
        let stage = fixture.prepare().await?;
        if intended {
            mark_mutation(&stage);
        }
        fixture.adapter.remove_failed_direct(stage).await?;
        assert_eq!(std::fs::read(fixture.target())?, b"original");
    }
    let fixture = Fixture::new()?;
    std::fs::write(fixture.target(), b"original")?;
    let stage = fixture.prepare().await?;
    std::fs::write(fixture.target(), b"external-change")?;
    fixture.adapter.remove_failed_direct(stage).await?;
    assert_eq!(std::fs::read(fixture.target())?, b"external-change");
    Ok(())
}

#[tokio::test]
async fn replaced_files_and_symlinks_are_refused_without_deleting_other_objects() -> TestResult {
    for linked in [false, true] {
        let fixture = Fixture::new()?;
        let stage = fixture.prepare().await?;
        fixture
            .adapter
            .write_single(&stage, Bytes::from_static(b"bad!"))
            .await?;
        std::fs::rename(fixture.target(), fixture.target().with_extension("held"))?;
        let outside = fixture.temporary.path().join("unrelated");
        std::fs::write(&outside, b"unrelated")?;
        if linked {
            symlink(&outside, fixture.target())?;
        } else {
            std::fs::write(fixture.target(), b"replacement")?;
        }
        let result = fixture.adapter.remove_failed_direct(stage).await;
        assert!(matches!(result, Err(StorageRoleFailure::Entry(error))
            if error.class() == FailureClass::Conflict));
        assert_eq!(std::fs::read(&outside)?, b"unrelated");
        assert!(fixture.target().symlink_metadata().is_ok());
        assert_eq!(
            std::fs::read(fixture.target().with_extension("held"))?,
            b"bad!"
        );
    }
    Ok(())
}

#[tokio::test]
async fn replaced_parent_or_endpoint_root_is_refused() -> TestResult {
    for endpoint in [false, true] {
        let fixture = Fixture::new()?;
        let stage = fixture.prepare().await?;
        let replaced = if endpoint {
            fixture.temporary.path().join("endpoint")
        } else {
            fixture.temporary.path().join("endpoint/tree")
        };
        std::fs::rename(&replaced, replaced.with_extension("held"))?;
        std::fs::create_dir_all(fixture.target().parent().ok_or("target parent")?)?;
        std::fs::write(fixture.target(), b"replacement")?;
        let result = fixture.adapter.remove_failed_direct(stage).await;
        assert!(matches!(result, Err(StorageRoleFailure::Entry(error))
            if error.class() == FailureClass::Conflict));
        assert_eq!(std::fs::read(fixture.target())?, b"replacement");
    }
    Ok(())
}

#[tokio::test]
async fn an_already_missing_leaf_is_idempotent() -> TestResult {
    let fixture = Fixture::new()?;
    let stage = fixture.prepare().await?;
    std::fs::remove_file(fixture.target())?;
    fixture.adapter.remove_failed_direct(stage).await?;
    assert!(!fixture.target().exists());
    Ok(())
}

#[tokio::test]
async fn directory_sync_failure_is_reported_after_removing_the_owned_output() -> TestResult {
    let fixture = Fixture::new()?;
    let stage = fixture.prepare().await?;
    let owned = LocalStagedDestination::local_state(&stage, Operation::Namespace)?
        .ok_or("missing Local state")?;
    owned
        .direct_cleanup
        .as_ref()
        .ok_or("missing cleanup evidence")?
        .fail_directory_sync
        .store(true, Ordering::Release);
    let error = fixture
        .adapter
        .remove_failed_direct(stage)
        .await
        .err()
        .ok_or("directory sync failure was ignored")?;
    assert!(matches!(&error, StorageRoleFailure::Entry(cause)
        if cause.class() == FailureClass::Protocol));
    assert!(matches!(&error, StorageRoleFailure::Entry(cause)
        if cause.diagnostic().contains("directory sync failure")));
    assert!(!fixture.target().exists());
    Ok(())
}

#[tokio::test]
async fn a_source_alias_is_rejected_before_cleanup_can_be_authorized() -> TestResult {
    let fixture = Fixture::new()?;
    let source = fixture.temporary.path().join("source");
    std::fs::write(&source, b"source")?;
    std::fs::hard_link(&source, fixture.target())?;
    let metadata = fixture.adapter.root_dir.symlink_metadata("tree/file")?;
    let path = StoragePath::new("source")?;
    let identity = super::super::super::observation::source_identity(
        &fixture.adapter.identity,
        &path,
        &metadata,
    )?;
    let result = fixture
        .adapter
        .prepare_direct(
            PrepareRequest {
                final_destination: FinalDestination::new(StoragePath::new("tree/file")?),
                source: SourceDescriptor::new(path, EntryKind::File, Some(6), identity),
                recovery_binding: [0; 32],
            },
            CancellationToken::new(),
        )
        .await;
    assert!(matches!(result, Err(StorageRoleFailure::Entry(error))
        if error.class() == FailureClass::Conflict));
    assert_eq!(std::fs::read(source)?, b"source");
    assert_eq!(std::fs::read(fixture.target())?, b"source");
    Ok(())
}
