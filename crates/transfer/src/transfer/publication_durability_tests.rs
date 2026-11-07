//! 使用实际文件及持久化调用探针，区分写入模式、续传和发布屏障。
use super::*;
use crate::storage::SourceQosStats;
use crate::transfer::ExpertSourceEvidence;
use crate::transfer::{EffectiveRecovery, PublicationDurability};
use bytes::Bytes;
use std::error::Error as _;
use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

#[tokio::test]
async fn required_direct_coalesces_data_and_readonly_metadata_into_one_final_barrier()
-> Result<(), Box<dyn std::error::Error>> {
    for size in [0, 17, 1024 * 1024 + 3] {
        let source_root = TestRoot::new("required-direct-source")?;
        let target_root = TestRoot::new("required-direct-target")?;
        let source_path = source_root.path().join("final.bin");
        let target_path = target_root.path().join("final.bin");
        let bytes = vec![0x5a; size];
        std::fs::write(&source_path, &bytes)?;
        std::fs::set_permissions(&source_path, std::fs::Permissions::from_mode(0o400))?;
        std::fs::write(&target_path, b"old")?;
        let inode = std::fs::metadata(&target_path)?.ino();
        let (target, role) =
            test_destination_storage_with_role(target_root.path(), "required-direct")?;
        let outcome = transfer(
            transfer_request(
                local_source(source_root.path())?,
                target,
                tokio_util::sync::CancellationToken::new(),
            )?
            .with_transfer_policy(TransferPolicy::Direct)
            .with_publication_durability(PublicationDurability::Required),
        )
        .await?;
        assert_eq!(outcome.recovery, EffectiveRecovery::Disabled);
        assert_eq!(role.final_sync_counts(), (1, 1));
        assert_eq!(role.metadata_batch_counts(), (1, 0));
        assert_eq!(std::fs::read(&target_path)?, bytes);
        assert_eq!(std::fs::read(&source_path)?, bytes);
        let metadata = std::fs::metadata(&target_path)?;
        assert_eq!(metadata.ino(), inode);
        assert_eq!(metadata.permissions().mode() & 0o7777, 0o400);
        assert_eq!(staging_entry_count(target_root.path())?, 0);
    }
    Ok(())
}

#[tokio::test]
async fn ordinary_direct_keeps_its_no_barrier_cost_even_when_sync_would_fail()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = TestRoot::new("default-direct-source")?;
    let target_root = TestRoot::new("default-direct-target")?;
    std::fs::write(source_root.path().join("final.bin"), b"payload")?;
    let (target, role) = test_destination_storage_with_role(target_root.path(), "default-direct")?;
    role.fail_direct_data_sync();
    role.fail_direct_directory_sync();
    transfer(
        transfer_request(
            local_source(source_root.path())?,
            target,
            tokio_util::sync::CancellationToken::new(),
        )?
        .with_transfer_policy(TransferPolicy::Direct),
    )
    .await?;
    assert_eq!(role.final_sync_counts(), (0, 0));
    assert_eq!(
        std::fs::read(target_root.path().join("final.bin"))?,
        b"payload"
    );
    Ok(())
}

#[tokio::test]
async fn a_failed_required_file_or_directory_barrier_never_reports_ready()
-> Result<(), Box<dyn std::error::Error>> {
    for fail_directory in [false, true] {
        let source_root = TestRoot::new("failed-direct-barrier-source")?;
        let target_root = TestRoot::new("failed-direct-barrier-target")?;
        std::fs::write(source_root.path().join("final.bin"), b"retained source")?;
        let (target, role) =
            test_destination_storage_with_role(target_root.path(), "failed-direct-barrier")?;
        if fail_directory {
            role.fail_direct_directory_sync();
        } else {
            role.fail_direct_data_sync();
        }
        let result = transfer(
            transfer_request(
                local_source(source_root.path())?,
                target,
                tokio_util::sync::CancellationToken::new(),
            )?
            .with_transfer_policy(TransferPolicy::Direct)
            .with_publication_durability(PublicationDurability::Required),
        )
        .await;
        let failure = result.err().ok_or("failed persistence was accepted")?;
        assert_eq!(failure.phase(), TransferPhase::Publish);
        assert!(failure.final_destination_changed());
        assert!(failure.source().is_some());
        assert!(
            failure.to_string().contains("persistence failure"),
            "{failure}"
        );
        assert_eq!(
            std::fs::read(source_root.path().join("final.bin"))?,
            b"retained source"
        );
        assert_eq!(
            std::fs::read(target_root.path().join("final.bin"))?,
            b"retained source"
        );
        assert_eq!(staging_entry_count(target_root.path())?, 0);
    }
    Ok(())
}

#[tokio::test]
async fn required_direct_persists_every_parent_but_creates_no_checkpoint()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = TestRoot::new("required-direct-nested-source")?;
    let target_root = TestRoot::new("required-direct-nested-target")?;
    std::fs::create_dir_all(source_root.path().join("one/two"))?;
    std::fs::write(source_root.path().join("one/two/final.bin"), b"nested")?;
    let (target, role) =
        test_destination_storage_with_role(target_root.path(), "required-direct-nested")?;
    let outcome = transfer(
        TransferRequest::new(
            local_source(source_root.path())?,
            StoragePath::new("one/two/final.bin")?,
            target,
            tokio_util::sync::CancellationToken::new(),
        )
        .with_transfer_policy(TransferPolicy::Direct)
        .with_publication_durability(PublicationDurability::Required),
    )
    .await?;
    assert_eq!(outcome.recovery, EffectiveRecovery::Disabled);
    assert_eq!(role.final_sync_counts(), (1, 3));
    assert_eq!(
        std::fs::read(target_root.path().join("one/two/final.bin"))?,
        b"nested"
    );
    for parent in ["", "one", "one/two"] {
        assert_eq!(
            std::fs::read_dir(target_root.path().join(parent))?.count(),
            1
        );
    }
    Ok(())
}

#[tokio::test]
async fn expert_nonresumable_metadata_barrier_obeys_explicit_durability()
-> Result<(), Box<dyn std::error::Error>> {
    for durability in [
        PublicationDurability::PolicyDefault,
        PublicationDurability::Required,
    ] {
        let source_root = TestRoot::new("required-expert-source")?;
        let target_root = TestRoot::new("required-expert-target")?;
        let payload = b"source payload";
        std::fs::write(source_root.path().join("final.bin"), payload)?;
        std::fs::write(target_root.path().join("final.bin"), b"retained target")?;
        let source = local_source(source_root.path())?;
        let observation = expert_observation(&source, "final.bin").await?;
        let identity_key = observation.identity_key();
        let (target, role) =
            test_destination_storage_with_role(target_root.path(), "required-expert")?;
        role.fail_metadata_sync();
        let prepared = ExpertDestinationSession::prepare(
            ExpertDestinationRequest::new(
                observation,
                1024,
                target,
                StoragePath::new("final.bin")?,
                tokio_util::sync::CancellationToken::new(),
            )
            .with_transfer_policy(TransferPolicy::AtomicReplace)
            .with_publication_durability(durability)
            .with_metadata_plan(local_xattr_plan()?),
        )
        .await?;
        assert!(!prepared.recovery_enabled());
        let written = prepared
            .write(Box::pin(futures::stream::iter([Ok(Bytes::from_static(
                payload,
            ))])))
            .await?;
        let result = written
            .complete(ExpertSourceEvidence {
                source_size: payload.len() as u64,
                blake3: *blake3::hash(payload).as_bytes(),
                identity_key,
                source_qos: SourceQosStats::default(),
            })
            .await;
        if durability == PublicationDurability::Required {
            let failure = result.err().ok_or("metadata barrier failure was ignored")?;
            assert_eq!(failure.phase(), TransferPhase::Metadata);
            assert_eq!(role.metadata_batch_counts(), (1, 1));
            assert_eq!(
                std::fs::read(target_root.path().join("final.bin"))?,
                b"retained target"
            );
            failure.discard_stage().await?;
        } else {
            result?;
            assert_eq!(role.metadata_batch_counts(), (1, 0));
            assert_eq!(
                std::fs::read(target_root.path().join("final.bin"))?,
                payload
            );
        }
        assert_eq!(
            std::fs::read(source_root.path().join("final.bin"))?,
            payload
        );
    }
    Ok(())
}
