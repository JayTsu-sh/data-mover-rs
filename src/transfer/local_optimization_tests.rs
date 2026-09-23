use super::*;
use crate::transfer::{EffectiveRecovery, ReadBackVerification};

#[tokio::test]
async fn optional_read_back_skips_verification_and_reports_no_digest()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = TestRoot::new("optional-verify-source")?;
    let destination_root = TestRoot::new("optional-verify-destination")?;
    let payload = vec![0x31; 4096];
    std::fs::write(source_root.path().join("source.bin"), &payload)?;
    let (destination, role) =
        test_destination_storage_with_role(destination_root.path(), "optional-verify")?;
    // This probe corrupts and fails only if destination verification is actually called.
    role.corrupt_before_verify();
    let outcome = transfer(
        transfer_request(
            local_source(source_root.path())?,
            destination,
            tokio_util::sync::CancellationToken::new(),
        )?
        .with_read_back_verification(ReadBackVerification::Disabled),
    )
    .await?;
    assert_eq!(outcome.blake3, None);
    assert_eq!(outcome.read_back, ReadBackVerification::Disabled);
    assert_eq!(
        std::fs::read(destination_root.path().join("final.bin"))?,
        payload
    );
    assert_eq!(staging_entry_count(destination_root.path())?, 0);
    Ok(())
}
#[tokio::test]
async fn automatic_small_multichunk_uses_inflight_without_checkpoints()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = TestRoot::new("auto-inflight-source")?;
    let destination_root = TestRoot::new("auto-inflight-destination")?;
    let payload = vec![0x47; 4 * 1024 * 1024];
    std::fs::write(source_root.path().join("source.bin"), &payload)?;
    let (source, source_role) = test_source_storage(source_root.path(), "auto-inflight-source")?;
    source_role.delay_reads(Duration::from_millis(1));
    let (destination, role) =
        test_destination_storage_with_role(destination_root.path(), "auto-inflight-destination")?;
    role.fail_checkpoint_at(1);
    let outcome = transfer(
        transfer_request(
            source,
            destination,
            tokio_util::sync::CancellationToken::new(),
        )?
        .with_transfer_policy(TransferPolicy::Checkpointed),
    )
    .await?;
    assert_eq!(
        outcome.recovery,
        EffectiveRecovery::SkippedBelowCheckpointThreshold
    );
    assert!(source_role.peak_read_concurrency() >= 2);
    assert_eq!(source_role.read_stream_count(), 1);
    assert_eq!(
        std::fs::read(destination_root.path().join("final.bin"))?,
        payload
    );
    assert_eq!(staging_entry_count(destination_root.path())?, 0);
    Ok(())
}

#[tokio::test]
async fn automatic_first_checkpoint_is_deferred_until_data_is_written()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = TestRoot::new("auto-deferred-source")?;
    let destination_root = TestRoot::new("auto-deferred-destination")?;
    std::fs::write(
        source_root.path().join("source.bin"),
        vec![0x51; 512 * 1024],
    )?;
    let (source, source_role) = test_source_storage(source_root.path(), "auto-deferred-source")?;
    let (destination, role) =
        test_destination_storage_with_role(destination_root.path(), "auto-deferred-destination")?;
    role.set_automatic_checkpoint_interval(128 * 1024);
    role.fail_checkpoint_at(1);
    let error = transfer(
        transfer_request(
            source,
            destination,
            tokio_util::sync::CancellationToken::new(),
        )?
        .with_transfer_policy(TransferPolicy::Checkpointed),
    )
    .await
    .err()
    .ok_or("checkpoint failure must surface")?;
    assert_eq!(error.phase(), TransferPhase::Transfer);
    assert!(source_role.read_call_count() >= 2);
    assert!(role.write_completion_count() >= 2);
    assert!(!error.has_recoverable_stage());
    assert!(!destination_root.path().join("final.bin").exists());
    error.discard_stage().await?;
    assert_eq!(staging_entry_count(destination_root.path())?, 0);
    Ok(())
}

#[tokio::test]
async fn automatic_checkpoint_can_resume_without_rewriting_saved_bytes()
-> Result<(), Box<dyn std::error::Error>> {
    assert_automatic_resume(ReadBackVerification::Enabled).await?;
    assert_automatic_resume(ReadBackVerification::Disabled).await
}

async fn assert_automatic_resume(
    read_back: ReadBackVerification,
) -> Result<(), Box<dyn std::error::Error>> {
    let source_root = TestRoot::new("auto-resume-source")?;
    let destination_root = TestRoot::new("auto-resume-destination")?;
    let payload = vec![0x61; 512 * 1024];
    std::fs::write(source_root.path().join("source.bin"), &payload)?;
    std::fs::write(destination_root.path().join("final.bin"), b"original")?;
    let (destination, role) =
        test_destination_storage_with_role(destination_root.path(), "auto-resume-destination")?;
    role.set_automatic_checkpoint_interval(128 * 1024);
    let request = transfer_request(
        local_source(source_root.path())?,
        destination,
        tokio_util::sync::CancellationToken::new(),
    )?
    .with_transfer_policy(TransferPolicy::Checkpointed)
    .with_read_back_verification(read_back);
    let staged = run_until_transferred(request.clone()).await?;
    assert_eq!(
        std::fs::read(destination_root.path().join("final.bin"))?,
        b"original"
    );
    let written = role.write_completion_count();
    drop(staged); // simulate process exit after durable transfer and before publication
    let outcome = transfer(
        request.with_source_qos(SourceQosGroup::new(SourceQosPolicy::new(
            None,
            64 * 1024,
            None,
        )?)),
    )
    .await?;
    if read_back == ReadBackVerification::Disabled {
        assert_eq!(outcome.source_qos.source_read_operations, 0);
        assert_eq!(outcome.blake3, None);
    }
    assert_eq!(outcome.recovery, EffectiveRecovery::Checkpointed);
    assert_eq!(role.write_completion_count(), written);
    assert_eq!(
        std::fs::read(destination_root.path().join("final.bin"))?,
        payload
    );
    assert_eq!(staging_entry_count(destination_root.path())?, 0);
    Ok(())
}

#[tokio::test]
async fn losing_deferred_registration_does_not_delete_the_winners_record()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = TestRoot::new("auto-competing-source")?;
    let destination_root = TestRoot::new("auto-competing-destination")?;
    let payload = vec![0x71; 512 * 1024];
    std::fs::write(source_root.path().join("source.bin"), &payload)?;
    std::fs::write(destination_root.path().join("final.bin"), b"old")?;
    let (source_a, role_a) = test_source_storage(source_root.path(), "auto-competing-source")?;
    let (source_b, role_b) = test_source_storage(source_root.path(), "auto-competing-source")?;
    let gate_a = role_a.gate_read_at(0);
    let gate_b = role_b.gate_read_at(0);
    let (destination, role) =
        test_destination_storage_with_role(destination_root.path(), "auto-competing-destination")?;
    role.set_automatic_checkpoint_interval(128 * 1024);
    let request_a = transfer_request(
        source_a,
        destination.clone(),
        tokio_util::sync::CancellationToken::new(),
    )?
    .with_transfer_policy(TransferPolicy::Checkpointed);
    let request_b = transfer_request(
        source_b,
        destination,
        tokio_util::sync::CancellationToken::new(),
    )?
    .with_transfer_policy(TransferPolicy::Checkpointed);
    let first = tokio::spawn(run_until_transferred(request_a.clone()));
    let second = tokio::spawn(transfer(request_b));
    tokio::time::timeout(Duration::from_secs(5), gate_a.wait_started()).await?;
    tokio::time::timeout(Duration::from_secs(5), gate_b.wait_started()).await?;
    gate_a.release();
    let winner = tokio::time::timeout(Duration::from_secs(5), first).await???;
    gate_b.release();
    let loser = tokio::time::timeout(Duration::from_secs(5), second)
        .await??
        .err()
        .ok_or("registration lease must conflict")?;
    assert_eq!(loser.phase(), TransferPhase::Transfer);
    loser.discard_stage().await?;
    drop(winner);
    let writes_before = role.write_completion_count();
    gate_a.release();
    let outcome = tokio::time::timeout(Duration::from_secs(5), transfer(request_a)).await??;
    assert_eq!(outcome.recovery, EffectiveRecovery::Checkpointed);
    assert_eq!(role.write_completion_count(), writes_before);
    assert_eq!(
        std::fs::read(destination_root.path().join("final.bin"))?,
        payload
    );
    assert_eq!(staging_entry_count(destination_root.path())?, 0);
    Ok(())
}

#[tokio::test]
async fn deferred_recovery_requires_more_than_one_interval()
-> Result<(), Box<dyn std::error::Error>> {
    for policy in [TransferPolicy::Checkpointed] {
        for size in [128 * 1024 - 1, 128 * 1024, 128 * 1024 + 1] {
            let source_root = TestRoot::new("threshold-source")?;
            let destination_root = TestRoot::new("threshold-destination")?;
            let payload = vec![0x81; size];
            std::fs::write(source_root.path().join("source.bin"), &payload)?;
            let (destination, role) = test_destination_storage_with_role(
                destination_root.path(),
                "threshold-destination",
            )?;
            role.set_automatic_checkpoint_interval(128 * 1024);
            role.fail_checkpoint_at(1);
            let result = transfer(
                transfer_request(
                    local_source(source_root.path())?,
                    destination,
                    tokio_util::sync::CancellationToken::new(),
                )?
                .with_transfer_policy(policy),
            )
            .await;
            if size <= 128 * 1024 {
                assert_eq!(
                    result?.recovery,
                    EffectiveRecovery::SkippedBelowCheckpointThreshold
                );
                assert_eq!(
                    std::fs::read(destination_root.path().join("final.bin"))?,
                    payload
                );
            } else {
                let failure = result
                    .err()
                    .ok_or("one byte above the interval must enable checkpointing")?;
                assert_eq!(failure.phase(), TransferPhase::Transfer);
                failure.discard_stage().await?;
            }
            assert_eq!(staging_entry_count(destination_root.path())?, 0);
        }
    }
    Ok(())
}

#[tokio::test]
async fn deferred_recovery_skips_checkpoint_at_end_of_file()
-> Result<(), Box<dyn std::error::Error>> {
    for policy in [TransferPolicy::Checkpointed] {
        // The first interval is crossed by the final 64 KiB source chunk.
        // The second case saves progress at 128 KiB and updates the existing record
        // after final synchronization, without an extra periodic checkpoint at EOF.
        for (size, interval, failed_checkpoint) in
            [(128 * 1024, 96 * 1024, 1), (256 * 1024, 128 * 1024, 3)]
        {
            let source_root = TestRoot::new("eof-checkpoint-source")?;
            let destination_root = TestRoot::new("eof-checkpoint-destination")?;
            let payload = vec![0x82; size];
            std::fs::write(source_root.path().join("source.bin"), &payload)?;
            let (destination, role) =
                test_destination_storage_with_role(destination_root.path(), "eof-checkpoint")?;
            role.set_automatic_checkpoint_interval(interval);
            role.fail_checkpoint_at(failed_checkpoint);
            transfer(
                transfer_request(
                    local_source(source_root.path())?,
                    destination,
                    tokio_util::sync::CancellationToken::new(),
                )?
                .with_transfer_policy(policy),
            )
            .await?;
            assert_eq!(
                std::fs::read(destination_root.path().join("final.bin"))?,
                payload
            );
            assert_eq!(staging_entry_count(destination_root.path())?, 0);
        }
    }
    Ok(())
}

#[tokio::test]
async fn automatic_empty_file_skips_checkpoints_but_is_published()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = TestRoot::new("checkpointed-empty-source")?;
    let destination_root = TestRoot::new("checkpointed-empty-destination")?;
    std::fs::write(source_root.path().join("source.bin"), [])?;
    let destination = test_destination_storage(destination_root.path(), "checkpointed-empty")?;
    let outcome = transfer(
        transfer_request(
            local_source(source_root.path())?,
            destination,
            tokio_util::sync::CancellationToken::new(),
        )?
        .with_transfer_policy(TransferPolicy::Checkpointed)
        .with_read_back_verification(ReadBackVerification::Disabled),
    )
    .await?;
    assert_eq!(
        outcome.recovery,
        EffectiveRecovery::SkippedSingleSourceChunk
    );
    assert_eq!(
        std::fs::metadata(destination_root.path().join("final.bin"))?.len(),
        0
    );
    assert_eq!(staging_entry_count(destination_root.path())?, 0);
    Ok(())
}

#[tokio::test]
async fn atomic_replace_skips_final_syncs_while_checkpointed_retains_them()
-> Result<(), Box<dyn std::error::Error>> {
    for policy in [TransferPolicy::Checkpointed, TransferPolicy::AtomicReplace] {
        for size in [4096, 4 * 1024 * 1024] {
            let source_root = TestRoot::new("policy-sync-source")?;
            let destination_root = TestRoot::new("policy-sync-destination")?;
            let payload = vec![0x53; size];
            std::fs::write(source_root.path().join("source.bin"), &payload)?;
            let (destination, role) =
                test_destination_storage_with_role(destination_root.path(), "policy-sync")?;
            if policy == TransferPolicy::AtomicReplace {
                // Even an eligible multi-chunk AtomicReplace copy must never create a checkpoint.
                role.set_automatic_checkpoint_interval(64 * 1024);
            }
            role.fail_checkpoint_at(1);
            let outcome = transfer(
                transfer_request(
                    local_source(source_root.path())?,
                    destination,
                    tokio_util::sync::CancellationToken::new(),
                )?
                .with_transfer_policy(policy),
            )
            .await?;
            assert_eq!(
                role.final_sync_counts(),
                if policy == TransferPolicy::AtomicReplace {
                    (0, 0)
                } else {
                    (1, 1)
                }
            );
            if policy == TransferPolicy::AtomicReplace {
                assert_eq!(outcome.recovery, EffectiveRecovery::Disabled);
            }
            assert_eq!(
                std::fs::read(destination_root.path().join("final.bin"))?,
                payload
            );
            assert_eq!(staging_entry_count(destination_root.path())?, 0);
        }
    }
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn checkpointed_and_atomic_replace_preserve_storage_enum_file_metadata()
-> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    use crate::metadata::{ApplicationOutcome, MetadataFamily};

    const SOURCE_MODE: u32 = 0o640;
    const SOURCE_ATIME: i64 = 1_700_000_011;
    const SOURCE_MTIME: i64 = 1_700_000_023;

    for policy in [TransferPolicy::Checkpointed, TransferPolicy::AtomicReplace] {
        let source_root = TestRoot::new("metadata-source")?;
        let destination_root = TestRoot::new("metadata-destination")?;
        let source_path = source_root.path().join("source.bin");
        std::fs::write(&source_path, vec![0x6d; 4096])?;
        std::fs::set_permissions(&source_path, std::fs::Permissions::from_mode(SOURCE_MODE))?;
        filetime::set_file_times(
            &source_path,
            filetime::FileTime::from_unix_time(SOURCE_ATIME, 0),
            filetime::FileTime::from_unix_time(SOURCE_MTIME, 0),
        )?;
        let source_metadata = std::fs::metadata(&source_path)?;

        let (destination, role) =
            test_destination_storage_with_role(destination_root.path(), "metadata-copy")?;
        let outcome = transfer(
            transfer_request(
                local_source(source_root.path())?,
                destination,
                tokio_util::sync::CancellationToken::new(),
            )?
            .with_transfer_policy(policy),
        )
        .await?;
        assert_eq!(
            role.metadata_batch_counts(),
            if policy == TransferPolicy::Checkpointed {
                (1, 1)
            } else {
                (1, 0)
            }
        );

        let published = std::fs::metadata(destination_root.path().join("final.bin"))?;
        assert_eq!(published.uid(), source_metadata.uid());
        assert_eq!(published.gid(), source_metadata.gid());
        assert_eq!(published.permissions().mode() & 0o7777, SOURCE_MODE);
        assert_eq!(
            published
                .modified()?
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs(),
            SOURCE_MTIME as u64
        );
        let report = outcome
            .metadata
            .as_ref()
            .ok_or("Local-to-Local transfer omitted copied metadata")?;
        for family in [MetadataFamily::OwnershipMode, MetadataFamily::Timestamps] {
            assert!(report.outcomes().iter().any(|application| {
                application.family == family && application.outcome == ApplicationOutcome::Applied
            }));
        }

        let published_path = destination_root.path().join("final.bin");
        std::fs::set_permissions(&published_path, std::fs::Permissions::from_mode(0o600))?;
        filetime::set_file_mtime(
            &published_path,
            filetime::FileTime::from_unix_time(SOURCE_MTIME + 100, 0),
        )?;
        let equivalent = transfer(
            transfer_request(
                local_source(source_root.path())?,
                local_destination(destination_root.path())?,
                tokio_util::sync::CancellationToken::new(),
            )?
            .with_transfer_policy(policy),
        )
        .await?;
        assert_eq!(
            equivalent.disposition,
            crate::storage::PublicationDisposition::Published
        );
        let published = std::fs::metadata(published_path)?;
        assert_eq!(published.uid(), source_metadata.uid());
        assert_eq!(published.gid(), source_metadata.gid());
        assert_eq!(published.permissions().mode() & 0o7777, SOURCE_MODE);
        assert_eq!(
            published
                .modified()?
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs(),
            SOURCE_MTIME as u64
        );
    }
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn direct_preserves_inode_and_metadata_without_stage_or_sync()
-> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
    for size in [0, 4096, 4 * 1024 * 1024] {
        for read_back in [
            ReadBackVerification::Enabled,
            ReadBackVerification::Disabled,
        ] {
            let source_root = TestRoot::new("direct-source")?;
            let destination_root = TestRoot::new("direct-target")?;
            let source_path = source_root.path().join("source.bin");
            let target_path = destination_root.path().join("final.bin");
            let payload = vec![0x3a; size];
            std::fs::write(&source_path, &payload)?;
            std::fs::set_permissions(&source_path, std::fs::Permissions::from_mode(0o640))?;
            filetime::set_file_mtime(
                &source_path,
                filetime::FileTime::from_unix_time(1_700_000_000, 123_456),
            )?;
            std::fs::write(&target_path, vec![0xff; size + 32])?;
            std::fs::hard_link(&target_path, destination_root.path().join("alias.bin"))?;
            let old_inode = std::fs::metadata(&target_path)?.ino();
            let (source, reader) = test_source_storage(source_root.path(), "direct-source")?;
            reader.delay_reads(Duration::from_millis(1));
            let (destination, writer) =
                test_destination_storage_with_role(destination_root.path(), "direct-target")?;
            writer.set_automatic_checkpoint_interval(64 * 1024);
            writer.fail_checkpoint_at(1);
            let outcome = transfer(
                transfer_request(
                    source,
                    destination,
                    tokio_util::sync::CancellationToken::new(),
                )?
                .with_transfer_policy(TransferPolicy::Direct)
                .with_read_back_verification(read_back),
            )
            .await?;
            assert_eq!(outcome.recovery, EffectiveRecovery::Disabled);
            assert_eq!(std::fs::read(&target_path)?, payload);
            assert_eq!(
                std::fs::read(destination_root.path().join("alias.bin"))?,
                payload
            );
            let actual = std::fs::metadata(&target_path)?;
            let expected = std::fs::metadata(&source_path)?;
            assert_eq!(actual.ino(), old_inode);
            assert_eq!(
                (
                    actual.uid(),
                    actual.gid(),
                    actual.mode(),
                    actual.mtime(),
                    actual.mtime_nsec()
                ),
                (
                    expected.uid(),
                    expected.gid(),
                    expected.mode(),
                    expected.mtime(),
                    expected.mtime_nsec()
                )
            );
            assert_eq!(writer.final_sync_counts(), (0, 0));
            assert_eq!(writer.metadata_batch_counts(), (1, 0));
            assert_eq!(std::fs::read_dir(destination_root.path())?.count(), 2);
            if size > 64 * 1024 {
                assert!(reader.peak_read_concurrency() >= 2);
            }
        }
    }
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn direct_rejects_source_aliases_and_destination_symlinks_before_truncation()
-> Result<(), Box<dyn std::error::Error>> {
    let root = TestRoot::new("direct-alias")?;
    let source_path = root.path().join("source.bin");
    let target_path = root.path().join("final.bin");
    std::fs::write(&source_path, b"keep source intact")?;
    for symlink in [false, true] {
        if symlink {
            std::os::unix::fs::symlink("source.bin", &target_path)?;
        } else {
            std::fs::hard_link(&source_path, &target_path)?;
        }
        let error = transfer(
            transfer_request(
                local_source(root.path())?,
                test_destination_storage(root.path(), "direct-alias-target")?,
                tokio_util::sync::CancellationToken::new(),
            )?
            .with_transfer_policy(TransferPolicy::Direct),
        )
        .await
        .err()
        .ok_or("alias accepted")?;
        assert!(!error.has_unpublished_stage());
        assert_eq!(std::fs::read(&source_path)?, b"keep source intact");
        std::fs::remove_file(&target_path)?;
    }
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn direct_verification_failure_retains_visible_target_without_cleanup_authority()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = TestRoot::new("direct-verify-source")?;
    let target_root = TestRoot::new("direct-verify-target")?;
    std::fs::write(source_root.path().join("source.bin"), vec![0x31; 4096])?;
    let (destination, writer) =
        test_destination_storage_with_role(target_root.path(), "direct-verify")?;
    writer.corrupt_before_verify();
    let error = transfer(
        transfer_request(
            local_source(source_root.path())?,
            destination,
            tokio_util::sync::CancellationToken::new(),
        )?
        .with_transfer_policy(TransferPolicy::Direct),
    )
    .await
    .err()
    .ok_or("verification should fail")?;
    assert!(error.final_destination_changed());
    assert!(!error.has_unpublished_stage());
    assert!(!error.has_recoverable_stage());
    assert!(!error.has_pending_cleanup());
    assert!(error.discard_stage().await.is_err());
    assert!(target_root.path().join("final.bin").is_file());
    assert_eq!(std::fs::read_dir(target_root.path())?.count(), 1);
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn direct_cancellation_returns_after_writes_settle_and_retains_target()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = TestRoot::new("direct-cancel-source")?;
    let target_root = TestRoot::new("direct-cancel-target")?;
    std::fs::write(
        source_root.path().join("source.bin"),
        vec![0x55; 4 * 1024 * 1024],
    )?;
    let (source, reader) = test_source_storage(source_root.path(), "direct-cancel")?;
    reader.delay_reads(Duration::from_millis(10));
    let cancel = tokio_util::sync::CancellationToken::new();
    let request = transfer_request(
        source,
        test_destination_storage(target_root.path(), "direct-cancel-target")?,
        cancel.clone(),
    )?
    .with_transfer_policy(TransferPolicy::Direct);
    let task = tokio::spawn(transfer(request));
    wait_for_read(&reader).await?;
    cancel.cancel();
    let error = task.await?.err().ok_or("cancelled direct copy succeeded")?;
    assert!(error.final_destination_changed());
    assert!(!error.has_unpublished_stage());
    let before = std::fs::read(target_root.path().join("final.bin"))?;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(std::fs::read(target_root.path().join("final.bin"))?, before);
    assert_eq!(std::fs::read_dir(target_root.path())?.count(), 1);
    Ok(())
}

/// A failed persistence barrier after every staged mutation applied belongs to no one mutation.
/// Local used to report it against the last one, so a `BestEffort` family there absorbed it and
/// the file was published with metadata that was never made durable, its report saying `Applied`.
/// An expert caller reaches this with any plan whose last family is `BestEffort`.
#[cfg(unix)]
#[tokio::test]
async fn a_failed_metadata_barrier_is_not_absorbed_by_a_best_effort_family()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = TestRoot::new("metadata-barrier-source")?;
    let destination_root = TestRoot::new("metadata-barrier-destination")?;
    std::fs::write(source_root.path().join("source.bin"), b"metadata payload")?;
    let source = local_source(source_root.path())?;
    let (destination, role) =
        test_destination_storage_with_role(destination_root.path(), "metadata-barrier")?;
    role.fail_metadata_sync();
    let observation = expert_observation(&source, "source.bin").await?;
    let observations = MetadataObservations::new(
        MetadataObservation::NotRequested,
        observed(vec![ExtendedAttribute::new(
            b"user.data-mover-barrier".to_vec(),
            b"not-durable".to_vec(),
        )?]),
        MetadataObservation::NotRequested,
        MetadataObservation::NotRequested,
        MetadataObservation::NotRequested,
    )?;
    let plan = compile_metadata_plan(&MetadataPlanRequest {
        observations: &observations,
        target: local_metadata_target(),
        policies: MetadataPolicies::default().with_xattrs(MetadataPolicy::BestEffort),
        principal_mapper: None,
    })?;

    let Err(error) = complete_expert_transfer(
        "metadata-barrier",
        source,
        destination,
        observation,
        plan,
        "must-not-publish.bin",
        // Checkpointed stages are durable, so their metadata batch ends in a barrier.
        TransferPolicy::Checkpointed,
    )
    .await
    else {
        return Err("a failed metadata barrier was tolerated and the file published".into());
    };

    assert_eq!(error.phase(), TransferPhase::Metadata);
    assert_eq!(role.metadata_batch_counts(), (1, 1));
    assert!(!error.final_destination_changed());
    assert!(
        !destination_root
            .path()
            .join("must-not-publish.bin")
            .exists()
    );
    error.discard_stage().await?;
    Ok(())
}

/// Ownership that has to be applied, then an xattr the filesystem refuses at run time (an unknown
/// namespace) that the caller only asked for on a best-effort basis — the last mutation.
#[cfg(unix)]
fn ownership_then_refused_xattr_plan(
    source: &Path,
) -> Result<crate::metadata::MetadataPlan, Box<dyn std::error::Error>> {
    use std::os::unix::fs::MetadataExt as _;

    let metadata = std::fs::metadata(source)?;
    let observations = MetadataObservations::new(
        MetadataObservation::NotRequested,
        observed(vec![ExtendedAttribute::new(
            b"bogus.data-mover-refused".to_vec(),
            b"refused".to_vec(),
        )?]),
        MetadataObservation::NotRequested,
        observed(crate::model::OwnershipMode {
            uid: metadata.uid(),
            gid: metadata.gid(),
            mode: 0o640,
        }),
        MetadataObservation::NotRequested,
    )?;
    Ok(compile_metadata_plan(&MetadataPlanRequest {
        observations: &observations,
        target: local_metadata_target(),
        policies: MetadataPolicies::default()
            .with_ownership_mode(MetadataPolicy::RequireExact)
            .with_xattrs(MetadataPolicy::BestEffort),
        principal_mapper: None,
    })?)
}

/// A refusal on the last mutation ends the batch with nothing left to resend, so the barrier has
/// to run before the refusal is reported — otherwise the ownership applied ahead of it is
/// published without ever being made durable. A barrier that runs can fail, so injecting that
/// failure is how this observes that it ran.
#[cfg(unix)]
#[tokio::test]
async fn a_tolerated_refusal_on_the_last_mutation_still_makes_the_rest_durable()
-> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::fs::PermissionsExt as _;

    use crate::metadata::MetadataFamily;

    for fail_barrier in [false, true] {
        let source_root = TestRoot::new("metadata-last-refusal-source")?;
        let destination_root = TestRoot::new("metadata-last-refusal-destination")?;
        let source_path = source_root.path().join("source.bin");
        std::fs::write(&source_path, b"metadata payload")?;
        let source = local_source(source_root.path())?;
        let (destination, role) =
            test_destination_storage_with_role(destination_root.path(), "metadata-last-refusal")?;
        if fail_barrier {
            role.fail_metadata_sync();
        }
        let observation = expert_observation(&source, "source.bin").await?;
        let result = complete_expert_transfer(
            "metadata-last-refusal",
            source,
            destination,
            observation,
            ownership_then_refused_xattr_plan(&source_path)?,
            "final.bin",
            TransferPolicy::Checkpointed,
        )
        .await;
        let published = destination_root.path().join("final.bin");
        if fail_barrier {
            let Err(error) = result else {
                return Err("the barrier after a tolerated refusal never ran".into());
            };
            assert_eq!(error.phase(), TransferPhase::Metadata);
            assert!(!published.exists());
            error.discard_stage().await?;
            continue;
        }
        let report = result?
            .metadata
            .ok_or("successful metadata transfer omitted its application report")?;
        let outcome = |family| {
            report
                .outcomes()
                .iter()
                .find(|item| item.family == family)
                .map(|item| item.outcome)
        };
        assert_eq!(
            outcome(MetadataFamily::Xattrs),
            Some(ApplicationOutcome::Failed)
        );
        assert_eq!(
            outcome(MetadataFamily::OwnershipMode),
            Some(ApplicationOutcome::Applied)
        );
        assert_eq!(
            std::fs::metadata(&published)?.permissions().mode() & 0o7777,
            0o640
        );
    }
    Ok(())
}
