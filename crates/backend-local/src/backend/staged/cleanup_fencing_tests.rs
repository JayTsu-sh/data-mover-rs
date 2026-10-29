use super::*;

const PAYLOAD: &[u8] = b"abcdefghijkl";

async fn retained_stage(
    root: &Root,
    lost_publication_reply: bool,
) -> Result<(LocalStagedDestination, PreparedStage), Box<dyn std::error::Error>> {
    let adapter = adapter(root)?;
    let stage = adapter
        .prepare_at_destination(hash_request(PAYLOAD.len())?)
        .await?;
    arm(&stage, PAYLOAD)?;
    adapter.persist_hash_checkpoint(&stage).await?;
    adapter
        .write_positioned(
            &stage,
            Box::pin(stream::iter([Ok(PositionedChunk {
                offset: 0,
                data: Bytes::from_static(PAYLOAD),
            })])),
        )
        .await?;
    if lost_publication_reply {
        stage.checkpoint_set_phase(CheckpointPhase::ReadyToPublish);
        adapter.persist_hash_checkpoint(&stage).await?;
        adapter
            .write_probe
            .fail_after_publication_commit
            .store(true, Ordering::SeqCst);
        let failure = try_publish(&adapter, &stage, PAYLOAD)
            .await
            .err()
            .ok_or("reply loss")?;
        assert!(failure.final_destination_changed);
    }
    Ok((adapter, stage))
}

async fn replace_owner(
    root: &Root,
) -> Result<(LocalStagedDestination, PreparedStage, Vec<PathBuf>), Box<dyn std::error::Error>> {
    std::fs::remove_file(root.artifact(ArtifactKind::Claim))?;
    let adapter = adapter(root)?;
    let mut request = hash_request(PAYLOAD.len())?.with_resume(ResumeMode::Restart);
    request.transfer_identity = [9; 32];
    let stage = adapter.prepare_at_destination(request).await?;
    arm(&stage, PAYLOAD)?;
    adapter.persist_hash_checkpoint(&stage).await?;
    std::fs::write(root.artifact(ArtifactKind::Stage), b"new stage")?;
    let temporary = root
        .0
        .join("dir")
        .join(artifact_temporary_name("final.bin", ArtifactKind::Pointer));
    std::fs::write(&temporary, b"new pointer temporary")?;
    let mut preserved = [
        ArtifactKind::Stage,
        ArtifactKind::Pointer,
        ArtifactKind::Claim,
    ]
    .map(|kind| root.artifact(kind))
    .to_vec();
    preserved.push(temporary);
    Ok((adapter, stage, preserved))
}

#[tokio::test]
async fn stale_discard_and_failed_publication_cleanup_preserve_the_new_owner() -> TestResult {
    for lost_reply in [false, true] {
        let root = Root::new()?;
        let final_path = root.0.join("dir/final.bin");
        std::fs::write(&final_path, b"existing final")?;
        let (old_adapter, old_stage) = retained_stage(&root, lost_reply).await?;
        let final_bytes = std::fs::read(&final_path)?;
        let (new_adapter, new_stage, preserved) = replace_owner(&root).await?;
        let contents: Vec<_> = preserved
            .iter()
            .map(std::fs::read)
            .collect::<Result<_, _>>()?;
        // TransferFailure::cleanup_published_stage delegates to this same discard entry point.
        let error = old_adapter
            .discard(old_stage)
            .await
            .err()
            .ok_or("stale discard succeeded")?;
        assert_eq!(
            class(&error).map(|(class, _)| class),
            Some(FailureClass::Conflict)
        );
        for (path, expected) in preserved.iter().zip(contents) {
            assert_eq!(std::fs::read(path)?, expected);
        }
        assert_eq!(std::fs::read(&final_path)?, final_bytes);
        new_adapter.discard(new_stage).await?;
        assert!(root.artifacts_left()?.is_empty());
        assert_eq!(std::fs::read(final_path)?, final_bytes);
    }
    Ok(())
}

#[tokio::test]
async fn an_unlinked_claim_refuses_discard_before_deleting_any_artifact() -> TestResult {
    let root = Root::new()?;
    let (adapter, stage) = retained_stage(&root, true).await?;
    let pointer = std::fs::read(root.artifact(ArtifactKind::Pointer))?;
    std::fs::write(root.artifact(ArtifactKind::Stage), b"new stage name")?;
    std::fs::remove_file(root.artifact(ArtifactKind::Claim))?;
    let error = adapter
        .discard(stage)
        .await
        .err()
        .ok_or("missing claim was accepted")?;
    assert_eq!(
        class(&error).map(|(class, _)| class),
        Some(FailureClass::Conflict)
    );
    assert_eq!(
        std::fs::read(root.artifact(ArtifactKind::Pointer))?,
        pointer
    );
    assert_eq!(
        std::fs::read(root.artifact(ArtifactKind::Stage))?,
        b"new stage name"
    );
    assert_eq!(std::fs::read(root.0.join("dir/final.bin"))?, PAYLOAD);
    Ok(())
}

#[tokio::test]
async fn release_cleans_up_under_its_existing_gate_without_relocking() -> TestResult {
    let root = Root::new()?;
    let (adapter, stage) = retained_stage(&root, false).await?;
    stage.checkpoint_set_phase(CheckpointPhase::ReadyToPublish);
    adapter.persist_hash_checkpoint(&stage).await?;
    publish(&adapter, &stage, PAYLOAD).await?;
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        adapter.release_hash_checkpoint(&stage),
    )
    .await??;
    assert!(root.artifacts_left()?.is_empty());
    assert_eq!(std::fs::read(root.0.join("dir/final.bin"))?, PAYLOAD);
    Ok(())
}
