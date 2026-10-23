use super::*;

const PAYLOAD: &[u8] = b"abcdefghijkl";

async fn leave_published(root: &Root, lose_reply: bool) -> TestResult {
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
    stage.checkpoint_set_phase(CheckpointPhase::ReadyToPublish);
    adapter.persist_hash_checkpoint(&stage).await?;
    if lose_reply {
        adapter
            .write_probe
            .fail_after_publication_commit
            .store(true, Ordering::SeqCst);
        assert!(
            try_publish(&adapter, &stage, PAYLOAD)
                .await
                .err()
                .ok_or("reply loss")?
                .final_destination_changed
        );
    } else {
        publish(&adapter, &stage, PAYLOAD).await?;
    }
    Ok(())
}

fn verify_request() -> VerifyRequest {
    VerifyRequest {
        expected_size: PAYLOAD.len() as u64,
        expected_blake3: *blake3::hash(PAYLOAD).as_bytes(),
        cancel: tokio_util::sync::CancellationToken::new(),
        published: None,
    }
}

#[tokio::test]
#[cfg(unix)]
async fn ready_and_published_resume_verify_the_final_without_recreating_a_stage() -> TestResult {
    for lose_reply in [true, false] {
        let root = Root::new()?;
        leave_published(&root, lose_reply).await?;
        let record = std::fs::read(root.artifact(ArtifactKind::Pointer))?;
        let adapter = adapter(&root)?;
        let stage = adapter
            .prepare_at_destination(hash_request(PAYLOAD.len())?)
            .await?;
        assert!(stage.recovered_publication.is_some());
        assert_eq!(stage.write_offset, PAYLOAD.len() as u64);
        assert_eq!(stage.checkpoint_phase(), CheckpointPhase::Published);
        assert!(!root.artifact(ArtifactKind::Stage).exists());
        assert_eq!(std::fs::read(root.artifact(ArtifactKind::Pointer))?, record);
        adapter.verify(&stage, verify_request()).await?;
        adapter.verify(&stage, verify_request()).await?;
        assert!(
            adapter
                .write(&stage, chunks(b"replacement!"))
                .await
                .is_err()
        );
        publish(&adapter, &stage, PAYLOAD).await?;
        adapter.release_hash_checkpoint(&stage).await?;
        assert!(root.artifacts_left()?.is_empty());
        assert_eq!(std::fs::read(root.0.join("dir/final.bin"))?, PAYLOAD);
    }
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn recovered_final_metadata_is_not_reapplied() -> TestResult {
    use std::os::unix::fs::PermissionsExt as _;
    let root = Root::new()?;
    leave_published(&root, true).await?;
    let path = root.0.join("dir/final.bin");
    let mode = std::fs::metadata(&path)?.permissions().mode();
    let adapter = adapter(&root)?;
    let stage = adapter
        .prepare_at_destination(hash_request(PAYLOAD.len())?)
        .await?;
    let cancel = tokio_util::sync::CancellationToken::new();
    adapter
        .apply_metadata(
            &stage,
            crate::storage::MetadataMutation::Mode(0),
            cancel.clone(),
        )
        .await?;
    adapter
        .apply_metadata_batch(
            &stage,
            vec![crate::storage::MetadataMutation::Mode(0)],
            cancel,
        )
        .await
        .map_err(|_| "recovered metadata batch failed")?;
    assert_eq!(std::fs::metadata(&path)?.permissions().mode(), mode);
    adapter.verify(&stage, verify_request()).await?;
    publish(&adapter, &stage, PAYLOAD).await?;
    adapter.release_hash_checkpoint(&stage).await?;
    Ok(())
}

#[tokio::test]
#[cfg(unix)]
async fn same_size_wrong_final_fails_verification_and_preserves_the_pointer() -> TestResult {
    let root = Root::new()?;
    leave_published(&root, true).await?;
    std::fs::write(root.0.join("dir/final.bin"), b"xxxxxxxxxxxx")?;
    let record = std::fs::read(root.artifact(ArtifactKind::Pointer))?;
    let adapter = adapter(&root)?;
    let stage = adapter
        .prepare_at_destination(hash_request(PAYLOAD.len())?)
        .await?;
    let error = adapter
        .verify(&stage, verify_request())
        .await
        .err()
        .ok_or("bad final accepted")?;
    assert_eq!(
        class(&error).map(|(class, _)| class),
        Some(FailureClass::Corruption)
    );
    assert_eq!(std::fs::read(root.artifact(ArtifactKind::Pointer))?, record);
    assert!(!root.artifact(ArtifactKind::Stage).exists());
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn replacement_of_a_pinned_final_is_refused() -> TestResult {
    let root = Root::new()?;
    leave_published(&root, false).await?;
    let adapter = adapter(&root)?;
    let stage = adapter
        .prepare_at_destination(hash_request(PAYLOAD.len())?)
        .await?;
    let path = root.0.join("dir/final.bin");
    std::fs::rename(&path, root.0.join("dir/old.bin"))?;
    std::fs::write(&path, PAYLOAD)?;
    assert!(adapter.verify(&stage, verify_request()).await.is_err());
    assert!(try_publish(&adapter, &stage, PAYLOAD).await.is_err());
    assert!(adapter.release_hash_checkpoint(&stage).await.is_err());
    assert!(root.artifact(ArtifactKind::Pointer).exists());
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn a_symlink_cannot_be_adopted_as_a_published_final() -> TestResult {
    let root = Root::new()?;
    leave_published(&root, true).await?;
    let path = root.0.join("dir/final.bin");
    let other = root.0.join("outside.bin");
    std::fs::rename(&path, &other)?;
    std::os::unix::fs::symlink(&other, &path)?;
    let adapter = adapter(&root)?;
    assert!(
        adapter
            .prepare_at_destination(hash_request(PAYLOAD.len())?)
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(other)?, PAYLOAD);
    assert!(root.artifact(ArtifactKind::Pointer).exists());
    assert!(!root.artifact(ArtifactKind::Stage).exists());
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn a_replaced_parent_cannot_validate_through_the_cached_directory() -> TestResult {
    let root = Root::new()?;
    leave_published(&root, false).await?;
    let adapter = adapter(&root)?;
    let stage = adapter
        .prepare_at_destination(hash_request(PAYLOAD.len())?)
        .await?;
    std::fs::rename(root.0.join("dir"), root.0.join("old-dir"))?;
    std::fs::create_dir(root.0.join("dir"))?;
    std::fs::write(root.0.join("dir/final.bin"), PAYLOAD)?;
    assert!(adapter.verify(&stage, verify_request()).await.is_err());
    assert!(try_publish(&adapter, &stage, PAYLOAD).await.is_err());
    assert!(adapter.release_hash_checkpoint(&stage).await.is_err());
    assert!(
        root.0
            .join("old-dir")
            .join(artifact_name("final.bin", ArtifactKind::Pointer))
            .exists()
    );
    Ok(())
}
