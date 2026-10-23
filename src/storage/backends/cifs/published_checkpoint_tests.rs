use super::*;

async fn leave_published(
    destination: &CifsStagedDestination,
    backend: &BackendIdentity,
    phase: CheckpointPhase,
) -> TestResult {
    let stage = destination
        .prepare_at_destination(hash_request(backend, 6)?)
        .await?;
    arm(&stage, b"abcdef")?;
    destination.persist_hash_checkpoint(&stage).await?;
    write(destination, &stage, b"abcdef").await?;
    stage.checkpoint_set_phase(CheckpointPhase::ReadyToPublish);
    destination.persist_hash_checkpoint(&stage).await?;
    if phase == CheckpointPhase::Published {
        publish(destination, &stage, b"abcdef")
            .await
            .map_err(|failure| failure.error)?;
    } else {
        // Model the crash between the server's rename and the Published pointer update.
        destination
            .protocol
            .rename(
                &destination.stage_path(&stage)?,
                stage.final_destination.path(),
                true,
            )
            .await?;
    }
    Ok(())
}

fn verification() -> crate::storage::VerifyRequest {
    crate::storage::VerifyRequest {
        expected_size: 6,
        expected_blake3: *blake3::hash(b"abcdef").as_bytes(),
        published: None,
        cancel: tokio_util::sync::CancellationToken::new(),
    }
}

#[tokio::test]
async fn missing_stage_recovers_both_publication_phases_and_verifies_final() -> TestResult {
    for phase in [CheckpointPhase::ReadyToPublish, CheckpointPhase::Published] {
        let (protocol, backend) = share()?;
        let first = adapter(&protocol, &backend);
        leave_published(&first, &backend, phase).await?;
        drop(first);
        let second = adapter(&protocol, &backend);
        let stage = second
            .prepare_at_destination(hash_request(&backend, 6)?)
            .await?;
        assert!(stage.checkpoint_enabled());
        assert_eq!(stage.write_offset, 6);
        assert!(stage.recovered_publication.is_some());
        assert_eq!(file(&protocol, &name(ArtifactKind::Stage, false)), None);
        assert_eq!(second.observe_checkpoint(&stage).await?.durable_prefix, 6);
        second.verify(&stage, verification()).await?;
        assert_eq!(protocol.readonly_opens.load(Ordering::SeqCst), 1);
        second
            .apply_metadata(
                &stage,
                crate::storage::MetadataMutation::Mode(0o444),
                tokio_util::sync::CancellationToken::new(),
            )
            .await?;
        publish(&second, &stage, b"abcdef")
            .await
            .map_err(|failure| failure.error)?;
        assert_eq!(checkpoint(&protocol)?.phase, CheckpointPhase::Published);
        second.release_hash_checkpoint(&stage).await?;
        assert!(artifacts_left(&protocol).is_empty());
        assert_eq!(file(&protocol, "dir/final.bin"), Some(b"abcdef".to_vec()));
    }
    Ok(())
}

#[tokio::test]
async fn changed_final_fails_without_recreating_stage_or_removing_hash_evidence() -> TestResult {
    for bytes in [b"wrong!".to_vec(), b"short".to_vec()] {
        let (protocol, backend) = share()?;
        let first = adapter(&protocol, &backend);
        leave_published(&first, &backend, CheckpointPhase::ReadyToPublish).await?;
        put(&protocol, "dir/final.bin", bytes.clone());
        let second = adapter(&protocol, &backend);
        let found = second
            .prepare_at_destination(hash_request(&backend, 6)?)
            .await;
        if bytes.len() == 6 {
            let stage = found?;
            assert!(second.verify(&stage, verification()).await.is_err());
        } else {
            assert!(found.is_err());
        }
        assert_eq!(file(&protocol, &name(ArtifactKind::Stage, false)), None);
        assert!(file(&protocol, &name(ArtifactKind::Pointer, false)).is_some());
        assert_eq!(file(&protocol, "dir/final.bin"), Some(bytes));
    }
    Ok(())
}
