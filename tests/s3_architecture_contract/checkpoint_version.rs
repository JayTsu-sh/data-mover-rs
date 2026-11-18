//! 真实 S3 checkpoint 的观察版本与创建回执分离；父进程独立核验对象和源根产物。
use std::{env, time::Duration};

use bytes::Bytes;
use futures::stream;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};
use tokio_util::sync::CancellationToken;
use tracing::Level;

use super::{TestResult, connected};
use data_mover::model::{
    BackendIdentity, BackendKind, EntryKind, FailureClass, IdentityStrength, SourceIdentity,
    StoragePath,
};
use data_mover::storage::checkpoint::CheckpointPhase;
use data_mover::storage::range_hash::RangeHash;
use data_mover::storage::{
    DestinationPrepareRequest, FinalDestination, PositionedChunk, PrepareRequest, PreparedStage,
    PublishRequest, ResumeMode, SourceDescriptor, StagedDestination, StorageRoleFailure,
    VerifyRequest,
};

const SIZE: usize = 9 * 1024 * 1024;

async fn control(action: &str) -> TestResult {
    let address = env::var("LAB_CHECKPOINT_VERSION_CONTROL")?;
    timeout(Duration::from_secs(10), async {
        let mut socket = TcpStream::connect(&address).await?;
        socket
            .write_all(format!("GET /{action} HTTP/1.0\r\nHost: owned\r\n\r\n").as_bytes())
            .await?;
        let mut response = Vec::new();
        socket.read_to_end(&mut response).await?;
        assert!(
            response.starts_with(b"HTTP/1.0 204"),
            "owned control failed"
        );
        Ok::<_, Box<dyn std::error::Error>>(())
    })
    .await??;
    Ok(())
}

fn request() -> TestResult<DestinationPrepareRequest> {
    Ok(DestinationPrepareRequest::new(
        PrepareRequest {
            final_destination: FinalDestination::new(StoragePath::new("payload")?),
            source: SourceDescriptor::new(
                StoragePath::new("generated-source")?,
                EntryKind::File,
                Some(SIZE as u64),
                SourceIdentity::new(
                    BackendIdentity::new(BackendKind::Local, "owned-checkpoint-source")?,
                    IdentityStrength::PathScoped,
                    b"source-v1",
                )?,
            ),
            recovery_binding: [7; 32],
        },
        [4; 32],
    )
    .with_checkpoint_hash(true)
    .with_resume(ResumeMode::Discover))
}

fn publish_request(payload: &Bytes) -> PublishRequest {
    PublishRequest {
        expected_size: SIZE as u64,
        expected_blake3: Some(*blake3::hash(payload).as_bytes()),
        cancel: CancellationToken::new(),
    }
}

async fn uploaded(
    destination: &dyn StagedDestination,
    payload: &Bytes,
) -> TestResult<PreparedStage> {
    let stage = destination.prepare_at_destination(request()?).await?;
    let mut hash = RangeHash::new(SIZE as u64);
    hash.observe(0, payload)?;
    stage.enable_checkpoint(SIZE as u64, hash.encode()?.into());
    destination.persist_hash_checkpoint(&stage).await?;
    destination
        .write_checkpointed(
            &stage,
            Box::pin(stream::iter([Ok(PositionedChunk {
                offset: 0,
                data: payload.clone(),
            })])),
        )
        .await?;
    stage.checkpoint_set_phase(CheckpointPhase::ReadyToPublish);
    destination.persist_hash_checkpoint(&stage).await?;
    Ok(stage)
}

#[tokio::test]
#[ignore = "requires run-checkpoint-version-local.py isolated S3 and owned proxy"]
async fn created_version_evidence_survives_only_when_saved_in_the_checkpoint() -> TestResult {
    let _ = tracing_subscriber::fmt()
        .with_max_level(Level::WARN)
        .with_ansi(false)
        .with_test_writer()
        .try_init();
    let mode = env::var("LAB_CHECKPOINT_VERSION_MODE")?;
    let url = env::var("LAB_CHECKPOINT_VERSION_URL")?;
    let payload = Bytes::from(vec![0x5a; SIZE]);
    let storage = connected(&url).await?;
    let destination = storage.staged_destination()?;
    let stage = uploaded(&*destination, &payload).await?;
    control("arm").await?;
    let result = destination.publish(&stage, publish_request(&payload)).await;
    let original = if mode == "pointer_refused" {
        let failure = result
            .err()
            .ok_or("published pointer refusal was not reported")?;
        assert!(
            matches!(failure.error, StorageRoleFailure::Entry(ref cause) if cause.class() == FailureClass::PermissionDenied)
        );
        assert!(failure.final_destination_changed);
        None
    } else {
        let proof = result.map_err(|failure| failure.error)?;
        assert_eq!(proof.version.is_some(), mode.starts_with("ack_"));
        proof.version
    };
    drop(stage);
    drop(destination);
    drop(storage);
    control("recover").await?;
    if mode.ends_with("_replaced") {
        control("replace").await?;
    }
    let storage = connected(&url).await?;
    let destination = storage.staged_destination()?;
    let resumed = destination.prepare_at_destination(request()?).await;
    if mode.ends_with("_replaced") {
        let failure = resumed
            .err()
            .ok_or("identical-body replacement was accepted")?;
        assert!(
            matches!(failure, StorageRoleFailure::Entry(ref cause) if cause.class() == FailureClass::Conflict),
            "{failure}"
        );
    } else {
        let resumed = resumed?;
        let proof = resumed
            .recovered_publication()
            .as_ref()
            .ok_or("published state was not recovered")?;
        assert_eq!(proof.version, original);
        let proof = destination
            .publish(&resumed, publish_request(&payload))
            .await
            .map_err(|failure| failure.error)?;
        destination
            .verify(
                &resumed,
                VerifyRequest {
                    expected_size: SIZE as u64,
                    expected_blake3: *blake3::hash(&payload).as_bytes(),
                    cancel: CancellationToken::new(),
                    published: Some(proof),
                },
            )
            .await?;
        destination.release_hash_checkpoint(&resumed).await?;
    }
    println!(
        "checkpoint version mode={mode} raw_receipt_preserved=true read_pin_preserved=true no_data_reupload=true"
    );
    Ok(())
}
