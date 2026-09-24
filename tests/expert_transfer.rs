use std::num::NonZeroUsize;

use data_mover::model::{ObservedEntry, StoragePath};
use data_mover::storage::{
    BackendConfig, LocalBackendConfig, PreflightPolicy, Storage, connect_backend,
};
use data_mover::transfer::{
    ExpertDestinationRequest, ExpertDestinationSession, ExpertSourceRequest, ExpertSourceSession,
    InflightLimits, TransferIdentity, TransferPolicy,
};
use tokio_util::sync::CancellationToken;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn local_storage(root: &std::path::Path) -> TestResult<Storage> {
    Ok(connect_backend(BackendConfig::Local(LocalBackendConfig {
        root: root.to_path_buf(),
        read_concurrency: NonZeroUsize::new(2).ok_or("non-zero read concurrency")?,
        write_concurrency: NonZeroUsize::new(2).ok_or("non-zero write concurrency")?,
    }))
    .await?)
}

async fn observe(storage: &Storage, path: &str) -> TestResult<ObservedEntry> {
    let source = storage.read_source(&PreflightPolicy::production())?;
    let descriptor = source.describe(&StoragePath::new(path)?).await?;
    Ok(ObservedEntry::new(
        descriptor.path,
        descriptor.kind,
        descriptor.size,
        None,
        descriptor.source_identity,
    )?)
}

#[tokio::test]
async fn expert_source_hashes_the_whole_file_but_emits_only_after_durable_prefix() -> TestResult {
    let root = tempfile::tempdir()?;
    let payload = (0_usize..(256 * 1024 + 17))
        .map(|index| u8::try_from(index % 251).unwrap_or_else(|_| unreachable!()))
        .collect::<Vec<_>>();
    tokio::fs::write(root.path().join("payload.bin"), &payload).await?;
    let storage = local_storage(root.path()).await?;
    let observation = observe(&storage, "payload.bin").await?;
    let session = ExpertSourceSession::open(ExpertSourceRequest::new(
        TransferIdentity::new("expert-source-attempt")?,
        storage,
        observation,
        InflightLimits::new(2, 128 * 1024, 2)?,
        CancellationToken::new(),
    ))
    .await?;
    let offer = session.offer();
    let durable_prefix = 64 * 1024_usize;
    let mut stream = session.stream_from(u64::try_from(durable_prefix)?)?;
    let mut emitted = Vec::new();
    while let Some(chunk) = stream.next_chunk().await? {
        assert!(chunk.len() <= offer.maximum_chunk_bytes);
        emitted.extend_from_slice(&chunk);
    }
    let evidence = stream.finish().await?;

    assert_eq!(emitted, payload[durable_prefix..]);
    assert_eq!(evidence.source_size, payload.len() as u64);
    assert_eq!(evidence.blake3, *blake3::hash(&payload).as_bytes());
    assert_eq!(evidence.identity_key, offer.identity_key);
    Ok(())
}

#[tokio::test]
async fn expert_source_and_destination_share_verified_publication_lifecycle() -> TestResult {
    let source_root = tempfile::tempdir()?;
    let destination_root = tempfile::tempdir()?;
    let payload = vec![0x5a; 256 * 1024 + 17];
    tokio::fs::write(source_root.path().join("payload.bin"), &payload).await?;
    let source_storage = local_storage(source_root.path()).await?;
    let destination_storage = local_storage(destination_root.path()).await?;
    let observation = observe(&source_storage, "payload.bin").await?;
    let identity = TransferIdentity::new("expert-e2e")?;
    let limits = InflightLimits::new(2, 128 * 1024, 2)?;
    let source = ExpertSourceSession::open(ExpertSourceRequest::new(
        identity,
        source_storage,
        observation.clone(),
        limits,
        CancellationToken::new(),
    ))
    .await?;
    let destination = ExpertDestinationSession::prepare(
        ExpertDestinationRequest::new(
            identity,
            observation,
            source.offer().maximum_chunk_bytes,
            destination_storage,
            StoragePath::new("published.bin")?,
            limits,
            CancellationToken::new(),
        )
        .with_transfer_policy(TransferPolicy::AtomicReplace),
    )
    .await?;
    let mut payload_stream = source.stream_from(destination.write_offset())?;
    let (tx, rx) = tokio::sync::mpsc::channel(2);
    let source_task = tokio::spawn(async move {
        while let Some(chunk) = payload_stream.next_chunk().await? {
            if tx.send(Ok(chunk)).await.is_err() {
                return Err("destination stopped consuming source payload".into());
            }
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(payload_stream.finish().await?)
    });
    let wire_stream: data_mover::storage::ByteStream =
        Box::pin(futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        }));

    let transferred = destination.write(wire_stream).await?;
    let source_evidence = source_task.await??;
    let outcome = transferred.complete(source_evidence).await?;

    assert_eq!(outcome.blake3, Some(*blake3::hash(&payload).as_bytes()));
    assert_eq!(
        tokio::fs::read(destination_root.path().join("published.bin")).await?,
        payload
    );
    Ok(())
}

#[tokio::test]
async fn expert_destination_checkpoints_only_multi_source_chunk_payloads() -> TestResult {
    let source_root = tempfile::tempdir()?;
    let destination_root = tempfile::tempdir()?;
    tokio::fs::write(
        source_root.path().join("large.bin"),
        vec![3; 256 * 1024 + 1],
    )
    .await?;
    tokio::fs::write(source_root.path().join("small.bin"), vec![4; 4 * 1024]).await?;
    let source_storage = local_storage(source_root.path()).await?;
    let destination_storage = local_storage(destination_root.path()).await?;
    let limits = InflightLimits::new(2, 128 * 1024, 2)?;

    let large = ExpertDestinationSession::prepare(
        ExpertDestinationRequest::new(
            TransferIdentity::new("expert-recoverable")?,
            observe(&source_storage, "large.bin").await?,
            64 * 1024,
            destination_storage.clone(),
            StoragePath::new("large.bin")?,
            limits,
            CancellationToken::new(),
        )
        .with_transfer_policy(TransferPolicy::Checkpointed),
    )
    .await?;
    assert!(large.recovery_enabled());
    large.discard().await?;

    let small = ExpertDestinationSession::prepare(
        ExpertDestinationRequest::new(
            TransferIdentity::new("expert-ephemeral")?,
            observe(&source_storage, "small.bin").await?,
            64 * 1024,
            destination_storage,
            StoragePath::new("small.bin")?,
            limits,
            CancellationToken::new(),
        )
        .with_transfer_policy(TransferPolicy::Checkpointed),
    )
    .await?;
    assert!(!small.recovery_enabled());
    small.discard().await?;
    Ok(())
}
