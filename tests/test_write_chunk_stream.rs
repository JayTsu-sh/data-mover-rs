//! The caller-owned split transport now uses expert source/destination sessions rather than
//! `ResumeContext` and offset-bearing `DataChunk`s. This regression keeps the key publication
//! guarantee: bytes altered in transit must not become the final destination.

use bytes::Bytes;
use data_mover::model::StoragePath;
use data_mover::storage::ByteStream;
use data_mover::transfer::{
    ExpertDestinationRequest, ExpertDestinationSession, ExpertSourceRequest, ExpertSourceSession,
    InflightLimits, TransferPhase, TransferPolicy,
};
use tokio_util::sync::CancellationToken;

mod common;

#[tokio::test]
async fn expert_destination_refuses_corrupt_transport_payload_before_publish()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let source_root = tempfile::tempdir()?;
    let destination_root = tempfile::tempdir()?;
    let payload = (0..256 * 1024)
        .map(|index| u8::try_from(index % 251).unwrap_or_default())
        .collect::<Vec<_>>();
    tokio::fs::write(source_root.path().join("blob.bin"), &payload).await?;

    let source_storage = common::connect_local(source_root.path(), 4).await?;
    let destination_storage = common::connect_local(destination_root.path(), 4).await?;
    let observation = common::observe_file(&source_storage, "blob.bin").await?;
    let limits = InflightLimits::new(2, 128 * 1024, 2)?;
    let source = ExpertSourceSession::open(ExpertSourceRequest::new(
        source_storage,
        observation.clone(),
        limits,
        CancellationToken::new(),
    ))
    .await?;
    let destination = ExpertDestinationSession::prepare(
        ExpertDestinationRequest::new(
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

    let mut payload_stream = source.stream_from(0)?;
    let (tx, rx) = tokio::sync::mpsc::channel(2);
    let source_task = tokio::spawn(async move {
        let mut changed = false;
        while let Some(chunk) = payload_stream.next_chunk().await? {
            let mut bytes = chunk.to_vec();
            if !changed && !bytes.is_empty() {
                bytes[0] ^= 0xff;
                changed = true;
            }
            tx.send(Ok(Bytes::from(bytes)))
                .await
                .map_err(|_| "destination closed its payload receiver")?;
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(payload_stream.finish().await?)
    });
    let stream: ByteStream = Box::pin(futures::stream::unfold(rx, |mut receiver| async move {
        receiver.recv().await.map(|item| (item, receiver))
    }));

    let transferred = destination.write(stream).await?;
    let source_evidence = source_task.await??;
    let Err(failure) = transferred.complete(source_evidence).await else {
        panic!("changed payload unexpectedly passed stage verification");
    };

    assert_eq!(failure.phase(), TransferPhase::Verify);
    failure.discard_stage().await?;
    assert!(!destination_root.path().join("published.bin").exists());
    assert_eq!(
        std::fs::read_dir(destination_root.path())?.count(),
        0,
        "rejected staged content must not leave transfer artifacts"
    );
    Ok(())
}
