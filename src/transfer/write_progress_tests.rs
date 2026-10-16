use std::{
    error::Error,
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::model::{EntryKind, IdentityStrength, ObservedEntry, SourceIdentity, StoragePath};
use crate::storage::artifacts::{ArtifactKind, artifact_name};
use crate::storage::backends::local::test_destination_storage;
use crate::storage::backends::s3::{
    S3Protocol, connect, connect_at_destination,
    tests::{MemoryS3, endpoint_of},
};
use crate::storage::{SourceQosStats, Storage, WriteProgress, WriteProgressObserver};
use crate::transfer::{
    ExpertDestinationRequest, ExpertDestinationSession, ExpertSourceEvidence, TransferPolicy,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
type Events = Arc<Mutex<Vec<WriteProgress>>>;

fn observation(storage: &Storage, size: u64) -> TestResult<ObservedEntry> {
    Ok(ObservedEntry::new(
        StoragePath::new("source")?,
        EntryKind::File,
        Some(size),
        None,
        SourceIdentity::new(
            storage.identity().clone(),
            IdentityStrength::PathScoped,
            b"source",
        )?,
    )?)
}

fn observer(events: &Events, wake: Option<Arc<Notify>>) -> WriteProgressObserver {
    let events = Arc::clone(events);
    WriteProgressObserver::new(move |event| {
        events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(event);
        if matches!(event, WriteProgress::Confirmed { .. })
            && let Some(wake) = &wake
        {
            wake.notify_one();
        }
    })
}

fn evidence(observed: &ObservedEntry, payload: &[u8]) -> ExpertSourceEvidence {
    ExpertSourceEvidence {
        source_size: payload.len() as u64,
        identity_key: observed.identity_key(),
        blake3: *blake3::hash(payload).as_bytes(),
        source_qos: SourceQosStats::default(),
    }
}

#[tokio::test]
async fn local_notifies_before_input_finishes_and_distinguishes_checkpoints() -> TestResult {
    for policy in [TransferPolicy::Checkpointed, TransferPolicy::AtomicReplace] {
        let root = tempfile::tempdir()?;
        let destination = test_destination_storage(root.path(), "progress")?;
        let observed = observation(&destination, 12)?;
        let evidence = evidence(&observed, b"abcdefghijkl");
        let events = Events::default();
        let wake = Arc::new(Notify::new());
        let session = ExpertDestinationSession::prepare(
            ExpertDestinationRequest::new(
                observed,
                3,
                destination,
                StoragePath::new("final")?,
                CancellationToken::new(),
            )
            .with_transfer_policy(policy)
            .with_write_observer(observer(&events, Some(wake.clone()))),
        )
        .await?;
        // The second input chunk cannot be supplied until the first write is confirmed.
        // A callback emitted only after the whole stream would deadlock and fail this timeout.
        let input = futures::stream::unfold((0_usize, wake), |(index, wake)| async move {
            if index == 4 {
                return None;
            }
            if index == 1 {
                wake.notified().await;
            }
            let bytes = Bytes::copy_from_slice(&b"abcdefghijkl"[index * 3..index * 3 + 3]);
            Some((Ok(bytes), (index + 1, wake)))
        });
        tokio::time::timeout(Duration::from_secs(10), session.write(Box::pin(input)))
            .await??
            .complete(evidence)
            .await?;
        let events = events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut confirmed: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                WriteProgress::Confirmed { offset, bytes } => Some((*offset, *bytes)),
                WriteProgress::Checkpoint { .. } => None,
            })
            .collect();
        confirmed.sort_unstable();
        assert_eq!(confirmed, vec![(0, 3), (3, 3), (6, 3), (9, 3)]);
        assert_eq!(
            events.contains(&WriteProgress::Checkpoint { durable_prefix: 12 }),
            policy == TransferPolicy::Checkpointed
        );
        assert_eq!(std::fs::read(root.path().join("final"))?, b"abcdefghijkl");
    }
    Ok(())
}

#[tokio::test]
async fn s3_single_put_reports_nothing_while_only_buffered() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let storage = connect(protocol.clone(), endpoint_of(&protocol), None)?;
    let observed = observation(&storage, 12)?;
    let evidence = evidence(&observed, b"abcdefghijkl");
    let events = Events::default();
    let session = ExpertDestinationSession::prepare(
        ExpertDestinationRequest::new(
            observed,
            12,
            storage,
            StoragePath::new("final")?,
            CancellationToken::new(),
        )
        .with_write_observer(observer(&events, None)),
    )
    .await?;
    let input = futures::stream::once(async { Ok(Bytes::from_static(b"abcdefghijkl")) });
    let written = session.write(Box::pin(input)).await?;
    assert!(
        events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
    );
    written.complete(evidence).await?;
    assert_eq!(
        *events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        vec![WriteProgress::Confirmed {
            offset: 0,
            bytes: 12
        }]
    );
    assert_eq!(
        protocol.objects.lock().await.get("final"),
        Some(&Bytes::from_static(b"abcdefghijkl"))
    );
    Ok(())
}

#[tokio::test]
async fn s3_multipart_reports_parts_before_publication() -> TestResult {
    // The production threshold deliberately keeps a 9 MiB multipart upload uncheckpointed.
    // The smaller existing test seam exercises real pointer persistence without a 65 MiB body.
    for interval in [64 * 1024 * 1024, 8 * 1024 * 1024] {
        multipart_progress(interval).await?;
    }
    Ok(())
}

async fn multipart_progress(checkpoint_interval: u64) -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let storage = connect_at_destination(
        protocol.clone(),
        endpoint_of(&protocol),
        checkpoint_interval,
    )?;
    let payload = Bytes::from(vec![7; 9 * 1024 * 1024]);
    let recoverable = payload.len() as u64 > checkpoint_interval;
    let observed = observation(&storage, payload.len() as u64)?;
    let evidence = evidence(&observed, &payload);
    let events = Events::default();
    let session = ExpertDestinationSession::prepare(
        ExpertDestinationRequest::new(
            observed,
            1024 * 1024,
            storage,
            StoragePath::new("final")?,
            CancellationToken::new(),
        )
        .with_write_observer(observer(&events, None)),
    )
    .await?;
    assert_eq!(session.recovery_enabled(), recoverable);
    let input = futures::stream::iter(
        payload
            .chunks(1024 * 1024)
            .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
            .collect::<Vec<_>>(),
    );
    let written = session.write(Box::pin(input)).await?;
    assert!(!protocol.objects.lock().await.contains_key("final"));
    let events_before = events
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    assert!(events_before.contains(&WriteProgress::Confirmed {
        offset: 0,
        bytes: 8 * 1024 * 1024
    }));
    assert!(events_before.contains(&WriteProgress::Confirmed {
        offset: 8 * 1024 * 1024,
        bytes: 1024 * 1024
    }));
    assert_eq!(
        events_before.contains(&WriteProgress::Checkpoint {
            durable_prefix: 9 * 1024 * 1024
        }),
        recoverable
    );
    if !recoverable {
        assert!(
            !events_before
                .iter()
                .any(|event| matches!(event, WriteProgress::Checkpoint { .. }))
        );
    }
    // A checkpoint is backed by the persisted upload pointer and acknowledged parts; confirmed
    // bytes without the pointer are useful progress but cannot promise fresh-process recovery.
    assert_eq!(
        protocol
            .objects
            .lock()
            .await
            .contains_key(&artifact_name("final", ArtifactKind::Upload)),
        recoverable
    );
    let uploads = protocol
        .list_uploads("final")
        .await
        .map_err(|e| format!("{e:?}"))?;
    assert_eq!(uploads.len(), 1);
    let mut parts = protocol
        .list_parts("final", &uploads[0])
        .await
        .map_err(|e| format!("{e:?}"))?;
    parts.sort_unstable_by_key(|part| part.number);
    assert_eq!(
        parts
            .iter()
            .map(|part| (part.number, part.size))
            .collect::<Vec<_>>(),
        [(1, 8 * 1024 * 1024), (2, 1024 * 1024)]
    );
    written.complete(evidence).await?;
    assert_eq!(protocol.objects.lock().await.get("final"), Some(&payload));
    Ok(())
}
