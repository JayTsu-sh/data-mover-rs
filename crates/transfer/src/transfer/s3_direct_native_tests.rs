//! Direct 原生复制在元数据阶段前完成，快速校验不读取对象数据。
use std::sync::Arc;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use crate::model::{FailureClass, ObjectTag, StoragePath, Transience};
use crate::storage::backends::s3::tests::{MemoryS3, endpoint_of, native_context, source_storage};
use crate::storage::backends::s3::{S3NativeContext, S3Protocol as _, S3ProtocolFailure, connect};
use crate::transfer::{
    EffectiveRecovery, FailedDataCleanup, MetadataFailurePolicy, MetadataTransferMode,
    ReadBackVerification, TransferPhase, TransferPolicy, TransferRequest, TransferRoute, transfer,
};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn request(protocol: &Arc<MemoryS3>, verify: ReadBackVerification) -> Result<TransferRequest> {
    Ok(TransferRequest::new(
        source_storage(protocol, "src")?,
        StoragePath::new("file")?,
        connect(
            protocol.clone(),
            endpoint_of(protocol),
            Some(native_context()),
        )?,
        CancellationToken::new(),
    )
    .with_transfer_policy(TransferPolicy::Direct)
    .with_read_back_verification(verify))
}

#[tokio::test]
async fn direct_native_copies_tags_without_client_payload_or_recovery_state() -> Result {
    for verify in [
        ReadBackVerification::Disabled,
        ReadBackVerification::Enabled,
    ] {
        let protocol = Arc::new(MemoryS3::default());
        let payload = Bytes::from(vec![0x7c; 1024]);
        protocol
            .objects
            .lock()
            .await
            .insert("src/file".into(), payload.clone());
        let tags = vec![ObjectTag::new("generation", "latest")?];
        protocol
            .put_tags("src/file", None, &tags)
            .await
            .map_err(|e| format!("{e:?}"))?;
        let outcome = transfer(request(&protocol, verify)?).await?;
        assert_eq!(outcome.route, TransferRoute::Native);
        assert_eq!(outcome.recovery, EffectiveRecovery::Disabled);
        assert_eq!(outcome.source_qos.native_requests, 1);
        assert_eq!(outcome.source_qos.native_bytes, 1024);
        assert_eq!(*protocol.native_copies.lock().await, 1);
        assert_eq!(*protocol.puts.lock().await, 0);
        assert_eq!(*protocol.multipart_begins.lock().await, 0);
        assert_eq!(protocol.objects.lock().await.len(), 2);
        assert_eq!(protocol.objects.lock().await.get("file"), Some(&payload));
        assert_eq!(
            protocol
                .get_tags("file", None)
                .await
                .map_err(|e| format!("{e:?}"))?,
            tags
        );
        if verify == ReadBackVerification::Disabled {
            assert!(protocol.range_dispatches.lock().await.is_empty());
            assert_eq!(outcome.source_qos.source_read_operations, 0);
        } else {
            assert_eq!(outcome.blake3, Some(*blake3::hash(&payload).as_bytes()));
            assert!(protocol.range_dispatches.lock().await.len() >= 2);
        }
    }
    Ok(())
}

#[tokio::test]
async fn direct_native_tag_failure_keeps_completed_verified_data_and_source() -> Result {
    for verify in [
        ReadBackVerification::Disabled,
        ReadBackVerification::Enabled,
    ] {
        let protocol = Arc::new(MemoryS3::default());
        let payload = Bytes::from_static(b"data survives failed metadata");
        protocol
            .objects
            .lock()
            .await
            .insert("src/file".into(), payload.clone());
        *protocol.tag_write_failure.lock().await = Some(S3ProtocolFailure::entry(
            FailureClass::PermissionDenied,
            Transience::Permanent,
            "injected target tags refusal",
        ));
        let failure = transfer(
            request(&protocol, verify)?
                .with_failed_data_cleanup(FailedDataCleanup::RemoveOwned)
                .with_metadata_failure_policy(MetadataFailurePolicy::PublishData),
        )
        .await
        .err()
        .ok_or("failed metadata unexpectedly succeeded")?;
        assert_eq!(failure.phase(), TransferPhase::Metadata);
        let data = failure
            .published_data()
            .ok_or("completed data evidence missing")?;
        assert_eq!(data.route, TransferRoute::Native);
        assert_eq!(data.transferred_bytes, payload.len() as u64);
        assert_eq!(protocol.objects.lock().await.get("file"), Some(&payload));
        assert_eq!(
            protocol.objects.lock().await.get("src/file"),
            Some(&payload)
        );
        assert_eq!(*protocol.native_copies.lock().await, 1);
        let failure = failure.cleanup_for_policy(TransferPolicy::Direct).await;
        assert!(failure.cleanup_failure().is_none());
        assert!(!failure.has_failed_data_cleanup());
        assert_eq!(protocol.objects.lock().await.get("file"), Some(&payload));
        if verify == ReadBackVerification::Enabled {
            assert!(protocol.range_dispatches.lock().await.len() >= 2);
        }
    }
    Ok(())
}

#[tokio::test]
async fn native_refusal_and_unknown_copy_keep_existing_target_with_precise_cleanup_result() -> Result
{
    for denied in [false, true] {
        let protocol = Arc::new(MemoryS3::default());
        let source = Bytes::from_static(b"source data");
        let prior = Bytes::from_static(b"unrelated existing data");
        protocol
            .objects
            .lock()
            .await
            .insert("src/file".into(), source.clone());
        protocol
            .objects
            .lock()
            .await
            .insert("file".into(), prior.clone());
        *protocol.native_failure.lock().await = Some(S3ProtocolFailure::entry(
            if denied {
                FailureClass::PermissionDenied
            } else {
                FailureClass::Connectivity
            },
            if denied {
                Transience::Permanent
            } else {
                Transience::Unknown
            },
            "native copy refused or unconfirmed",
        ));
        let failure = transfer(
            request(&protocol, ReadBackVerification::Disabled)?
                .with_metadata_mode(MetadataTransferMode::CallerManaged)
                .with_failed_data_cleanup(FailedDataCleanup::RemoveOwned),
        )
        .await
        .err()
        .ok_or("injected failure must stop native copy")?;
        assert_eq!(failure.phase(), TransferPhase::Transfer);
        let failure = failure.cleanup_for_policy(TransferPolicy::Direct).await;
        assert_eq!(failure.cleanup_failure().is_none(), denied, "{failure}");
        if !denied {
            assert!(failure.to_string().contains("write outcome is unconfirmed"));
        }
        assert_eq!(protocol.objects.lock().await.get("src/file"), Some(&source));
        assert_eq!(protocol.objects.lock().await.get("file"), Some(&prior));
        assert!(protocol.version_deletes().is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn direct_native_refusal_and_lost_reply_preserve_source_and_do_not_stream() -> Result {
    let protocol = Arc::new(MemoryS3::default());
    let payload = Bytes::from_static(b"source");
    protocol
        .objects
        .lock()
        .await
        .insert("src/file".into(), payload.clone());
    protocol
        .objects
        .lock()
        .await
        .insert("file".into(), Bytes::from_static(b"old target"));
    *protocol.native_failure.lock().await = Some(S3ProtocolFailure::entry(
        FailureClass::PermissionDenied,
        Transience::Permanent,
        "copy denied",
    ));
    let failure = transfer(request(&protocol, ReadBackVerification::Disabled)?)
        .await
        .err()
        .ok_or("copy refusal unexpectedly succeeded")?;
    assert_eq!(failure.phase(), TransferPhase::Transfer);
    assert!(failure.published_data().is_none());
    assert_eq!(
        protocol.objects.lock().await.get("file"),
        Some(&Bytes::from_static(b"old target"))
    );
    *protocol.native_failure.lock().await = None;
    *protocol.copy_commits_then_fails.lock().await = true;
    let outcome = transfer(request(&protocol, ReadBackVerification::Enabled)?).await?;
    assert_eq!(outcome.route, TransferRoute::Native);
    assert_eq!(protocol.objects.lock().await.get("file"), Some(&payload));
    assert_eq!(
        protocol.objects.lock().await.get("src/file"),
        Some(&payload)
    );
    assert_eq!(*protocol.puts.lock().await, 0);
    assert_eq!(*protocol.native_copies.lock().await, 1);
    Ok(())
}

#[tokio::test]
async fn direct_native_large_objects_keep_the_existing_streaming_path() -> Result {
    let protocol = Arc::new(MemoryS3::default());
    let payload = Bytes::from(vec![0x42; 9 * 1024 * 1024]);
    protocol
        .objects
        .lock()
        .await
        .insert("src/file".into(), payload.clone());
    let outcome = transfer(request(&protocol, ReadBackVerification::Disabled)?).await?;
    assert_eq!(outcome.route, TransferRoute::Streaming);
    assert_eq!(*protocol.native_copies.lock().await, 0);
    assert_eq!(protocol.objects.lock().await.get("file"), Some(&payload));
    Ok(())
}

#[tokio::test]
async fn direct_native_refuses_the_same_actual_key_under_different_roots() -> Result {
    let protocol = Arc::new(MemoryS3::default());
    let payload = Bytes::from_static(b"preserve source");
    protocol
        .objects
        .lock()
        .await
        .insert("src/file".into(), payload.clone());
    let request = TransferRequest::new(
        source_storage(&protocol, "src")?,
        StoragePath::new("file")?,
        connect(
            protocol.source_root("src"),
            endpoint_of(&protocol),
            Some(S3NativeContext::new(
                "memory://s3",
                "standard",
                "memory".into(),
                Some("src".into()),
            )),
        )?,
        CancellationToken::new(),
    )
    .with_transfer_policy(TransferPolicy::Direct);
    let failure = transfer(request)
        .await
        .err()
        .ok_or("aliased source was copied")?;
    assert!(failure.published_data().is_none());
    assert_eq!(*protocol.native_copies.lock().await, 0);
    assert_eq!(*protocol.puts.lock().await, 0);
    assert_eq!(
        protocol.objects.lock().await.get("src/file"),
        Some(&payload)
    );
    Ok(())
}
