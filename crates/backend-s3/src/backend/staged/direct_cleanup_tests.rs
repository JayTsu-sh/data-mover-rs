//! 直接操作既有 role，核验回执归属、旧版本与替换保护。
use std::sync::Arc;

use bytes::Bytes;

use super::{TestResult, chunks, destination, prepared};
use crate::model::{FailureClass, Transience};
use crate::storage::backends::s3::tests::{MemoryS3, Versioning};
use crate::storage::{StagedDestination, StorageRoleFailure};

#[tokio::test]
async fn no_write_and_lost_reply_never_authorize_removing_preexisting_content() -> TestResult {
    for versioned in [false, true] {
        for lost in [false, true] {
            let protocol = Arc::new(MemoryS3::default());
            let body = Bytes::from_static(b"preexisting matching body");
            if versioned {
                protocol.set_versioning(Versioning::Enabled);
                protocol.put_version("dir/final", "old", body.clone()).await;
            } else {
                protocol
                    .objects
                    .lock()
                    .await
                    .insert("dir/final".into(), body.clone());
            }
            let target = destination(&protocol);
            let stage = prepared(&target, Some(body.len() as u64)).await?;
            if lost {
                *protocol.put_fails_without_commit.lock().await = true;
                target.write(&stage, chunks(&body)).await?;
            }
            let result = target.remove_failed_direct(stage).await;
            if lost {
                assert!(matches!(result, Err(StorageRoleFailure::Entry(error))
                    if error.class() == FailureClass::Conflict && error.transience() == Transience::Unknown));
            } else {
                result?;
            }
            assert_eq!(protocol.objects.lock().await.get("dir/final"), Some(&body));
            assert!(protocol.version_deletes().is_empty());
        }
    }
    Ok(())
}

#[tokio::test]
async fn replaced_current_output_is_retained_and_exact_owned_version_can_be_removed() -> TestResult
{
    for versioned in [false, true] {
        let protocol = Arc::new(MemoryS3::default());
        if versioned {
            protocol.set_versioning(Versioning::Enabled);
        }
        let target = destination(&protocol);
        let body = Bytes::from_static(b"our output");
        let stage = prepared(&target, Some(body.len() as u64)).await?;
        target.write(&stage, chunks(&body)).await?;
        let replacement = Bytes::from_static(b"unrelated replacement");
        if versioned {
            protocol
                .put_version("dir/final", "replacement", replacement.clone())
                .await;
        } else {
            protocol
                .objects
                .lock()
                .await
                .insert("dir/final".into(), replacement.clone());
        }
        let result = target.remove_failed_direct(stage).await;
        if versioned {
            result?;
            assert_eq!(protocol.version_deletes().len(), 1);
            assert_ne!(protocol.version_deletes()[0].1, "replacement");
        } else {
            assert!(
                matches!(result, Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Conflict)
            );
            assert!(protocol.version_deletes().is_empty());
        }
        assert_eq!(
            protocol.objects.lock().await.get("dir/final"),
            Some(&replacement)
        );
    }
    Ok(())
}

#[tokio::test]
async fn missing_version_echo_cannot_authorize_a_delete() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    protocol.set_versioning(Versioning::Enabled);
    let target = destination(&protocol);
    let body = Bytes::from_static(b"our output");
    let stage = prepared(&target, Some(body.len() as u64)).await?;
    target.write(&stage, chunks(&body)).await?;
    protocol
        .put_version("dir/final", "replacement", body.clone())
        .await;
    *protocol.ignores_version_id.lock().await = true;
    assert!(target.remove_failed_direct(stage).await.is_err());
    assert!(protocol.version_deletes().is_empty());
    assert_eq!(protocol.objects.lock().await.get("dir/final"), Some(&body));
    Ok(())
}
