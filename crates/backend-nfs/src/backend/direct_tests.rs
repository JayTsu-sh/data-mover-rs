//! Direct 的可见性、失败清理归属及发布后保护使用真实引擎相同的 role 契约。

use std::sync::{Arc, PoisonError};

use bytes::Bytes;
use futures::stream;
use tokio_util::sync::CancellationToken;

use super::staged::{NfsStagedDestinationAdapter, tests::FakeProtocol};
use crate::model::{
    BackendIdentity, BackendKind, EntryKind, FailureClass, IdentityStrength, Operation,
    SourceIdentity, StoragePath, Transience,
};
use crate::storage::{
    FinalDestination, MetadataMutation, PrepareRequest, PreparedStage, PublicationDisposition,
    PublishRequest, SourceDescriptor, StagedDestination, StorageRoleFailure,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn fixture(
    size: u64,
) -> TestResult<(
    Arc<FakeProtocol>,
    NfsStagedDestinationAdapter,
    PrepareRequest,
)> {
    let identity = BackendIdentity::new(BackendKind::Nfs, "nfs-direct")?;
    let protocol = Arc::new(FakeProtocol::default());
    let adapter = NfsStagedDestinationAdapter::new(protocol.clone(), identity.clone());
    let request = PrepareRequest {
        final_destination: FinalDestination::new(StoragePath::new("dir/final.bin")?),
        source: SourceDescriptor::new(
            StoragePath::new("source.bin")?,
            EntryKind::File,
            Some(size),
            SourceIdentity::new(identity, IdentityStrength::StableWithinBackend, b"source")?,
        ),
        recovery_binding: [7; 32],
    };
    Ok((protocol, adapter, request))
}

#[tokio::test]
async fn direct_new_file_writes_the_final_name_without_artifacts() -> TestResult {
    let (protocol, adapter, request) = fixture(3)?;
    assert!(adapter.supports_direct());
    let path = request.final_destination.path().as_str().to_owned();
    let stage = adapter
        .prepare_direct(request, CancellationToken::new())
        .await?;
    assert!(stage.direct());
    assert!(!stage.recovery_enabled());
    adapter
        .write(
            &stage,
            Box::pin(stream::iter([Ok(Bytes::from_static(b"new"))])),
        )
        .await?;
    assert_eq!(
        protocol
            .files
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&path),
        Some(&b"new".to_vec())
    );
    assert_eq!(
        protocol
            .files
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len(),
        1
    );
    let evidence = adapter
        .publish(
            &stage,
            PublishRequest {
                cancel: CancellationToken::new(),
                expected_size: 3,
                expected_blake3: Some(*blake3::hash(b"new").as_bytes()),
            },
        )
        .await
        .map_err(|error| error.error)?;
    assert_eq!(evidence.disposition, PublicationDisposition::Published);
    Ok(())
}

async fn write(
    adapter: &NfsStagedDestinationAdapter,
    stage: &PreparedStage,
    data: &'static [u8],
) -> TestResult {
    adapter
        .write(
            stage,
            Box::pin(stream::iter([Ok(Bytes::from_static(data))])),
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn direct_overwrite_truncates_only_after_the_input_finishes() -> TestResult {
    let (protocol, adapter, request) = fixture(3)?;
    let path = request.final_destination.path().as_str().to_owned();
    protocol
        .files
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(path.clone(), b"old long payload".to_vec());
    let stage = adapter
        .prepare_direct(request, CancellationToken::new())
        .await?;
    assert_eq!(
        protocol
            .files
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&path),
        Some(&b"old long payload".to_vec())
    );
    write(&adapter, &stage, b"new").await?;
    assert_eq!(
        protocol
            .files
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&path),
        Some(&b"new".to_vec())
    );
    adapter.discard(stage).await?;
    Ok(())
}

#[tokio::test]
async fn direct_data_failure_removes_only_the_output_that_was_mutated() -> TestResult {
    for existing in [false, true] {
        let (protocol, adapter, request) = fixture(8)?;
        let path = request.final_destination.path().as_str().to_owned();
        if existing {
            protocol
                .files
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(path.clone(), b"old content".to_vec());
        }
        let stage = adapter
            .prepare_direct(request, CancellationToken::new())
            .await?;
        let error = super::source::entry_failure(
            &StoragePath::new("source.bin")?,
            Operation::Read,
            FailureClass::Corruption,
            Transience::Permanent,
        );
        assert!(
            adapter
                .write(
                    &stage,
                    Box::pin(stream::iter([Ok(Bytes::from_static(b"part")), Err(error)]))
                )
                .await
                .is_err()
        );
        adapter.remove_failed_direct(stage).await?;
        assert!(
            !protocol
                .files
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .contains_key(&path)
        );
    }
    Ok(())
}

#[tokio::test]
async fn direct_source_failure_before_writing_retains_the_old_target() -> TestResult {
    for existing in [false, true] {
        let (protocol, adapter, request) = fixture(8)?;
        let path = request.final_destination.path().as_str().to_owned();
        if existing {
            protocol
                .files
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(path.clone(), b"old content".to_vec());
        }
        let stage = adapter
            .prepare_direct(request, CancellationToken::new())
            .await?;
        let error = super::source::entry_failure(
            &StoragePath::new("source.bin")?,
            Operation::Read,
            FailureClass::NotFound,
            Transience::Permanent,
        );
        assert!(
            adapter
                .write(&stage, Box::pin(stream::iter([Err(error)])))
                .await
                .is_err()
        );
        adapter.remove_failed_direct(stage).await?;
        assert_eq!(
            protocol
                .files
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get(&path),
            existing.then_some(&b"old content".to_vec())
        );
    }
    Ok(())
}

#[tokio::test]
async fn direct_cleanup_refuses_a_replacement_even_when_size_and_bytes_match() -> TestResult {
    let (protocol, adapter, request) = fixture(3)?;
    let path = request.final_destination.path().as_str().to_owned();
    let stage = adapter
        .prepare_direct(request, CancellationToken::new())
        .await?;
    write(&adapter, &stage, b"new").await?;
    protocol
        .identities
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(path.clone(), 999);
    let error = adapter
        .remove_failed_direct(stage)
        .await
        .err()
        .ok_or("replacement was deleted")?;
    assert!(
        matches!(error, StorageRoleFailure::Entry(error) if error.class() == FailureClass::Conflict)
    );
    assert_eq!(
        protocol
            .files
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&path),
        Some(&b"new".to_vec())
    );
    Ok(())
}

#[tokio::test]
async fn direct_metadata_failure_retains_data_and_published_output_cannot_be_cleaned() -> TestResult
{
    let (protocol, adapter, request) = fixture(3)?;
    let path = request.final_destination.path().as_str().to_owned();
    let stage = adapter
        .prepare_direct(request, CancellationToken::new())
        .await?;
    write(&adapter, &stage, b"new").await?;
    assert!(
        adapter
            .apply_metadata(
                &stage,
                MetadataMutation::Mode(0o640),
                CancellationToken::new()
            )
            .await
            .is_err()
    );
    adapter
        .publish(
            &stage,
            PublishRequest {
                expected_size: 3,
                expected_blake3: None,
                cancel: CancellationToken::new(),
            },
        )
        .await
        .map_err(|error| error.error)?;
    assert!(adapter.remove_failed_direct(stage).await.is_err());
    assert_eq!(
        protocol
            .files
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&path),
        Some(&b"new".to_vec())
    );
    Ok(())
}

#[tokio::test]
async fn direct_empty_input_truncates_an_old_file_and_cancel_before_prepare_has_no_effect()
-> TestResult {
    let (protocol, adapter, request) = fixture(0)?;
    let path = request.final_destination.path().as_str().to_owned();
    protocol
        .files
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(path.clone(), b"old".to_vec());
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(
        adapter
            .prepare_direct(request.clone(), cancel)
            .await
            .is_err()
    );
    assert_eq!(
        protocol
            .files
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&path),
        Some(&b"old".to_vec())
    );
    let stage = adapter
        .prepare_direct(request, CancellationToken::new())
        .await?;
    write(&adapter, &stage, b"").await?;
    adapter
        .publish(
            &stage,
            PublishRequest {
                expected_size: 0,
                expected_blake3: None,
                cancel: CancellationToken::new(),
            },
        )
        .await
        .map_err(|error| error.error)?;
    assert_eq!(
        protocol
            .files
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&path),
        Some(&Vec::new())
    );
    Ok(())
}

#[tokio::test]
async fn direct_metadata_failure_cannot_be_reclassified_as_failed_data_cleanup() -> TestResult {
    let (protocol, adapter, request) = fixture(3)?;
    let path = request.final_destination.path().as_str().to_owned();
    let stage = adapter
        .prepare_direct(request, CancellationToken::new())
        .await?;
    write(&adapter, &stage, b"new").await?;
    assert!(
        adapter
            .apply_metadata_batch(
                &stage,
                vec![MetadataMutation::Mode(0o640)],
                CancellationToken::new()
            )
            .await
            .is_err()
    );
    let error = adapter
        .remove_failed_direct(stage)
        .await
        .err()
        .ok_or("metadata failure authorized data deletion")?;
    assert!(matches!(error, StorageRoleFailure::Entry(error)
        if error.diagnostic().contains("metadata work cannot authorize data cleanup")));
    assert_eq!(
        protocol
            .files
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&path),
        Some(&b"new".to_vec())
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn failed_direct_close_is_bounded_and_cleanup_remains_unknown() -> TestResult {
    let (protocol, adapter, request) = fixture(8)?;
    let path = request.final_destination.path().as_str().to_owned();
    let stage = adapter
        .prepare_direct(request, CancellationToken::new())
        .await?;
    protocol
        .close_stalls
        .store(true, std::sync::atomic::Ordering::Release);
    let input = stream::iter([
        Ok(Bytes::from_static(b"head")),
        Err(super::source::entry_failure(
            stage.final_destination().path(),
            Operation::Read,
            FailureClass::Protocol,
            Transience::Permanent,
        )),
    ]);
    let failure = tokio::time::timeout(
        std::time::Duration::from_secs(4),
        adapter.write(&stage, Box::pin(input)),
    )
    .await?
    .err()
    .ok_or("failed source and stalled CLOSE must not succeed")?;
    assert!(
        matches!(&failure, StorageRoleFailure::Session(error) if error.class() == FailureClass::Timeout && error.diagnostic().contains("close result is unknown"))
    );
    let failure = adapter
        .remove_failed_direct(stage)
        .await
        .err()
        .ok_or("unknown close cannot authorize cleanup")?;
    assert!(
        matches!(failure, StorageRoleFailure::Entry(error) if error.diagnostic().contains("close result is unknown"))
    );
    assert!(
        protocol
            .files
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(&path)
    );
    Ok(())
}

#[tokio::test]
async fn direct_rejects_a_source_handle_alias_before_mutation() -> TestResult {
    let (protocol, adapter, mut request) = fixture(4)?;
    let path = request.final_destination.path().as_str().to_owned();
    protocol
        .files
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(path.clone(), b"safe".to_vec());
    request.source.source_identity = SourceIdentity::new(
        BackendIdentity::new(BackendKind::Nfs, "another-connection")?,
        IdentityStrength::StableWithinBackend,
        Bytes::from(format!("fh:{path}:0")),
    )?;
    let failure = adapter
        .prepare_direct(request, CancellationToken::new())
        .await
        .err()
        .ok_or("source alias accepted")?;
    assert!(
        matches!(failure, StorageRoleFailure::Entry(error) if error.class() == FailureClass::Conflict)
    );
    assert_eq!(
        protocol
            .files
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&path),
        Some(&b"safe".to_vec())
    );
    Ok(())
}

#[tokio::test]
async fn issued_write_with_unchanged_attributes_never_claims_cleanup_success() -> TestResult {
    let (protocol, adapter, request) = fixture(4)?;
    let path = request.final_destination.path().as_str().to_owned();
    protocol
        .files
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(path.clone(), b"same".to_vec());
    let stage = adapter
        .prepare_direct(request, CancellationToken::new())
        .await?;
    write(&adapter, &stage, b"same").await?;
    let failure = adapter
        .remove_failed_direct(stage)
        .await
        .err()
        .ok_or("uncertain cleanup was confirmed")?;
    assert!(
        matches!(failure, StorageRoleFailure::Entry(error) if error.diagnostic().contains("cannot confirm target mutation or cleanup"))
    );
    assert_eq!(
        protocol
            .files
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&path),
        Some(&b"same".to_vec())
    );
    Ok(())
}

#[tokio::test]
async fn durable_direct_commits_bounded_receipt_batches_during_input() -> TestResult {
    let (protocol, adapter, request) = fixture(16)?;
    protocol
        .unstable_pressure
        .store(true, std::sync::atomic::Ordering::Release);
    let stage = adapter
        .prepare_direct(request, CancellationToken::new())
        .await?;
    assert!(stage.durable_publication());
    let input = stream::iter((0..16).map(|_| Ok(Bytes::from_static(b"x"))));
    adapter.write(&stage, Box::pin(input)).await?;
    assert_eq!(
        protocol
            .deferred_writes
            .load(std::sync::atomic::Ordering::Acquire),
        16
    );
    assert!(
        protocol
            .checkpoints
            .load(std::sync::atomic::Ordering::Acquire)
            >= 5
    );
    adapter.discard(stage).await?;
    Ok(())
}
