use std::sync::Arc;

use bytes::Bytes;

use super::tests::{MemoryS3, identity, native_context, validation_policy};
use super::{S3ProtocolFailure, connect};
use crate::model::{
    EntryKind, FailureClass, IdentityStrength, SourceIdentity, StoragePath, Transience,
};
use crate::storage::{
    FinalDestination, PrepareRequest, PreparedStage, RecoverRequest, RecoveryIdentity,
    SourceDescriptor, StagedDestination, StorageRoleFailure,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

struct Fixture {
    protocol: Arc<MemoryS3>,
    destination: Arc<dyn StagedDestination>,
    prepare: PrepareRequest,
    stage: PreparedStage,
}

async fn fixture(name: &str, binding: [u8; 32]) -> TestResult<Fixture> {
    let protocol = Arc::new(MemoryS3::default());
    let destination = connect(protocol.clone(), identity(), Some(native_context()))?
        .staged_destination(&validation_policy())?;
    let source = SourceDescriptor {
        path: StoragePath::new("source")?,
        kind: EntryKind::File,
        size: None,
        source_identity: SourceIdentity::new(identity(), IdentityStrength::PathScoped, b"source")?,
    };
    let prepare = PrepareRequest {
        final_destination: FinalDestination::new(StoragePath::new(name)?),
        source,
        recovery_binding: binding,
    };
    let stage = destination.prepare(prepare.clone()).await?;
    Ok(Fixture {
        protocol,
        destination,
        prepare,
        stage,
    })
}

fn upload_id(stage: &PreparedStage) -> TestResult<String> {
    Ok(String::from_utf8(
        stage
            .token
            .split(|byte| *byte == 0)
            .nth(1)
            .ok_or("missing upload identity")?
            .to_vec(),
    )?)
}

fn recover_request(
    identity: RecoveryIdentity,
    prepare: &PrepareRequest,
    claim_token: [u8; 32],
) -> RecoverRequest {
    RecoverRequest {
        identity,
        final_destination: prepare.final_destination.clone(),
        source: prepare.source.clone(),
        recovery_binding: prepare.recovery_binding,
        claim_token,
    }
}

async fn inject_gap(fixture: &Fixture) -> TestResult<String> {
    let upload_id = upload_id(&fixture.stage)?;
    fixture
        .protocol
        .uploads
        .lock()
        .await
        .get_mut(&upload_id)
        .ok_or("missing upload")?
        .1
        .push((2, Bytes::from(vec![7; 8 * 1024 * 1024])));
    Ok(upload_id)
}

fn abort_failure() -> S3ProtocolFailure {
    S3ProtocolFailure::session(
        FailureClass::Connectivity,
        Transience::Transient,
        "abort failed",
    )
}

#[tokio::test]
async fn invalid_manifest_is_owned_cleaned_and_rejected() -> TestResult {
    let fixture = fixture("invalid-final", [5; 32]).await?;
    let recovery = fixture
        .destination
        .recovery_identity(&fixture.stage)
        .await?;
    let upload_id = inject_gap(&fixture).await?;
    let reconnected = connect(fixture.protocol.clone(), identity(), Some(native_context()))?
        .staged_destination(&validation_policy())?;
    let result = reconnected
        .recover(recover_request(recovery, &fixture.prepare, [7; 32]))
        .await;
    assert!(matches!(result, Err(StorageRoleFailure::Entry(ref failure))
        if failure.class() == FailureClass::Corruption));
    assert!(
        !fixture
            .protocol
            .uploads
            .lock()
            .await
            .contains_key(&upload_id)
    );
    assert!(fixture.protocol.claims.lock().await.is_empty());
    assert_eq!(*fixture.protocol.aborts.lock().await, 1);
    Ok(())
}

#[tokio::test]
async fn missing_upload_releases_claim_and_allows_fresh_prepare() -> TestResult {
    let fixture = fixture("missing-final", [4; 32]).await?;
    let recovery = fixture
        .destination
        .recovery_identity(&fixture.stage)
        .await?;
    fixture.protocol.uploads.lock().await.clear();
    let reconnected = connect(fixture.protocol.clone(), identity(), Some(native_context()))?
        .staged_destination(&validation_policy())?;
    let result = reconnected
        .recover(recover_request(recovery, &fixture.prepare, [2; 32]))
        .await;
    assert!(matches!(result, Err(StorageRoleFailure::Entry(ref failure))
        if failure.class() == FailureClass::NotFound));
    assert!(fixture.protocol.claims.lock().await.is_empty());
    assert_eq!(
        fixture
            .destination
            .prepare(fixture.prepare)
            .await?
            .write_offset,
        0
    );
    Ok(())
}

#[tokio::test]
async fn invalid_cleanup_failure_retains_claim_and_upload() -> TestResult {
    let fixture = fixture("cleanup-final", [8; 32]).await?;
    let recovery = fixture
        .destination
        .recovery_identity(&fixture.stage)
        .await?;
    let upload_id = inject_gap(&fixture).await?;
    *fixture.protocol.abort_failure.lock().await = Some(abort_failure());
    let reconnected = connect(fixture.protocol.clone(), identity(), Some(native_context()))?
        .staged_destination(&validation_policy())?;
    let result = reconnected
        .recover(recover_request(recovery, &fixture.prepare, [3; 32]))
        .await;
    assert!(matches!(result, Err(StorageRoleFailure::Session(_))));
    assert!(
        fixture
            .protocol
            .uploads
            .lock()
            .await
            .contains_key(&upload_id)
    );
    assert_eq!(fixture.protocol.claims.lock().await.len(), 1);
    Ok(())
}

#[tokio::test]
async fn discard_failure_can_reconnect_and_retry_cleanup() -> TestResult {
    let fixture = fixture("discard-final", [1; 32]).await?;
    let recovery = fixture
        .destination
        .recovery_identity(&fixture.stage)
        .await?;
    *fixture.protocol.abort_failure.lock().await = Some(abort_failure());
    assert!(matches!(
        fixture.destination.discard(fixture.stage).await,
        Err(StorageRoleFailure::Session(_))
    ));
    *fixture.protocol.abort_failure.lock().await = None;
    let reconnected = connect(fixture.protocol.clone(), identity(), Some(native_context()))?
        .staged_destination(&validation_policy())?;
    let recovered = reconnected
        .recover(recover_request(recovery, &fixture.prepare, [6; 32]))
        .await?;
    reconnected.discard(recovered).await?;
    assert_eq!(*fixture.protocol.aborts.lock().await, 1);
    Ok(())
}
