//! Transfers to an ONTAP NAS bucket (P7 §14.2, `compat=ontap&bucket_type=nas`) over the in-memory
//! bucket in NAS mode: what the bucket cannot write is refused at `Preflight` before any
//! destination request, and a destination that keeps no progress makes every `Checkpointed`
//! transfer report `UnsupportedByDestination`, looking for nothing at the destination.

use std::error::Error;
use std::fs;
use std::sync::Arc;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use crate::model::{
    EntryKind, FailureClass, IdentityStrength, ObservedEntry, SourceIdentity, StoragePath,
};
use crate::storage::PrepareFact;
use crate::storage::artifacts::{ArtifactKind, artifact_name};
use crate::storage::backends::local::test_source_storage;
use crate::storage::backends::s3::connect;
use crate::storage::backends::s3::tests::{MemoryS3, endpoint_of, identity, native_context};
use crate::storage::{Storage, StorageRoleFailure};
use crate::transfer::{
    EffectiveRecovery, ExpertDestinationRequest, ExpertDestinationSession, InflightLimits,
    PayloadShapingPolicy, ReadBackVerification, TransferFailure, TransferPhase, TransferPolicy,
    TransferRequest, TransferRoute, TransferSide, transfer,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

/// One more byte than one `PutObject` carries.
const OVER_ONE_REQUEST: u64 = 5 * 1024 * 1024 * 1024 + 1;

fn nas() -> Arc<MemoryS3> {
    Arc::new(MemoryS3::nas_bucket())
}

fn bucket(protocol: &Arc<MemoryS3>) -> TestResult<Storage> {
    connect(
        protocol.clone(),
        endpoint_of(protocol),
        Some(native_context()),
    )
}

fn request(
    source: Storage,
    source_path: &str,
    destination: Storage,
    final_path: &str,
) -> TestResult<TransferRequest> {
    Ok(TransferRequest::new(
        source,
        StoragePath::new(source_path)?,
        destination,
        StoragePath::new(final_path)?,
        InflightLimits::new(2, 64 * 1024, 2)?,
        CancellationToken::new(),
    ))
}

/// (phase, side, class) of a failed transfer.
fn refusal(failure: &TransferFailure) -> (TransferPhase, TransferSide, Option<FailureClass>) {
    let role = Error::source(failure).and_then(|cause| cause.downcast_ref::<StorageRoleFailure>());
    let class = match role {
        Some(StorageRoleFailure::Entry(entry)) => Some(entry.class()),
        _ => None,
    };
    (failure.phase(), failure.side(), class)
}

const REFUSED_AT_PREFLIGHT: (TransferPhase, TransferSide, Option<FailureClass>) = (
    TransferPhase::Preflight,
    TransferSide::Destination,
    Some(FailureClass::Unsupported),
);

/// Every request the destination bucket received that could write or look at it.
async fn destination_requests(protocol: &MemoryS3) -> (u32, u32, u32) {
    (
        *protocol.puts.lock().await,
        *protocol.multipart_begins.lock().await,
        protocol.nas().heads,
    )
}

/// More than 5 GiB, and keys a NAS bucket would store under another name (or cannot store), are
/// the destination's `Unsupported` at `Preflight`: no request reaches the bucket — neither for
/// the ordinary transfer nor for the expert destination half.
#[tokio::test]
async fn what_a_nas_bucket_cannot_write_is_refused_before_any_request() -> TestResult {
    let root = tempfile::tempdir()?;
    fs::File::create(root.path().join("big"))?.set_len(OVER_ONE_REQUEST)?;
    fs::write(root.path().join("small"), b"x")?;
    let long = "n".repeat(256);
    let long_key = format!("d/{long}");
    for (source, final_path) in [
        ("big", "big"),
        ("small", "a/../x"),
        ("small", "a/./y"),
        ("small", "a//b"),
        ("small", "/x"),
        ("small", long_key.as_str()),
    ] {
        let protocol = nas();
        let (local, _) = test_source_storage(root.path(), "nas-preflight")?;
        let Err(failure) = transfer(request(local, source, bucket(&protocol)?, final_path)?).await
        else {
            return Err(format!("{final_path} was written").into());
        };
        assert_eq!(refusal(&failure), REFUSED_AT_PREFLIGHT, "{final_path}");
        assert_eq!(
            destination_requests(&protocol).await,
            (0, 0, 0),
            "{final_path}"
        );
    }
    for (size, final_path) in [(OVER_ONE_REQUEST, "big"), (1, "a/../x")] {
        let protocol = nas();
        let observation = ObservedEntry::new(
            StoragePath::new("src/file")?,
            EntryKind::File,
            Some(size),
            None,
            SourceIdentity::new(identity(), IdentityStrength::PathScoped, b"file")?,
        )?;
        let prepared = ExpertDestinationSession::prepare(ExpertDestinationRequest::new(
            observation,
            1024,
            bucket(&protocol)?,
            StoragePath::new(final_path)?,
            InflightLimits::new(2, 1024, 2)?,
            CancellationToken::new(),
        ))
        .await;
        let Err(failure) = prepared else {
            return Err(format!("the expert half prepared {final_path}").into());
        };
        assert_eq!(refusal(&failure), REFUSED_AT_PREFLIGHT, "{final_path}");
        assert_eq!(destination_requests(&protocol).await, (0, 0, 0));
    }
    Ok(())
}

/// A `Checkpointed` transfer to a NAS bucket keeps nothing for a later attempt and says so,
/// whatever the route: streamed from a local file, or a native copy within the bucket (not
/// `NotApplicableNative`). Nothing is looked for at the destination before the write: a pointer
/// planted beside each final key is neither read nor removed, and the prepare is `Fresh`.
/// `AtomicReplace` and `Direct` report `Disabled` as anywhere.
#[tokio::test]
async fn a_checkpointed_transfer_to_a_nas_bucket_reports_unsupported_by_destination() -> TestResult
{
    let root = tempfile::tempdir()?;
    fs::write(root.path().join("file"), vec![7; 100 * 1024])?;
    let protocol = nas();
    let planted = [
        artifact_name("dst/streamed", ArtifactKind::Upload),
        artifact_name("dst/native", ArtifactKind::Upload),
    ];
    for pointer in &planted {
        protocol
            .objects
            .lock()
            .await
            .insert(pointer.clone(), Bytes::from_static(b"a leftover pointer"));
    }
    let (local, _) = test_source_storage(root.path(), "nas-checkpointed")?;
    let streamed = request(local, "file", bucket(&protocol)?, "dst/streamed")?
        .with_transfer_policy(TransferPolicy::Checkpointed)
        .with_read_back_verification(ReadBackVerification::Disabled);
    let outcome = transfer(streamed).await?;
    assert_eq!(outcome.route, TransferRoute::Streaming);
    assert_eq!(
        outcome.recovery,
        EffectiveRecovery::UnsupportedByDestination
    );
    assert_eq!(outcome.prepare, PrepareFact::Fresh);
    assert_eq!(protocol.nas().heads, 0, "no pointer HEAD");
    assert_eq!(*protocol.puts.lock().await, 1);

    protocol
        .objects
        .lock()
        .await
        .insert("src/file".into(), Bytes::from(vec![8; 100 * 1024]));
    let native = request(
        bucket(&protocol)?,
        "src/file",
        bucket(&protocol)?,
        "dst/native",
    )?
    .with_transfer_policy(TransferPolicy::Checkpointed)
    .with_payload_shaping(PayloadShapingPolicy::AllowUnshapedNative)
    .with_read_back_verification(ReadBackVerification::Disabled);
    let outcome = transfer(native).await?;
    assert_eq!(outcome.route, TransferRoute::Native);
    assert_eq!(
        outcome.recovery,
        EffectiveRecovery::UnsupportedByDestination
    );
    assert_eq!(outcome.prepare, PrepareFact::Fresh);
    for pointer in &planted {
        assert!(
            protocol.objects.lock().await.contains_key(pointer),
            "{pointer}"
        );
    }
    for policy in [TransferPolicy::AtomicReplace, TransferPolicy::Direct] {
        let (local, _) = test_source_storage(root.path(), "nas-other-policies")?;
        let other = request(local, "file", bucket(&protocol)?, "dst/other")?
            .with_transfer_policy(policy)
            .with_read_back_verification(ReadBackVerification::Disabled);
        assert_eq!(transfer(other).await?.recovery, EffectiveRecovery::Disabled);
    }

    // The expert destination half keeps no recovery either.
    let observation = ObservedEntry::new(
        StoragePath::new("src/file")?,
        EntryKind::File,
        Some(100 * 1024),
        None,
        SourceIdentity::new(identity(), IdentityStrength::PathScoped, b"file")?,
    )?;
    let session = ExpertDestinationSession::prepare(
        ExpertDestinationRequest::new(
            observation,
            1024,
            bucket(&protocol)?,
            StoragePath::new("dst/expert")?,
            InflightLimits::new(2, 1024, 2)?,
            CancellationToken::new(),
        )
        .with_transfer_policy(TransferPolicy::Checkpointed),
    )
    .await?;
    assert!(!session.recovery_enabled());
    assert_eq!(session.prepare_fact(), PrepareFact::Fresh);
    session.discard().await?;
    Ok(())
}
