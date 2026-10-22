//! Transfers to an ONTAP NAS bucket (P7 §14.2, `compat=ontap&bucket_type=nas`) over the in-memory
//! bucket in NAS mode: what the bucket cannot write is refused at `Preflight` before any
//! destination request, and a destination that keeps no progress makes every `Checkpointed`
//! transfer report `UnsupportedByDestination`, looking for nothing at the destination.

use std::error::Error;
use std::fs;
use std::sync::Arc;

use bytes::Bytes;
use futures::channel::mpsc;
use tokio_util::sync::CancellationToken;

use crate::model::{
    EntryKind, FailureClass, IdentityStrength, ObservedEntry, SourceIdentity, StoragePath,
};
use crate::storage::PrepareFact;
use crate::storage::artifacts::{ArtifactKind, artifact_name};
use crate::storage::backends::local::{test_destination_storage, test_source_storage};
use crate::storage::backends::s3::tests::{
    MemoryS3, content_md5, endpoint_of, identity, native_context, source_storage,
};
use crate::storage::backends::s3::{S3Protocol as _, connect, connect_native_at_destination};
use crate::storage::{Storage, StorageRoleFailure};
use crate::transfer::{
    EffectiveRecovery, ExpertDestinationRequest, ExpertDestinationSession, ExpertSourceRequest,
    ExpertSourceSession, ReadBackMethod, ReadBackVerification, TransferFailure, TransferPhase,
    TransferPolicy, TransferRequest, TransferRoute, TransferSide, UnpinnedReadReason, UploadDigest,
    UploadDigestGap, transfer,
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
) -> TestResult<TransferRequest> {
    Ok(TransferRequest::new(
        source,
        StoragePath::new(source_path)?,
        destination,
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
        let input = if source == "big" {
            test_source_storage(root.path(), "nas-preflight")?.0
        } else {
            let source_protocol = Arc::new(MemoryS3::default());
            source_protocol
                .objects
                .lock()
                .await
                .insert(final_path.to_owned(), Bytes::from_static(b"x"));
            bucket(&source_protocol)?
        };
        let Err(failure) = transfer(request(input, final_path, bucket(&protocol)?)?).await else {
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
        artifact_name("file", ArtifactKind::Upload),
        artifact_name("native", ArtifactKind::Upload),
    ];
    for pointer in &planted {
        protocol
            .objects
            .lock()
            .await
            .insert(pointer.clone(), Bytes::from_static(b"a leftover pointer"));
    }
    let (local, _) = test_source_storage(root.path(), "nas-checkpointed")?;
    let streamed = request(local, "file", bucket(&protocol)?)?
        .with_transfer_policy(TransferPolicy::Checkpointed)
        .with_read_back_verification(ReadBackVerification::Disabled);
    let outcome = transfer(streamed).await?;
    assert_eq!(outcome.route, TransferRoute::Streaming);
    assert_eq!(
        outcome.recovery,
        EffectiveRecovery::UnsupportedByDestination
    );
    assert_eq!(outcome.prepare, PrepareFact::Fresh);
    assert_eq!(
        protocol.nas().heads,
        1,
        "one HEAD of the final key, none of a pointer"
    );
    assert_eq!(*protocol.puts.lock().await, 1);

    protocol
        .objects
        .lock()
        .await
        .insert("src/native".into(), Bytes::from(vec![8; 100 * 1024]));
    let native = request(
        source_storage(&protocol, "src")?,
        "native",
        bucket(&protocol)?,
    )?
    .with_transfer_policy(TransferPolicy::Checkpointed)
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
        let other = request(local, "file", bucket(&protocol)?)?
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

/// Over the single-PUT threshold a NAS bucket gets one streamed `PutObject` under every policy —
/// no multipart request — holding exactly the source's bytes; `Checkpointed` reports
/// `UnsupportedByDestination`.
#[tokio::test]
async fn an_object_over_the_threshold_is_one_streamed_put() -> TestResult {
    let root = tempfile::tempdir()?;
    let bytes: Vec<u8> = (0..=250_u8).cycle().take(8 * 1024 * 1024 + 1).collect();
    fs::write(root.path().join("big"), &bytes)?;
    for policy in [
        TransferPolicy::Checkpointed,
        TransferPolicy::AtomicReplace,
        TransferPolicy::Direct,
    ] {
        let protocol = nas();
        let (local, _) = test_source_storage(root.path(), "nas-streamed")?;
        let outcome = transfer(
            request(local, "big", bucket(&protocol)?)?
                .with_transfer_policy(policy)
                .with_read_back_verification(ReadBackVerification::Disabled),
        )
        .await?;
        let expected = if policy == TransferPolicy::Checkpointed {
            EffectiveRecovery::UnsupportedByDestination
        } else {
            EffectiveRecovery::Disabled
        };
        assert_eq!(outcome.recovery, expected, "{policy:?}");
        assert_eq!(
            protocol.objects.lock().await.get("big").map(Bytes::as_ref),
            Some(bytes.as_slice()),
            "{policy:?}"
        );
        assert_eq!(protocol.streaming().puts, 1, "{policy:?}");
        assert_eq!(*protocol.multipart_begins.lock().await, 0, "{policy:?}");
    }
    Ok(())
}

/// The outcome says how the copy was checked (N4): read back bracketed by `ETag` on a NAS bucket,
/// pinned on an object bucket and on a filesystem, not at all when read-back is off; the upload's
/// MD5 was confirmed by the S3 stores, and has no meaning for a filesystem.
#[tokio::test]
async fn the_outcome_says_how_the_copy_was_checked() -> TestResult {
    let root = tempfile::tempdir()?;
    let big: Vec<u8> = (0..=250_u8).cycle().take(8 * 1024 * 1024 + 1).collect();
    fs::write(root.path().join("big"), &big)?;
    fs::write(root.path().join("small"), vec![5; 100 * 1024])?;
    let bracketed = ReadBackMethod::EtagBracketed {
        reason: UnpinnedReadReason::ConditionalReadsUnsupported,
    };
    let local_destination = tempfile::tempdir()?;
    for (label, destination, read_back, expected) in [
        (
            "nas big",
            bucket(&nas())?,
            true,
            (bracketed, UploadDigest::Matched),
        ),
        (
            "nas small",
            bucket(&nas())?,
            true,
            (bracketed, UploadDigest::Matched),
        ),
        (
            "nas off",
            bucket(&nas())?,
            false,
            (ReadBackMethod::NotPerformed, UploadDigest::Matched),
        ),
        (
            "object",
            bucket(&Arc::new(MemoryS3::default()))?,
            true,
            (ReadBackMethod::Pinned, UploadDigest::Matched),
        ),
        (
            "local",
            test_destination_storage(local_destination.path(), "nas-verification")?,
            true,
            (
                ReadBackMethod::Pinned,
                UploadDigest::Unchecked(UploadDigestGap::NotApplicable),
            ),
        ),
    ] {
        let (local, _) = test_source_storage(root.path(), "nas-verification-source")?;
        let source = if label == "nas small" { "small" } else { "big" };
        let outcome = transfer(
            request(local, source, destination)?.with_read_back_verification(if read_back {
                ReadBackVerification::Enabled
            } else {
                ReadBackVerification::Disabled
            }),
        )
        .await
        .map_err(|failure| format!("{label}: {failure:?}"))?;
        let verification = outcome.verification;
        assert_eq!(
            (verification.read_back_method, verification.upload_digest),
            expected,
            "{label}"
        );
    }
    Ok(())
}

/// Both expert halves to a NAS bucket: `Checkpointed` reports `UnsupportedByDestination` and the
/// read-back is bracketed.
#[tokio::test]
async fn an_expert_copy_to_a_nas_bucket_reports_what_it_could_do() -> TestResult {
    let protocol = nas();
    let storage = bucket(&protocol)?;
    let body = Bytes::from(vec![3_u8; 100 * 1024]);
    protocol
        .put_object("src/file", body.clone().into(), &content_md5(&body))
        .await
        .map_err(|failure| format!("{failure:?}"))?;
    let path = StoragePath::new("src/file")?;
    let described = storage.read_source()?.describe(&path).await?;
    let observation = ObservedEntry::new(
        path,
        EntryKind::File,
        Some(body.len() as u64),
        None,
        described.source_identity,
    )?;
    let source = ExpertSourceSession::open(ExpertSourceRequest::new(
        storage.clone(),
        observation.clone(),
        CancellationToken::new(),
    ))
    .await?;
    let session = ExpertDestinationSession::prepare(
        ExpertDestinationRequest::new(
            observation,
            source.offer().maximum_chunk_bytes,
            storage.clone(),
            StoragePath::new("dst/expert")?,
            CancellationToken::new(),
        )
        .with_transfer_policy(TransferPolicy::Checkpointed),
    )
    .await?;
    let mut payload = source.stream_from(session.write_offset())?;
    let (sender, receiver) = mpsc::unbounded();
    let pump = async move {
        while let Some(chunk) = payload.next_chunk().await? {
            if sender.unbounded_send(Ok(chunk)).is_err() {
                break;
            }
        }
        drop(sender);
        payload.finish().await
    };
    let (written, evidence) = futures::join!(session.write(Box::pin(receiver)), pump);
    let outcome = written?.complete(evidence?).await?;
    assert_eq!(
        outcome.recovery,
        EffectiveRecovery::UnsupportedByDestination
    );
    assert_eq!(
        outcome.verification.read_back_method,
        ReadBackMethod::EtagBracketed {
            reason: UnpinnedReadReason::ConditionalReadsUnsupported,
        }
    );
    assert_eq!(protocol.objects.lock().await.get("dst/expert"), Some(&body));
    Ok(())
}

/// A server-side copy sent no bytes: its digest is `Unchecked(NativeCopy)`, whether one
/// `CopyObject` or an upload filled with `UploadPartCopy`.
#[tokio::test]
async fn a_native_copy_confirms_no_upload_digest() -> TestResult {
    for (label, size) in [("single", 1024 * 1024), ("parts", 12 * 1024 * 1024)] {
        let protocol = Arc::new(MemoryS3::default());
        protocol
            .objects
            .lock()
            .await
            .insert("src/file".into(), Bytes::from(vec![4; size]));
        let connect = || {
            connect_native_at_destination(
                protocol.clone(),
                endpoint_of(&protocol),
                64 * 1024 * 1024,
                (5 * 1024 * 1024, 5 * 1024 * 1024),
            )
        };
        let outcome = transfer(
            request(source_storage(&protocol, "src")?, "file", connect()?)?
                .with_read_back_verification(ReadBackVerification::Disabled),
        )
        .await
        .map_err(|failure| format!("{label}: {failure:?}"))?;
        assert_eq!(outcome.route, TransferRoute::Native, "{label}");
        assert_eq!(
            outcome.verification.upload_digest,
            UploadDigest::Unchecked(UploadDigestGap::NativeCopy),
            "{label}"
        );
    }
    Ok(())
}

/// A native copy within one NAS bucket (N5) is one `CopyObject` even over the native single-copy
/// limit (no `UploadPartCopy`, no multipart request), read back bracketed, and reports
/// `UnsupportedByDestination` under `Checkpointed`.
#[tokio::test]
async fn a_native_copy_within_a_nas_bucket_is_one_copy_object() -> TestResult {
    let protocol = nas();
    let body = Bytes::from(vec![6_u8; 12 * 1024 * 1024]);
    protocol
        .put_object("src/big", body.clone().into(), &content_md5(&body))
        .await
        .map_err(|failure| format!("{failure:?}"))?;
    let connect = || {
        connect_native_at_destination(
            protocol.clone(),
            endpoint_of(&protocol),
            64 * 1024 * 1024,
            (5 * 1024 * 1024, 5 * 1024 * 1024),
        )
    };
    let outcome = transfer(
        request(source_storage(&protocol, "src")?, "big", connect()?)?
            .with_transfer_policy(TransferPolicy::Checkpointed),
    )
    .await?;
    assert_eq!(outcome.route, TransferRoute::Native);
    assert_eq!(
        outcome.recovery,
        EffectiveRecovery::UnsupportedByDestination
    );
    assert_eq!(
        outcome.verification.read_back_method,
        ReadBackMethod::EtagBracketed {
            reason: UnpinnedReadReason::ConditionalReadsUnsupported,
        }
    );
    assert_eq!(
        outcome.verification.upload_digest,
        UploadDigest::Unchecked(UploadDigestGap::NativeCopy)
    );
    assert_eq!(*protocol.native_copies.lock().await, 1);
    assert_eq!(*protocol.part_copies.lock().await, 0);
    assert_eq!(*protocol.multipart_begins.lock().await, 0);
    assert_eq!(protocol.objects.lock().await.get("big"), Some(&body));
    Ok(())
}
