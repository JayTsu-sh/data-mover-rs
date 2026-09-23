use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use super::engine::{TransferDataPath, TransferPhase, TransferSide, run_until_transferred};
use super::{
    ExpertDestinationRequest, ExpertDestinationSession, ExpertSourceRequest, ExpertSourceSession,
    InflightLimits, SourceQosGroup, SourceQosPolicy, TransferIdentity, TransferPolicy,
    TransferRequest, transfer,
};
use crate::metadata::{
    AclTarget, ApplicationOutcome, MetadataPlanRequest, MetadataPolicies, MetadataPolicy,
    MetadataTarget, OwnershipTarget, TimestampTargetCapability, ValueTarget, compile_metadata_plan,
};
use crate::model::{
    ExtendedAttribute, MetadataObservation, MetadataObservations, MetadataProvenance, ObjectTag,
    ObservedEntry,
};

use crate::model::StoragePath;
use crate::storage::PublicationDisposition;
use crate::storage::RecoveryIdentity;
use crate::storage::Storage;
use crate::storage::backends::local::{
    source::LocalReadSource, test_destination_storage, test_destination_storage_with_role,
    test_source_storage, test_unsupported_storage,
};

fn observed<T>(value: T) -> MetadataObservation<T> {
    MetadataObservation::Value {
        value,
        provenance: MetadataProvenance::Inline,
    }
}

fn local_metadata_target() -> MetadataTarget {
    MetadataTarget {
        acl: AclTarget::Encoding(crate::model::AclEncoding::Posix),
        xattrs: ValueTarget::Supported,
        tags: ValueTarget::NotApplicable,
        ownership_mode: OwnershipTarget::Numeric,
        timestamps: TimestampTargetCapability::Unsupported,
    }
}

fn local_xattr_plan() -> Result<crate::metadata::MetadataPlan, Box<dyn std::error::Error>> {
    let observations = MetadataObservations::new(
        MetadataObservation::NotRequested,
        observed(vec![ExtendedAttribute::new(
            b"user.data-mover-order".to_vec(),
            b"before-publish".to_vec(),
        )?]),
        MetadataObservation::NotRequested,
        MetadataObservation::NotRequested,
        MetadataObservation::NotRequested,
    )?;
    Ok(compile_metadata_plan(&MetadataPlanRequest {
        observations: &observations,
        target: local_metadata_target(),
        policies: MetadataPolicies::default().with_xattrs(MetadataPolicy::RequireExact),
        principal_mapper: None,
    })?)
}

async fn expert_observation(
    storage: &Storage,
    path: &str,
) -> Result<ObservedEntry, Box<dyn std::error::Error>> {
    let descriptor = storage
        .read_source(&crate::storage::PreflightPolicy::production())?
        .describe(&StoragePath::new(path)?)
        .await?;
    Ok(ObservedEntry::new(
        descriptor.path,
        descriptor.kind,
        descriptor.size,
        None,
        descriptor.source_identity,
    )?)
}

async fn complete_expert_transfer(
    identity: &str,
    source: Storage,
    destination: Storage,
    observation: ObservedEntry,
    plan: crate::metadata::MetadataPlan,
    final_path: &str,
    policy: TransferPolicy,
) -> Result<super::TransferOutcome, super::TransferFailure> {
    let limits = InflightLimits::new(2, 128 * 1024, 2)
        .unwrap_or_else(|error| panic!("valid test inflight limits: {error}"));
    let source = ExpertSourceSession::open(ExpertSourceRequest::new(
        TransferIdentity::new(identity)
            .unwrap_or_else(|error| panic!("valid test identity: {error}")),
        source,
        observation.clone(),
        limits,
        tokio_util::sync::CancellationToken::new(),
    ))
    .await?;
    let destination = ExpertDestinationSession::prepare(
        ExpertDestinationRequest::new(
            TransferIdentity::new(identity)
                .unwrap_or_else(|error| panic!("valid test identity: {error}")),
            observation,
            source.offer().maximum_chunk_bytes,
            destination,
            StoragePath::new(final_path)
                .unwrap_or_else(|error| panic!("valid test destination: {error}")),
            limits,
            tokio_util::sync::CancellationToken::new(),
        )
        .with_transfer_policy(policy)
        .with_metadata_plan(plan),
    )
    .await?;
    let mut source_stream = source.stream_from(destination.write_offset())?;
    let mut chunks = Vec::new();
    while let Some(chunk) = source_stream.next_chunk().await? {
        chunks.push(Ok(chunk));
    }
    let evidence = source_stream.finish().await?;
    let transferred = destination
        .write(Box::pin(futures::stream::iter(chunks)))
        .await?;
    transferred.complete(evidence).await
}

#[cfg(unix)]
#[tokio::test]
async fn expert_metadata_is_applied_to_the_stage_before_publication()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = TestRoot::new("expert-metadata-source")?;
    let destination_root = TestRoot::new("expert-metadata-destination")?;
    std::fs::write(source_root.path().join("source.bin"), b"metadata payload")?;
    let source = local_source(source_root.path())?;
    let destination = local_destination(destination_root.path())?;
    let observation = expert_observation(&source, "source.bin").await?;
    let plan = local_xattr_plan()?;

    let outcome = complete_expert_transfer(
        "expert-metadata-success",
        source,
        destination,
        observation,
        plan,
        "published.bin",
        TransferPolicy::AtomicReplace,
    )
    .await?;

    assert_eq!(
        xattr::get(
            destination_root.path().join("published.bin"),
            "user.data-mover-order"
        )?,
        Some(b"before-publish".to_vec())
    );
    let report = outcome
        .metadata
        .ok_or("successful metadata transfer omitted its application report")?;
    assert!(report.outcomes().iter().any(|item| {
        item.family == crate::metadata::MetadataFamily::Xattrs
            && item.outcome == ApplicationOutcome::Applied
    }));
    Ok(())
}
#[tokio::test]
async fn expert_metadata_failure_does_not_publish_and_retains_the_stage()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = TestRoot::new("expert-metadata-failure-source")?;
    let destination_root = TestRoot::new("expert-metadata-failure-destination")?;
    std::fs::write(source_root.path().join("source.bin"), b"metadata payload")?;
    let source = local_source(source_root.path())?;
    let destination = local_destination(destination_root.path())?;
    let observation = expert_observation(&source, "source.bin").await?;
    let observations = MetadataObservations::new(
        MetadataObservation::NotRequested,
        MetadataObservation::NotRequested,
        observed(vec![ObjectTag::new("class", "restricted")?]),
        MetadataObservation::NotRequested,
        MetadataObservation::NotRequested,
    )?;
    let plan = compile_metadata_plan(&MetadataPlanRequest {
        observations: &observations,
        target: MetadataTarget {
            tags: ValueTarget::Supported,
            ..local_metadata_target()
        },
        policies: MetadataPolicies::default().with_tags(MetadataPolicy::RequireExact),
        principal_mapper: None,
    })?;

    let Err(error) = complete_expert_transfer(
        "expert-metadata-failure",
        source,
        destination,
        observation,
        plan,
        "must-not-publish.bin",
        TransferPolicy::AtomicReplace,
    )
    .await
    else {
        return Err("unsupported staged tags unexpectedly succeeded".into());
    };

    assert_eq!(error.phase(), TransferPhase::Metadata);
    assert_eq!(error.side(), TransferSide::Destination);
    assert!(error.has_unpublished_stage());
    assert!(!error.final_destination_changed());
    assert!(
        !destination_root
            .path()
            .join("must-not-publish.bin")
            .exists()
    );
    error.discard_stage().await?;
    Ok(())
}

#[test]
fn transfer_inputs_reject_ambiguous_identity_and_unbounded_limits() {
    assert!(TransferIdentity::new("").is_err());
    assert!(TransferIdentity::new("logical-copy-7").is_ok());
    assert!(InflightLimits::new(0, 64 * 1024, 1).is_err());
    assert!(InflightLimits::new(2, 0, 1).is_err());
    assert!(InflightLimits::new(2, 64 * 1024, 0).is_err());
    assert!(InflightLimits::new(2, 64 * 1024, 1).is_ok());
    assert!(RecoveryIdentity::from_bytes(bytes::Bytes::new()).is_err());
    let opaque = RecoveryIdentity::from_bytes(bytes::Bytes::from_static(b"secret-stage"))
        .unwrap_or_else(|error| panic!("unexpected recovery identity failure: {error}"));
    assert_eq!(format!("{opaque:?}"), "RecoveryIdentity(<opaque>)");
}

#[tokio::test]
async fn source_qos_shapes_local_reads_once_and_reports_truthful_work()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = TestRoot::new("qos-source")?;
    let destination_root = TestRoot::new("qos-destination")?;
    let payload = vec![0x5a; 100 * 1024];
    std::fs::write(source_root.path().join("source.bin"), &payload)?;
    let qos = SourceQosGroup::new(SourceQosPolicy::new(None, 16 * 1024, None)?);

    let outcome = transfer(
        transfer_request(
            local_source(source_root.path())?,
            local_destination(destination_root.path())?,
            tokio_util::sync::CancellationToken::new(),
        )?
        .with_source_qos(qos),
    )
    .await?;

    assert_eq!(outcome.source_qos.logical_bytes, payload.len() as u64);
    assert_eq!(
        outcome.source_qos.client_streamed_shaped_bytes,
        payload.len() as u64
    );
    assert_eq!(outcome.source_qos.source_read_operations, 7);
    assert_eq!(outcome.source_qos.native_bytes, 0);
    assert_eq!(outcome.source_qos.native_requests, 0);
    assert!(!outcome.source_qos.native_payload_shaped);
    assert_eq!(
        std::fs::read(destination_root.path().join("final.bin"))?,
        payload
    );
    Ok(())
}

#[tokio::test]
async fn source_qos_cancellation_wait_is_fast_and_does_not_charge_an_unissued_read()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = TestRoot::new("qos-cancel-source")?;
    let destination_root = TestRoot::new("qos-cancel-destination")?;
    std::fs::write(source_root.path().join("source.bin"), b"payload")?;
    let qos = SourceQosGroup::new(SourceQosPolicy::new(Some((1, 1, Duration::ZERO)), 1, None)?);
    let cancel = tokio_util::sync::CancellationToken::new();
    let request = transfer_request(
        local_source(source_root.path())?,
        local_destination(destination_root.path())?,
        cancel.clone(),
    )?
    .with_source_qos(qos);
    let started = std::time::Instant::now();
    let attempt = tokio::spawn(transfer(request));
    let staging = destination_root.path();
    for _ in 0..100 {
        if staging_entry_count(staging)? > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    cancel.cancel();
    let result = attempt.await?;
    let Err(error) = result else {
        return Err("cancelled QoS transfer unexpectedly succeeded".into());
    };

    assert!(started.elapsed() < Duration::from_millis(500));
    assert_eq!(error.source_qos().logical_bytes, 7);
    assert_eq!(error.source_qos().client_streamed_shaped_bytes, 0);
    assert_eq!(error.source_qos().source_read_operations, 0);
    assert!(!error.has_recoverable_stage());
    assert!(error.has_unpublished_stage());
    error.discard_stage().await?;
    Ok(())
}

#[tokio::test]
async fn concurrent_public_transfers_share_one_source_qos_group_without_starvation()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = TestRoot::new("qos-shared-source")?;
    let first_destination = TestRoot::new("qos-shared-first")?;
    let second_destination = TestRoot::new("qos-shared-second")?;
    std::fs::write(source_root.path().join("source.bin"), b"x")?;
    let group = SourceQosGroup::new(SourceQosPolicy::new(
        None,
        1,
        Some((20, 20, Duration::ZERO)),
    )?);
    let first = transfer_request(
        local_source(source_root.path())?,
        local_destination(first_destination.path())?,
        tokio_util::sync::CancellationToken::new(),
    )?
    .with_source_qos(group.clone());
    let second = transfer_request(
        local_source(source_root.path())?,
        local_destination(second_destination.path())?,
        tokio_util::sync::CancellationToken::new(),
    )?
    .with_source_qos(group);

    let started = std::time::Instant::now();
    let (first, second) = tokio::join!(transfer(first), transfer(second));
    let first = first?;
    let second = second?;
    assert!(started.elapsed() >= Duration::from_millis(90));
    assert_eq!(first.source_qos.source_read_operations, 1);
    assert_eq!(second.source_qos.source_read_operations, 1);
    assert_eq!(first.source_qos.client_streamed_shaped_bytes, 1);
    assert_eq!(second.source_qos.client_streamed_shaped_bytes, 1);
    Ok(())
}

#[tokio::test]
async fn local_transfer_verifies_blake3_then_atomically_overwrites_final()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = TestRoot::new("publish-source")?;
    let destination_root = TestRoot::new("publish-destination")?;
    let payload = vec![0x37; 192 * 1024 + 11];
    std::fs::write(source_root.path().join("source.bin"), &payload)?;
    std::fs::write(destination_root.path().join("final.bin"), b"old-final")?;

    let outcome = transfer(transfer_request(
        local_source(source_root.path())?,
        local_destination(destination_root.path())?,
        tokio_util::sync::CancellationToken::new(),
    )?)
    .await?;

    assert_eq!(outcome.disposition, PublicationDisposition::Published);
    assert_eq!(outcome.transferred_bytes, payload.len() as u64);
    assert_eq!(outcome.blake3, Some(*blake3::hash(&payload).as_bytes()));
    assert_eq!(
        std::fs::read(destination_root.path().join("final.bin"))?,
        payload
    );
    assert_eq!(staging_entry_count(destination_root.path())?, 0);
    Ok(())
}

#[tokio::test]
async fn single_chunk_local_transfer_omits_recovery_checkpoint()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = TestRoot::new("single-chunk-source")?;
    let destination_root = TestRoot::new("single-chunk-destination")?;
    let payload = vec![0x4b; 4 * 1024];
    std::fs::write(source_root.path().join("source.bin"), &payload)?;
    let (destination, destination_role) =
        test_destination_storage_with_role(destination_root.path(), "single-chunk-destination")?;
    destination_role.fail_checkpoint_at(1);
    let outcome = transfer(
        transfer_request(
            local_source(source_root.path())?,
            destination,
            tokio_util::sync::CancellationToken::new(),
        )?
        .with_transfer_policy(TransferPolicy::Checkpointed),
    )
    .await?;

    assert_eq!(outcome.transferred_bytes, payload.len() as u64);
    assert_eq!(
        std::fs::read(destination_root.path().join("final.bin"))?,
        payload
    );
    assert_eq!(
        outcome.recovery,
        super::engine::EffectiveRecovery::SkippedSingleSourceChunk
    );
    assert_eq!(staging_entry_count(destination_root.path())?, 0);
    Ok(())
}

#[tokio::test]
async fn checkpointed_multichunk_below_threshold_skips_checkpoint_setup()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = TestRoot::new("multi-chunk-source")?;
    let destination_root = TestRoot::new("multi-chunk-destination")?;
    let payload = vec![0x6d; 128 * 1024 + 1];
    std::fs::write(source_root.path().join("source.bin"), &payload)?;
    let (destination, destination_role) =
        test_destination_storage_with_role(destination_root.path(), "multi-chunk-destination")?;
    destination_role.fail_checkpoint_at(1);

    let result = transfer(recoverable_request(
        transfer_request(
            local_source(source_root.path())?,
            destination,
            tokio_util::sync::CancellationToken::new(),
        )?,
        None,
    ))
    .await;

    let outcome = result?;
    assert_eq!(
        outcome.recovery,
        super::engine::EffectiveRecovery::SkippedBelowCheckpointThreshold
    );
    assert_eq!(
        std::fs::read(destination_root.path().join("final.bin"))?,
        payload
    );
    assert_eq!(staging_entry_count(destination_root.path())?, 0);
    Ok(())
}

#[tokio::test]
async fn disabled_multi_chunk_transfer_omits_recovery_checkpoint()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = TestRoot::new("disabled-multi-chunk-source")?;
    let destination_root = TestRoot::new("disabled-multi-chunk-destination")?;
    let payload = vec![0x71; 128 * 1024 + 1];
    std::fs::write(source_root.path().join("source.bin"), &payload)?;
    let (destination, destination_role) = test_destination_storage_with_role(
        destination_root.path(),
        "disabled-multi-chunk-destination",
    )?;
    destination_role.fail_checkpoint_at(1);

    let outcome = transfer(
        transfer_request(
            local_source(source_root.path())?,
            destination,
            tokio_util::sync::CancellationToken::new(),
        )?
        .with_transfer_policy(TransferPolicy::AtomicReplace),
    )
    .await?;

    assert_eq!(outcome.transferred_bytes, payload.len() as u64);
    assert_eq!(
        std::fs::read(destination_root.path().join("final.bin"))?,
        payload
    );
    assert_eq!(staging_entry_count(destination_root.path())?, 0);
    Ok(())
}

#[tokio::test]
async fn streaming_transfer_uses_one_source_stream_with_inflight_reads()
-> Result<(), Box<dyn std::error::Error>> {
    const CHUNK_BYTES: usize = 64 * 1024;
    let source_root = TestRoot::new("source-range-inflight")?;
    let destination_root = TestRoot::new("destination-range-inflight")?;
    let payload = vec![0x6d; 4 * CHUNK_BYTES];
    std::fs::write(source_root.path().join("source.bin"), &payload)?;
    let (source, source_role) = test_source_storage(source_root.path(), "inflight-source")?;
    source_role.delay_reads(Duration::from_millis(30));
    let request = TransferRequest::new(
        TransferIdentity::new("source-range-inflight")?,
        source,
        StoragePath::new("source.bin")?,
        local_destination(destination_root.path())?,
        StoragePath::new("final.bin")?,
        InflightLimits::new(4, CHUNK_BYTES, 4)?,
        tokio_util::sync::CancellationToken::new(),
    )
    .with_transfer_policy(TransferPolicy::AtomicReplace);

    transfer(request).await?;

    assert!(
        source_role.peak_read_concurrency() >= 2,
        "the backend source stream must keep more than one read in flight"
    );
    assert_eq!(
        source_role.read_stream_count(),
        1,
        "one transfer must use one backend source stream"
    );
    assert_eq!(
        std::fs::read(destination_root.path().join("final.bin"))?,
        payload
    );
    Ok(())
}

#[tokio::test]
async fn verification_mismatch_fast_fails_without_changing_final()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = TestRoot::new("verify-failure-source")?;
    let destination_root = TestRoot::new("verify-failure-destination")?;
    std::fs::write(source_root.path().join("source.bin"), vec![0x22; 64 * 1024])?;
    std::fs::write(destination_root.path().join("final.bin"), b"old-final")?;
    let (destination, role) =
        test_destination_storage_with_role(destination_root.path(), "verify-failure-destination")?;
    role.corrupt_before_verify();

    let qos = SourceQosGroup::new(SourceQosPolicy::new(None, 16 * 1024, None)?);
    let result = transfer(
        transfer_request(
            local_source(source_root.path())?,
            destination,
            tokio_util::sync::CancellationToken::new(),
        )?
        .with_source_qos(qos),
    )
    .await;
    let Err(error) = result else {
        return Err("corrupt staged content unexpectedly published".into());
    };

    assert_eq!(error.phase(), TransferPhase::Verify);
    assert!(!error.has_recoverable_stage());
    assert!(error.has_unpublished_stage());
    assert_eq!(error.source_qos().logical_bytes, 64 * 1024);
    assert_eq!(error.source_qos().client_streamed_shaped_bytes, 64 * 1024);
    assert_eq!(error.source_qos().source_read_operations, 4);
    assert_eq!(
        std::fs::read(destination_root.path().join("final.bin"))?,
        b"old-final"
    );
    error.discard_stage().await?;
    Ok(())
}
#[tokio::test]
async fn failure_after_atomic_publication_reports_changed_final_not_recoverable_stage()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = TestRoot::new("post-commit-source")?;
    let destination_root = TestRoot::new("post-commit-destination")?;
    std::fs::write(source_root.path().join("source.bin"), b"new-final")?;
    std::fs::write(destination_root.path().join("final.bin"), b"old-final")?;
    let (destination, role) =
        test_destination_storage_with_role(destination_root.path(), "post-commit-destination")?;
    role.fail_after_publication_commit();

    let result = transfer(transfer_request(
        local_source(source_root.path())?,
        destination,
        tokio_util::sync::CancellationToken::new(),
    )?)
    .await;
    let Err(error) = result else {
        return Err("injected post-commit failure unexpectedly succeeded".into());
    };
    assert_eq!(error.phase(), TransferPhase::Publish);
    assert!(error.final_destination_changed());
    assert!(!error.has_recoverable_stage());
    assert!(error.has_pending_cleanup());
    assert_eq!(
        std::fs::read(destination_root.path().join("final.bin"))?,
        b"new-final"
    );
    error.cleanup_published_stage().await?;
    assert_eq!(staging_entry_count(destination_root.path())?, 0);
    Ok(())
}
#[tokio::test]
async fn enabled_identity_reuses_a_reobserved_complete_local_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = TestRoot::new("resume-source")?;
    let destination_root = TestRoot::new("resume-destination")?;
    let payload = vec![0x61; 192 * 1024 + 7];
    std::fs::write(source_root.path().join("source.bin"), &payload)?;
    std::fs::write(destination_root.path().join("final.bin"), b"existing")?;
    let (destination, role) =
        test_destination_storage_with_role(destination_root.path(), "transfer-destination")?;
    role.set_automatic_checkpoint_interval(64 * 1024);
    let first = run_until_transferred(recoverable_request(
        transfer_request(
            local_source(source_root.path())?,
            destination.clone(),
            tokio_util::sync::CancellationToken::new(),
        )?,
        None,
    ))
    .await?;
    drop(first);
    let writes_before_resume = role.write_completion_count();

    let outcome = transfer(recoverable_request(
        transfer_request(
            local_source(source_root.path())?,
            destination,
            tokio_util::sync::CancellationToken::new(),
        )?,
        None,
    ))
    .await?;

    assert_eq!(outcome.transferred_bytes, payload.len() as u64);
    assert_eq!(role.write_completion_count(), writes_before_resume);
    assert_eq!(
        std::fs::read(destination_root.path().join("final.bin"))?,
        payload
    );
    assert_eq!(staging_entry_count(destination_root.path())?, 0);
    Ok(())
}

#[tokio::test]
async fn cancellation_preserves_and_resumes_a_partial_durable_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = TestRoot::new("partial-resume-source")?;
    let destination_root = TestRoot::new("partial-resume-destination")?;
    let payload = vec![0x73; 3 * 64 * 1024];
    std::fs::write(source_root.path().join("source.bin"), &payload)?;
    let (source, source_role) = test_source_storage(source_root.path(), "transfer-source")?;
    let second_read = source_role.gate_read_at(64 * 1024);
    let (destination, destination_role) =
        test_destination_storage_with_role(destination_root.path(), "transfer-destination")?;
    destination_role.set_automatic_checkpoint_interval(64 * 1024);
    let cancel = tokio_util::sync::CancellationToken::new();
    let request = recoverable_request(
        transfer_request(source, destination.clone(), cancel.clone())?,
        None,
    );
    let task = tokio::spawn(transfer(request));
    second_read.wait_started().await;
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while destination_role.write_completion_count() < 1 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    cancel.cancel();
    second_read.release();
    let result = task.await?;
    let Err(failure) = result else {
        return Err("partially cancelled transfer unexpectedly succeeded".into());
    };
    drop(failure);
    let writes_before = destination_role.write_completion_count();
    assert_eq!(writes_before, 1);
    let qos = SourceQosGroup::new(SourceQosPolicy::new(None, 64 * 1024, None)?);

    let outcome = transfer(
        recoverable_request(
            transfer_request(
                local_source(source_root.path())?,
                destination,
                tokio_util::sync::CancellationToken::new(),
            )?,
            None,
        )
        .with_source_qos(qos),
    )
    .await?;
    assert_eq!(outcome.transferred_bytes, payload.len() as u64);
    assert_eq!(destination_role.write_completion_count() - writes_before, 2);
    assert_eq!(outcome.source_qos.logical_bytes, payload.len() as u64);
    assert_eq!(
        outcome.source_qos.client_streamed_shaped_bytes,
        payload.len() as u64
    );
    assert_eq!(outcome.source_qos.source_read_operations, 3);
    assert_eq!(
        std::fs::read(destination_root.path().join("final.bin"))?,
        payload
    );
    Ok(())
}
#[tokio::test]
async fn recovery_restarts_after_same_size_source_edit() -> Result<(), Box<dyn std::error::Error>> {
    for verification in [
        super::ReadBackVerification::Enabled,
        super::ReadBackVerification::Disabled,
    ] {
        let source_root = TestRoot::new("changed-resume-source")?;
        let destination_root = TestRoot::new("changed-resume-destination")?;
        let path = source_root.path().join("source.bin");
        let payload = vec![0x73; 3 * 64 * 1024];
        std::fs::write(&path, &payload)?;
        let original_mtime = std::fs::metadata(&path)?.modified()?;
        let (source, source_role) = test_source_storage(source_root.path(), "transfer-source")?;
        let second_read = source_role.gate_read_at(64 * 1024);
        let (destination, destination_role) =
            test_destination_storage_with_role(destination_root.path(), "transfer-destination")?;
        destination_role.set_automatic_checkpoint_interval(64 * 1024);
        let cancel = tokio_util::sync::CancellationToken::new();
        let request = recoverable_request(
            transfer_request(source, destination.clone(), cancel.clone())?,
            None,
        )
        .with_read_back_verification(verification);
        let task = tokio::spawn(transfer(request));
        second_read.wait_started().await;
        tokio::time::timeout(Duration::from_secs(1), async {
            while destination_role.write_completion_count() < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        cancel.cancel();
        second_read.release();
        assert!(task.await?.is_err());
        let writes_before = destination_role.write_completion_count();
        assert_eq!(writes_before, 1);

        // Keep the inode and length, and on Unix restore mtime to exercise ctime.
        let replacement = vec![0xa5; payload.len()];
        std::fs::write(&path, &replacement)?;
        #[cfg(unix)]
        std::fs::File::options()
            .write(true)
            .open(&path)?
            .set_modified(original_mtime)?;
        #[cfg(not(unix))]
        let _ = original_mtime;
        transfer(
            recoverable_request(
                transfer_request(
                    local_source(source_root.path())?,
                    destination,
                    tokio_util::sync::CancellationToken::new(),
                )?,
                None,
            )
            .with_read_back_verification(verification),
        )
        .await?;
        assert_eq!(destination_role.write_completion_count() - writes_before, 3);
        assert_eq!(
            std::fs::read(destination_root.path().join("final.bin"))?,
            replacement
        );
    }
    Ok(())
}

#[tokio::test]
async fn quick_discards_persisted_stage_before_reupload() -> Result<(), Box<dyn std::error::Error>>
{
    let source_root = TestRoot::new("restart-cleans-source")?;
    let destination_root = TestRoot::new("restart-cleans-destination")?;
    let payload = vec![0x39; 2 * 64 * 1024 + 1];
    std::fs::write(source_root.path().join("source.bin"), &payload)?;
    let source = local_source(source_root.path())?;
    let destination = local_destination(destination_root.path())?;
    let checkpointed = recoverable_request(
        transfer_request(
            source.clone(),
            destination.clone(),
            tokio_util::sync::CancellationToken::new(),
        )?,
        None,
    );

    let staged = run_until_transferred(checkpointed).await?;
    drop(staged);
    assert!(staging_entry_count(destination_root.path())? > 0);

    let outcome = transfer(
        transfer_request(
            source,
            destination,
            tokio_util::sync::CancellationToken::new(),
        )?
        .with_transfer_policy(TransferPolicy::AtomicReplace),
    )
    .await?;

    assert_eq!(outcome.recovery, super::engine::EffectiveRecovery::Disabled);
    assert_eq!(
        std::fs::read(destination_root.path().join("final.bin"))?,
        payload
    );
    assert_eq!(staging_entry_count(destination_root.path())?, 0);
    Ok(())
}

#[tokio::test]
async fn one_recovery_binding_can_have_only_one_active_attempt()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = TestRoot::new("claim-source")?;
    let destination_root = TestRoot::new("claim-destination")?;
    let payload = vec![0x52; 1024 * 1024];
    std::fs::write(source_root.path().join("source.bin"), &payload)?;
    std::fs::write(destination_root.path().join("final.bin"), b"existing")?;
    let first = run_until_transferred(recoverable_request(
        transfer_request(
            local_source(source_root.path())?,
            local_destination(destination_root.path())?,
            tokio_util::sync::CancellationToken::new(),
        )?,
        None,
    ))
    .await?;
    drop(first);
    let (source, source_role) = test_source_storage(source_root.path(), "transfer-source")?;
    source_role.delay_reads(std::time::Duration::from_millis(30));
    let first_request = recoverable_request(
        transfer_request(
            source.clone(),
            local_destination(destination_root.path())?,
            tokio_util::sync::CancellationToken::new(),
        )?,
        None,
    );
    let second_request = recoverable_request(
        transfer_request(
            source,
            local_destination(destination_root.path())?,
            tokio_util::sync::CancellationToken::new(),
        )?,
        None,
    );

    let (first_result, second_result) =
        tokio::join!(transfer(first_request), transfer(second_request));
    let successes = usize::from(first_result.is_ok()) + usize::from(second_result.is_ok());
    assert_eq!(successes, 1);
    let failure = if let Err(error) = first_result {
        error
    } else if let Err(error) = second_result {
        error
    } else {
        return Err("both concurrent recoveries unexpectedly succeeded".into());
    };
    assert_eq!(failure.phase(), TransferPhase::RecoveryRegistration);
    assert_eq!(
        std::fs::read(destination_root.path().join("final.bin"))?,
        payload
    );
    Ok(())
}

#[tokio::test]
async fn local_transfer_reaches_durable_unpublished_state() -> Result<(), Box<dyn std::error::Error>>
{
    let source_root = TestRoot::new("source")?;
    let destination_root = TestRoot::new("destination")?;
    let payload = vec![0x5a; 160 * 1024 + 17];
    std::fs::write(source_root.path().join("source.bin"), &payload)?;
    std::fs::write(destination_root.path().join("final.bin"), b"old-final")?;
    let source = local_source(source_root.path())?;
    let destination = local_destination(destination_root.path())?;
    let request = recoverable_request(
        TransferRequest::new(
            TransferIdentity::new("local-copy")?,
            source,
            StoragePath::new("source.bin")?,
            destination,
            StoragePath::new("final.bin")?,
            InflightLimits::new(2, 64 * 1024, 2)?,
            tokio_util::sync::CancellationToken::new(),
        ),
        None,
    );

    let transferred = run_until_transferred(request).await?;

    assert_eq!(transferred.data_path(), TransferDataPath::Streaming);
    assert_eq!(transferred.durable_prefix(), payload.len() as u64);
    assert_eq!(
        std::fs::read(destination_root.path().join("final.bin"))?,
        b"old-final"
    );
    transferred.discard().await?;
    Ok(())
}

#[tokio::test]
async fn cancellation_before_prepare_leaves_no_staged_or_final_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = TestRoot::new("cancel-source")?;
    let destination_root = TestRoot::new("cancel-destination")?;
    std::fs::write(source_root.path().join("source.bin"), b"payload")?;
    std::fs::write(destination_root.path().join("final.bin"), b"old-final")?;
    let cancel = tokio_util::sync::CancellationToken::new();
    cancel.cancel();
    let request = TransferRequest::new(
        TransferIdentity::new("cancelled-copy")?,
        local_source(source_root.path())?,
        StoragePath::new("source.bin")?,
        local_destination(destination_root.path())?,
        StoragePath::new("final.bin")?,
        InflightLimits::new(2, 64 * 1024, 2)?,
        cancel,
    );

    let Err(error) = run_until_transferred(request).await else {
        return Err("pre-cancelled transfer unexpectedly succeeded".into());
    };

    assert_eq!(error.phase(), TransferPhase::Preflight);
    assert_eq!(
        std::fs::read(destination_root.path().join("final.bin"))?,
        b"old-final"
    );
    assert!(!destination_root.path().join(".data-mover-staging").exists());
    Ok(())
}

#[tokio::test]
async fn preflight_refusal_happens_before_destination_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = TestRoot::new("preflight-source")?;
    let destination_root = TestRoot::new("preflight-destination")?;
    std::fs::write(source_root.path().join("source.bin"), b"payload")?;
    let destination = test_unsupported_storage("unsupported-destination")?;
    let request = TransferRequest::new(
        TransferIdentity::new("preflight-copy")?,
        local_source(source_root.path())?,
        StoragePath::new("source.bin")?,
        destination,
        StoragePath::new("final.bin")?,
        InflightLimits::new(1, 4096, 1)?,
        tokio_util::sync::CancellationToken::new(),
    );

    let Err(error) = run_until_transferred(request).await else {
        return Err("unsupported destination unexpectedly transferred".into());
    };
    assert_eq!(error.phase(), TransferPhase::Preflight);
    assert_eq!(error.side(), TransferSide::Destination);
    assert!(!destination_root.path().join(".data-mover-staging").exists());
    assert!(!destination_root.path().join("final.bin").exists());
    Ok(())
}

#[tokio::test]
async fn cancellation_during_describe_stops_before_prepare()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = TestRoot::new("describe-cancel-source")?;
    let destination_root = TestRoot::new("describe-cancel-destination")?;
    std::fs::write(source_root.path().join("source.bin"), b"payload")?;
    let (source, source_role) = test_source_storage(source_root.path(), "describe-source")?;
    source_role.delay_description("source.bin", std::time::Duration::from_millis(200));
    let cancel = tokio_util::sync::CancellationToken::new();
    let request = transfer_request(
        source,
        local_destination(destination_root.path())?,
        cancel.clone(),
    )?;
    let task = tokio::spawn(run_until_transferred(request));
    wait_for_description(&source_role).await?;
    cancel.cancel();

    let Err(error) = task.await? else {
        return Err("describe-cancelled transfer unexpectedly succeeded".into());
    };
    assert_eq!(error.phase(), TransferPhase::Describe);
    assert!(!destination_root.path().join(".data-mover-staging").exists());
    assert!(!destination_root.path().join("final.bin").exists());
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn copied_metadata_follows_the_described_source_identity()
-> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    const OLD_MODE: u32 = 0o640;
    const REPLACEMENT_MODE: u32 = 0o604;
    const REPLACEMENT_MTIME: i64 = 1_710_000_123;

    let source_root = TestRoot::new("metadata-identity-source")?;
    let destination_root = TestRoot::new("metadata-identity-destination")?;
    let source_path = source_root.path().join("source.bin");
    let replacement_path = source_root.path().join("replacement.bin");
    std::fs::write(&source_path, b"old source")?;
    std::fs::set_permissions(&source_path, std::fs::Permissions::from_mode(OLD_MODE))?;
    std::fs::write(&replacement_path, b"replacement source")?;
    std::fs::set_permissions(
        &replacement_path,
        std::fs::Permissions::from_mode(REPLACEMENT_MODE),
    )?;
    filetime::set_file_mtime(
        &replacement_path,
        filetime::FileTime::from_unix_time(REPLACEMENT_MTIME, 0),
    )?;

    let (source, source_role) =
        test_source_storage(source_root.path(), "metadata-identity-source")?;
    source_role.delay_description("source.bin", std::time::Duration::from_millis(200));
    let request = transfer_request(
        source,
        local_destination(destination_root.path())?,
        tokio_util::sync::CancellationToken::new(),
    )?;
    let task = tokio::spawn(transfer(request));
    wait_for_description(&source_role).await?;
    std::fs::rename(&replacement_path, &source_path)?;

    let outcome = task.await??;
    assert!(outcome.metadata.is_some());
    let published_path = destination_root.path().join("final.bin");
    assert_eq!(std::fs::read(&published_path)?, b"replacement source");
    let metadata = std::fs::metadata(published_path)?;
    assert_eq!(metadata.permissions().mode() & 0o7777, REPLACEMENT_MODE);
    assert_eq!(metadata.mtime(), REPLACEMENT_MTIME);
    Ok(())
}

#[tokio::test]
async fn cancellation_during_source_read_preserves_unpublished_stage()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = TestRoot::new("active-cancel-source")?;
    let destination_root = TestRoot::new("active-cancel-destination")?;
    std::fs::write(
        source_root.path().join("source.bin"),
        vec![7_u8; 128 * 1024],
    )?;
    std::fs::write(destination_root.path().join("final.bin"), b"old-final")?;
    let (source, source_role) = test_source_storage(source_root.path(), "delayed-source")?;
    source_role.delay_reads(std::time::Duration::from_millis(200));
    let cancel = tokio_util::sync::CancellationToken::new();
    let request = recoverable_request(
        transfer_request(
            source,
            local_destination(destination_root.path())?,
            cancel.clone(),
        )?,
        None,
    );
    let task = tokio::spawn(run_until_transferred(request));
    wait_for_read(&source_role).await?;
    cancel.cancel();

    let result = task.await?;
    let Err(error) = result else {
        return Err("cancelled active transfer unexpectedly succeeded".into());
    };
    assert_eq!(error.phase(), TransferPhase::Transfer);
    assert_eq!(error.side(), TransferSide::Source);
    assert!(!error.has_recoverable_stage());
    assert_eq!(
        std::fs::read(destination_root.path().join("final.bin"))?,
        b"old-final"
    );
    assert!(staging_entry_count(destination_root.path())? > 0);
    error.discard_stage().await?;
    assert_eq!(staging_entry_count(destination_root.path())?, 0);
    Ok(())
}

fn staging_entry_count(root: &Path) -> io::Result<usize> {
    std::fs::read_dir(root)?.try_fold(0, |count, entry| {
        let entry = entry?;
        Ok(count
            + usize::from(
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".data-mover-"),
            ))
    })
}

fn transfer_request(
    source: Storage,
    destination: Storage,
    cancel: tokio_util::sync::CancellationToken,
) -> Result<TransferRequest, Box<dyn std::error::Error>> {
    Ok(TransferRequest::new(
        TransferIdentity::new("active-cancel-copy")?,
        source,
        StoragePath::new("source.bin")?,
        destination,
        StoragePath::new("final.bin")?,
        // Two 64 KiB reads may be resident together, so the byte budget covers both chunks.
        InflightLimits::new(2, 2 * 64 * 1024, 2)?,
        cancel,
    )
    .with_transfer_policy(TransferPolicy::AtomicReplace))
}

fn recoverable_request(request: TransferRequest, _resume_marker: Option<()>) -> TransferRequest {
    request.with_transfer_policy(TransferPolicy::Checkpointed)
}

async fn wait_for_read(source: &LocalReadSource) -> Result<(), Box<dyn std::error::Error>> {
    wait_for_read_count(source, 1).await
}

async fn wait_for_read_count(
    source: &LocalReadSource,
    count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while source.read_call_count() < count {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}

async fn wait_for_description(source: &LocalReadSource) -> Result<(), Box<dyn std::error::Error>> {
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !source.description_started() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}

fn local_source(root: &Path) -> Result<Storage, Box<dyn std::error::Error>> {
    test_source_storage(root, "transfer-source").map(|(storage, _)| storage)
}

fn local_destination(root: &Path) -> Result<Storage, Box<dyn std::error::Error>> {
    let (storage, role) = test_destination_storage_with_role(root, "transfer-destination")?;
    // Keep recovery fixtures small while exercising the deferred threshold path.
    role.set_automatic_checkpoint_interval(64 * 1024);
    Ok(storage)
}

struct TestRoot(PathBuf);

impl TestRoot {
    fn new(label: &str) -> io::Result<Self> {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "data-mover-transfer-{label}-{}-{id}",
            std::process::id()
        ));
        std::fs::create_dir(&path)?;
        Ok(Self(path))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[path = "local_optimization_tests.rs"]
mod local_optimization;
