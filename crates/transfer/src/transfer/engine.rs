use std::sync::Arc;

use futures::StreamExt as _;

use super::{ReadBackVerification, TransferPolicy, TransferRequest};
use crate::model::{FailureClass, StoragePath};
use crate::runtime::inflight::SequentialRanges;
use crate::storage::{
    CheckpointObservation, NativePair, PrepareFact, PreparedStage, PublicationDisposition,
    PublicationEvidence, ReadBackMethod, ReadRequest, ReadSource, SourceDescriptor,
    SourceQosBudget, SourceQosStats, StagedDestination, StorageRoleFailure, UploadDigest,
    WriteEvidence,
};
use failure::source_failure;
use negotiation::{CopiedMetadataPlan, copied_metadata_plan};
use payload::{TransferEvidence, transfer_stage};
use plan::{
    TransferRoles, describe_source, final_recovery, lend_transfer_roles, plan_request,
    recovery_binding, recovery_binding_for,
};
use producer::{ProducerRequest, produce, read_exact_range};
use publication::{content_mismatch, discard_stale_stage, verify_published};

mod at_destination;
mod checkpointed;
mod delete_marker;
mod directory_object;
mod expert;
mod failure;
mod guard;
mod native;
mod negotiation;
mod observation;
mod payload;
mod plan;
mod producer;
#[cfg(test)]
mod producer_tests;
mod publication;
mod single;
mod version;
pub use delete_marker::{destination_keeps_history, write_delete_marker};
pub use directory_object::{
    DIRECTORY_OBJECT_SKIP_REASON, DIRECTORY_OBJECT_TOO_LARGE_REASON, DirectoryObjectAction,
    DirectoryObjectDestination, directory_object_action,
};
pub use expert::{
    ExpertDestinationRequest, ExpertDestinationSession, ExpertDestinationTransferred,
    ExpertSourceEvidence, ExpertSourceOffer, ExpertSourcePayload, ExpertSourceRequest,
    ExpertSourceSession,
};
pub use failure::TransferFailure;

/// Lifecycle stage at which one transfer attempt stopped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransferPhase {
    Preflight,
    Describe,
    Prepare,
    Transfer,
    Checkpoint,
    Verify,
    Metadata,
    Publish,
}

/// Side responsible for a transfer failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransferSide {
    Source,
    Destination,
    Orchestration,
}

/// Payload route actually selected by data-mover.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransferRoute {
    Streaming,
    Native,
}

/// Effective recovery behavior after route and source-chunk planning.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EffectiveRecovery {
    /// The request explicitly chose non-checkpointed transfer.
    Disabled,
    /// Checkpointing was requested but one source read can cover the complete payload.
    SkippedSingleSourceChunk,
    /// Automatic recovery was not worthwhile for the known payload size.
    SkippedBelowCheckpointThreshold,
    /// The transfer retains durable intermediate progress at the destination: a streamed one, or
    /// a native multipart copy once its parts reach the automatic interval (ADR-0006 C18).
    Checkpointed,
    /// A native copy under `TransferPolicy::Checkpointed` that is one server-side operation (S3:
    /// one `CopyObject` up to 64 MiB): it has no intermediate state and restarts through that
    /// operation's own semantics.
    NotApplicableNative,
    /// `TransferPolicy::Checkpointed` was requested, but the destination keeps no progress for a
    /// later attempt (`StagedDestination::resumable_at_destination`; an ONTAP NAS bucket, P7
    /// §14.2): nothing was looked for or kept there, and a cancelled or failed attempt starts
    /// again from zero. Reported for every size and route.
    UnsupportedByDestination,
}

/// Backend-neutral data path selected during planning.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransferDataPath {
    Streaming,
    Native,
}

#[derive(Clone, Copy)]
struct TransferPlan {
    data_path: TransferDataPath,
    source_size: u64,
    chunk_bytes: usize,
    recovery_enabled: bool,
    effective_recovery: EffectiveRecovery,
    automatic_interval: Option<u64>,
}

pub(crate) struct Transferred {
    identity: super::TransferIdentity,
    destination: Arc<dyn StagedDestination>,
    stage: PreparedStage,
    write: WriteEvidence,
    checkpoint: CheckpointObservation,
    #[allow(dead_code)]
    source: SourceDescriptor,
    data_path: TransferDataPath,
    source_blake3: Option<[u8; 32]>,
    source_qos: Option<SourceQosBudget>,
    native_bytes: u64,
    native_requests: u64,
    effective_recovery: EffectiveRecovery,
    copied_metadata_plan: Option<CopiedMetadataPlan>,
}

/// Successful final outcome of one transfer attempt.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransferOutcome {
    /// The identity the transfer ran under: derived from its endpoints and paths, or the
    /// caller's override.
    pub identity: super::TransferIdentity,
    pub final_destination: StoragePath,
    /// What prepare found at the destination and did about it.
    pub prepare: PrepareFact,
    /// Bytes the destination already held and the transfer did not write again
    /// (`prepare.reused_bytes()`); the source may still have been read for them, for the digest.
    pub reused_bytes: u64,
    pub disposition: PublicationDisposition,
    pub transferred_bytes: u64,
    /// Present only when destination read-back verification was performed.
    pub blake3: Option<[u8; 32]>,
    pub read_back: ReadBackVerification,
    pub source_qos: SourceQosStats,
    pub metadata: Option<crate::metadata::MetadataApplicationReport>,
    pub route: TransferRoute,
    pub recovery: EffectiveRecovery,
    /// The version the publication created, for a destination that versions its objects.
    pub destination_version: Option<String>,
    /// How the copy was checked (P7 §14.2 N4): the read-back method used (`NotPerformed` when
    /// read-back is disabled; `EtagBracketed` with its reason where reads cannot be pinned) and
    /// whether the store confirmed the bytes it received. A weaker method is not a failure.
    pub verification: TransferVerification,
}

/// How one transfer's copy was checked ([`TransferOutcome::verification`]).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransferVerification {
    /// How read-back verification read the destination (`NotPerformed` when it was disabled).
    pub read_back_method: ReadBackMethod,
    /// Whether the store confirmed that the bytes it stored are the bytes sent.
    pub upload_digest: UploadDigest,
}

impl TransferVerification {
    /// What `destination` reports for `stage`, read back or not.
    fn of(destination: &dyn StagedDestination, stage: &PreparedStage, read_back: bool) -> Self {
        Self {
            read_back_method: if read_back {
                destination.read_back_method(stage)
            } else {
                ReadBackMethod::NotPerformed
            },
            upload_digest: destination.upload_digest(stage),
        }
    }
}

/// Transfers, optionally verifies by read-back, and publishes one request.
///
/// When the source and destination advertise baseline metadata support, copies compatible mode,
/// ownership, and mtime facts automatically. Local and NFS preserve numeric uid/gid and mode;
/// HDFS preserves mode while retaining its destination-native owner/group principals. Metadata
/// is observed against the described source identity and applied before publication; callers
/// need not apply it again. A destination that stores none of it (S3) has every family reported
/// as skipped, without the source being read for metadata.
/// Access time and change time are not part of any copy. ACLs and extended attributes are not
/// part of the default one either, but a caller can ask for them with
/// [`TransferRequest::with_copied_metadata`]; whether they are carried then depends on what both
/// ends can do. S3-to-S3 transfers always carry object tags when both ends support them; other
/// backend pairs omit tags. See `.claude/docs/metadata-negotiation.md`.
///
/// # Errors
/// Returns a phase- and side-attributed failure while retaining an owned staged state whenever
/// publication has not completed — except a resumed stage kept at the destination that failed
/// verification, which is cleaned up in place.
pub async fn transfer(request: TransferRequest) -> Result<TransferOutcome, TransferFailure> {
    let read_back = request.read_back;
    let cancel = request.cancel.clone();
    let transferred = run_until_transferred(request).await?;
    let source_qos = transferred_source_qos(&transferred);
    let (evidence, transferred, metadata) =
        publication::complete_transfer(transferred, read_back, cancel, source_qos).await?;
    let expected_size = transferred.checkpoint.durable_prefix;
    let source_digest = transferred.source_blake3;
    let prepare = transferred.stage.prepare_fact();
    let blake3 = if read_back == ReadBackVerification::Enabled {
        source_digest
    } else {
        None
    };
    let verification = TransferVerification::of(
        &*transferred.destination,
        &transferred.stage,
        read_back == ReadBackVerification::Enabled,
    );
    let PublicationEvidence {
        final_destination,
        disposition,
        version: destination_version,
        ..
    } = evidence;
    Ok(TransferOutcome {
        identity: transferred.identity,
        final_destination,
        prepare,
        reused_bytes: prepare.reused_bytes(),
        disposition,
        transferred_bytes: expected_size,
        blake3,
        read_back,
        source_qos,
        metadata,
        route: transfer_route(transferred.data_path),
        recovery: transferred.effective_recovery,
        destination_version,
        verification,
    })
}

const fn transfer_route(data_path: TransferDataPath) -> TransferRoute {
    match data_path {
        TransferDataPath::Streaming => TransferRoute::Streaming,
        TransferDataPath::Native => TransferRoute::Native,
    }
}

fn transferred_source_qos(transferred: &Transferred) -> SourceQosStats {
    transferred.source_qos.as_ref().map_or(
        SourceQosStats {
            logical_bytes: transferred.checkpoint.durable_prefix,
            native_bytes: transferred.native_bytes,
            native_requests: transferred.native_requests,
            ..SourceQosStats::default()
        },
        SourceQosBudget::stats,
    )
}

impl Transferred {
    pub(crate) const fn durable_prefix(&self) -> u64 {
        self.checkpoint.durable_prefix
    }

    #[cfg(test)]
    pub(crate) fn recovery_enabled(&self) -> bool {
        self.stage.recovery_enabled()
    }

    pub(crate) const fn data_path(&self) -> TransferDataPath {
        self.data_path
    }

    pub(crate) async fn discard(self) -> Result<(), StorageRoleFailure> {
        self.destination.discard(self.stage).await
    }
}

pub(crate) async fn run_until_transferred(
    request: TransferRequest,
) -> Result<Transferred, TransferFailure> {
    let source_qos = request
        .source_qos
        .as_ref()
        .map(crate::runtime::qos::SourceQosGroup::transfer_budget);
    run_until_transferred_inner(request, source_qos.clone())
        .await
        .map_err(|failure| match source_qos {
            Some(budget) => failure.with_source_qos(budget.stats()),
            None => failure,
        })
}

async fn run_until_transferred_inner(
    request: TransferRequest,
    source_qos: Option<SourceQosBudget>,
) -> Result<Transferred, TransferFailure> {
    let TransferRoles {
        source,
        destination,
    } = lend_transfer_roles(&request)?;
    let descriptor = describe_source(&request, &*source, source_qos.as_ref()).await?;
    // Before any destination I/O, now that the size is known (P7 D13, §14.2).
    directory_object::refuse_unwritable(request.final_path(), descriptor.size, &*destination)?;
    let copied_metadata_plan = copied_metadata_plan(&request, &descriptor).await?;
    let recovery_binding = recovery_binding(&request, &descriptor);
    at_destination::run_until_transferred(
        &request,
        (source, destination),
        descriptor,
        copied_metadata_plan,
        recovery_binding,
        source_qos,
    )
    .await
}

#[cfg(test)]
mod plan_tests {
    use super::plan::plan_transfer;
    use super::*;
    use crate::model::{BackendIdentity, EntryKind, IdentityStrength, SourceIdentity};
    use crate::storage::backends::local::{
        test_destination_storage_with_role, test_source_storage,
    };

    fn descriptor(size: u64) -> Result<SourceDescriptor, Box<dyn std::error::Error>> {
        let backend = BackendIdentity::new("local".parse()?, "source-chunk-plan")?;
        let identity =
            SourceIdentity::new(backend, IdentityStrength::StableWithinBackend, b"entry")?;
        Ok(SourceDescriptor::new(
            StoragePath::new("final.bin")?,
            EntryKind::File,
            Some(size),
            identity,
        ))
    }

    #[test]
    fn single_chunk_is_decided_only_from_the_effective_source_read_size()
    -> Result<(), Box<dyn std::error::Error>> {
        const SOURCE_CHUNK: usize = 6 * 1024 * 1024;
        let plan = plan_transfer(&descriptor(SOURCE_CHUNK as u64)?, SOURCE_CHUNK, false, true)?;

        assert_eq!(plan.chunk_bytes, SOURCE_CHUNK);
        assert!(!plan.recovery_enabled);
        assert_eq!(
            plan.effective_recovery,
            EffectiveRecovery::SkippedSingleSourceChunk
        );
        Ok(())
    }

    /// The plan arms no recovery for a native copy — its destination turns it on itself — but
    /// expects what a streamed copy would, so the outcome reports the same (ADR-0006 C18).
    #[test]
    fn native_copy_never_enables_checkpoint_recovery_regardless_of_size()
    -> Result<(), Box<dyn std::error::Error>> {
        let plan = plan_transfer(
            &descriptor(6 * 1024 * 1024 * 1024)?,
            5 * 1024 * 1024,
            true,
            true,
        )?;

        assert!(!plan.recovery_enabled);
        assert_eq!(plan.effective_recovery, EffectiveRecovery::Checkpointed);
        Ok(())
    }

    /// A hash-ready root survives a committed rename and lets a new attempt verify the final
    /// without reading the source again.
    #[tokio::test]
    async fn a_hash_ready_pointer_left_after_publication_resumes_without_source_reads()
    -> Result<(), Box<dyn std::error::Error>> {
        let nonce = uuid::Uuid::new_v4();
        let source_root = std::env::temp_dir().join(format!("data-mover-publish-source-{nonce}"));
        let destination_root =
            std::env::temp_dir().join(format!("data-mover-publish-destination-{nonce}"));
        std::fs::create_dir(&source_root)?;
        std::fs::create_dir(&destination_root)?;
        let payload = vec![0x6a; 2 * 64 * 1024 + 1];
        std::fs::write(source_root.join("final.bin"), &payload)?;
        let (source, reader) = test_source_storage(&source_root, "publishing-source")?;
        let (destination, role) =
            test_destination_storage_with_role(&destination_root, "publishing-destination")?;
        role.set_automatic_checkpoint_interval(64 * 1024);
        let request = TransferRequest::new(
            source,
            StoragePath::new("final.bin")?,
            destination,
            tokio_util::sync::CancellationToken::new(),
        )
        .with_transfer_policy(TransferPolicy::Checkpointed);

        let transferred = run_until_transferred(request.clone()).await?;
        let source_qos = transferred_source_qos(&transferred);
        role.fail_after_publication_commit();
        let failed = publication::complete_transfer(
            transferred,
            ReadBackVerification::Enabled,
            tokio_util::sync::CancellationToken::new(),
            source_qos,
        )
        .await
        .err()
        .ok_or("the injected post-commit failure")?;
        assert!(failed.final_destination_changed());
        assert!(failed.has_pending_cleanup());
        drop(failed);
        let reads_before = reader.read_call_count();

        let outcome = transfer(request).await?;
        assert_eq!(
            outcome.prepare,
            PrepareFact::Resumed {
                bytes: payload.len() as u64
            }
        );
        assert_eq!(reader.read_call_count(), reads_before);
        assert_eq!(outcome.transferred_bytes, payload.len() as u64);
        assert_eq!(std::fs::read(destination_root.join("final.bin"))?, payload);
        // The unlocked claim the dropped stage left goes with the restarted run's artifacts.
        for entry in std::fs::read_dir(&destination_root)? {
            let name = entry?.file_name();
            assert!(
                !name.to_string_lossy().starts_with(".data-mover-"),
                "{name:?}"
            );
        }
        std::fs::remove_dir_all(source_root)?;
        std::fs::remove_dir_all(destination_root)?;
        Ok(())
    }
}
