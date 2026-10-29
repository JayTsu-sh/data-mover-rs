//! Preflight and planning: lend the roles, describe the source, and choose the data path and
//! recovery for one transfer.

use std::sync::Arc;

use super::directory_object::refuse_unholdable;
use super::{
    EffectiveRecovery, TransferDataPath, TransferFailure, TransferPhase, TransferPlan,
    TransferSide, native, version,
};
use crate::model::{EntryKind, StoragePath};
use crate::storage::{
    NativePair, PreparedStage, ReadSource, SourceDescriptor, SourceQosBudget, StagedDestination,
};
use crate::transfer::identity::{BindingSource, binding_hash};
use crate::transfer::{TransferIdentity, TransferPolicy, TransferRequest};

pub(super) fn final_recovery(stage: &PreparedStage, plan: TransferPlan) -> EffectiveRecovery {
    if plan.effective_recovery == EffectiveRecovery::UnsupportedByDestination {
        // The destination keeps no progress: whatever the stage says, nothing is kept.
        EffectiveRecovery::UnsupportedByDestination
    } else if stage.recovery_enabled() {
        EffectiveRecovery::Checkpointed
    } else if plan.effective_recovery == EffectiveRecovery::Checkpointed {
        // The destination declined recovery for this stage (reachable only for a native copy, a
        // streamed one to a destination without an automatic interval — `plan_request` reports
        // the others — or an S3 directory object, always one request: P7 D13).
        EffectiveRecovery::SkippedBelowCheckpointThreshold
    } else {
        plan.effective_recovery
    }
}

pub(super) async fn describe_source(
    request: &TransferRequest,
    source: &dyn ReadSource,
    source_qos: Option<&SourceQosBudget>,
) -> Result<SourceDescriptor, TransferFailure> {
    if request.cancel.is_cancelled() {
        return Err(TransferFailure::orchestration(
            TransferPhase::Preflight,
            "transfer was cancelled",
        ));
    }
    version::preflight_source_version(request, source)?;
    if let Some(observed) = request.source_observation()
        && observed.source_version().as_ref() != Some(request.source_version())
    {
        return Err(TransferFailure::orchestration(
            TransferPhase::Preflight,
            "selected source version differs from the observation",
        ));
    }
    // A versioned source pins the version it describes, `Current` included.
    let descriptor = source
        .describe_version(request.source_path(), request.source_version())
        .await
        .map_err(|error| {
            TransferFailure::role(TransferPhase::Describe, TransferSide::Source, error)
        })?;
    if let Some(observed) = request.source_observation() {
        super::observation::validate_observation(source, observed, &descriptor)?;
    }
    if let (Some(budget), Some(size)) = (source_qos, descriptor.size) {
        budget.set_logical_bytes(size);
    }
    if request.cancel.is_cancelled() {
        return Err(TransferFailure::orchestration(
            TransferPhase::Describe,
            "transfer was cancelled before prepare",
        ));
    }
    Ok(descriptor)
}

pub(super) fn plan_request(
    request: &TransferRequest,
    source: &dyn ReadSource,
    destination: &dyn StagedDestination,
    descriptor: &SourceDescriptor,
) -> Result<(TransferPlan, Option<NativePair>), TransferFailure> {
    let native_pair = (request.transfer_policy != TransferPolicy::Direct)
        .then(|| native::eligible_native_pair(request))
        .flatten();
    let mut plan = plan_transfer(
        descriptor,
        source.maximum_read_chunk_bytes(),
        native_pair.is_some(),
        request.transfer_policy == TransferPolicy::Checkpointed,
    )?;
    // S3's `pointer_before_checkpoint` mirrors when this arms the interval (a `Discover` prepare
    // of a known size over it): keep the two in step. A native copy arms none; its S3 fill turns
    // recovery on itself (ADR-0006 C18).
    if request.transfer_policy == TransferPolicy::Checkpointed
        && native_pair.is_none()
        && let Some(interval) = destination
            .automatic_checkpoint_interval_bytes()
            .filter(|value| *value > 0)
    {
        plan.recovery_enabled = false;
        plan.effective_recovery = if plan.source_size <= plan.chunk_bytes as u64 {
            EffectiveRecovery::SkippedSingleSourceChunk
        } else {
            EffectiveRecovery::SkippedBelowCheckpointThreshold
        };
        if plan.source_size > interval && plan.source_size > plan.chunk_bytes as u64 {
            plan.automatic_interval = Some(interval);
        }
    }
    if request.transfer_policy == TransferPolicy::Checkpointed
        && !destination.resumable_at_destination()
    {
        // Nothing is looked for or kept at the destination: the prepare restarts (`resumable` in
        // `at_destination::prepare` follows from these two), every attempt starts from zero, and
        // the outcome says why whatever the size or route.
        plan.recovery_enabled = false;
        plan.automatic_interval = None;
        plan.effective_recovery = EffectiveRecovery::UnsupportedByDestination;
    }
    Ok((plan, native_pair))
}

/// The recovery an expert destination half plans before it prepares: (whether the stage is
/// recoverable from the start, what the outcome reports). `AtomicReplace` is `Disabled`; a
/// `Checkpointed` transfer to a destination that keeps no progress is `UnsupportedByDestination`;
/// otherwise a source over one chunk is `Checkpointed`, one within it `SkippedSingleSourceChunk`.
pub(super) fn expert_recovery(
    policy: TransferPolicy,
    resumable: bool,
    source_size: u64,
    maximum_chunk_bytes: usize,
) -> (bool, EffectiveRecovery) {
    let checkpointed = policy == TransferPolicy::Checkpointed;
    let recovery_enabled = checkpointed && resumable && source_size > maximum_chunk_bytes as u64;
    let effective = if policy == TransferPolicy::AtomicReplace {
        EffectiveRecovery::Disabled
    } else if checkpointed && !resumable {
        EffectiveRecovery::UnsupportedByDestination
    } else if recovery_enabled {
        EffectiveRecovery::Checkpointed
    } else {
        EffectiveRecovery::SkippedSingleSourceChunk
    };
    (recovery_enabled, effective)
}

pub(super) fn recovery_binding(
    request: &TransferRequest,
    descriptor: &SourceDescriptor,
) -> [u8; 32] {
    recovery_binding_for(
        &request.identity(),
        &request.destination,
        request.final_path(),
        descriptor,
    )
}

pub(super) fn recovery_binding_for(
    identity: &TransferIdentity,
    destination: &crate::storage::Storage,
    final_path: &StoragePath,
    descriptor: &SourceDescriptor,
) -> [u8; 32] {
    binding_hash(
        identity,
        &BindingSource {
            path: &descriptor.path,
            identity_key: descriptor.source_identity.identity_key(),
            size: descriptor.size,
            content_version: descriptor.content_version.as_deref(),
        },
        destination.identity(),
        final_path,
    )
}

pub(super) struct TransferRoles {
    pub(super) source: Arc<dyn ReadSource>,
    pub(super) destination: Arc<dyn StagedDestination>,
}

pub(super) fn lend_transfer_roles(
    request: &TransferRequest,
) -> Result<TransferRoles, TransferFailure> {
    let source = request.source.read_source().map_err(|_| {
        TransferFailure::capability(TransferSide::Source, "source capability unavailable")
    })?;
    let destination = request.destination.staged_destination().map_err(|_| {
        TransferFailure::capability(
            TransferSide::Destination,
            "destination capability unavailable",
        )
    })?;
    // A directory object (a path ending in '/', P7 F5) only on a side that holds them.
    refuse_unholdable(
        request.final_path(),
        &request.destination,
        TransferSide::Destination,
    )?;
    refuse_unholdable(request.source_path(), &request.source, TransferSide::Source)?;
    if request.transfer_policy == TransferPolicy::Direct && !destination.supports_direct() {
        return Err(TransferFailure::capability(
            TransferSide::Destination,
            "Direct requires a destination that supports direct writes",
        ));
    }
    Ok(TransferRoles {
        source,
        destination,
    })
}

pub(super) fn plan_transfer(
    descriptor: &SourceDescriptor,
    backend_chunk_bytes: usize,
    native: bool,
    recovery_allowed: bool,
) -> Result<TransferPlan, TransferFailure> {
    if descriptor.kind != EntryKind::File {
        return Err(TransferFailure::orchestration(
            TransferPhase::Describe,
            "ordinary transfer source must be a file",
        ));
    }
    let source_size = descriptor.size.ok_or_else(|| {
        TransferFailure::orchestration(TransferPhase::Describe, "source has no byte size")
    })?;
    let chunk_bytes = backend_chunk_bytes.max(1);
    Ok(TransferPlan {
        data_path: if native {
            TransferDataPath::Native
        } else {
            TransferDataPath::Streaming
        },
        source_size,
        chunk_bytes,
        automatic_interval: None,
        // A native copy arms no recovery here; its destination turns it on itself (ADR-0006 C18),
        // and it reports what a streamed copy of the same stage would (`native::recovery`).
        recovery_enabled: recovery_allowed && !native && source_size > chunk_bytes as u64,
        effective_recovery: if !recovery_allowed {
            EffectiveRecovery::Disabled
        } else if source_size <= chunk_bytes as u64 {
            EffectiveRecovery::SkippedSingleSourceChunk
        } else {
            EffectiveRecovery::Checkpointed
        },
    })
}
