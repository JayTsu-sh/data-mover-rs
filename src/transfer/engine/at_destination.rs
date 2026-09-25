//! The engine side of destination-resident recovery (ADR-0006): every destination keeps its
//! recovery state beside the final file, prepare asks the destination — under the per-file guard —
//! and nothing about the transfer is recorded where data-mover runs.

use super::guard::{self, DestinationLease};
use super::{
    Arc, CopiedMetadataPlan, NativePair, PreparedStage, ReadSource, SourceDescriptor,
    SourceQosBudget, StagedDestination, TransferFailure, TransferPhase, TransferPlan,
    TransferPolicy, TransferRequest, TransferSide, Transferred, final_recovery, native,
    plan_request, transfer_stage,
};
use tokio_util::sync::CancellationToken;

use crate::model::{EntryOperationFailure, FailureClass, Operation, StoragePath, Transience};
use crate::storage::{
    DestinationPrepareRequest, FinalDestination, PrepareRequest, ResumeMode, StorageRoleFailure,
};
use crate::transfer::TransferIdentity;

/// Takes the per-file lease for this request's final file, or fails with a transient conflict
/// while another transfer in this process writes it.
pub(super) fn acquire(request: &TransferRequest) -> Result<DestinationLease, TransferFailure> {
    acquire_for(request.destination.identity(), &request.final_path)
}

/// [`acquire`] for a final file of an endpoint.
pub(super) fn acquire_for(
    endpoint: &crate::model::BackendIdentity,
    final_path: &StoragePath,
) -> Result<DestinationLease, TransferFailure> {
    guard::try_acquire(endpoint, final_path).ok_or_else(|| {
        let conflict = EntryOperationFailure::new(
            final_path.clone(),
            Operation::Prepare,
            FailureClass::Conflict,
            Transience::Transient,
            "another transfer in this process is writing this destination file",
        );
        match conflict {
            Ok(conflict) => TransferFailure::role(
                TransferPhase::Prepare,
                TransferSide::Destination,
                StorageRoleFailure::Entry(conflict),
            ),
            Err(_) => TransferFailure::orchestration(
                TransferPhase::Prepare,
                "another transfer in this process is writing this destination file",
            ),
        }
    })
}

/// What an at-destination prepare needs beyond the stage inputs.
pub(super) struct Spec {
    pub(super) policy: TransferPolicy,
    pub(super) identity: TransferIdentity,
    /// Whether an equal binding found at the destination may be continued.
    pub(super) resumable: bool,
    /// Whether a fresh stage writes its pointer from the start.
    pub(super) recoverable: bool,
    pub(super) cancel: CancellationToken,
}

/// Prepares at the destination: resumes what it finds there when the policy allows, cleans it up
/// otherwise, and hands the lease to the stage so it lives exactly as long as the stage.
pub(super) async fn prepare(
    request: &TransferRequest,
    destination: &Arc<dyn StagedDestination>,
    descriptor: &SourceDescriptor,
    recovery_binding: [u8; 32],
    plan: TransferPlan,
    lease: DestinationLease,
) -> Result<PreparedStage, TransferFailure> {
    let spec = Spec {
        policy: request.transfer_policy,
        identity: request.identity,
        // Only a checkpointed transfer that keeps checkpoints may continue a stage; AtomicReplace
        // cleans up whatever it finds and starts from zero. Direct writes the final file in place
        // through `prepare_direct`; a destination that keeps artifacts cleans them there (Local
        // since ADR-0006 C9c).
        resumable: request.transfer_policy == TransferPolicy::Checkpointed
            && (plan.recovery_enabled || plan.automatic_interval.is_some()),
        recoverable: plan.recovery_enabled,
        cancel: request.cancel.clone(),
    };
    let prepare = PrepareRequest {
        final_destination: FinalDestination::new(request.final_path.clone()),
        source: descriptor.clone(),
        recovery_binding,
    };
    prepare_with(destination, prepare, &spec, lease).await
}

/// [`prepare`] from plain inputs, shared with the expert destination half.
pub(super) async fn prepare_with(
    destination: &Arc<dyn StagedDestination>,
    prepare: PrepareRequest,
    spec: &Spec,
    lease: DestinationLease,
) -> Result<PreparedStage, TransferFailure> {
    let prepared = if spec.policy == TransferPolicy::Direct {
        destination
            .prepare_direct(prepare, spec.cancel.clone())
            .await
    } else {
        destination
            .prepare_at_destination(
                DestinationPrepareRequest::new(prepare, *spec.identity.as_bytes())
                    .with_resume(if spec.resumable {
                        ResumeMode::Discover
                    } else {
                        ResumeMode::Restart
                    })
                    .with_recoverable(spec.recoverable),
            )
            .await
    };
    let mut stage = prepared.map_err(|error| {
        TransferFailure::role(TransferPhase::Prepare, TransferSide::Destination, error)
    })?;
    if !stage.at_destination {
        // A direct prepare (or a backend that forgot) keeps whatever fact it set.
        let fact = stage.prepare_fact;
        stage.mark_at_destination(fact);
    }
    stage.exclusive = Some(Box::new(lease));
    Ok(stage)
}

/// The whole transfer up to verification.
pub(super) async fn run_until_transferred(
    request: &TransferRequest,
    roles: (Arc<dyn ReadSource>, Arc<dyn StagedDestination>),
    descriptor: SourceDescriptor,
    copied_metadata_plan: Option<CopiedMetadataPlan>,
    recovery_binding: [u8; 32],
    source_qos: Option<SourceQosBudget>,
) -> Result<Transferred, TransferFailure> {
    let (source, destination) = roles;
    let (plan, native_pair) = plan_request(request, &*source, &*destination, &descriptor)?;
    let lease = acquire(request)?;
    if let Some(pair) = native_pair {
        return native_transfer(
            request,
            (source, destination),
            descriptor,
            pair,
            (recovery_binding, plan, lease),
            source_qos,
            copied_metadata_plan,
        )
        .await;
    }
    let mut stage = prepare(
        request,
        &destination,
        &descriptor,
        recovery_binding,
        plan,
        lease,
    )
    .await?;
    stage.durable_publication = request.transfer_policy == TransferPolicy::Checkpointed;
    if let Some(interval_bytes) = plan.automatic_interval {
        // The destination writes its own pointer at the deferred checkpoint.
        stage.deferred_checkpoint = Some(crate::storage::DeferredCheckpoint {
            interval_bytes,
            source_size: plan.source_size,
        });
    }
    let evidence = match transfer_stage(
        request,
        source,
        &destination,
        &descriptor,
        &stage,
        plan,
        source_qos.clone(),
    )
    .await
    {
        Ok(evidence) => evidence,
        Err(error) => return Err(error.with_stage(destination, stage)),
    };
    let effective_recovery = final_recovery(&stage, plan);
    Ok(Transferred {
        identity: request.identity,
        destination,
        stage,
        write: evidence.write,
        checkpoint: evidence.checkpoint,
        source: descriptor,
        data_path: plan.data_path,
        source_blake3: evidence.source_blake3,
        source_qos,
        native_bytes: 0,
        native_requests: 0,
        effective_recovery,
        copied_metadata_plan,
    })
}

async fn native_transfer(
    request: &TransferRequest,
    (source, destination): (Arc<dyn ReadSource>, Arc<dyn StagedDestination>),
    descriptor: SourceDescriptor,
    pair: NativePair,
    (recovery_binding, plan, lease): ([u8; 32], TransferPlan, DestinationLease),
    source_qos: Option<SourceQosBudget>,
    copied_metadata_plan: Option<CopiedMetadataPlan>,
) -> Result<Transferred, TransferFailure> {
    native::transfer_native(
        request,
        native::NativeTransferInput {
            source,
            destination,
            descriptor,
            pair,
            recovery_binding,
            source_qos,
            plan,
            copied_metadata_plan,
        },
        lease,
    )
    .await
}
