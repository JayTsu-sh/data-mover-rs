//! Per-entry observation: turns one listed descriptor into an immutable `ObservedEntry`,
//! consulting the metadata role only when the listing did not already supply what the plan asks.

use std::sync::Arc;

use super::entry_failure;
use crate::model::{
    EntryKind, FailureClass, MetadataObservation, MetadataObservations, MetadataProvenance,
    ObservationMode, ObservationPlan, ObservedEntry,
};
use crate::storage::{
    Metadata, Namespace, NamespaceRequest, NamespaceResult, SourceDescriptor, StorageRoleFailure,
};

/// Whether the metadata role must be consulted for `descriptor` under `plan`, or whether the
/// listing already supplied everything the plan asks for.
fn needs_metadata_role(plan: ObservationPlan, descriptor: &SourceDescriptor) -> bool {
    let optional_requested = [
        plan.acl(),
        plan.xattrs(),
        plan.tags(),
        plan.ownership_mode(),
    ]
    .into_iter()
    .any(|mode| mode != ObservationMode::Omit);
    optional_requested
        || (plan.timestamps() != ObservationMode::Omit && descriptor.inline_timestamps.is_none())
}

fn inline_observations(
    plan: ObservationPlan,
    descriptor: &SourceDescriptor,
) -> Result<MetadataObservations, StorageRoleFailure> {
    let timestamps = match (plan.timestamps(), descriptor.inline_timestamps) {
        (ObservationMode::Omit, _) | (_, None) => MetadataObservation::NotRequested,
        (_, Some(value)) => MetadataObservation::Value {
            value,
            provenance: MetadataProvenance::Inline,
        },
    };
    MetadataObservations::new(
        MetadataObservation::NotRequested,
        MetadataObservation::NotRequested,
        MetadataObservation::NotRequested,
        MetadataObservation::NotRequested,
        timestamps,
    )
    .map_err(|_| StorageRoleFailure::Entry(entry_failure(&descriptor.path, FailureClass::Protocol)))
}

pub(super) async fn observe(
    namespace: Arc<dyn Namespace>,
    metadata_role: Arc<dyn Metadata>,
    descriptor: SourceDescriptor,
    plan: ObservationPlan,
) -> Result<ObservedEntry, StorageRoleFailure> {
    let backend_fact = descriptor.backend_fact.clone();
    let metadata = if needs_metadata_role(plan, &descriptor) {
        metadata_role.observe(&descriptor.path, plan).await?
    } else {
        inline_observations(plan, &descriptor)?
    };
    let modified = metadata
        .timestamps()
        .value()
        .and_then(|value| value.modified);
    let entry = if descriptor.kind == EntryKind::Symlink {
        let result = namespace
            .execute(NamespaceRequest::ReadLink(descriptor.path.clone()))
            .await?;
        let NamespaceResult::LinkTarget(target) = result else {
            return Err(StorageRoleFailure::Entry(entry_failure(
                &descriptor.path,
                FailureClass::Protocol,
            )));
        };
        ObservedEntry::new_symlink(
            descriptor.path.clone(),
            modified,
            descriptor.source_identity,
            target,
        )
    } else {
        ObservedEntry::new(
            descriptor.path.clone(),
            descriptor.kind,
            descriptor.size,
            modified,
            descriptor.source_identity,
        )
    }
    .map_err(|_| {
        StorageRoleFailure::Entry(entry_failure(&descriptor.path, FailureClass::Protocol))
    })?;
    let entry = entry.with_metadata(metadata);
    match backend_fact {
        Some(fact) => entry.with_backend_fact_bytes(fact.to_vec()).map_err(|_| {
            StorageRoleFailure::Entry(entry_failure(&descriptor.path, FailureClass::Protocol))
        }),
        None => Ok(entry),
    }
}
