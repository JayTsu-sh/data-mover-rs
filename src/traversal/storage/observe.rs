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

/// A requested family of a child that has nothing behind it to observe (an S3 prefix, a delete
/// marker): not applicable, rather than a failed lookup of an object that does not exist.
fn nothing_behind<T>(mode: ObservationMode) -> MetadataObservation<T> {
    if mode == ObservationMode::Omit {
        MetadataObservation::NotRequested
    } else {
        MetadataObservation::NotApplicable
    }
}

/// Observations of a listing-only child: what the listing gave, and nothing else.
fn listed_observations(
    plan: ObservationPlan,
    descriptor: &SourceDescriptor,
) -> Result<MetadataObservations, StorageRoleFailure> {
    let timestamps = match (plan.timestamps(), descriptor.inline_timestamps) {
        (ObservationMode::Omit, _) => MetadataObservation::NotRequested,
        (_, Some(value)) => MetadataObservation::Value {
            value,
            provenance: MetadataProvenance::Inline,
        },
        (_, None) => MetadataObservation::NotApplicable,
    };
    MetadataObservations::new(
        nothing_behind(plan.acl()),
        nothing_behind(plan.xattrs()),
        nothing_behind(plan.tags()),
        nothing_behind(plan.ownership_mode()),
        timestamps,
    )
    .map_err(|_| StorageRoleFailure::Entry(entry_failure(&descriptor.path, FailureClass::Protocol)))
}

/// The metadata of `descriptor`: from the listing alone when that is all there is or all the
/// plan asks for; otherwise from the metadata role, bound to the listed version when the child is
/// one, so an old version is never described by the current object's tags and time.
async fn observations(
    metadata_role: &dyn Metadata,
    descriptor: &SourceDescriptor,
    plan: ObservationPlan,
) -> Result<MetadataObservations, StorageRoleFailure> {
    if descriptor.listing.listing_only {
        return listed_observations(plan, descriptor);
    }
    if !needs_metadata_role(plan, descriptor) {
        return inline_observations(plan, descriptor);
    }
    if descriptor.listing.version.is_none() {
        return metadata_role.observe(&descriptor.path, plan).await;
    }
    // A listed version — the latest `"null"` one included, whose selector is `Current` — is
    // observed only while the store still holds exactly what was listed.
    metadata_role
        .observe_copy_bound_version(
            &descriptor.path,
            &descriptor.source_identity,
            &descriptor.version,
            plan,
        )
        .await
        .map(|observed| observed.observations)
}

pub(super) async fn observe(
    namespace: Arc<dyn Namespace>,
    metadata_role: Arc<dyn Metadata>,
    descriptor: SourceDescriptor,
    plan: ObservationPlan,
) -> Result<ObservedEntry, StorageRoleFailure> {
    let backend_fact = descriptor.backend_fact.clone();
    let listed_version = descriptor.listing.version.clone();
    let metadata = observations(metadata_role.as_ref(), &descriptor, plan).await?;
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
    let entry = entry.with_metadata(metadata).with_version(listed_version);
    match backend_fact {
        Some(fact) => entry.with_backend_fact_bytes(fact.to_vec()).map_err(|_| {
            StorageRoleFailure::Entry(entry_failure(&descriptor.path, FailureClass::Protocol))
        }),
        None => Ok(entry),
    }
}
