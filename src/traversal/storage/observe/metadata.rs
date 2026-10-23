//! Reuse listed observations, authorize additional calls, and validate required results.

use super::entry_failure;

use crate::model::{
    EntryKind, EntryOperationFailure, FailureClass, MetadataCapabilities, MetadataFamily as F,
    MetadataObservation as O, MetadataObservations, MetadataProvenance, ObservationMode as M,
    ObservationPlan, ObservationSupport as S, Operation, StorageTimestamp, TimestampMetadata,
    Transience,
};
use crate::storage::{InlineOwnership, Metadata, SourceDescriptor, StorageRoleFailure};

fn unavailable<T>(mode: M, support: S) -> O<T> {
    match (mode, support) {
        (M::Omit, _) => O::NotRequested,
        (_, S::NotApplicable) => O::NotApplicable,
        _ => O::Unsupported,
    }
}

fn known<T>(mode: M, support: S, value: O<T>) -> O<T> {
    if mode == M::Omit {
        O::NotRequested
    } else if matches!(value, O::Value { .. } | O::NotApplicable | O::Failed { .. }) {
        value
    } else {
        unavailable(mode, support)
    }
}

fn apply_support(
    metadata: MetadataObservations,
    plan: ObservationPlan,
    capabilities: MetadataCapabilities,
) -> MetadataObservations {
    MetadataObservations {
        acl: known(plan.acl(), capabilities.support(F::Acl), metadata.acl),
        xattrs: known(
            plan.xattrs(),
            capabilities.support(F::Xattrs),
            metadata.xattrs,
        ),
        tags: known(plan.tags(), capabilities.support(F::Tags), metadata.tags),
        ownership_mode: known(
            plan.ownership_mode(),
            capabilities.support(F::OwnershipMode),
            metadata.ownership_mode,
        ),
        timestamps: known(
            plan.timestamps(),
            capabilities.support(F::Timestamps),
            metadata.timestamps,
        ),
    }
}

/// Version listings carry a modification time, but no per-version optional families.
pub(super) fn record_inline(
    modified: Option<StorageTimestamp>,
    plan: ObservationPlan,
    capabilities: MetadataCapabilities,
) -> MetadataObservations {
    let timestamps = match (plan.timestamps(), modified) {
        (M::Omit, _) => O::NotRequested,
        (_, Some(modified)) => O::Value {
            value: TimestampMetadata {
                accessed: None,
                modified: Some(modified),
                created: None,
            },
            provenance: MetadataProvenance::Inline,
        },
        _ => O::Unsupported,
    };
    MetadataObservations {
        acl: unavailable(plan.acl(), capabilities.support(F::Acl)),
        xattrs: unavailable(plan.xattrs(), capabilities.support(F::Xattrs)),
        tags: unavailable(plan.tags(), capabilities.support(F::Tags)),
        ownership_mode: unavailable(
            plan.ownership_mode(),
            capabilities.support(F::OwnershipMode),
        ),
        timestamps,
    }
}

/// A completed snapshot of only information that can be obtained without another call.
pub(super) fn inline(
    role: &dyn Metadata,
    descriptor: &SourceDescriptor,
    plan: ObservationPlan,
) -> Result<MetadataObservations, StorageRoleFailure> {
    let capabilities = role.observation_capabilities();
    if !descriptor.listing.listing_only {
        let inline_plan = F::ALL
            .into_iter()
            .fold(ObservationPlan::default(), |p, family| {
                p.with(
                    family,
                    if plan.mode(family) == M::Omit {
                        M::Omit
                    } else {
                        M::InlineOnly
                    },
                )
            });
        if let Some(answer) = role.observe_listed(descriptor, inline_plan) {
            return answer.map(|metadata| apply_support(metadata, plan, capabilities));
        }
    }
    let mut metadata = descriptor_fallback(descriptor, plan, capabilities);
    apply_descriptor_fields(&mut metadata, descriptor, plan);
    Ok(metadata)
}

fn descriptor_fallback(
    descriptor: &SourceDescriptor,
    plan: ObservationPlan,
    capabilities: MetadataCapabilities,
) -> MetadataObservations {
    let support = |family| {
        if descriptor.listing.listing_only
            || (descriptor.kind == EntryKind::Symlink && matches!(family, F::Acl | F::Xattrs))
        {
            S::NotApplicable
        } else {
            capabilities.support(family)
        }
    };
    MetadataObservations {
        acl: unavailable(plan.acl(), support(F::Acl)),
        xattrs: unavailable(plan.xattrs(), support(F::Xattrs)),
        tags: unavailable(plan.tags(), support(F::Tags)),
        ownership_mode: unavailable(plan.ownership_mode(), support(F::OwnershipMode)),
        timestamps: unavailable(plan.timestamps(), support(F::Timestamps)),
    }
}

fn apply_descriptor_fields(
    metadata: &mut MetadataObservations,
    descriptor: &SourceDescriptor,
    plan: ObservationPlan,
) {
    if plan.timestamps() != M::Omit
        && let Some(value) = descriptor.inline_timestamps
    {
        metadata.timestamps = O::Value {
            value,
            provenance: MetadataProvenance::Inline,
        };
    }
    if plan.ownership_mode() != M::Omit
        && let Some(InlineOwnership::Known(value)) = descriptor.inline_ownership
    {
        metadata.ownership_mode = O::Value {
            value,
            provenance: MetadataProvenance::Inline,
        };
    }
}

fn reusable<T>(value: &O<T>) -> bool {
    matches!(value, O::Value { .. } | O::NotApplicable | O::Failed { .. })
}

fn family_reusable(metadata: &MetadataObservations, family: F) -> bool {
    match family {
        F::Acl => reusable(&metadata.acl),
        F::Xattrs => reusable(&metadata.xattrs),
        F::Tags => reusable(&metadata.tags),
        F::OwnershipMode => reusable(&metadata.ownership_mode),
        F::Timestamps => reusable(&metadata.timestamps),
    }
}

/// Only requested, still missing, potentially supported families may cause I/O.
pub(super) fn io_plan(
    plan: ObservationPlan,
    metadata: &MetadataObservations,
    capabilities: MetadataCapabilities,
) -> ObservationPlan {
    F::ALL
        .into_iter()
        .fold(ObservationPlan::default(), |result, family| {
            if plan.mode(family).permits_io()
                && !family_reusable(metadata, family)
                && !matches!(
                    capabilities.support(family),
                    S::Unsupported | S::NotApplicable
                )
            {
                result.with(family, plan.mode(family))
            } else {
                result
            }
        })
}

pub(super) fn merge(
    mut inline: MetadataObservations,
    observed: MetadataObservations,
    requested: ObservationPlan,
) -> MetadataObservations {
    if requested.acl() != M::Omit {
        inline.acl = observed.acl;
    }
    if requested.xattrs() != M::Omit {
        inline.xattrs = observed.xattrs;
    }
    if requested.tags() != M::Omit {
        inline.tags = observed.tags;
    }
    if requested.ownership_mode() != M::Omit {
        inline.ownership_mode = observed.ownership_mode;
    }
    if requested.timestamps() != M::Omit {
        inline.timestamps = observed.timestamps;
    }
    inline
}

fn missing<T>(observation: &O<T>) -> Option<(FailureClass, Transience)> {
    match observation {
        O::Value { .. } | O::NotApplicable => None,
        O::Failed { class, transience } => Some((*class, *transience)),
        O::Unsupported | O::NotRequested => {
            Some((FailureClass::Unsupported, Transience::Permanent))
        }
    }
}

pub(super) fn validate(
    descriptor: &SourceDescriptor,
    plan: ObservationPlan,
    metadata: MetadataObservations,
) -> Result<MetadataObservations, StorageRoleFailure> {
    // Separate valid partial observations may exceed the aggregate payload bound when merged.
    let metadata = MetadataObservations::new(
        metadata.acl,
        metadata.xattrs,
        metadata.tags,
        metadata.ownership_mode,
        metadata.timestamps,
    )
    .map_err(|_| entry_failure(&descriptor.path, FailureClass::Protocol))
    .map_err(StorageRoleFailure::Entry)?;
    for family in F::ALL {
        if plan.mode(family) != M::Required {
            continue;
        }
        let failure = match family {
            F::Acl => missing(&metadata.acl),
            F::Xattrs => missing(&metadata.xattrs),
            F::Tags => missing(&metadata.tags),
            F::OwnershipMode => missing(&metadata.ownership_mode),
            F::Timestamps => missing(&metadata.timestamps),
        };
        if let Some((class, transience)) = failure {
            return Err(StorageRoleFailure::Entry(
                EntryOperationFailure::new(
                    descriptor.path.clone(),
                    Operation::Observe,
                    class,
                    transience,
                    format!("required metadata was not obtained: {family:?}"),
                )
                .unwrap_or_else(|_| unreachable!("static metadata diagnostic")),
            ));
        }
    }
    Ok(metadata)
}

pub(crate) fn without_io(
    role: &dyn Metadata,
    plan: ObservationPlan,
    descriptor: &SourceDescriptor,
) -> Option<Result<MetadataObservations, StorageRoleFailure>> {
    if plan == ObservationPlan::default() {
        return Some(Ok(MetadataObservations::default()));
    }
    if descriptor
        .listing
        .history
        .as_deref()
        .is_some_and(|history| {
            history
                .records()
                .iter()
                .any(|record| super::record_needs_role(plan, record))
        })
    {
        return None;
    }
    dependency_without_io(role, plan, descriptor)
}

/// A filter reads the key's current metadata. Older versions needed only for output must not
/// delay its decision when the listing already answers those dependencies.
pub(crate) fn dependency_without_io(
    role: &dyn Metadata,
    plan: ObservationPlan,
    descriptor: &SourceDescriptor,
) -> Option<Result<MetadataObservations, StorageRoleFailure>> {
    if plan == ObservationPlan::default() {
        return Some(Ok(MetadataObservations::default()));
    }
    if !descriptor.listing.listing_only
        && let Some(answer) = role.observe_listed(descriptor, plan)
    {
        return Some(answer.and_then(|metadata| {
            validate(
                descriptor,
                plan,
                apply_support(metadata, plan, role.observation_capabilities()),
            )
        }));
    }
    let metadata = match inline(role, descriptor, plan) {
        Ok(metadata) => metadata,
        Err(error) => return Some(Err(error)),
    };
    if !descriptor.listing.listing_only
        && io_plan(plan, &metadata, role.observation_capabilities()).permits_io()
    {
        return None;
    }
    Some(validate(descriptor, plan, metadata))
}
