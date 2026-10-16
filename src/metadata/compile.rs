//! Compiling a metadata plan: which families a copy carries, exactly or with which loss, and the
//! mutations that carry them — decided before any write is issued.

use super::{
    AclTarget, FamilyMapping, LossReport, MappingDecision, MetadataFamily, MetadataPlan,
    MetadataPlanError, MetadataPlanRequest, MetadataPolicies, MetadataPolicy, OwnershipTarget,
    RefusalCause, SemanticLoss, SkippedFamily, TimestampTargetCapability, ValueTarget,
};
use crate::model::{
    MetadataObservation, StorageTimestamp, TimePrecision, TimestampMetadata, without_unowned_set_id,
};
use crate::storage::MetadataMutation;

/// Compiles all mappings before a target mutation can be issued.
///
/// # Errors
/// Returns the first policy, observation, capability, or principal-mapping refusal.
pub fn compile_metadata_plan(
    request: &MetadataPlanRequest<'_>,
) -> Result<MetadataPlan, MetadataPlanError> {
    let mut plan = MetadataPlan {
        mappings: Vec::with_capacity(5),
        mutations: Vec::with_capacity(5),
        losses: LossReport::default(),
        skipped: Vec::new(),
    };
    // Ownership first, and mode with it: writing permission bits recomputes the ACL — the POSIX
    // mask entry, and on most NFSv4 servers (ONTAP among them) the whole ACL. Compiling the ACL
    // first would put it earlier in `mutations`, which is also the order they are applied in, so
    // the copied ACL would be silently overwritten by the mode that follows it and the report
    // would still say it was applied.
    compile_ownership(request, &mut plan)?;
    compile_acl(request, &mut plan)?;
    compile_value(
        MetadataFamily::Xattrs,
        request.observations.xattrs(),
        request.target.xattrs,
        request.policies,
        &mut plan,
        |value| MetadataMutation::Xattrs(value.clone()),
    )?;
    compile_value(
        MetadataFamily::Tags,
        request.observations.tags(),
        request.target.tags,
        request.policies,
        &mut plan,
        |value| MetadataMutation::Tags(value.clone()),
    )?;
    compile_timestamps(request, &mut plan)?;
    Ok(plan)
}

/// The mode a copy writes when it does not carry owner and group: the file ends up owned by
/// whoever writes it, so no set-id bit may go with it (`model::without_unowned_set_id`). The copy
/// path carries files only, never directories.
const fn mode_only(mode: u32) -> u32 {
    without_unowned_set_id(mode & 0o7777, false, false, false)
}

/// Compiles automatic copy facts without fabricating numeric owner/group IDs.
pub(crate) fn compile_copied_metadata_plan(
    request: &MetadataPlanRequest<'_>,
    mode_without_ownership: Option<u32>,
    owner_names_unmapped: bool,
) -> Result<MetadataPlan, MetadataPlanError> {
    let projected = MetadataPlanRequest {
        observations: request.observations,
        target: request.target,
        policies: if mode_without_ownership.is_some() {
            request.policies.with_ownership_mode(MetadataPolicy::Omit)
        } else {
            request.policies
        },
        principal_mapper: request.principal_mapper,
    };
    let mut plan = compile_metadata_plan(&projected)?;
    if let Some(mode) = mode_without_ownership
        .filter(|_| request.policies.get(MetadataFamily::OwnershipMode) != MetadataPolicy::Omit)
    {
        let family = MetadataFamily::OwnershipMode;
        // This alternative observation replaces the absent numeric ownership family.
        plan.mappings.retain(|value| value.family != family);
        plan.mutations.retain(|(value, _)| *value != family);
        plan.losses.0.retain(|(value, _)| *value != family);
        plan.skipped.retain(|value| value.family != family);
        let supported = matches!(
            request.target.ownership_mode,
            OwnershipTarget::Numeric | OwnershipTarget::ModeOnly | OwnershipTarget::NotPermitted
        );
        drop_with_loss(
            &mut plan,
            family,
            request.policies.get(family),
            match (supported, owner_names_unmapped) {
                (true, true) => SemanticLoss::OwnerAndGroupUnmapped,
                (true, false) => SemanticLoss::OwnerAndGroupDropped,
                (false, _) => SemanticLoss::OwnershipModeDropped,
            },
        )?;
        if supported {
            // The ownership mutation was just retained away, so every mutation left is one that
            // has to observe the new mode: the ACL is recomputed by it, and the timestamps have
            // to be stamped after it. Front of the queue is the only correct place.
            plan.mutations
                .insert(0, (family, MetadataMutation::Mode(mode_only(mode))));
        }
    }
    Ok(plan)
}

/// The plan for a destination that stores nothing a copy carries: every family the policies ask
/// for is skipped because of the destination, the rest omitted — decided without reading the
/// source, since no value there could change it. Any policy but `Omit` counts as asked, whatever
/// its strength: what a destination cannot store at all is not the copy's to refuse. The reason
/// is always the destination's, even where the source could not have read the family either.
pub(crate) fn compile_nothing_stored_plan(policies: MetadataPolicies) -> MetadataPlan {
    let mut plan = MetadataPlan {
        mappings: Vec::with_capacity(5),
        mutations: Vec::new(),
        losses: LossReport::default(),
        skipped: Vec::new(),
    };
    for family in [
        MetadataFamily::OwnershipMode,
        MetadataFamily::Acl,
        MetadataFamily::Xattrs,
        MetadataFamily::Tags,
        MetadataFamily::Timestamps,
    ] {
        if policies.get(family) == MetadataPolicy::Omit {
            plan.mappings.push(FamilyMapping {
                family,
                decision: MappingDecision::OmittedByPolicy,
            });
            continue;
        }
        plan.mappings.push(FamilyMapping {
            family,
            decision: MappingDecision::Unsupported,
        });
        plan.skipped.push(SkippedFamily {
            family,
            reason: RefusalCause::DestinationCannotStore,
        });
    }
    plan
}

fn compile_acl(
    request: &MetadataPlanRequest<'_>,
    plan: &mut MetadataPlan,
) -> Result<(), MetadataPlanError> {
    let family = MetadataFamily::Acl;
    let policy = request.policies.get(family);
    let Some(value) = observed_value(request.observations.acl(), family, policy, plan)? else {
        return Ok(());
    };
    match request.target.acl {
        AclTarget::Encoding(encoding) if encoding == value.encoding() => {
            exact(plan, family, MetadataMutation::Acl(value.clone()));
            Ok(())
        }
        // Not a downgrade: without an external mapping the destination cannot hold this ACL at
        // all, so `AllowKnownLoss`, which requires the family to be carried, refuses too.
        AclTarget::Encoding(destination) => unavailable(
            plan,
            family,
            policy,
            MappingDecision::RequiresExternalMapping,
            RefusalCause::EncodingsDiffer {
                source: value.encoding(),
                destination,
            },
        ),
        AclTarget::Unsupported => unavailable(
            plan,
            family,
            policy,
            MappingDecision::Unsupported,
            RefusalCause::DestinationCannotStore,
        ),
        AclTarget::NotApplicable => drop_with_loss(plan, family, policy, SemanticLoss::AclDropped),
    }
}

fn compile_value<T>(
    family: MetadataFamily,
    observation: &MetadataObservation<T>,
    target: ValueTarget,
    policies: MetadataPolicies,
    plan: &mut MetadataPlan,
    mutation: impl FnOnce(&T) -> MetadataMutation,
) -> Result<(), MetadataPlanError> {
    let policy = policies.get(family);
    let Some(value) = observed_value(observation, family, policy, plan)? else {
        return Ok(());
    };
    match target {
        ValueTarget::Supported => {
            exact(plan, family, mutation(value));
            Ok(())
        }
        ValueTarget::Unsupported => unavailable(
            plan,
            family,
            policy,
            MappingDecision::Unsupported,
            RefusalCause::DestinationCannotStore,
        ),
        ValueTarget::NotApplicable => {
            let loss = match family {
                MetadataFamily::Xattrs => SemanticLoss::XattrsDropped,
                MetadataFamily::Tags => SemanticLoss::TagsDropped,
                _ => unreachable!("value metadata is xattrs or tags"),
            };
            drop_with_loss(plan, family, policy, loss)
        }
    }
}

fn compile_ownership(
    request: &MetadataPlanRequest<'_>,
    plan: &mut MetadataPlan,
) -> Result<(), MetadataPlanError> {
    let family = MetadataFamily::OwnershipMode;
    let policy = request.policies.get(family);
    let Some(value) = observed_value(request.observations.ownership_mode(), family, policy, plan)?
    else {
        return Ok(());
    };
    match request.target.ownership_mode {
        OwnershipTarget::Numeric => {
            exact(plan, family, MetadataMutation::NumericOwnership(*value));
            Ok(())
        }
        OwnershipTarget::ExternalMapping => {
            let Some(mapper) = request.principal_mapper else {
                return if policy == MetadataPolicy::AllowKnownLoss {
                    drop_with_loss(plan, family, policy, SemanticLoss::OwnershipModeDropped)
                } else {
                    unavailable(
                        plan,
                        family,
                        policy,
                        MappingDecision::RequiresExternalMapping,
                        RefusalCause::PrincipalMapperMissing,
                    )
                };
            };
            let ownership = mapper.map(*value).map_err(|_| {
                MetadataPlanError::new(family, policy, RefusalCause::PrincipalMappingFailed)
            })?;
            exact(plan, family, MetadataMutation::MappedOwnership(ownership));
            Ok(())
        }
        OwnershipTarget::ModeOnly => carry_mode_only(
            plan,
            policy,
            SemanticLoss::OwnerAndGroupDropped,
            value.mode & 0o7777,
        ),
        OwnershipTarget::NotPermitted => carry_mode_only(
            plan,
            policy,
            SemanticLoss::OwnerAndGroupNotPermitted,
            mode_only(value.mode),
        ),
        OwnershipTarget::Unsupported => unavailable(
            plan,
            family,
            policy,
            MappingDecision::Unsupported,
            RefusalCause::DestinationCannotStore,
        ),
        OwnershipTarget::NotApplicable => {
            drop_with_loss(plan, family, policy, SemanticLoss::OwnershipModeDropped)
        }
    }
}

const NO_TIMES: TimestampMetadata = TimestampMetadata {
    accessed: None,
    modified: None,
    created: None,
};

/// Carries the mode without owner and group, naming why they are not carried.
fn carry_mode_only(
    plan: &mut MetadataPlan,
    policy: MetadataPolicy,
    loss: SemanticLoss,
    mode: u32,
) -> Result<(), MetadataPlanError> {
    let family = MetadataFamily::OwnershipMode;
    if policy == MetadataPolicy::RequireExact {
        return Err(MetadataPlanError::new(
            family,
            policy,
            RefusalCause::LossRejected(loss),
        ));
    }
    plan.losses.0.push((family, loss));
    plan.mappings.push(FamilyMapping {
        family,
        decision: MappingDecision::Lossy(vec![loss]),
    });
    plan.mutations.push((family, MetadataMutation::Mode(mode)));
    Ok(())
}

fn compile_timestamps(
    request: &MetadataPlanRequest<'_>,
    plan: &mut MetadataPlan,
) -> Result<(), MetadataPlanError> {
    let family = MetadataFamily::Timestamps;
    let policy = request.policies.get(family);
    let Some(value) = observed_value(request.observations.timestamps(), family, policy, plan)?
    else {
        return Ok(());
    };
    let target = match request.target.timestamps {
        TimestampTargetCapability::Supported(target) => target,
        TimestampTargetCapability::Unsupported => {
            return unavailable(
                plan,
                family,
                policy,
                MappingDecision::Unsupported,
                RefusalCause::DestinationCannotStore,
            );
        }
        TimestampTargetCapability::NotApplicable => {
            let mut losses = Vec::new();
            if value.accessed.is_some() {
                losses.push(SemanticLoss::AccessedTimestampDropped);
            }
            if value.modified.is_some() {
                losses.push(SemanticLoss::ModifiedTimestampDropped);
            }
            if value.created.is_some() {
                losses.push(SemanticLoss::CreatedTimestampDropped);
            }
            return drop_with_losses(plan, family, policy, losses);
        }
    };
    let mut losses = Vec::new();
    let mapped = TimestampMetadata {
        accessed: map_timestamp(
            value.accessed,
            target.accessed,
            target.precision,
            SemanticLoss::AccessedTimestampDropped,
            &mut losses,
        ),
        modified: map_timestamp(
            value.modified,
            target.modified,
            target.precision,
            SemanticLoss::ModifiedTimestampDropped,
            &mut losses,
        ),
        created: map_timestamp(
            value.created,
            target.created,
            target.precision,
            SemanticLoss::CreatedTimestampDropped,
            &mut losses,
        ),
    };
    if losses.is_empty() && mapped == NO_TIMES {
        // The source gave no time at all (an S3 server that sent no `Last-Modified`): nothing to
        // carry, and nothing to report as applied.
        plan.mappings.push(FamilyMapping {
            family,
            decision: MappingDecision::NotApplicable,
        });
        return Ok(());
    }
    if losses.is_empty() {
        exact(plan, family, MetadataMutation::Timestamps(mapped));
        return Ok(());
    }
    if policy == MetadataPolicy::RequireExact {
        return Err(MetadataPlanError::new(
            family,
            policy,
            RefusalCause::LossRejected(losses[0]),
        ));
    }
    for loss in &losses {
        plan.losses.0.push((family, *loss));
    }
    plan.mappings.push(FamilyMapping {
        family,
        decision: MappingDecision::Lossy(losses),
    });
    plan.mutations
        .push((family, MetadataMutation::Timestamps(mapped)));
    Ok(())
}

fn map_timestamp(
    value: Option<StorageTimestamp>,
    supported: bool,
    precision: TimePrecision,
    dropped: SemanticLoss,
    losses: &mut Vec<SemanticLoss>,
) -> Option<StorageTimestamp> {
    let value = value?;
    if !supported {
        losses.push(dropped);
        return None;
    }
    if value.precision() > precision {
        losses.push(SemanticLoss::TimestampPrecisionReduced);
        let step = precision_step(precision);
        let nanos = value.unix_nanos().div_euclid(step) * step;
        return StorageTimestamp::new(nanos, precision).ok();
    }
    Some(value)
}

const fn precision_step(precision: TimePrecision) -> i128 {
    match precision {
        TimePrecision::Seconds => 1_000_000_000,
        TimePrecision::Milliseconds => 1_000_000,
        TimePrecision::Microseconds => 1_000,
        TimePrecision::HundredNanoseconds => 100,
        TimePrecision::Nanoseconds => 1,
    }
}

fn observed_value<'a, T>(
    observation: &'a MetadataObservation<T>,
    family: MetadataFamily,
    policy: MetadataPolicy,
    plan: &mut MetadataPlan,
) -> Result<Option<&'a T>, MetadataPlanError> {
    if policy == MetadataPolicy::Omit {
        plan.mappings.push(FamilyMapping {
            family,
            decision: MappingDecision::OmittedByPolicy,
        });
        return Ok(None);
    }
    match observation {
        MetadataObservation::Value { value, .. } => Ok(Some(value)),
        MetadataObservation::NotRequested => {
            if policy != MetadataPolicy::BestEffort {
                return Err(MetadataPlanError::new(
                    family,
                    policy,
                    RefusalCause::SourceDidNotObserve,
                ));
            }
            plan.mappings.push(FamilyMapping {
                family,
                decision: MappingDecision::NotObserved,
            });
            Ok(None)
        }
        MetadataObservation::NotApplicable => {
            plan.mappings.push(FamilyMapping {
                family,
                decision: MappingDecision::NotApplicable,
            });
            Ok(None)
        }
        MetadataObservation::Unsupported => unavailable(
            plan,
            family,
            policy,
            MappingDecision::Unsupported,
            RefusalCause::SourceCannotObserve,
        )
        .map(|()| None),
        MetadataObservation::Failed { class, transience } => Err(MetadataPlanError::new(
            family,
            policy,
            RefusalCause::SourceObservationFailed {
                class: *class,
                transience: *transience,
            },
        )),
    }
}

fn drop_with_loss(
    plan: &mut MetadataPlan,
    family: MetadataFamily,
    policy: MetadataPolicy,
    loss: SemanticLoss,
) -> Result<(), MetadataPlanError> {
    drop_with_losses(plan, family, policy, vec![loss])
}

fn drop_with_losses(
    plan: &mut MetadataPlan,
    family: MetadataFamily,
    policy: MetadataPolicy,
    losses: Vec<SemanticLoss>,
) -> Result<(), MetadataPlanError> {
    match policy {
        MetadataPolicy::AllowKnownLoss | MetadataPolicy::BestEffort => {
            plan.losses
                .0
                .extend(losses.iter().map(|loss| (family, *loss)));
            plan.mappings.push(FamilyMapping {
                family,
                decision: MappingDecision::Lossy(losses),
            });
            Ok(())
        }
        // Nothing to lose — no value was there to drop — so nothing for a policy to refuse, and
        // "lossy" or "omitted" would both misreport it.
        _ if losses.is_empty() => {
            plan.mappings.push(FamilyMapping {
                family,
                decision: MappingDecision::NotApplicable,
            });
            Ok(())
        }
        MetadataPolicy::RequireExact | MetadataPolicy::Omit => Err(MetadataPlanError::new(
            family,
            policy,
            RefusalCause::LossRejected(losses[0]),
        )),
    }
}

fn exact(plan: &mut MetadataPlan, family: MetadataFamily, mutation: MetadataMutation) {
    plan.mappings.push(FamilyMapping {
        family,
        decision: MappingDecision::Exact,
    });
    plan.mutations.push((family, mutation));
}

fn unavailable(
    plan: &mut MetadataPlan,
    family: MetadataFamily,
    policy: MetadataPolicy,
    decision: MappingDecision,
    cause: RefusalCause,
) -> Result<(), MetadataPlanError> {
    if policy == MetadataPolicy::BestEffort {
        plan.mappings.push(FamilyMapping { family, decision });
        plan.skipped.push(SkippedFamily {
            family,
            reason: cause,
        });
        return Ok(());
    }
    Err(MetadataPlanError::new(family, policy, cause))
}
