//! Strict ACL planning uses complete numeric ownership and validates native payloads.
use super::{
    AclTarget, FamilyMapping, MappingDecision, MetadataFamily, MetadataPlan, MetadataPlanError,
    MetadataPlanRequest, MetadataPolicy, RefusalCause,
};
use crate::model::acl::map_acl;
use crate::model::{AclMetadata, MetadataObservation};
use crate::storage::MetadataMutation;

pub(super) fn compile(
    request: &MetadataPlanRequest<'_>,
    plan: &mut MetadataPlan,
) -> Result<(), MetadataPlanError> {
    let value = required_acl(request)?;
    let AclTarget::Encoding(target) = request.target.acl else {
        return Err(reject(RefusalCause::DestinationCannotStore));
    };
    let owner = match request.observations.ownership_mode() {
        MetadataObservation::Value { value, .. } => *value,
        _ => {
            return Err(reject(RefusalCause::AclMappingRejected {
                reason: "complete numeric ownership is required",
            }));
        }
    };
    let mapped = map_acl(value, target, owner).map_err(|error| {
        reject(RefusalCause::AclMappingRejected {
            reason: error.reason(),
        })
    })?;
    plan.mappings.push(FamilyMapping {
        family: MetadataFamily::Acl,
        decision: if value.encoding() == target {
            MappingDecision::Exact
        } else {
            MappingDecision::Equivalent
        },
    });
    plan.mutations
        .push((MetadataFamily::Acl, MetadataMutation::Acl(mapped)));
    Ok(())
}

fn required_acl<'a>(
    request: &MetadataPlanRequest<'a>,
) -> Result<&'a AclMetadata, MetadataPlanError> {
    match request.observations.acl() {
        MetadataObservation::Value { value, .. } => Ok(value),
        MetadataObservation::Failed { class, transience } => {
            Err(reject(RefusalCause::SourceObservationFailed {
                class: *class,
                transience: *transience,
            }))
        }
        MetadataObservation::NotRequested => Err(reject(RefusalCause::SourceDidNotObserve)),
        _ => Err(reject(RefusalCause::SourceCannotObserve)),
    }
}

const fn reject(cause: RefusalCause) -> MetadataPlanError {
    MetadataPlanError::new(
        MetadataFamily::Acl,
        MetadataPolicy::RequireEquivalent,
        cause,
    )
}
