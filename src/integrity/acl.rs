//! Complete ACL observations and explicit cross-model access equivalence.
use super::permissions::optional_value;
use super::{
    AclComparisonScope, Endpoint, IntegrityDifference, IntegrityFailure, IntegrityIgnored,
    IntegrityPlan,
};
use crate::model::acl::compare_acl;
use crate::model::{
    AclMetadata, EntryKind, MetadataObservation, MetadataObservations, OwnershipMode,
};
use crate::storage::SourceDescriptor;

pub(super) fn compare(
    source: &Endpoint,
    destination: &Endpoint,
    entries: (&SourceDescriptor, &SourceDescriptor),
    observed: (&MetadataObservations, &MetadataObservations),
    plan: IntegrityPlan,
    differences: &mut Vec<IntegrityDifference>,
    ignored: &mut Vec<IntegrityIgnored>,
) -> Result<Option<AclComparisonScope>, IntegrityFailure> {
    let source_acl = optional_value(source, observed.0.acl(), "ACL observation failed")?;
    let destination_acl = optional_value(destination, observed.1.acl(), "ACL observation failed")?;
    if !plan.acl_required && !plan.acl_equivalent {
        compare_optional(source_acl, destination_acl, differences, ignored);
        return Ok(None);
    }
    match compare_required(
        (source_acl, destination_acl),
        entries,
        observed,
        plan.acl_equivalent,
    ) {
        Ok(scope) => Ok(Some(scope)),
        Err(difference) => {
            differences.push(difference);
            Ok(None)
        }
    }
}

fn compare_required(
    acls: (Option<&AclMetadata>, Option<&AclMetadata>),
    entries: (&SourceDescriptor, &SourceDescriptor),
    observed: (&MetadataObservations, &MetadataObservations),
    equivalent: bool,
) -> Result<AclComparisonScope, IntegrityDifference> {
    let (Some(a), Some(b)) = acls else {
        return Err(unverified("required ACL observation is unavailable"));
    };
    let cross = a.encoding() != b.encoding();
    if cross && (entries.0.kind != EntryKind::File || entries.1.kind != EntryKind::File) {
        return Err(unverified(
            "cross-model ACL equivalence requires regular files",
        ));
    }
    let (Some(owner_a), Some(owner_b)) = (ownership(observed.0), ownership(observed.1)) else {
        return Err(unverified("required numeric ACL ownership is unavailable"));
    };
    let equal = compare_acl(a, b, owner_a, owner_b, equivalent)
        .map_err(|error| unverified(error.reason()))?;
    if !equal {
        return Err(IntegrityDifference::Acl {
            encoding: a.encoding(),
        });
    }
    Ok(if cross {
        AclComparisonScope::CurrentRegularFileAccess
    } else {
        AclComparisonScope::NativeConfiguration {
            encoding: a.encoding(),
        }
    })
}

const fn unverified(reason: &'static str) -> IntegrityDifference {
    IntegrityDifference::AclUnverified { reason }
}

fn ownership(observed: &MetadataObservations) -> Option<OwnershipMode> {
    match observed.ownership_mode() {
        MetadataObservation::Value { value, .. } => Some(*value),
        _ => None,
    }
}

fn compare_optional(
    source: Option<&AclMetadata>,
    destination: Option<&AclMetadata>,
    differences: &mut Vec<IntegrityDifference>,
    ignored: &mut Vec<IntegrityIgnored>,
) {
    match (source, destination) {
        (Some(a), Some(b)) if a.encoding() == b.encoding() => {
            if a != b {
                differences.push(IntegrityDifference::Acl {
                    encoding: a.encoding(),
                });
            }
        }
        (a, b) => ignored.push(IntegrityIgnored::Acl {
            source: a.map(AclMetadata::encoding),
            destination: b.map(AclMetadata::encoding),
        }),
    }
}
