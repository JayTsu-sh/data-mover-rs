//! Automatic permission comparison and explicitly requested ACL/xattr comparison.

use super::acl::compare as compare_acl;
use super::{
    AclComparisonScope, Endpoint, IntegrityDifference, IntegrityFailure, IntegrityIgnored,
    IntegrityPlan, reconcile,
};
use crate::model::{
    EntryKind, EntryOperationFailure, ExtendedAttribute, FailureClass, MetadataFamily,
    MetadataObservation, MetadataObservations, ObservationMode, ObservationPlan,
    ObservationSupport, Operation, SourceVersion,
};
use crate::storage::PrincipalIdentity::{Named, Numeric};
use crate::storage::{PermissionFacts, PrincipalIdentity, SourceDescriptor, StorageRoleFailure};

pub(super) async fn compare_permissions(
    source: &Endpoint,
    destination: &Endpoint,
    source_entry: &SourceDescriptor,
    destination_entry: &SourceDescriptor,
    cross_acl_verified: bool,
    differences: &mut Vec<IntegrityDifference>,
    ignored: &mut Vec<IntegrityIgnored>,
) -> Result<(), IntegrityFailure> {
    let (source_facts, destination_facts) = reconcile(futures::join!(
        permission_facts(source, source_entry),
        permission_facts(destination, destination_entry),
    ))?;
    compare_permission_facts(
        source_facts,
        destination_facts,
        source_entry.kind == EntryKind::Symlink || destination_entry.kind == EntryKind::Symlink,
        cross_acl_verified,
        differences,
        ignored,
    );
    Ok(())
}

fn compare_permission_facts(
    source_facts: PermissionFacts,
    destination_facts: PermissionFacts,
    symlink: bool,
    cross_acl_verified: bool,
    differences: &mut Vec<IntegrityDifference>,
    ignored: &mut Vec<IntegrityIgnored>,
) {
    let principal = |a: &Option<PrincipalIdentity>, b: &Option<PrincipalIdentity>| match (a, b) {
        (Some(Numeric(a)), Some(Numeric(b))) => Some(a != b),
        (Some(Named(a)), Some(Named(b))) => Some(a != b),
        _ => None,
    };
    let owner = principal(&source_facts.owner, &destination_facts.owner);
    let group = principal(&source_facts.group, &destination_facts.group);
    let mode = if symlink {
        None
    } else {
        source_facts.mode.zip(destination_facts.mode).map(|(a, b)| {
            if cross_acl_verified {
                (a & 0o7000) != (b & 0o7000)
            } else {
                a != b
            }
        })
    };
    if owner.is_none() {
        ignored.push(IntegrityIgnored::Owner);
    }
    if group.is_none() {
        ignored.push(IntegrityIgnored::Group);
    }
    if mode.is_none() {
        ignored.push(IntegrityIgnored::Mode);
    }
    if owner == Some(true) || group == Some(true) || mode == Some(true) {
        differences.push(IntegrityDifference::Permissions {
            source: source_facts,
            destination: destination_facts,
        });
    }
}

async fn permission_facts(
    endpoint: &Endpoint,
    entry: &SourceDescriptor,
) -> Result<PermissionFacts, IntegrityFailure> {
    if matches!(entry.version(), SourceVersion::Current) {
        return match endpoint.metadata.permissions(entry).await {
            Ok(facts) => Ok(facts),
            Err(failure) if is_unsupported(&failure) => Ok(PermissionFacts::default()),
            Err(failure) => Err(endpoint.attribute(failure)),
        };
    }
    if matches!(
        endpoint
            .metadata
            .observation_capabilities()
            .support(MetadataFamily::OwnershipMode),
        ObservationSupport::NotApplicable | ObservationSupport::Unsupported
    ) {
        return Ok(PermissionFacts::default());
    }
    let plan = ObservationPlan::default().with_ownership_mode(ObservationMode::BestEffort);
    let bound = match endpoint
        .metadata
        .observe_copy_bound_version(
            &endpoint.path,
            &entry.source_identity,
            entry.version(),
            plan,
        )
        .await
    {
        Ok(bound) => bound,
        Err(failure) if is_unsupported(&failure) => {
            return Ok(PermissionFacts::default());
        }
        Err(failure) => return Err(endpoint.attribute(failure)),
    };
    let ownership = optional_value(
        endpoint,
        bound.observations.ownership_mode(),
        "permission observation failed",
    )?;
    let mut facts = match ownership {
        Some(value) => PermissionFacts {
            owner: Some(PrincipalIdentity::Numeric(value.uid)),
            group: Some(PrincipalIdentity::Numeric(value.gid)),
            mode: Some(value.mode & 0o7777),
        },
        None => PermissionFacts::default(),
    };
    if facts.mode.is_none() {
        facts.mode = bound.mode_without_ownership;
    }
    Ok(facts)
}

fn is_unsupported(failure: &StorageRoleFailure) -> bool {
    matches!(failure, StorageRoleFailure::Entry(entry) if entry.class() == FailureClass::Unsupported)
}

pub(super) async fn compare_optional_metadata(
    source: &Endpoint,
    destination: &Endpoint,
    source_entry: &SourceDescriptor,
    destination_entry: &SourceDescriptor,
    requested: IntegrityPlan,
    differences: &mut Vec<IntegrityDifference>,
    ignored: &mut Vec<IntegrityIgnored>,
) -> Result<Option<AclComparisonScope>, IntegrityFailure> {
    let mode = |enabled| {
        if enabled {
            ObservationMode::BestEffort
        } else {
            ObservationMode::Omit
        }
    };
    let plan = ObservationPlan::default()
        .with_acl(mode(
            requested.acl || requested.acl_required || requested.acl_equivalent,
        ))
        .with_ownership_mode(mode(requested.acl_required || requested.acl_equivalent))
        .with_xattrs(mode(requested.xattrs));
    let (source_observed, destination_observed) = reconcile(futures::join!(
        observe_optional(source, source_entry, plan),
        observe_optional(destination, destination_entry, plan),
    ))?;
    let cross_acl_verified = if requested.acl || requested.acl_required || requested.acl_equivalent
    {
        compare_acl(
            source,
            destination,
            (source_entry, destination_entry),
            (&source_observed, &destination_observed),
            requested,
            differences,
            ignored,
        )?
    } else {
        None
    };
    if requested.xattrs {
        compare_xattr_values(
            source,
            destination,
            source_observed.xattrs(),
            destination_observed.xattrs(),
            differences,
            ignored,
        )?;
    }
    Ok(cross_acl_verified)
}

async fn observe_optional(
    endpoint: &Endpoint,
    entry: &SourceDescriptor,
    plan: ObservationPlan,
) -> Result<MetadataObservations, IntegrityFailure> {
    endpoint
        .metadata
        .observe_copy_bound_version(
            &endpoint.path,
            &entry.source_identity,
            entry.version(),
            plan,
        )
        .await
        .map(|bound| bound.observations)
        .map_err(|error| endpoint.attribute(error))
}

fn compare_xattr_values(
    source: &Endpoint,
    destination: &Endpoint,
    source_observed: &MetadataObservation<Vec<ExtendedAttribute>>,
    destination_observed: &MetadataObservation<Vec<ExtendedAttribute>>,
    differences: &mut Vec<IntegrityDifference>,
    ignored: &mut Vec<IntegrityIgnored>,
) -> Result<(), IntegrityFailure> {
    let source_xattrs = optional_value(source, source_observed, "xattr observation failed")?;
    let destination_xattrs = optional_value(
        destination,
        destination_observed,
        "xattr observation failed",
    )?;
    match (source_xattrs, destination_xattrs) {
        (Some(source_xattrs), Some(destination_xattrs)) => {
            if sorted_xattrs(source_xattrs) != sorted_xattrs(destination_xattrs) {
                differences.push(IntegrityDifference::Xattrs);
            }
        }
        _ => ignored.push(IntegrityIgnored::Xattrs),
    }
    Ok(())
}

fn sorted_xattrs(values: &[ExtendedAttribute]) -> Vec<&ExtendedAttribute> {
    let mut sorted: Vec<_> = values.iter().collect();
    sorted.sort_unstable_by(|left, right| {
        left.name()
            .cmp(right.name())
            .then_with(|| left.value().cmp(right.value()))
    });
    sorted
}

pub(super) fn optional_value<'a, T>(
    endpoint: &Endpoint,
    observed: &'a MetadataObservation<T>,
    reason: &'static str,
) -> Result<Option<&'a T>, IntegrityFailure> {
    match observed {
        MetadataObservation::Value { value, .. } => Ok(Some(value)),
        MetadataObservation::Failed { class, transience } => {
            Err(endpoint.attribute(StorageRoleFailure::Entry(
                EntryOperationFailure::new(
                    endpoint.path.clone(),
                    Operation::Observe,
                    *class,
                    *transience,
                    reason,
                )
                .unwrap_or_else(|_| unreachable!("static metadata diagnostic is valid")),
            )))
        }
        MetadataObservation::NotRequested
        | MetadataObservation::NotApplicable
        | MetadataObservation::Unsupported => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mixed_principal_namespaces_ignore_owners_but_still_compare_mode() {
        let mut differences = Vec::new();
        let mut ignored = Vec::new();
        compare_permission_facts(
            PermissionFacts {
                owner: Some(PrincipalIdentity::Numeric(1000)),
                group: Some(PrincipalIdentity::Numeric(100)),
                mode: Some(0o640),
            },
            PermissionFacts {
                owner: Some(PrincipalIdentity::Named("alice".into())),
                group: Some(PrincipalIdentity::Named("staff".into())),
                mode: Some(0o600),
            },
            false,
            false,
            &mut differences,
            &mut ignored,
        );
        assert_eq!(ignored, [IntegrityIgnored::Owner, IntegrityIgnored::Group]);
        assert!(matches!(
            differences.as_slice(),
            [IntegrityDifference::Permissions { .. }]
        ));
    }

    #[test]
    fn missing_permission_facts_are_ignored_individually() {
        let mut differences = Vec::new();
        let mut ignored = Vec::new();
        compare_permission_facts(
            PermissionFacts {
                owner: None,
                group: Some(PrincipalIdentity::Numeric(100)),
                mode: Some(0o640),
            },
            PermissionFacts {
                owner: Some(PrincipalIdentity::Numeric(1000)),
                group: Some(PrincipalIdentity::Numeric(200)),
                mode: None,
            },
            false,
            false,
            &mut differences,
            &mut ignored,
        );
        assert_eq!(ignored, [IntegrityIgnored::Owner, IntegrityIgnored::Mode]);
        assert!(matches!(
            differences.as_slice(),
            [IntegrityDifference::Permissions { .. }]
        ));
    }
}
