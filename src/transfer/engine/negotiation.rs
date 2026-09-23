//! Which metadata a copy carries: the baseline every copy takes, the families the caller asks
//! for, and what the two ends can do. See `.claude/docs/metadata-negotiation.md`.

use std::fmt;
use std::sync::Arc;

use super::{TransferFailure, TransferPhase, TransferSide, Transferred};
use crate::metadata::{
    AclTarget, MetadataApplicationFailure, MetadataPlan, MetadataPlanError, MetadataPlanRequest,
    MetadataPolicies, MetadataPolicy, MetadataTarget, OwnershipTarget, RefusalSide,
    TimestampTarget, TimestampTargetCapability, ValueTarget, compile_copied_metadata_plan,
    role_failure_text,
};
use crate::model::ObservationPlan;
use crate::storage::{
    CopiedAclTarget, CopiedMetadataTarget, CopiedOwnershipTarget, CopiedValueTarget, Metadata,
    PreflightPolicy, SourceDescriptor, StorageRoleFailure,
};
use crate::transfer::{CopiedMetadataRequest, TransferRequest};

pub(super) async fn apply_copied_metadata(
    transferred: &Transferred,
    copied: Option<&CopiedMetadataPlan>,
    cancel: tokio_util::sync::CancellationToken,
) -> Result<
    Option<crate::metadata::MetadataApplicationReport>,
    crate::metadata::MetadataApplicationFailure,
> {
    let Some(copied) = copied else {
        return Ok(None);
    };
    copied
        .plan
        .apply_to_stage(transferred.destination.as_ref(), &transferred.stage, cancel)
        .await
        .map(Some)
}

pub(super) struct CopiedMetadataPlan {
    plan: MetadataPlan,
}

pub(super) async fn copied_metadata_plan(
    request: &TransferRequest,
    descriptor: &SourceDescriptor,
) -> Result<Option<CopiedMetadataPlan>, TransferFailure> {
    let requested = request.copied_metadata;
    let Some(MetadataRoles {
        source: metadata,
        baseline,
        target,
    }) = metadata_roles(request)?
    else {
        return Ok(None);
    };
    let observation_plan = baseline
        .with_acl(observation_mode(requested.acl()))
        .with_xattrs(observation_mode(requested.xattrs()));
    let observations = metadata
        .observe_copy_bound(
            &request.source_path,
            &descriptor.source_identity,
            observation_plan,
        )
        .await
        .map_err(|error| {
            let mut failure =
                TransferFailure::role(TransferPhase::Metadata, TransferSide::Source, error);
            failure.message = "observing source metadata failed";
            failure
        })?;
    let (target, ownership_policy) = metadata_target(target);
    let policies = copied_policies(
        requested,
        ownership_policy,
        observations.mode_without_ownership.is_some(),
    );
    compile_copied_metadata_plan(
        &MetadataPlanRequest {
            observations: &observations.observations,
            target,
            policies,
            principal_mapper: None,
        },
        observations.mode_without_ownership,
    )
    .map(|plan| Some(CopiedMetadataPlan { plan }))
    .map_err(TransferFailure::refused_metadata)
}

/// The source's metadata role with the baseline it observes for a copy, and what the destination
/// accepts.
struct MetadataRoles {
    source: Arc<dyn Metadata>,
    baseline: ObservationPlan,
    target: CopiedMetadataTarget,
}

/// Both ends' metadata roles, or `None` when an end has none and the request can go on without
/// metadata.
fn metadata_roles(request: &TransferRequest) -> Result<Option<MetadataRoles>, TransferFailure> {
    let requested = request.copied_metadata;
    let Ok(metadata) = request.source.metadata(&PreflightPolicy::production()) else {
        refuse_unavailable(
            requested,
            "copied metadata was requested but the source has no metadata role",
        )?;
        return Ok(None);
    };
    let Some(baseline) = metadata.copied_metadata_observation_plan() else {
        refuse_unavailable(
            requested,
            "copied metadata was requested but the source copies none",
        )?;
        return Ok(None);
    };
    let Ok(destination) = request
        .destination
        .staged_destination(&PreflightPolicy::production())
    else {
        refuse_unavailable(
            requested,
            "copied metadata was requested but the destination cannot stage",
        )?;
        return Ok(None);
    };
    let Some(target) = destination.copied_metadata_target() else {
        refuse_unavailable(
            requested,
            "copied metadata was requested but the destination copies none",
        )?;
        return Ok(None);
    };
    Ok(Some(MetadataRoles {
        source: metadata,
        baseline,
        target,
    }))
}

/// Translates what the destination accepts across the layer boundary — `storage` cannot name
/// `metadata`'s vocabulary, and this is the layer that sees both — together with the ownership
/// policy that target implies. Whether a family is carried at all is the caller's request, which
/// reaches the compiler as a policy.
fn metadata_target(target: CopiedMetadataTarget) -> (MetadataTarget, MetadataPolicy) {
    let (ownership_mode, ownership_policy) = match target.ownership {
        CopiedOwnershipTarget::Numeric => (OwnershipTarget::Numeric, MetadataPolicy::RequireExact),
        CopiedOwnershipTarget::Unsupported => (
            OwnershipTarget::NotApplicable,
            MetadataPolicy::AllowKnownLoss,
        ),
        CopiedOwnershipTarget::ModeOnly => {
            (OwnershipTarget::ModeOnly, MetadataPolicy::AllowKnownLoss)
        }
    };
    let target = MetadataTarget {
        acl: match target.acl {
            CopiedAclTarget::Encoding(encoding) => AclTarget::Encoding(encoding),
            CopiedAclTarget::Unsupported => AclTarget::Unsupported,
        },
        xattrs: match target.xattrs {
            CopiedValueTarget::Supported => ValueTarget::Supported,
            CopiedValueTarget::Unsupported => ValueTarget::Unsupported,
        },
        tags: ValueTarget::NotApplicable,
        ownership_mode,
        timestamps: TimestampTargetCapability::Supported(TimestampTarget {
            precision: target.timestamp_precision,
            accessed: false,
            modified: true,
            created: false,
        }),
    };
    (target, ownership_policy)
}

/// The baseline every copy carries — ownership/mode as the target allows, mtime — plus the
/// optional families at whatever the caller asked for.
fn copied_policies(
    requested: CopiedMetadataRequest,
    ownership_policy: MetadataPolicy,
    mode_without_ownership: bool,
) -> MetadataPolicies {
    let ownership_policy = if mode_without_ownership {
        MetadataPolicy::AllowKnownLoss
    } else {
        ownership_policy
    };
    MetadataPolicies::default()
        .with_ownership_mode(ownership_policy)
        .with_timestamps(MetadataPolicy::AllowKnownLoss)
        .with_acl(requested.acl())
        .with_xattrs(requested.xattrs())
}

impl TransferFailure {
    /// Copied metadata refused while planning: the headline names the end the cause is about.
    pub(super) fn refused_metadata(error: MetadataPlanError) -> Self {
        let side = match error.cause().side() {
            RefusalSide::Source => TransferSide::Source,
            RefusalSide::Destination => TransferSide::Destination,
            _ => TransferSide::Orchestration,
        };
        let mut failure = Self::orchestration(
            TransferPhase::Metadata,
            "copied metadata was refused while planning",
        );
        failure.side = side;
        failure.refusal = Some(error);
        failure
    }
}

/// The part of a metadata-phase failure a caller acts on: which family was refused while
/// planning and why, or which write failed and how, or what reading the source reported — each
/// with the adapter's (already redacted) diagnostic.
pub(super) fn write_metadata_detail(
    formatter: &mut fmt::Formatter<'_>,
    refusal: Option<MetadataPlanError>,
    application: Option<&MetadataApplicationFailure>,
    role: Option<&StorageRoleFailure>,
) -> fmt::Result {
    if let Some(refusal) = refusal {
        write!(formatter, ": {refusal}")?;
    }
    if let Some(application) = application {
        write!(formatter, ": {application}")?;
    } else if let Some(role) = role {
        write!(formatter, ": {}", role_failure_text(role))?;
    }
    Ok(())
}

/// What a family that was asked for but cannot be reached means for the transfer.
///
/// `BestEffort` is the one policy that says "carry it if you can" rather than "carry it", so it
/// is the only one that may proceed with the baseline alone. Anything else asked for a family
/// that no longer has a route, and silently dropping it is exactly the failure this parameter
/// exists to prevent.
fn refuse_unavailable(
    requested: CopiedMetadataRequest,
    reason: &'static str,
) -> Result<Option<CopiedMetadataPlan>, TransferFailure> {
    let demanded = [requested.acl(), requested.xattrs()]
        .into_iter()
        .any(|policy| !matches!(policy, MetadataPolicy::Omit | MetadataPolicy::BestEffort));
    if demanded {
        return Err(TransferFailure::orchestration(
            TransferPhase::Metadata,
            reason,
        ));
    }
    Ok(None)
}

/// Translates a copy policy into how hard the source should look. `Omit` leaves the backend's own
/// baseline alone rather than overriding it.
fn observation_mode(policy: MetadataPolicy) -> crate::model::ObservationMode {
    match policy {
        MetadataPolicy::Omit => crate::model::ObservationMode::Omit,
        MetadataPolicy::BestEffort => crate::model::ObservationMode::BestEffort,
        MetadataPolicy::RequireExact | MetadataPolicy::AllowKnownLoss => {
            crate::model::ObservationMode::Required
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{MetadataFamily, RefusalCause};
    use crate::model::AclEncoding;

    /// `Omit` must leave the backend's own baseline plan alone rather than overriding it with
    /// `Omit`, and the two demanding policies must both make the source actually look.
    #[test]
    fn a_policy_decides_how_hard_the_source_looks() {
        assert_eq!(
            observation_mode(MetadataPolicy::Omit),
            crate::model::ObservationMode::Omit
        );
        assert_eq!(
            observation_mode(MetadataPolicy::BestEffort),
            crate::model::ObservationMode::BestEffort
        );
        for policy in [MetadataPolicy::AllowKnownLoss, MetadataPolicy::RequireExact] {
            assert_eq!(
                observation_mode(policy),
                crate::model::ObservationMode::Required
            );
        }
    }

    /// A family that was asked for and has no route must not be dropped quietly — silently
    /// copying less than asked is the failure this parameter exists to prevent. `BestEffort` is
    /// the one policy that permits it, because it asks rather than requires.
    #[test]
    fn a_route_that_does_not_exist_is_only_tolerated_by_best_effort() {
        let reason = "no route";
        assert!(refuse_unavailable(CopiedMetadataRequest::default(), reason).is_ok());
        assert!(
            refuse_unavailable(
                CopiedMetadataRequest::default().with_acl(MetadataPolicy::BestEffort),
                reason
            )
            .is_ok()
        );
        for policy in [MetadataPolicy::AllowKnownLoss, MetadataPolicy::RequireExact] {
            assert!(
                refuse_unavailable(CopiedMetadataRequest::default().with_acl(policy), reason)
                    .is_err(),
                "{policy:?} asked for the ACL and must not proceed without it"
            );
            assert!(
                refuse_unavailable(CopiedMetadataRequest::default().with_xattrs(policy), reason)
                    .is_err()
            );
        }
    }

    fn refused(cause: RefusalCause) -> String {
        TransferFailure::refused_metadata(MetadataPlanError::new(
            MetadataFamily::Acl,
            MetadataPolicy::RequireExact,
            cause,
        ))
        .to_string()
    }

    /// A refusal names the family, the policy it was asked under, and which end lacks it — "the
    /// source cannot read it" and "the destination cannot store it" send a caller to different
    /// places, and so does "the encodings differ", which also names both.
    #[test]
    fn a_planning_refusal_says_what_which_end_and_why() {
        let source = refused(RefusalCause::SourceCannotObserve);
        let destination = refused(RefusalCause::DestinationCannotStore);
        assert!(source.contains("on Source"), "{source}");
        assert!(
            source.contains("ACL (RequireExact): the source cannot read it"),
            "{source}"
        );
        assert!(destination.contains("on Destination"), "{destination}");
        assert!(
            destination.contains("the destination cannot store it"),
            "{destination}"
        );
        let encodings = refused(RefusalCause::EncodingsDiffer {
            source: AclEncoding::NfsV4,
            destination: AclEncoding::WindowsSecurityDescriptor,
        });
        assert!(encodings.contains("NfsV4"), "{encodings}");
        assert!(
            encodings.contains("WindowsSecurityDescriptor"),
            "{encodings}"
        );
    }
}
