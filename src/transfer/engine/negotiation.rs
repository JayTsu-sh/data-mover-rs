//! Which metadata a copy carries: the baseline every copy takes, the families the caller asks
//! for, and what the two ends can do. See `.claude/docs/metadata-negotiation.md`.

use std::sync::Arc;

use super::{TransferFailure, TransferPhase, TransferSide, Transferred};
use crate::metadata::{
    AclTarget, MetadataFamily, MetadataPlan, MetadataPlanError, MetadataPlanErrorKind,
    MetadataPlanRequest, MetadataPolicies, MetadataPolicy, MetadataTarget, OwnershipTarget,
    TimestampTarget, TimestampTargetCapability, ValueTarget, compile_copied_metadata_plan,
};
use crate::model::ObservationPlan;
use crate::storage::{
    CopiedAclTarget, CopiedMetadataTarget, CopiedOwnershipTarget, CopiedValueTarget, Metadata,
    PreflightPolicy, SourceDescriptor,
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
            TransferFailure::role(TransferPhase::Metadata, TransferSide::Source, error)
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
    .map_err(|error| TransferFailure::orchestration(TransferPhase::Metadata, refusal(error)))
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

/// Names the refusal a caller can act on. "One side cannot store ACLs at all" and "both sides can
/// but their encodings differ" lead to different decisions, and a single message for every
/// planning refusal hides which one happened. `TransferFailure` carries a `&'static str`, so the
/// distinctions that matter get their own sentence and the rest share one.
const fn refusal(error: MetadataPlanError) -> &'static str {
    match (error.family(), error.kind()) {
        (MetadataFamily::Acl, MetadataPlanErrorKind::Unsupported) => {
            "copied ACL: one side does not support ACLs"
        }
        (MetadataFamily::Acl, MetadataPlanErrorKind::ExternalMappingRequired) => {
            "copied ACL: the two sides use different encodings and need an external mapping"
        }
        (MetadataFamily::Acl, _) => "copied ACL: refused by policy",
        (MetadataFamily::Xattrs, MetadataPlanErrorKind::Unsupported) => {
            "copied extended attributes: one side does not support them"
        }
        (MetadataFamily::Xattrs, _) => "copied extended attributes: refused by policy",
        _ => "copied metadata could not be planned",
    }
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

    /// "Neither side can" and "both can but they disagree on the encoding" send a caller to
    /// different places, so they must not share a message.
    #[test]
    fn a_refusal_says_which_kind_it_was() {
        let unsupported = refusal(MetadataPlanError::new(
            MetadataFamily::Acl,
            MetadataPlanErrorKind::Unsupported,
        ));
        let mapping = refusal(MetadataPlanError::new(
            MetadataFamily::Acl,
            MetadataPlanErrorKind::ExternalMappingRequired,
        ));
        assert_ne!(unsupported, mapping);
        assert!(unsupported.contains("does not support"));
        assert!(mapping.contains("encodings"));
        assert_ne!(
            refusal(MetadataPlanError::new(
                MetadataFamily::Xattrs,
                MetadataPlanErrorKind::Unsupported,
            )),
            unsupported
        );
    }
}
