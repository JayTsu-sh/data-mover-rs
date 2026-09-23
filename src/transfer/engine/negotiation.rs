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
use crate::model::{ObservationMode, ObservationPlan};
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
    }) = metadata_roles(request)
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

/// Both ends' metadata roles, or `None` when an end has none. The copy then goes on without
/// metadata — mtime included, which the mandatory-mtime rule still has to close.
fn metadata_roles(request: &TransferRequest) -> Option<MetadataRoles> {
    let source = request
        .source
        .metadata(&PreflightPolicy::production())
        .ok()?;
    let baseline = source.copied_metadata_observation_plan()?;
    let destination = request
        .destination
        .staged_destination(&PreflightPolicy::production())
        .ok()?;
    let target = destination.copied_metadata_target()?;
    Some(MetadataRoles {
        source,
        baseline,
        target,
    })
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
        .with_acl(optional_policy(requested.acl()))
        .with_xattrs(optional_policy(requested.xattrs()))
}

/// How an optional feature is planned: not asked for, it is not even read; asked for, it is
/// carried exactly or with a named loss when both ends can, and skipped with the reason when
/// either cannot — never refused, because what a user asks the storage cannot do is the caller's
/// to judge. A read that fails is still a failure (`BestEffort` no longer skips those).
const fn optional_policy(asked: bool) -> MetadataPolicy {
    if asked {
        MetadataPolicy::BestEffort
    } else {
        MetadataPolicy::Omit
    }
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

/// How hard the source looks for an optional feature: not at all unless asked, and when asked,
/// as far as it can — a source that cannot read the feature says so rather than failing.
const fn observation_mode(asked: bool) -> ObservationMode {
    if asked {
        ObservationMode::BestEffort
    } else {
        ObservationMode::Omit
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{MetadataFamily, RefusalCause};
    use crate::model::AclEncoding;

    /// Not asked for, a feature is not even read; asked for, the source reads it as far as it can.
    #[test]
    fn asking_decides_whether_the_source_looks() {
        assert_eq!(observation_mode(false), ObservationMode::Omit);
        assert_eq!(observation_mode(true), ObservationMode::BestEffort);
        assert_eq!(optional_policy(false), MetadataPolicy::Omit);
        assert_eq!(optional_policy(true), MetadataPolicy::BestEffort);
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
