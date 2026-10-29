//! Automatic ACL projection and structured reporting of omitted permissions.
use super::{
    AclCopyReport, AclTarget, FamilyMapping, MappingDecision, MetadataFamily, MetadataPlan,
    MetadataPlanError, MetadataPlanRequest, MetadataPolicy, RefusalCause, SemanticLoss,
    SkippedFamily,
};
use crate::model::acl::{
    AclCopyIssue, AclCopyIssueKind, AclError, AclIssueLocation, AclProjection, decode_nfs4,
    decode_posix, project_acl, safe_deny_all_acl, uncopied_acl_entries,
};
use crate::model::{AclEncoding, AclMetadata, MetadataObservation, OwnershipMode};
use crate::storage::MetadataMutation;

/// Project what can be proven, falling back to an explicit data-denying ACL when it cannot.
pub(super) fn compile(
    request: &MetadataPlanRequest<'_>,
    plan: &mut MetadataPlan,
    mode_hint: Option<u32>,
) -> Result<(), MetadataPlanError> {
    if let MetadataObservation::Failed { class, transience } = request.observations.acl() {
        return Err(refusal(RefusalCause::SourceObservationFailed {
            class: *class,
            transience: *transience,
        }));
    }
    let AclTarget::Encoding(target) = request.target.acl else {
        plan.mappings.push(FamilyMapping {
            family: MetadataFamily::Acl,
            decision: MappingDecision::Unsupported,
        });
        plan.skipped.push(SkippedFamily {
            family: MetadataFamily::Acl,
            reason: RefusalCause::DestinationCannotStore,
        });
        return Ok(());
    };
    let source = match request.observations.acl() {
        MetadataObservation::Value { value, .. } => Some(value),
        _ => None,
    };
    let source_mode = request
        .observations
        .ownership_mode()
        .value()
        .map(|owner| owner.mode)
        .or(mode_hint);
    if let Some(value) = source {
        validate(value)?;
        let mut hash = blake3::Hasher::new();
        super::checkpoint_binding::acl(&mut hash, value);
        if let Some(mode) = source_mode {
            hash.update(&mode.to_le_bytes());
        }
        plan.acl_source_binding = Some(*hash.finalize().as_bytes());
    }
    let owner = plan
        .mutations
        .iter()
        .find_map(|(_, mutation)| match mutation {
            MetadataMutation::NumericOwnership(owner) => Some(*owner),
            _ => None,
        });
    let projected = project(source, target, owner)?;
    record(plan, source, target, source_mode, projected)
}

fn validate(value: &AclMetadata) -> Result<(), MetadataPlanError> {
    let result = match value.encoding() {
        AclEncoding::Posix => decode_posix(value).map(|_| ()),
        AclEncoding::NfsV4 => decode_nfs4(value).map(|_| ()),
        AclEncoding::WindowsSecurityDescriptor => Ok(()),
    };
    result.map_err(reject_acl)
}

fn project(
    source: Option<&AclMetadata>,
    target: AclEncoding,
    owner: Option<OwnershipMode>,
) -> Result<AclProjection, MetadataPlanError> {
    if let (Some(value), Some(owner)) = (source, owner) {
        return project_acl(value, target, owner).map_err(reject_acl);
    }
    let Some(value) = source else {
        return Ok(AclProjection {
            candidate: None,
            issues: vec![issue(AclCopyIssueKind::SourceUnavailable)],
        });
    };
    let mut issues = vec![issue(AclCopyIssueKind::OwnershipNotPreserved)];
    // Windows copies a DACL natively without a numeric owner observation.
    let candidate = (value.encoding() == AclEncoding::WindowsSecurityDescriptor
        && target == value.encoding())
    .then(|| value.clone());
    if value.encoding() != target
        && !matches!(
            (value.encoding(), target),
            (AclEncoding::Posix, AclEncoding::NfsV4) | (AclEncoding::NfsV4, AclEncoding::Posix)
        )
    {
        issues.push(issue(AclCopyIssueKind::UnsupportedEncoding));
    }
    Ok(AclProjection { candidate, issues })
}

fn record(
    plan: &mut MetadataPlan,
    source: Option<&AclMetadata>,
    target: AclEncoding,
    source_mode: Option<u32>,
    mut projection: AclProjection,
) -> Result<(), MetadataPlanError> {
    let fallback = projection.candidate.is_none();
    let candidate = if let Some(candidate) = projection.candidate {
        candidate
    } else {
        if let Some(source) = source {
            // Replace any partial-entry summaries with the full fallback inventory.
            projection
                .issues
                .retain(|issue| !matches!(issue.kind, AclCopyIssueKind::UncopiedEntry { .. }));
            projection
                .issues
                .extend(uncopied_acl_entries(source, source_mode).map_err(reject_acl)?);
        }
        if !projection
            .issues
            .iter()
            .any(|v| matches!(v.kind, AclCopyIssueKind::NoSafeCandidate))
        {
            projection
                .issues
                .push(issue(AclCopyIssueKind::NoSafeCandidate));
        }
        safe_deny_all_acl(target).map_err(reject_acl)?
    };
    let decision = if projection.issues.is_empty() {
        if source.is_some_and(|value| value.encoding() == target) {
            MappingDecision::Exact
        } else {
            MappingDecision::Equivalent
        }
    } else {
        plan.losses
            .0
            .push((MetadataFamily::Acl, SemanticLoss::AclPartiallyCopied));
        MappingDecision::Lossy(vec![SemanticLoss::AclPartiallyCopied])
    };
    plan.mappings.push(FamilyMapping {
        family: MetadataFamily::Acl,
        decision,
    });
    plan.mutations
        .push((MetadataFamily::Acl, MetadataMutation::Acl(candidate)));
    plan.acl_copy = Some(AclCopyReport {
        issues: projection.issues,
        conservative_fallback: fallback,
    });
    Ok(())
}

fn reject_acl(error: AclError) -> MetadataPlanError {
    refusal(RefusalCause::AclMappingRejected {
        reason: error.reason(),
    })
}

const fn refusal(cause: RefusalCause) -> MetadataPlanError {
    MetadataPlanError::new(MetadataFamily::Acl, MetadataPolicy::BestEffort, cause)
}

fn issue(kind: AclCopyIssueKind) -> AclCopyIssue {
    AclCopyIssue {
        location: AclIssueLocation::Acl,
        principal: None,
        kind,
    }
}
