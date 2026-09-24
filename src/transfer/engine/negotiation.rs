//! Which metadata a copy carries: the baseline every copy takes, the families the caller asks
//! for, and what the two ends can do. See `.claude/docs/metadata-negotiation.md`.

use std::fmt;
use std::sync::Arc;

use super::{TransferFailure, TransferPhase, TransferSide, Transferred};
use crate::metadata::{
    AclTarget, MetadataApplicationFailure, MetadataPlan, MetadataPlanError, MetadataPlanRequest,
    MetadataPolicies, MetadataPolicy, MetadataTarget, OwnershipTarget, RefusalSide,
    TimestampTarget, TimestampTargetCapability, ValueTarget, compile_copied_metadata_plan,
    compile_nothing_stored_plan, role_failure_text,
};
use crate::model::{MetadataObservation, MetadataObservations, ObservationMode, ObservationPlan};
use crate::storage::{
    CopiedAclTarget, CopiedMetadataTarget, CopiedOwnershipTarget, CopiedTimestampTarget,
    CopiedValueTarget, Metadata, PreflightPolicy, SourceDescriptor, StagedDestination,
    StorageRoleFailure,
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
        destination,
        target,
    }) = metadata_roles(request)
    else {
        return Ok(None);
    };
    if target.stores_nothing() {
        return Ok(Some(nothing_stored(requested)));
    }
    let timestamps_stored = matches!(target.timestamps, CopiedTimestampTarget::Stored(_));
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
    let owner_permitted = owner_permitted(
        |uid, gid| destination.may_set_owner(uid, gid),
        &observations.observations,
    );
    let (target, ownership_policy) = metadata_target(target, owner_permitted);
    let policies = copied_policies(
        requested,
        ownership_policy,
        observations.mode_without_ownership.is_some(),
        timestamps_stored,
    );
    compile_copied_metadata_plan(
        &MetadataPlanRequest {
            observations: &observations.observations,
            target,
            policies,
            principal_mapper: None,
        },
        observations.mode_without_ownership,
        observations.owner_names_unmapped,
    )
    .map(|plan| Some(CopiedMetadataPlan { plan }))
    .map_err(TransferFailure::refused_metadata)
}

/// A destination that keeps nothing a copy carries: nothing the source holds could change the
/// outcome, so the source is not read — no request per file just to report that the destination
/// keeps none of it. The baseline and whatever the caller asked for are each skipped with that
/// reason. The source identity is still enforced by every read of the data.
fn nothing_stored(requested: CopiedMetadataRequest) -> CopiedMetadataPlan {
    let policies = MetadataPolicies::default()
        .with_ownership_mode(MetadataPolicy::AllowKnownLoss)
        .with_timestamps(MetadataPolicy::AllowKnownLoss)
        .with_acl(optional_policy(requested.acl()))
        .with_xattrs(optional_policy(requested.xattrs()));
    CopiedMetadataPlan {
        plan: compile_nothing_stored_plan(policies),
    }
}

/// The source's metadata role with the baseline it observes for a copy, and what the destination
/// accepts.
struct MetadataRoles {
    source: Arc<dyn Metadata>,
    baseline: ObservationPlan,
    destination: Arc<dyn StagedDestination>,
    target: CopiedMetadataTarget,
}

/// Both ends' metadata roles, or `None` when an end has none. The copy then goes on without
/// metadata and without a report — today only a source with no copy baseline (S3, K8), since
/// every staged destination but a non-unix Local declares a target.
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
        destination,
        target,
    })
}

/// Whether the destination may give this file the source's owner and group — asked per file,
/// because an unprivileged writer may keep its own files' owners but not give files away.
fn owner_permitted(
    may_set_owner: impl Fn(u32, u32) -> bool,
    observations: &MetadataObservations,
) -> bool {
    match observations.ownership_mode() {
        MetadataObservation::Value { value, .. } => may_set_owner(value.uid, value.gid),
        _ => true,
    }
}

/// Translates what the destination accepts across the layer boundary — `storage` cannot name
/// `metadata`'s vocabulary, and this is the layer that sees both — together with the ownership
/// policy that target implies. Whether a family is carried at all is the caller's request, which
/// reaches the compiler as a policy.
fn metadata_target(
    target: CopiedMetadataTarget,
    owner_permitted: bool,
) -> (MetadataTarget, MetadataPolicy) {
    let (ownership_mode, ownership_policy) = match target.ownership {
        CopiedOwnershipTarget::Numeric if owner_permitted => {
            (OwnershipTarget::Numeric, MetadataPolicy::RequireExact)
        }
        CopiedOwnershipTarget::Numeric => (
            OwnershipTarget::NotPermitted,
            MetadataPolicy::AllowKnownLoss,
        ),
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
        timestamps: match target.timestamps {
            CopiedTimestampTarget::Stored(precision) => {
                TimestampTargetCapability::Supported(TimestampTarget {
                    precision,
                    accessed: false,
                    modified: true,
                    created: false,
                })
            }
            CopiedTimestampTarget::NotStored => TimestampTargetCapability::Unsupported,
        },
    };
    (target, ownership_policy)
}

/// The baseline every copy carries — ownership/mode as the target allows, mtime — plus the
/// optional families at whatever the caller asked for. A destination that does not store the
/// modification time has it skipped with that reason; it is not the copy's to refuse.
fn copied_policies(
    requested: CopiedMetadataRequest,
    ownership_policy: MetadataPolicy,
    mode_without_ownership: bool,
    timestamps_stored: bool,
) -> MetadataPolicies {
    let ownership_policy = if mode_without_ownership {
        MetadataPolicy::AllowKnownLoss
    } else {
        ownership_policy
    };
    MetadataPolicies::default()
        .with_ownership_mode(ownership_policy)
        .with_timestamps(if timestamps_stored {
            MetadataPolicy::AllowKnownLoss
        } else {
            MetadataPolicy::BestEffort
        })
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

    fn numeric_destination() -> CopiedMetadataTarget {
        CopiedMetadataTarget {
            timestamps: CopiedTimestampTarget::Stored(crate::model::TimePrecision::Nanoseconds),
            ownership: CopiedOwnershipTarget::Numeric,
            acl: CopiedAclTarget::Unsupported,
            xattrs: CopiedValueTarget::Unsupported,
        }
    }

    /// A numeric destination that may not give this file its source owner carries the mode alone,
    /// as a known loss — never a refusal that fails the file on the write.
    #[test]
    fn an_owner_the_writer_may_not_set_becomes_mode_only_with_a_named_loss() {
        let (target, policy) = metadata_target(numeric_destination(), true);
        assert_eq!(target.ownership_mode, OwnershipTarget::Numeric);
        assert_eq!(policy, MetadataPolicy::RequireExact);
        let (target, policy) = metadata_target(numeric_destination(), false);
        assert_eq!(target.ownership_mode, OwnershipTarget::NotPermitted);
        assert_eq!(policy, MetadataPolicy::AllowKnownLoss);
    }

    /// The destination is asked about the owner the source actually has; with no owner observed
    /// there is nothing to ask, and nothing to lose.
    #[test]
    fn the_destination_is_asked_about_this_files_owner() {
        let owned_by = |uid, gid| MetadataObservations {
            ownership_mode: MetadataObservation::Value {
                value: crate::model::OwnershipMode {
                    uid,
                    gid,
                    mode: 0o100_644,
                },
                provenance: crate::model::MetadataProvenance::Inline,
            },
            ..MetadataObservations::default()
        };
        let only_mine = |uid, gid| uid == 1000 && gid == 1000;
        assert!(owner_permitted(only_mine, &owned_by(1000, 1000)));
        assert!(!owner_permitted(only_mine, &owned_by(0, 0)));
        assert!(!owner_permitted(only_mine, &owned_by(1000, 0)));
        assert!(owner_permitted(
            |_, _| false,
            &MetadataObservations::default()
        ));
    }

    /// A destination that keeps owner and mode but not the modification time: mtime is skipped
    /// because of the destination, and the file is not failed for it.
    #[test]
    fn a_modification_time_the_destination_does_not_store_is_skipped_not_refused()
    -> Result<(), Box<dyn std::error::Error>> {
        let (target, ownership_policy) = metadata_target(
            CopiedMetadataTarget {
                timestamps: CopiedTimestampTarget::NotStored,
                ..numeric_destination()
            },
            true,
        );
        assert_eq!(target.timestamps, TimestampTargetCapability::Unsupported);
        let policies = copied_policies(
            CopiedMetadataRequest::default(),
            ownership_policy,
            false,
            false,
        );
        let observations = MetadataObservations {
            timestamps: MetadataObservation::Value {
                value: crate::model::TimestampMetadata {
                    accessed: None,
                    modified: Some(crate::model::StorageTimestamp::new(
                        1_000_000_000,
                        crate::model::TimePrecision::Nanoseconds,
                    )?),
                    created: None,
                },
                provenance: crate::model::MetadataProvenance::Inline,
            },
            ownership_mode: MetadataObservation::Value {
                value: crate::model::OwnershipMode {
                    uid: 1000,
                    gid: 1000,
                    mode: 0o100_644,
                },
                provenance: crate::model::MetadataProvenance::Inline,
            },
            ..MetadataObservations::default()
        };
        let plan = compile_copied_metadata_plan(
            &MetadataPlanRequest {
                observations: &observations,
                target,
                policies,
                principal_mapper: None,
            },
            None,
            false,
        )?;
        assert_eq!(
            plan.skipped(),
            [crate::metadata::SkippedFamily {
                family: MetadataFamily::Timestamps,
                reason: RefusalCause::DestinationCannotStore,
            }]
        );
        Ok(())
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
