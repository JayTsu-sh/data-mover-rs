//! A refusal has to say which family, under which policy, and which end lacks it — and a failed
//! write which family, how, and what the storage reported. These are what a caller acts on.

use super::*;

fn refusal(
    observations: &MetadataObservations,
    target: MetadataTarget,
    policies: MetadataPolicies,
) -> MetadataPlanError {
    compile_metadata_plan(&MetadataPlanRequest {
        observations,
        target,
        policies,
        principal_mapper: None,
    })
    .unwrap_err()
}

fn with_acl(acl: MetadataObservation<AclMetadata>) -> MetadataObservations {
    let exact = exact_observations();
    MetadataObservations::new(
        acl,
        exact.xattrs().clone(),
        exact.tags().clone(),
        exact.ownership_mode().clone(),
        exact.timestamps().clone(),
    )
    .unwrap()
}

/// "The source cannot read it" and "the destination cannot store it" used to be one kind.
#[test]
fn a_refusal_names_the_end_that_lacks_the_family() {
    let source = refusal(
        &with_acl(MetadataObservation::Unsupported),
        exact_target(),
        all_exact(),
    );
    assert_eq!(source.cause(), RefusalCause::SourceCannotObserve);
    assert_eq!(source.cause().side(), RefusalSide::Source);
    let destination = refusal(
        &exact_observations(),
        MetadataTarget {
            acl: AclTarget::Unsupported,
            ..exact_target()
        },
        all_exact(),
    );
    assert_eq!(destination.cause(), RefusalCause::DestinationCannotStore);
    assert_eq!(destination.cause().side(), RefusalSide::Destination);
    assert_eq!(
        destination.to_string(),
        "ACL (RequireExact): the destination cannot store it"
    );
    assert_ne!(source.to_string(), destination.to_string());
}

#[test]
fn an_observation_failure_carries_what_the_source_reported() {
    let error = refusal(
        &with_acl(MetadataObservation::Failed {
            class: FailureClass::PermissionDenied,
            transience: Transience::Permanent,
        }),
        exact_target(),
        all_exact(),
    );
    assert_eq!(
        error.cause(),
        RefusalCause::SourceObservationFailed {
            class: FailureClass::PermissionDenied,
            transience: Transience::Permanent,
        }
    );
    assert!(error.to_string().contains("PermissionDenied"), "{error}");
}

#[test]
fn differing_acl_encodings_are_both_named() {
    let error = refusal(
        &exact_observations(),
        MetadataTarget {
            acl: AclTarget::Encoding(AclEncoding::WindowsSecurityDescriptor),
            ..exact_target()
        },
        all_exact(),
    );
    assert_eq!(
        error.cause(),
        RefusalCause::EncodingsDiffer {
            source: AclEncoding::Posix,
            destination: AclEncoding::WindowsSecurityDescriptor,
        }
    );
    let text = error.to_string();
    assert!(
        text.contains("Posix") && text.contains("WindowsSecurityDescriptor"),
        "{text}"
    );
}

#[test]
fn a_rejected_loss_names_the_loss() {
    let error = refusal(
        &exact_observations(),
        MetadataTarget {
            timestamps: TimestampTargetCapability::Supported(TimestampTarget {
                precision: TimePrecision::Seconds,
                accessed: true,
                modified: true,
                created: true,
            }),
            ..exact_target()
        },
        all_exact(),
    );
    assert_eq!(
        error.cause(),
        RefusalCause::LossRejected(SemanticLoss::TimestampPrecisionReduced)
    );
    assert_eq!(
        error.to_string(),
        "timestamps (RequireExact): copying it would lose timestamp precision, and the policy \
         does not allow that"
    );
}

/// A destination where timestamps do not apply, and a source with none to give: nothing is lost,
/// so no policy has anything to refuse, and the report must not call it lossy or omitted.
#[test]
fn nothing_to_lose_is_not_applicable_under_any_policy() {
    let observations = MetadataObservations::new(
        value(AclMetadata::new(AclEncoding::Posix, vec![1]).unwrap()),
        value(Vec::new()),
        value(vec![ObjectTag::new("class", "secret").unwrap()]),
        value(OwnershipMode {
            uid: 1000,
            gid: 1001,
            mode: 0o640,
        }),
        value(TimestampMetadata {
            accessed: None,
            modified: None,
            created: None,
        }),
    )
    .unwrap();
    let plan = compile_metadata_plan(&MetadataPlanRequest {
        observations: &observations,
        target: MetadataTarget {
            timestamps: TimestampTargetCapability::NotApplicable,
            ..exact_target()
        },
        policies: all_exact(),
        principal_mapper: None,
    })
    .unwrap();
    assert_eq!(
        decision_for(&plan, MetadataFamily::Timestamps),
        MappingDecision::NotApplicable
    );
}

/// Differing encodings are about how the two ends pair, not one end lacking something.
#[test]
fn differing_encodings_are_neither_ends_fault() {
    let cause = RefusalCause::EncodingsDiffer {
        source: AclEncoding::NfsV4,
        destination: AclEncoding::WindowsSecurityDescriptor,
    };
    assert_eq!(cause.side(), RefusalSide::Mapping);
    assert_eq!(
        RefusalCause::SourceDidNotObserve.side(),
        RefusalSide::Mapping
    );
}
