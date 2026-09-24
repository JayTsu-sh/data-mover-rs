//! A family asked for that an end cannot do is skipped, not failed — and the report has to say
//! why, precisely enough for the caller to tell its user which end lacks what.

use super::*;

fn recording() -> RecordingMetadata {
    RecordingMetadata {
        mutations: Mutex::new(Vec::new()),
        fail_at: None,
        cancel_after: None,
    }
}

/// One family per reason: the destination stores ACLs in another encoding, cannot store xattrs at
/// all, and the source cannot read tags.
fn skipping_plan() -> MetadataPlan {
    let observations = MetadataObservations {
        tags: MetadataObservation::Unsupported,
        ..exact_observations()
    };
    compile_metadata_plan(&MetadataPlanRequest {
        observations: &observations,
        target: MetadataTarget {
            acl: AclTarget::Encoding(AclEncoding::WindowsSecurityDescriptor),
            xattrs: ValueTarget::Unsupported,
            ..exact_target()
        },
        policies: all_exact()
            .with_acl(MetadataPolicy::BestEffort)
            .with_xattrs(MetadataPolicy::BestEffort)
            .with_tags(MetadataPolicy::BestEffort),
        principal_mapper: None,
    })
    .unwrap()
}

#[tokio::test]
async fn every_skipped_family_is_reported_with_its_reason() {
    let plan = skipping_plan();
    let expected = [
        SkippedFamily {
            family: MetadataFamily::Acl,
            reason: RefusalCause::EncodingsDiffer {
                source: AclEncoding::Posix,
                destination: AclEncoding::WindowsSecurityDescriptor,
            },
        },
        SkippedFamily {
            family: MetadataFamily::Xattrs,
            reason: RefusalCause::DestinationCannotStore,
        },
        SkippedFamily {
            family: MetadataFamily::Tags,
            reason: RefusalCause::SourceCannotObserve,
        },
    ];
    assert_eq!(plan.skipped(), expected);

    let report = plan
        .apply(
            &recording(),
            &StoragePath::new("file").unwrap(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(report.skipped(), expected);
    // Every `Unsupported` outcome is explained; nothing else is listed as skipped.
    for outcome in report.outcomes() {
        let explained = report
            .skipped()
            .iter()
            .any(|skip| skip.family == outcome.family);
        assert_eq!(
            explained,
            outcome.outcome == ApplicationOutcome::Unsupported,
            "{outcome:?}"
        );
    }
}

/// A failed item still says what was skipped: the caller reports both.
#[tokio::test]
async fn a_failed_item_still_reports_what_was_skipped() {
    let observations = exact_observations();
    let plan = compile_metadata_plan(&MetadataPlanRequest {
        observations: &observations,
        target: MetadataTarget {
            xattrs: ValueTarget::Unsupported,
            ..exact_target()
        },
        policies: all_exact().with_xattrs(MetadataPolicy::BestEffort),
        principal_mapper: None,
    })
    .unwrap();
    let failure = plan
        .apply(
            &RefusesAcl {
                applied: Mutex::new(Vec::new()),
            },
            &StoragePath::new("file").unwrap(),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(
        failure.report().skipped(),
        [SkippedFamily {
            family: MetadataFamily::Xattrs,
            reason: RefusalCause::DestinationCannotStore,
        }]
    );
}

#[test]
fn carried_omitted_and_not_applicable_families_are_not_skipped() {
    // A symlink has no ACL: nothing to carry, so nothing to explain.
    let observations = MetadataObservations {
        acl: MetadataObservation::NotApplicable,
        ..exact_observations()
    };
    let plan = compile_metadata_plan(&MetadataPlanRequest {
        observations: &observations,
        target: exact_target(),
        policies: all_exact()
            .with_acl(MetadataPolicy::BestEffort)
            .with_xattrs(MetadataPolicy::BestEffort)
            .with_tags(MetadataPolicy::Omit),
        principal_mapper: None,
    })
    .unwrap();
    assert_eq!(plan.skipped(), []);
}

#[test]
fn a_skip_reads_as_the_family_and_the_end_that_lacks_it() {
    let skip = SkippedFamily {
        family: MetadataFamily::Xattrs,
        reason: RefusalCause::DestinationCannotStore,
    };
    assert_eq!(
        skip.to_string(),
        "extended attributes skipped: the destination cannot store it"
    );
}

/// When neither end can do a family, the reason names the source: it is checked first, and a
/// destination that could store it would not help.
#[test]
fn a_family_both_ends_lack_is_blamed_on_the_source() {
    let observations = MetadataObservations {
        xattrs: MetadataObservation::Unsupported,
        ..exact_observations()
    };
    let plan = compile_metadata_plan(&MetadataPlanRequest {
        observations: &observations,
        target: MetadataTarget {
            xattrs: ValueTarget::Unsupported,
            ..exact_target()
        },
        policies: all_exact().with_xattrs(MetadataPolicy::BestEffort),
        principal_mapper: None,
    })
    .unwrap();
    assert_eq!(
        plan.skipped(),
        [SkippedFamily {
            family: MetadataFamily::Xattrs,
            reason: RefusalCause::SourceCannotObserve,
        }]
    );
}

#[test]
fn ownership_that_needs_a_missing_mapper_is_skipped_with_that_reason() {
    let observations = exact_observations();
    let plan = compile_metadata_plan(&MetadataPlanRequest {
        observations: &observations,
        target: MetadataTarget {
            ownership_mode: OwnershipTarget::ExternalMapping,
            ..exact_target()
        },
        policies: all_exact().with_ownership_mode(MetadataPolicy::BestEffort),
        principal_mapper: None,
    })
    .unwrap();
    assert_eq!(
        plan.skipped(),
        [SkippedFamily {
            family: MetadataFamily::OwnershipMode,
            reason: RefusalCause::PrincipalMapperMissing,
        }]
    );
}

/// The copy path compiles through `compile_copied_metadata_plan`, which rewrites the ownership
/// family; the reasons for the others have to survive it. Both apply paths build the report with
/// the same `MetadataPlan::report`, so applying it one way covers the other.
#[tokio::test]
async fn the_copied_plan_keeps_the_reasons() {
    let observations = MetadataObservations {
        tags: MetadataObservation::Unsupported,
        ..exact_observations()
    };
    let plan = compile_copied_metadata_plan(
        &MetadataPlanRequest {
            observations: &observations,
            target: MetadataTarget {
                acl: AclTarget::Unsupported,
                ..exact_target()
            },
            // As the copy path plans it when it carries a mode without its owner.
            policies: all_exact()
                .with_ownership_mode(MetadataPolicy::AllowKnownLoss)
                .with_acl(MetadataPolicy::BestEffort)
                .with_tags(MetadataPolicy::BestEffort),
            principal_mapper: None,
        },
        Some(0o100_640),
        false,
    )
    .unwrap();
    let expected = [
        SkippedFamily {
            family: MetadataFamily::Acl,
            reason: RefusalCause::DestinationCannotStore,
        },
        SkippedFamily {
            family: MetadataFamily::Tags,
            reason: RefusalCause::SourceCannotObserve,
        },
    ];
    assert_eq!(plan.skipped(), expected);
    let report = plan
        .apply(
            &recording(),
            &StoragePath::new("file").unwrap(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(report.skipped(), expected);
}

/// A destination that stores nothing (an object store) skips every family asked for — the
/// baseline included — because of the destination, without the source having been read.
#[tokio::test]
async fn a_destination_that_stores_nothing_skips_everything_asked_for() {
    let plan = compile_nothing_stored_plan(
        MetadataPolicies::default()
            .with_ownership_mode(MetadataPolicy::AllowKnownLoss)
            .with_timestamps(MetadataPolicy::AllowKnownLoss)
            .with_acl(MetadataPolicy::BestEffort),
    );
    let skipped = |family| SkippedFamily {
        family,
        reason: RefusalCause::DestinationCannotStore,
    };
    let expected = [
        skipped(MetadataFamily::OwnershipMode),
        skipped(MetadataFamily::Acl),
        skipped(MetadataFamily::Timestamps),
    ];
    assert_eq!(plan.skipped(), expected);
    assert!(!plan.has_mutations());
    let report = plan
        .apply(
            &recording(),
            &StoragePath::new("object").unwrap(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(report.skipped(), expected);
    for outcome in report.outcomes() {
        let explained = report
            .skipped()
            .iter()
            .any(|skip| skip.family == outcome.family);
        let expected = if explained {
            ApplicationOutcome::Unsupported
        } else {
            ApplicationOutcome::OmittedByPolicy
        };
        assert_eq!(outcome.outcome, expected, "{outcome:?}");
    }
}
