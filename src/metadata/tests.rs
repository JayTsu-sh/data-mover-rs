use std::sync::Mutex;

use async_trait::async_trait;

use super::*;
use crate::model::{
    AclMetadata, EntryOperationFailure, FailureClass, MetadataProvenance, ObjectTag, Operation,
    OwnershipMode, Transience,
};

/// Looks a family's outcome up by name. Indexing into `outcomes()` ties a test to the order the
/// plan happens to compile families in, and that order is a deliberate invariant of its own
/// ([`acl_is_applied_after_the_mode_that_would_rewrite_it`]) rather than something every other
/// test should have an opinion about.
fn outcome_for(report: &MetadataApplicationReport, family: MetadataFamily) -> ApplicationOutcome {
    report
        .outcomes()
        .iter()
        .find(|value| value.family == family)
        .map_or_else(
            || panic!("no outcome for {family:?}"),
            |value| value.outcome,
        )
}

fn decision_for(plan: &MetadataPlan, family: MetadataFamily) -> MappingDecision {
    plan.mappings()
        .iter()
        .find(|value| value.family == family)
        .map_or_else(
            || panic!("no mapping for {family:?}"),
            |value| value.decision.clone(),
        )
}

/// Refuses exactly one family and accepts everything else. `RecordingMetadata::fail_at` counts
/// *applied* mutations, so once a tolerated failure stops advancing that count every later
/// mutation fails too — which is the wrong instrument for asking "does the rest still get
/// applied".
struct RefusesAcl {
    applied: Mutex<Vec<MetadataMutation>>,
}

#[async_trait]
impl Metadata for RefusesAcl {
    async fn observe(
        &self,
        _path: &StoragePath,
        _plan: crate::model::ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        Ok(MetadataObservations::default())
    }

    async fn apply(
        &self,
        path: &StoragePath,
        mutation: MetadataMutation,
        _cancel: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        if matches!(mutation, MetadataMutation::Acl(_)) {
            return Err(StorageRoleFailure::Entry(
                EntryOperationFailure::new(
                    path.clone(),
                    Operation::Metadata,
                    FailureClass::Unsupported,
                    Transience::Permanent,
                    "destination refused the ACL",
                )
                .unwrap(),
            ));
        }
        self.applied.lock().unwrap().push(mutation);
        Ok(())
    }
}

struct RecordingMetadata {
    mutations: Mutex<Vec<MetadataMutation>>,
    fail_at: Option<usize>,
    cancel_after: Option<usize>,
}

#[async_trait]
impl Metadata for RecordingMetadata {
    async fn observe(
        &self,
        _path: &StoragePath,
        _plan: crate::model::ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        Ok(MetadataObservations::default())
    }

    async fn apply(
        &self,
        path: &StoragePath,
        mutation: MetadataMutation,
        cancel: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        let completed = self.mutations.lock().unwrap().len();
        if self.fail_at == Some(completed) {
            return Err(StorageRoleFailure::Entry(
                EntryOperationFailure::new(
                    path.clone(),
                    Operation::Metadata,
                    FailureClass::Protocol,
                    Transience::Permanent,
                    "injected metadata failure",
                )
                .unwrap(),
            ));
        }
        self.mutations.lock().unwrap().push(mutation);
        if self.cancel_after == Some(completed + 1) {
            cancel.cancel();
        }
        Ok(())
    }
}

fn value<T>(value: T) -> MetadataObservation<T> {
    MetadataObservation::Value {
        value,
        provenance: MetadataProvenance::Inline,
    }
}

fn exact_observations() -> MetadataObservations {
    MetadataObservations::new(
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
            modified: Some(
                StorageTimestamp::new(1_234_567_890, TimePrecision::Nanoseconds).unwrap(),
            ),
            created: None,
        }),
    )
    .unwrap()
}

fn exact_target() -> MetadataTarget {
    MetadataTarget {
        acl: AclTarget::Encoding(AclEncoding::Posix),
        xattrs: ValueTarget::Supported,
        tags: ValueTarget::Supported,
        ownership_mode: OwnershipTarget::Numeric,
        timestamps: TimestampTargetCapability::Supported(TimestampTarget {
            precision: TimePrecision::Nanoseconds,
            accessed: true,
            modified: true,
            created: true,
        }),
    }
}

/// Writing permission bits recomputes the ACL — the POSIX mask entry, and on most `NFSv4` servers
/// (ONTAP among them) the whole ACL. A plan's mutation order is also the order they are applied
/// in, so an ACL compiled ahead of ownership would be overwritten by the mode that follows it
/// while the report still said it was applied. Nothing else pins this.
#[test]
fn acl_is_applied_after_the_mode_that_would_rewrite_it() {
    let observations = exact_observations();
    let plan = compile_metadata_plan(&MetadataPlanRequest {
        observations: &observations,
        target: exact_target(),
        policies: all_exact(),
        principal_mapper: None,
    })
    .unwrap();
    assert!(
        family_order(&plan, MetadataFamily::OwnershipMode)
            < family_order(&plan, MetadataFamily::Acl),
        "ownership must precede the ACL it would rewrite: {:?}",
        families(&plan)
    );
}

/// The copy path can lose numeric ownership and keep the mode, and that replacement mode is
/// inserted into an already compiled plan. It has to land ahead of the ACL for the same reason.
#[test]
fn the_mode_that_replaces_ownership_is_applied_before_the_acl_too() {
    let observations = exact_observations();
    let plan = compile_copied_metadata_plan(
        &MetadataPlanRequest {
            observations: &observations,
            target: exact_target(),
            policies: all_exact().with_ownership_mode(MetadataPolicy::AllowKnownLoss),
            principal_mapper: None,
        },
        Some(0o640),
    )
    .unwrap();
    assert!(
        matches!(
            plan.mutations.first(),
            Some((MetadataFamily::OwnershipMode, MetadataMutation::Mode(_)))
        ),
        "the replacement mode must be applied first: {:?}",
        families(&plan)
    );
    assert!(
        family_order(&plan, MetadataFamily::OwnershipMode)
            < family_order(&plan, MetadataFamily::Acl)
    );
}

/// A destination can advertise a capability and still refuse the write — `NFSv4` `SETACL` is the
/// standing example. `BestEffort` is the policy that says that must not take the copy down with
/// it, and the families after the failed one still have to be applied.
#[tokio::test]
async fn a_best_effort_family_that_fails_to_apply_leaves_the_rest_alone() {
    let observations = exact_observations();
    let plan = compile_metadata_plan(&MetadataPlanRequest {
        observations: &observations,
        target: exact_target(),
        policies: all_exact().with_acl(MetadataPolicy::BestEffort),
        principal_mapper: None,
    })
    .unwrap();
    let target = RefusesAcl {
        applied: Mutex::new(Vec::new()),
    };
    let report = plan
        .apply(
            &target,
            &StoragePath::new("file").unwrap(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        outcome_for(&report, MetadataFamily::Acl),
        ApplicationOutcome::Failed
    );
    assert_eq!(
        outcome_for(&report, MetadataFamily::Timestamps),
        ApplicationOutcome::Applied
    );
}

/// The same refusal under a policy that requires the family must still fail the copy — that is
/// the whole difference between asking for a family and requiring it.
#[tokio::test]
async fn a_required_family_that_fails_to_apply_still_fails_the_copy() {
    let observations = exact_observations();
    let plan = compile_metadata_plan(&MetadataPlanRequest {
        observations: &observations,
        target: exact_target(),
        policies: all_exact(),
        principal_mapper: None,
    })
    .unwrap();
    let target = RefusesAcl {
        applied: Mutex::new(Vec::new()),
    };
    let failure = plan
        .apply(
            &target,
            &StoragePath::new("file").unwrap(),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(failure.family(), MetadataFamily::Acl);
}

/// A destination that stores ACLs in another encoding cannot hold this one at all without an
/// external mapping. That is absence, not a downgrade: `AllowKnownLoss` requires the family to
/// be carried, so it refuses like `RequireExact`, and only `BestEffort` goes on without it.
#[test]
fn an_acl_in_another_encoding_is_absence_not_a_known_loss() {
    let observations = exact_observations();
    let target = MetadataTarget {
        acl: AclTarget::Encoding(AclEncoding::WindowsSecurityDescriptor),
        ..exact_target()
    };
    let compile = |policy| {
        compile_metadata_plan(&MetadataPlanRequest {
            observations: &observations,
            target,
            policies: all_exact().with_acl(policy),
            principal_mapper: None,
        })
    };
    for policy in [MetadataPolicy::AllowKnownLoss, MetadataPolicy::RequireExact] {
        let error = compile(policy).unwrap_err();
        assert_eq!(error.family(), MetadataFamily::Acl, "{policy:?}");
        assert_eq!(
            error.kind(),
            MetadataPlanErrorKind::ExternalMappingRequired,
            "{policy:?}"
        );
    }
    let plan = compile(MetadataPolicy::BestEffort).unwrap();
    assert_eq!(
        decision_for(&plan, MetadataFamily::Acl),
        MappingDecision::RequiresExternalMapping
    );
    assert!(!families(&plan).contains(&MetadataFamily::Acl));
}

fn families(plan: &MetadataPlan) -> Vec<MetadataFamily> {
    plan.mutations.iter().map(|(family, _)| *family).collect()
}

fn family_order(plan: &MetadataPlan, family: MetadataFamily) -> usize {
    families(plan)
        .iter()
        .position(|value| *value == family)
        .unwrap_or_else(|| panic!("no mutation for {family:?}"))
}

fn all_exact() -> MetadataPolicies {
    MetadataPolicies::default()
        .with_acl(MetadataPolicy::RequireExact)
        .with_xattrs(MetadataPolicy::RequireExact)
        .with_tags(MetadataPolicy::RequireExact)
        .with_ownership_mode(MetadataPolicy::RequireExact)
        .with_timestamps(MetadataPolicy::RequireExact)
}

#[tokio::test]
async fn exact_plan_applies_every_family_without_loss() {
    let observations = exact_observations();
    let plan = compile_metadata_plan(&MetadataPlanRequest {
        observations: &observations,
        target: exact_target(),
        policies: all_exact(),
        principal_mapper: None,
    })
    .unwrap();
    assert!(plan.loss_report().is_empty());
    assert_eq!(plan.mappings().len(), 5);

    let target = RecordingMetadata {
        mutations: Mutex::new(Vec::new()),
        fail_at: None,
        cancel_after: None,
    };
    let report = plan
        .apply(
            &target,
            &StoragePath::new("file").unwrap(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert!(
        report
            .outcomes()
            .iter()
            .all(|item| item.outcome == ApplicationOutcome::Applied)
    );
    assert_eq!(target.mutations.lock().unwrap().len(), 5);
}

#[test]
fn known_timestamp_loss_is_explicit_and_exact_policy_rejects_it() {
    let observations = exact_observations();
    let mut target = exact_target();
    target.timestamps = TimestampTargetCapability::Supported(TimestampTarget {
        precision: TimePrecision::Milliseconds,
        accessed: true,
        modified: true,
        created: false,
    });
    let exact_error = compile_metadata_plan(&MetadataPlanRequest {
        observations: &observations,
        target,
        policies: all_exact(),
        principal_mapper: None,
    })
    .unwrap_err();
    assert_eq!(exact_error.kind(), MetadataPlanErrorKind::KnownLossRejected);

    let plan = compile_metadata_plan(&MetadataPlanRequest {
        observations: &observations,
        target,
        policies: all_exact().with_timestamps(MetadataPolicy::AllowKnownLoss),
        principal_mapper: None,
    })
    .unwrap();
    assert_eq!(plan.loss_report().losses().len(), 1);
    assert!(matches!(
        plan.mappings().last().unwrap().decision,
        MappingDecision::Lossy(_)
    ));
}

#[tokio::test]
async fn unsupported_and_not_applicable_have_distinct_results() {
    let observations = exact_observations();
    let mut target = exact_target();
    target.tags = ValueTarget::NotApplicable;
    let rejected = compile_metadata_plan(&MetadataPlanRequest {
        observations: &observations,
        target,
        policies: all_exact(),
        principal_mapper: None,
    })
    .unwrap_err();
    assert_eq!(rejected.kind(), MetadataPlanErrorKind::KnownLossRejected);

    let plan = compile_metadata_plan(&MetadataPlanRequest {
        observations: &observations,
        target,
        policies: all_exact().with_tags(MetadataPolicy::AllowKnownLoss),
        principal_mapper: None,
    })
    .unwrap();
    assert!(
        plan.loss_report()
            .losses()
            .contains(&(MetadataFamily::Tags, SemanticLoss::TagsDropped))
    );
    let application = plan
        .apply(
            &RecordingMetadata {
                mutations: Mutex::new(Vec::new()),
                fail_at: None,
                cancel_after: None,
            },
            &StoragePath::new("file").unwrap(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        outcome_for(&application, MetadataFamily::Tags),
        ApplicationOutcome::OmittedByPolicy
    );

    target.tags = ValueTarget::Unsupported;
    let unsupported = compile_metadata_plan(&MetadataPlanRequest {
        observations: &observations,
        target,
        policies: all_exact().with_tags(MetadataPolicy::BestEffort),
        principal_mapper: None,
    })
    .unwrap();
    assert_eq!(
        decision_for(&unsupported, MetadataFamily::Tags),
        MappingDecision::Unsupported
    );
    assert!(
        !unsupported
            .loss_report()
            .losses()
            .iter()
            .any(|(family, _)| *family == MetadataFamily::Tags)
    );
}

#[test]
fn not_applicable_timestamps_report_observed_field_drop() {
    let observations = exact_observations();
    let mut target = exact_target();
    target.timestamps = TimestampTargetCapability::NotApplicable;
    let plan = compile_metadata_plan(&MetadataPlanRequest {
        observations: &observations,
        target,
        policies: all_exact().with_timestamps(MetadataPolicy::AllowKnownLoss),
        principal_mapper: None,
    })
    .unwrap();
    assert_eq!(
        plan.loss_report().losses(),
        &[(
            MetadataFamily::Timestamps,
            SemanticLoss::ModifiedTimestampDropped
        )]
    );
}

#[test]
fn exact_policy_rejects_a_missing_observation() {
    let observations = MetadataObservations::default();
    let error = compile_metadata_plan(&MetadataPlanRequest {
        observations: &observations,
        target: exact_target(),
        policies: all_exact(),
        principal_mapper: None,
    })
    .unwrap_err();
    assert_eq!(error.kind(), MetadataPlanErrorKind::ObservationRequired);
}

#[tokio::test]
async fn cancellation_and_storage_failure_are_distinct_and_stop_application() {
    let observations = exact_observations();
    let plan = compile_metadata_plan(&MetadataPlanRequest {
        observations: &observations,
        target: exact_target(),
        policies: all_exact(),
        principal_mapper: None,
    })
    .unwrap();
    let cancelled_target = RecordingMetadata {
        mutations: Mutex::new(Vec::new()),
        fail_at: None,
        cancel_after: None,
    };
    let cancel = CancellationToken::new();
    cancel.cancel();
    let cancelled = plan
        .apply(
            &cancelled_target,
            &StoragePath::new("file").unwrap(),
            cancel,
        )
        .await
        .unwrap_err();
    assert!(cancelled.storage_error().is_none());
    assert!(cancelled_target.mutations.lock().unwrap().is_empty());

    let failing_target = RecordingMetadata {
        mutations: Mutex::new(Vec::new()),
        fail_at: Some(0),
        cancel_after: None,
    };
    let failed = plan
        .apply(
            &failing_target,
            &StoragePath::new("file").unwrap(),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert!(failed.storage_error().is_some());
    assert_eq!(
        failed.report().outcomes()[0].outcome,
        ApplicationOutcome::Failed
    );

    let mid_cancel_target = RecordingMetadata {
        mutations: Mutex::new(Vec::new()),
        fail_at: None,
        cancel_after: Some(1),
    };
    let mid_cancel = plan
        .apply(
            &mid_cancel_target,
            &StoragePath::new("file").unwrap(),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert!(mid_cancel.storage_error().is_none());
    assert_eq!(mid_cancel_target.mutations.lock().unwrap().len(), 1);
    assert_eq!(
        mid_cancel.report().outcomes()[0].outcome,
        ApplicationOutcome::Applied
    );

    let mid_failure_target = RecordingMetadata {
        mutations: Mutex::new(Vec::new()),
        fail_at: Some(1),
        cancel_after: None,
    };
    let mid_failure = plan
        .apply(
            &mid_failure_target,
            &StoragePath::new("file").unwrap(),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(mid_failure_target.mutations.lock().unwrap().len(), 1);
    assert_eq!(
        mid_failure.report().outcomes()[0].outcome,
        ApplicationOutcome::Applied
    );
    assert_eq!(
        mid_failure.report().outcomes()[1].outcome,
        ApplicationOutcome::Failed
    );
}

struct Mapper;

impl PrincipalMapper for Mapper {
    fn map(&self, source: OwnershipMode) -> Result<MappedOwnership, PrincipalMappingFailure> {
        MappedOwnership::new("domain\\owner", "domain\\group", source.mode)
            .map_err(|_| PrincipalMappingFailure)
    }
}

#[test]
fn external_principal_mapping_is_required_and_redacted() {
    let observations = exact_observations();
    let mut target = exact_target();
    target.ownership_mode = OwnershipTarget::ExternalMapping;
    let error = compile_metadata_plan(&MetadataPlanRequest {
        observations: &observations,
        target,
        policies: all_exact(),
        principal_mapper: None,
    })
    .unwrap_err();
    assert_eq!(error.kind(), MetadataPlanErrorKind::ExternalMappingRequired);

    let mapped = Mapper
        .map(*observations.ownership_mode().value().unwrap())
        .unwrap();
    assert!(!format!("{mapped:?}").contains("domain"));
    assert!(
        compile_metadata_plan(&MetadataPlanRequest {
            observations: &observations,
            target,
            policies: all_exact(),
            principal_mapper: Some(&Mapper),
        })
        .is_ok()
    );
}

#[test]
fn mode_only_target_preserves_permissions_and_reports_principal_loss() {
    let observations = exact_observations();
    let mut target = exact_target();
    target.ownership_mode = OwnershipTarget::ModeOnly;
    let policies = all_exact().with_ownership_mode(MetadataPolicy::AllowKnownLoss);
    let plan = compile_metadata_plan(&MetadataPlanRequest {
        observations: &observations,
        target,
        policies,
        principal_mapper: None,
    })
    .unwrap();

    assert!(plan.loss_report().losses().contains(&(
        MetadataFamily::OwnershipMode,
        SemanticLoss::OwnerAndGroupDropped,
    )));
    assert!(
        plan.mutations
            .contains(&(MetadataFamily::OwnershipMode, MetadataMutation::Mode(0o640),))
    );
    assert!(
        plan.mutations
            .iter()
            .all(|(_, mutation)| !matches!(mutation, MetadataMutation::MappedOwnership(_)))
    );
}

#[test]
fn smb_timestamp_precision_is_reported_and_quantized_before_application() {
    let observations = exact_observations();
    let mut target = exact_target();
    target.timestamps = TimestampTargetCapability::Supported(TimestampTarget {
        precision: TimePrecision::HundredNanoseconds,
        accessed: false,
        modified: true,
        created: false,
    });
    let plan = compile_metadata_plan(&MetadataPlanRequest {
        observations: &observations,
        target,
        policies: MetadataPolicies::default().with_timestamps(MetadataPolicy::AllowKnownLoss),
        principal_mapper: None,
    })
    .unwrap();
    assert!(plan.loss_report().losses().contains(&(
        MetadataFamily::Timestamps,
        SemanticLoss::TimestampPrecisionReduced
    )));
    let MetadataMutation::Timestamps(value) = &plan.mutations[0].1 else {
        panic!("missing timestamps");
    };
    assert!(value.accessed.is_none() && value.created.is_none());
    let modified = value.modified.unwrap();
    assert_eq!(modified.unix_nanos(), 1_234_567_800);
    assert_eq!(modified.precision(), TimePrecision::HundredNanoseconds);
}

#[test]
fn copied_mode_never_synthesizes_numeric_ownership() {
    let observations = MetadataObservations::default();
    for ownership in [
        OwnershipTarget::Numeric,
        OwnershipTarget::ModeOnly,
        OwnershipTarget::NotApplicable,
    ] {
        let mut target = exact_target();
        target.ownership_mode = ownership;
        let plan = compile_copied_metadata_plan(
            &MetadataPlanRequest {
                observations: &observations,
                target,
                policies: MetadataPolicies::default()
                    .with_ownership_mode(MetadataPolicy::AllowKnownLoss),
                principal_mapper: None,
            },
            Some(0o100_640),
        )
        .unwrap();
        if ownership == OwnershipTarget::NotApplicable {
            assert!(plan.mutations.is_empty());
            assert_eq!(
                plan.loss_report().losses(),
                &[(
                    MetadataFamily::OwnershipMode,
                    SemanticLoss::OwnershipModeDropped
                )]
            );
        } else {
            assert_eq!(
                plan.mutations,
                [(MetadataFamily::OwnershipMode, MetadataMutation::Mode(0o640))]
            );
            assert_eq!(
                plan.loss_report().losses(),
                &[(
                    MetadataFamily::OwnershipMode,
                    SemanticLoss::OwnerAndGroupDropped
                )]
            );
        }
    }
}

#[path = "stage_tests.rs"]
mod stage;
