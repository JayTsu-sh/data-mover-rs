use std::sync::Mutex;

use async_trait::async_trait;

use super::*;
use crate::model::{
    AclMetadata, EntryOperationFailure, FailureClass, MetadataProvenance, ObjectTag, Operation,
    OwnershipMode, Transience,
};

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
        application.outcomes()[2].outcome,
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
        unsupported.mappings()[2].decision,
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
