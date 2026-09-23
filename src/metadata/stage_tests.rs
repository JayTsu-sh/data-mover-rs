//! `apply_to_stage` is the path real transfers take: one ordered batch per stage, resumed after
//! a tolerated refusal. It shares its tolerance rules with `apply` but not its code, so it needs
//! its own fake destination.

use bytes::Bytes;

use super::*;
use crate::model::{BackendIdentity, BackendKind, BackendSessionFailure};
use crate::storage::{
    ByteStream, CheckpointObservation, FinalDestination, PrepareRequest, PublicationEvidence,
    PublicationFailure, PublishRequest, RecoverRequest, RecoveryIdentity,
    StagedMetadataApplicationFailure, VerificationEvidence, VerifyRequest, WriteEvidence,
};

/// How the fake reports the families it refuses. Everything else is accepted.
#[derive(Clone, Copy, Debug)]
enum Refusal {
    /// Applies in order and stops at the refusal, as the default per-mutation batch does.
    InOrder,
    /// Rejects the batch before touching anything, as Local's pre-validation does, so the failed
    /// index runs ahead of the completed count.
    UpFront,
    /// Applies only the first mutation of the batch before rejecting it, so the completed count
    /// sits strictly between zero and the failed index.
    Partway,
    /// The token fires between mutations; the batch reports no error.
    CancelledBetween,
    /// The token fires while the refused mutation is in flight; the backend reports a
    /// `Cancelled` failure.
    CancelledDuring,
    /// The token fires while the refused mutation is in flight, and the backend reports whatever
    /// the interrupted request surfaced as rather than `Cancelled`.
    CancelledAsProtocol,
    /// The session is lost while the refused mutation is in flight.
    SessionLost,
    /// Applies the whole batch and then fails its persistence barrier, as Local's durable
    /// `sync_all` can: no single mutation failed, the batch did.
    BarrierFailed,
    /// Fails before touching the batch for a reason that is not about any mutation in it, as
    /// Local's stage open can.
    StageUnavailable,
    /// Stops at the refused mutation, then fails the barrier over the ones ahead of it, as Local
    /// does when both happen in one batch.
    RefusedThenBarrierFailed,
}

struct ScriptedStage {
    refusal: Refusal,
    refused: &'static [MetadataFamily],
    applied: Mutex<Vec<MetadataMutation>>,
}

impl ScriptedStage {
    fn new(refusal: Refusal) -> Self {
        Self::refusing(refusal, &[MetadataFamily::Acl])
    }

    fn refusing(refusal: Refusal, refused: &'static [MetadataFamily]) -> Self {
        Self {
            refusal,
            refused,
            applied: Mutex::new(Vec::new()),
        }
    }

    fn refuses(&self, mutation: &MetadataMutation) -> bool {
        self.refused.contains(&family_of(mutation))
    }

    /// What the refused mutation fails with, after firing the token for the cancelling modes.
    fn refusal_error(&self, cancel: &CancellationToken) -> Option<StorageRoleFailure> {
        match self.refusal {
            Refusal::InOrder | Refusal::UpFront | Refusal::Partway => {
                Some(refusal(FailureClass::Unsupported))
            }
            Refusal::CancelledBetween => {
                cancel.cancel();
                None
            }
            Refusal::CancelledDuring => {
                cancel.cancel();
                Some(refusal(FailureClass::Cancelled))
            }
            Refusal::CancelledAsProtocol => {
                cancel.cancel();
                Some(refusal(FailureClass::Protocol))
            }
            Refusal::SessionLost => Some(StorageRoleFailure::Session(
                BackendSessionFailure::new(
                    Operation::Metadata,
                    FailureClass::Connectivity,
                    Transience::Transient,
                    "session lost",
                )
                .unwrap(),
            )),
            Refusal::BarrierFailed
            | Refusal::StageUnavailable
            | Refusal::RefusedThenBarrierFailed => Some(refusal(FailureClass::Protocol)),
        }
    }

    fn applied_families(&self) -> Vec<MetadataFamily> {
        self.applied.lock().unwrap().iter().map(family_of).collect()
    }
}

fn family_of(mutation: &MetadataMutation) -> MetadataFamily {
    match mutation {
        MetadataMutation::Acl(_) => MetadataFamily::Acl,
        MetadataMutation::Xattrs(_) => MetadataFamily::Xattrs,
        MetadataMutation::Tags(_) => MetadataFamily::Tags,
        MetadataMutation::NumericOwnership(_)
        | MetadataMutation::MappedOwnership(_)
        | MetadataMutation::Mode(_) => MetadataFamily::OwnershipMode,
        MetadataMutation::Timestamps(_) => MetadataFamily::Timestamps,
    }
}

fn refusal(class: FailureClass) -> StorageRoleFailure {
    StorageRoleFailure::Entry(
        EntryOperationFailure::new(
            StoragePath::new("file").unwrap(),
            Operation::Metadata,
            class,
            Transience::Permanent,
            "destination refused the ACL",
        )
        .unwrap(),
    )
}

fn batch_failure(
    failed_index: usize,
    completed: usize,
    error: Option<StorageRoleFailure>,
) -> StagedMetadataApplicationFailure {
    StagedMetadataApplicationFailure {
        failed_index,
        completed,
        error,
    }
}

#[async_trait]
impl Metadata for ScriptedStage {
    async fn observe(
        &self,
        _path: &StoragePath,
        _plan: crate::model::ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        Ok(MetadataObservations::default())
    }

    async fn apply(
        &self,
        _path: &StoragePath,
        mutation: MetadataMutation,
        cancel: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        if !self.refuses(&mutation) {
            self.applied.lock().unwrap().push(mutation);
            return Ok(());
        }
        Err(self
            .refusal_error(&cancel)
            .unwrap_or_else(|| refusal(FailureClass::Cancelled)))
    }
}

#[async_trait]
impl StagedDestination for ScriptedStage {
    async fn apply_metadata_batch(
        &self,
        _stage: &PreparedStage,
        mutations: Vec<MetadataMutation>,
        cancel: CancellationToken,
    ) -> Result<(), StagedMetadataApplicationFailure> {
        if cancel.is_cancelled() {
            return Err(batch_failure(0, 0, None));
        }
        let len = mutations.len();
        match self.refusal {
            Refusal::StageUnavailable => {
                return Err(StagedMetadataApplicationFailure::whole_batch(
                    len,
                    0,
                    self.refusal_error(&cancel),
                ));
            }
            Refusal::BarrierFailed => {
                self.applied.lock().unwrap().extend(mutations);
                return Err(StagedMetadataApplicationFailure::whole_batch(
                    len,
                    len,
                    self.refusal_error(&cancel),
                ));
            }
            _ => {}
        }
        let Some(index) = mutations.iter().position(|mutation| self.refuses(mutation)) else {
            self.applied.lock().unwrap().extend(mutations);
            return Ok(());
        };
        if let Refusal::RefusedThenBarrierFailed = self.refusal {
            self.applied
                .lock()
                .unwrap()
                .extend(mutations.into_iter().take(index));
            return Err(StagedMetadataApplicationFailure::whole_batch(
                len,
                index,
                self.refusal_error(&cancel),
            ));
        }
        let completed = match self.refusal {
            Refusal::UpFront => 0,
            Refusal::Partway => index.min(1),
            _ => index,
        };
        self.applied
            .lock()
            .unwrap()
            .extend(mutations.into_iter().take(completed));
        Err(batch_failure(index, completed, self.refusal_error(&cancel)))
    }

    async fn prepare(&self, _request: PrepareRequest) -> Result<PreparedStage, StorageRoleFailure> {
        panic!("metadata tests never prepare")
    }
    async fn recovery_identity(
        &self,
        _stage: &PreparedStage,
    ) -> Result<RecoveryIdentity, StorageRoleFailure> {
        panic!("metadata tests never recover")
    }
    async fn recover(&self, _request: RecoverRequest) -> Result<PreparedStage, StorageRoleFailure> {
        panic!("metadata tests never recover")
    }
    async fn write(
        &self,
        _stage: &PreparedStage,
        _input: ByteStream,
    ) -> Result<WriteEvidence, StorageRoleFailure> {
        panic!("metadata tests never write")
    }
    async fn observe_checkpoint(
        &self,
        _stage: &PreparedStage,
    ) -> Result<CheckpointObservation, StorageRoleFailure> {
        panic!("metadata tests never checkpoint")
    }
    async fn verify(
        &self,
        _stage: &PreparedStage,
        _request: VerifyRequest,
    ) -> Result<VerificationEvidence, StorageRoleFailure> {
        panic!("metadata tests never verify")
    }
    async fn publish(
        &self,
        _stage: &PreparedStage,
        _request: PublishRequest,
    ) -> Result<PublicationEvidence, PublicationFailure> {
        panic!("metadata tests never publish")
    }
    async fn discard(&self, _stage: PreparedStage) -> Result<(), StorageRoleFailure> {
        panic!("metadata tests never discard")
    }
}

fn stage() -> PreparedStage {
    PreparedStage::new(
        BackendIdentity::new(BackendKind::Local, "destination").unwrap(),
        FinalDestination::new(StoragePath::new("file").unwrap()),
        Bytes::new(),
        [0; 32],
        0,
        None,
    )
}

fn plan(policies: MetadataPolicies) -> MetadataPlan {
    let observations = exact_observations();
    compile_metadata_plan(&MetadataPlanRequest {
        observations: &observations,
        target: exact_target(),
        policies,
        principal_mapper: None,
    })
    .unwrap()
}

/// Every family that can be `BestEffort` without changing which family is refused.
fn best_effort_after_ownership() -> MetadataPolicies {
    all_exact()
        .with_acl(MetadataPolicy::BestEffort)
        .with_xattrs(MetadataPolicy::BestEffort)
        .with_tags(MetadataPolicy::BestEffort)
        .with_timestamps(MetadataPolicy::BestEffort)
}

const EVERYTHING_BUT_THE_ACL: [MetadataFamily; 4] = [
    MetadataFamily::OwnershipMode,
    MetadataFamily::Xattrs,
    MetadataFamily::Tags,
    MetadataFamily::Timestamps,
];

async fn tolerated(refusal: Refusal) -> (ScriptedStage, MetadataApplicationReport) {
    let plan = plan(all_exact().with_acl(MetadataPolicy::BestEffort));
    let target = ScriptedStage::new(refusal);
    let report = plan
        .apply_to_stage(&target, &stage(), CancellationToken::new())
        .await
        .unwrap();
    (target, report)
}

fn assert_only_the_acl_was_lost(target: &ScriptedStage, report: &MetadataApplicationReport) {
    assert_eq!(
        outcome_for(report, MetadataFamily::Acl),
        ApplicationOutcome::Failed
    );
    for family in EVERYTHING_BUT_THE_ACL {
        assert_eq!(
            outcome_for(report, family),
            ApplicationOutcome::Applied,
            "{family:?}"
        );
    }
    // Order is an invariant of its own: the resumed batch must not reshuffle what is left.
    assert_eq!(target.applied_families(), EVERYTHING_BUT_THE_ACL);
}

#[tokio::test]
async fn a_staged_best_effort_refusal_resumes_with_the_next_family() {
    let (target, report) = tolerated(Refusal::InOrder).await;
    assert_only_the_acl_was_lost(&target, &report);
}

/// A backend may reject a batch before applying any of it, so the failed index is not the
/// number of mutations already applied. Resuming after the failed index would skip the families
/// ahead of it without applying them — and without anything in the result saying so.
#[tokio::test]
async fn a_refusal_found_before_anything_was_applied_does_not_skip_earlier_families() {
    let (target, report) = tolerated(Refusal::UpFront).await;
    assert_only_the_acl_was_lost(&target, &report);
}

#[tokio::test]
async fn a_staged_required_refusal_still_fails_the_copy() {
    let target = ScriptedStage::new(Refusal::InOrder);
    let failure = plan(all_exact())
        .apply_to_stage(&target, &stage(), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(failure.family(), MetadataFamily::Acl);
    assert!(failure.storage_error().is_some());
    assert_eq!(target.applied_families(), [MetadataFamily::OwnershipMode]);
}

/// A resent batch can be refused again. Both tolerated families are lost, nothing else is, and
/// nothing that was applied once is applied twice.
#[tokio::test]
async fn a_second_refusal_in_the_resent_batch_is_tolerated_too() {
    for refusal in [Refusal::InOrder, Refusal::UpFront] {
        let target =
            ScriptedStage::refusing(refusal, &[MetadataFamily::Acl, MetadataFamily::Xattrs]);
        let plan = plan(
            all_exact()
                .with_acl(MetadataPolicy::BestEffort)
                .with_xattrs(MetadataPolicy::BestEffort),
        );
        let report = plan
            .apply_to_stage(&target, &stage(), CancellationToken::new())
            .await
            .unwrap();
        for family in [MetadataFamily::Acl, MetadataFamily::Xattrs] {
            assert_eq!(
                outcome_for(&report, family),
                ApplicationOutcome::Failed,
                "{refusal:?} {family:?}"
            );
        }
        assert_eq!(
            target.applied_families(),
            [
                MetadataFamily::OwnershipMode,
                MetadataFamily::Tags,
                MetadataFamily::Timestamps
            ],
            "{refusal:?}"
        );
    }
}

/// Some of the batch applied, some of it not, and then the refusal: the part in between still
/// has to go out, and the part already applied must not go out again.
#[tokio::test]
async fn a_refusal_after_a_partial_batch_resends_only_what_was_not_applied() {
    let target = ScriptedStage::refusing(Refusal::Partway, &[MetadataFamily::Xattrs]);
    let report = plan(all_exact().with_xattrs(MetadataPolicy::BestEffort))
        .apply_to_stage(&target, &stage(), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        outcome_for(&report, MetadataFamily::Xattrs),
        ApplicationOutcome::Failed
    );
    assert_eq!(
        target.applied_families(),
        [
            MetadataFamily::OwnershipMode,
            MetadataFamily::Acl,
            MetadataFamily::Tags,
            MetadataFamily::Timestamps
        ]
    );
}

const CANCELLATIONS: [Refusal; 3] = [
    Refusal::CancelledBetween,
    Refusal::CancelledDuring,
    Refusal::CancelledAsProtocol,
];

/// A cancelled family was neither applied nor refused, and the report must not claim either.
fn assert_cancelled_on_the_acl(failure: &MetadataApplicationFailure, refusal: Refusal) {
    assert_eq!(failure.family(), MetadataFamily::Acl, "{refusal:?}");
    let acl = outcome_for(failure.report(), MetadataFamily::Acl);
    assert!(
        !matches!(
            acl,
            ApplicationOutcome::Applied | ApplicationOutcome::Failed
        ),
        "{refusal:?}: {acl:?}"
    );
}

/// `BestEffort` tolerates a destination saying no. Cancellation is not the destination saying
/// no: it has to stop the copy even when the family it lands on is a tolerant one, even when
/// every family after it is tolerant too, and whatever class the interrupted request reported.
#[tokio::test]
async fn cancellation_on_a_tolerant_staged_family_still_stops_the_copy() {
    for refusal in CANCELLATIONS {
        let target = ScriptedStage::new(refusal);
        let failure = plan(best_effort_after_ownership())
            .apply_to_stage(&target, &stage(), CancellationToken::new())
            .await
            .unwrap_err();
        assert_cancelled_on_the_acl(&failure, refusal);
        assert_eq!(
            target.applied_families(),
            [MetadataFamily::OwnershipMode],
            "{refusal:?}"
        );
    }
}

/// The per-mutation path has the same rule.
#[tokio::test]
async fn cancellation_on_a_tolerant_published_family_still_stops_the_copy() {
    for refusal in [Refusal::CancelledDuring, Refusal::CancelledAsProtocol] {
        let target = ScriptedStage::new(refusal);
        let failure = plan(best_effort_after_ownership())
            .apply(
                &target,
                &StoragePath::new("file").unwrap(),
                CancellationToken::new(),
            )
            .await
            .unwrap_err();
        assert_cancelled_on_the_acl(&failure, refusal);
        assert_eq!(
            target.applied_families(),
            [MetadataFamily::OwnershipMode],
            "{refusal:?}"
        );
    }
}

/// Every family `BestEffort`, so any failure that is tolerated at all would be tolerated here.
fn everything_best_effort() -> MetadataPolicies {
    best_effort_after_ownership().with_ownership_mode(MetadataPolicy::BestEffort)
}

/// `BestEffort` tolerates the destination declining one write. A batch whose persistence barrier
/// failed, or that could not be started, did not decline anything: the metadata that is there is
/// not durable, or none of it is there. Pinning it on one family and tolerating that family would
/// publish a file whose report says its metadata was applied.
#[tokio::test]
async fn a_failure_of_the_whole_batch_is_never_tolerated() {
    for (refusal, family, applied) in [
        (
            Refusal::BarrierFailed,
            MetadataFamily::Timestamps,
            &[
                MetadataFamily::OwnershipMode,
                MetadataFamily::Acl,
                MetadataFamily::Xattrs,
                MetadataFamily::Tags,
                MetadataFamily::Timestamps,
            ][..],
        ),
        (
            Refusal::StageUnavailable,
            MetadataFamily::OwnershipMode,
            &[][..],
        ),
        // The refused ACL is not what the caller has to look at: the ownership ahead of it was
        // applied and is not durable.
        (
            Refusal::RefusedThenBarrierFailed,
            MetadataFamily::OwnershipMode,
            &[MetadataFamily::OwnershipMode][..],
        ),
    ] {
        let refused: &'static [MetadataFamily] = match refusal {
            Refusal::RefusedThenBarrierFailed => &[MetadataFamily::Acl],
            _ => &[],
        };
        let target = ScriptedStage::refusing(refusal, refused);
        let failure = plan(everything_best_effort())
            .apply_to_stage(&target, &stage(), CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(failure.family(), family, "{refusal:?}");
        assert!(failure.storage_error().is_some(), "{refusal:?}");
        assert_eq!(
            failure.kind(),
            ApplicationFailureKind::BatchFailed,
            "{refusal:?}"
        );
        assert!(failure.to_string().contains("as a whole"), "{failure}");
        assert_eq!(target.applied_families(), applied, "{refusal:?}");
        // Applied to the stage is not applied: after a failed barrier none of it is durable.
        assert!(
            failure
                .report()
                .outcomes()
                .iter()
                .all(|value| value.outcome != ApplicationOutcome::Applied),
            "{refusal:?}: {:?}",
            failure.report()
        );
    }
}

/// A lost session is not the destination declining this write either, on either path.
#[tokio::test]
async fn a_lost_session_on_a_tolerant_family_still_stops_the_copy() {
    let target = ScriptedStage::new(Refusal::SessionLost);
    let staged = plan(best_effort_after_ownership())
        .apply_to_stage(&target, &stage(), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(staged.family(), MetadataFamily::Acl);
    assert_eq!(staged.kind(), ApplicationFailureKind::SessionLost);
    let target = ScriptedStage::new(Refusal::SessionLost);
    let published = plan(best_effort_after_ownership())
        .apply(
            &target,
            &StoragePath::new("file").unwrap(),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(published.family(), MetadataFamily::Acl);
    assert_eq!(target.applied_families(), [MetadataFamily::OwnershipMode]);
}

/// A required family the destination refuses: the failure says it was refused, and the caller can
/// reach what the storage reported, both through `source()` and in the text.
#[tokio::test]
async fn a_refused_write_reaches_the_storage_diagnostic() {
    let target = ScriptedStage::new(Refusal::InOrder);
    let failure = plan(all_exact())
        .apply_to_stage(&target, &stage(), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(failure.kind(), ApplicationFailureKind::Refused);
    assert!(failure.storage_error().is_some());
    let text = failure.to_string();
    assert!(
        text.starts_with("applying ACL failed: the destination refused the write"),
        "{text}"
    );
    assert!(text.contains("destination refused the ACL"), "{text}");
}

#[tokio::test]
async fn a_cancelled_application_says_so() {
    let target = ScriptedStage::new(Refusal::CancelledBetween);
    let failure = plan(best_effort_after_ownership())
        .apply_to_stage(&target, &stage(), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(failure.kind(), ApplicationFailureKind::Cancelled);
}
