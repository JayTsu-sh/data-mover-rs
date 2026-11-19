//! Deterministic metadata planning, semantic-loss reporting, and application.

use std::fmt;

use tokio_util::sync::CancellationToken;

use crate::model::{
    AclEncoding, EntryIdentityKey, FailureClass, MappedOwnership, MetadataObservations,
    OwnershipMode, SourceVersion, StoragePath, TimePrecision, Transience,
};
use crate::storage::{
    Metadata, MetadataBatchFailure, MetadataMutation, PreparedStage, StagedDestination,
    StorageRoleFailure,
};

mod acl_mapping;
#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod acl_policy_tests;
mod checkpoint_binding;
mod compile;
mod errors;
mod verify;
#[cfg(test)]
mod verify_anchor_tests;
#[cfg(test)]
mod verify_cancel_tests;
#[cfg(test)]
mod verify_tests;
#[cfg(test)]
mod version_tests;
pub use verify::MetadataVerificationError;

pub use compile::compile_metadata_plan;
pub use compile::{compile_copied_metadata_plan, compile_unavailable_plan};
use errors::family_name;
pub use errors::role_failure_text;
pub use errors::{
    ApplicationFailureKind, FamilyFailure, MetadataApplicationFailure, MetadataPlanError,
    MetadataPlanErrorKind, RefusalCause, RefusalSide,
};

pub use crate::model::MetadataFamily;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MetadataPolicy {
    RequireExact,
    AllowKnownLoss,
    BestEffort,
    #[default]
    Omit,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MetadataPolicies {
    acl: MetadataPolicy,
    xattrs: MetadataPolicy,
    tags: MetadataPolicy,
    ownership_mode: MetadataPolicy,
    timestamps: MetadataPolicy,
}

impl MetadataPolicies {
    #[must_use]
    pub const fn with_acl(mut self, policy: MetadataPolicy) -> Self {
        self.acl = policy;
        self
    }
    #[must_use]
    pub const fn with_xattrs(mut self, policy: MetadataPolicy) -> Self {
        self.xattrs = policy;
        self
    }
    #[must_use]
    pub const fn with_tags(mut self, policy: MetadataPolicy) -> Self {
        self.tags = policy;
        self
    }
    #[must_use]
    pub const fn with_ownership_mode(mut self, policy: MetadataPolicy) -> Self {
        self.ownership_mode = policy;
        self
    }
    #[must_use]
    pub const fn with_timestamps(mut self, policy: MetadataPolicy) -> Self {
        self.timestamps = policy;
        self
    }

    const fn get(self, family: MetadataFamily) -> MetadataPolicy {
        match family {
            MetadataFamily::Acl => self.acl,
            MetadataFamily::Xattrs => self.xattrs,
            MetadataFamily::Tags => self.tags,
            MetadataFamily::OwnershipMode => self.ownership_mode,
            MetadataFamily::Timestamps => self.timestamps,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AclTarget {
    Encoding(AclEncoding),
    Unsupported,
    NotApplicable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValueTarget {
    Supported,
    Unsupported,
    NotApplicable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OwnershipTarget {
    Numeric,
    ExternalMapping,
    ModeOnly,
    /// Stores numeric owner and group, but whoever writes the copy may not give it this one (no
    /// privilege to chown to another user): the mode is kept without its set-id bits, since the
    /// file stays owned by the writer.
    NotPermitted,
    Unsupported,
    NotApplicable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimestampTarget {
    pub precision: TimePrecision,
    pub accessed: bool,
    pub modified: bool,
    pub created: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimestampTargetCapability {
    Supported(TimestampTarget),
    Unsupported,
    NotApplicable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataTarget {
    pub acl: AclTarget,
    pub xattrs: ValueTarget,
    pub tags: ValueTarget,
    pub ownership_mode: OwnershipTarget,
    pub timestamps: TimestampTargetCapability,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PrincipalMappingFailure;

impl fmt::Display for PrincipalMappingFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("principal mapping failed")
    }
}

impl std::error::Error for PrincipalMappingFailure {}

pub trait PrincipalMapper: Send + Sync {
    /// Maps numeric source ownership into target-native principals.
    ///
    /// # Errors
    /// Returns an error when the configured identity source cannot produce both principals.
    fn map(&self, source: OwnershipMode) -> Result<MappedOwnership, PrincipalMappingFailure>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SemanticLoss {
    AclDropped,
    /// The ACL was projected with known omissions or a conservative fallback.
    AclPartiallyCopied,
    XattrsDropped,
    TagsDropped,
    OwnershipModeDropped,
    OwnerAndGroupDropped,
    /// Owner and group dropped because the source named them and the names could not be mapped
    /// to ids; the mode is kept.
    OwnerAndGroupUnmapped,
    /// Owner and group dropped because the writer may not give the file this owner or this group
    /// (no `CAP_CHOWN`, and not its own uid with a group it is in); the mode is kept without its
    /// set-id bits.
    OwnerAndGroupNotPermitted,
    TimestampPrecisionReduced,
    AccessedTimestampDropped,
    ModifiedTimestampDropped,
    CreatedTimestampDropped,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MappingDecision {
    Exact,
    /// Proven current regular-file access equivalence across ACL models.
    Equivalent,
    Lossy(Vec<SemanticLoss>),
    RequiresExternalMapping,
    Unsupported,
    NotObserved,
    NotApplicable,
    OmittedByPolicy,
    /// No longer produced: a read that fails is a failure under every policy. Kept so matching
    /// code outside the crate still compiles.
    ObservationFailed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FamilyMapping {
    pub family: MetadataFamily,
    pub decision: MappingDecision,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LossReport(Vec<(MetadataFamily, SemanticLoss)>);

impl LossReport {
    #[must_use]
    pub fn losses(&self) -> &[(MetadataFamily, SemanticLoss)] {
        &self.0
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// A family that was asked for and is not carried, because an end cannot do it — decided while
/// planning, before any write, and not a failure. The reason names which end and what it lacks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct SkippedFamily {
    pub family: MetadataFamily,
    /// Which end lacks the family: `SourceCannotObserve`, `DestinationCannotStore`,
    /// `EncodingsDiffer`, or `PrincipalMapperMissing` — never a failure or a refused loss. When
    /// both ends lack it, the source is named: it is checked first — except for a destination
    /// that stores nothing at all, whose copies do not read the source and name the destination.
    pub reason: RefusalCause,
}

impl fmt::Display for SkippedFamily {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} skipped: {}",
            family_name(self.family),
            self.reason
        )
    }
}

pub struct MetadataPlanRequest<'a> {
    pub observations: &'a MetadataObservations,
    pub target: MetadataTarget,
    pub policies: MetadataPolicies,
    pub principal_mapper: Option<&'a dyn PrincipalMapper>,
}

#[derive(Clone, Debug)]
pub struct MetadataPlan {
    mappings: Vec<FamilyMapping>,
    mutations: Vec<(MetadataFamily, MetadataMutation)>,
    losses: LossReport,
    skipped: Vec<SkippedFamily>,
    acl_copy: Option<AclCopyReport>,
    acl_source_binding: Option<[u8; 32]>,
}

/// Details of ACL permissions that could not be copied.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AclCopyReport {
    pub issues: Vec<crate::model::acl::AclCopyIssue>,
    /// A data-denying ACL was used because no safe projection could be proven.
    pub conservative_fallback: bool,
}

impl MetadataPlan {
    #[must_use]
    pub fn mappings(&self) -> &[FamilyMapping] {
        &self.mappings
    }

    #[must_use]
    pub const fn loss_report(&self) -> &LossReport {
        &self.losses
    }

    /// Detailed ACL omissions and whether a conservative fallback was needed.
    #[must_use]
    pub const fn acl_copy_report(&self) -> Option<&AclCopyReport> {
        self.acl_copy.as_ref()
    }

    /// The families asked for that one end cannot do, each with the reason.
    #[must_use]
    pub fn skipped(&self) -> &[SkippedFamily] {
        &self.skipped
    }

    #[must_use]
    pub fn has_mutations(&self) -> bool {
        !self.mutations.is_empty()
    }

    /// Whether trusted source permissions will replace the data-file creation default.
    #[must_use]
    pub fn carries_mode(&self) -> bool {
        self.mutations.iter().any(|(_, mutation)| {
            matches!(
                mutation,
                MetadataMutation::NumericOwnership(_)
                    | MetadataMutation::MappedOwnership(_)
                    | MetadataMutation::Mode(_)
            )
        })
    }

    /// Applies the immutable plan in family order. A family the target refuses does not stop the
    /// others; the item fails afterwards with every refusal listed.
    ///
    /// version 指定既有目标版本，Id 不会创建新版本或回退修改当前对象。
    ///
    /// # Errors
    /// Returns every failed family and the partial application report on cancellation or target
    /// storage failure.
    pub async fn apply(
        &self,
        target: &dyn Metadata,
        path: &StoragePath,
        version: &SourceVersion,
        cancel: CancellationToken,
    ) -> Result<MetadataApplicationReport, MetadataApplicationFailure> {
        let destination = match version {
            SourceVersion::Current => ApplicationTarget::Published { target, path },
            SourceVersion::Id(version_id) => ApplicationTarget::Version {
                target,
                path,
                version_id,
            },
        };
        self.apply_to(destination, cancel).await
    }

    /// Checks that recovered staged metadata still satisfies this plan.
    ///
    /// # Errors
    /// Returns a storage failure if the stage cannot attest the requested metadata.
    pub async fn validate_recovered_stage(
        &self,
        target: &dyn StagedDestination,
        stage: &PreparedStage,
        cancel: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        let mutations = self
            .mutations
            .iter()
            .map(|(_, mutation)| mutation.clone())
            .collect();
        target
            .validate_recovered_metadata(stage, mutations, cancel)
            .await
    }

    /// Applies the planned mutations in order to the published directory with the expected identity.
    ///
    /// Uses the backend's identity-bound write capability. An empty plan does not write; callers
    /// still verify the current name with [`Self::verify_published`].
    ///
    /// # Errors
    /// Returns a typed conflict or unsupported operation before writes, and preserves confirmed
    /// progress and the storage error if application is interrupted after writes begin.
    pub async fn apply_bound_directory(
        &self,
        target: &dyn Metadata,
        path: &StoragePath,
        expected: EntryIdentityKey,
        cancel: CancellationToken,
    ) -> Result<MetadataApplicationReport, MetadataApplicationFailure> {
        self.apply_batch(
            BatchApplicationTarget::Directory {
                target,
                path,
                expected,
            },
            cancel,
        )
        .await
    }

    /// Applies planned metadata before the destination publishes its prepared stage.
    ///
    /// # Errors
    /// Returns cancellation or a classified application failure with partial progress.
    pub async fn apply_to_stage(
        &self,
        target: &dyn StagedDestination,
        stage: &PreparedStage,
        cancel: CancellationToken,
    ) -> Result<MetadataApplicationReport, MetadataApplicationFailure> {
        self.apply_batch(BatchApplicationTarget::Stage { target, stage }, cancel)
            .await
    }

    async fn apply_batch(
        &self,
        target: BatchApplicationTarget<'_>,
        cancel: CancellationToken,
    ) -> Result<MetadataApplicationReport, MetadataApplicationFailure> {
        let mut outcomes = self.planned_outcomes();
        let mut failures = Vec::new();
        let mut pending = self.mutations.iter().collect::<Vec<_>>();
        if let Some((family, _)) = pending.first()
            && cancel.is_cancelled()
        {
            failures.push(cancelled_at(*family));
            return Err(self.failed(failures, outcomes));
        }
        while !pending.is_empty() {
            let mutations = pending
                .iter()
                .map(|(_, mutation)| mutation.clone())
                .collect::<Vec<_>>();
            let Err(failure) = target.apply(mutations, cancel.clone()).await else {
                for (family, _) in &pending {
                    set_outcome(&mut outcomes, *family, ApplicationOutcome::Applied);
                }
                break;
            };
            let Some(next) = resume_after(
                &pending,
                failure,
                &mut outcomes,
                &mut failures,
                &cancel,
                &target,
            ) else {
                return Err(self.failed(failures, outcomes));
            };
            pending = next;
        }
        self.finish(failures, outcomes)
    }

    fn finish(
        &self,
        failures: Vec<FamilyFailure>,
        outcomes: Vec<FamilyApplication>,
    ) -> Result<MetadataApplicationReport, MetadataApplicationFailure> {
        if failures.is_empty() {
            return Ok(self.report(outcomes));
        }
        Err(self.failed(failures, outcomes))
    }

    fn failed(
        &self,
        failures: Vec<FamilyFailure>,
        outcomes: Vec<FamilyApplication>,
    ) -> MetadataApplicationFailure {
        MetadataApplicationFailure {
            failures: failures.into_boxed_slice(),
            report: self.report(outcomes),
        }
    }

    async fn apply_to(
        &self,
        target: ApplicationTarget<'_>,
        cancel: CancellationToken,
    ) -> Result<MetadataApplicationReport, MetadataApplicationFailure> {
        let mut outcomes = self.planned_outcomes();
        let mut failures = Vec::new();
        for (family, mutation) in &self.mutations {
            if cancel.is_cancelled() {
                failures.push(cancelled_at(*family));
                return Err(self.failed(failures, outcomes));
            }
            let Err(error) = target.apply(mutation.clone(), cancel.clone()).await else {
                set_outcome(&mut outcomes, *family, ApplicationOutcome::Applied);
                continue;
            };
            let cancelled = cancel.is_cancelled() || is_cancellation(&error);
            if !cancelled {
                set_outcome(&mut outcomes, *family, ApplicationOutcome::Failed);
            }
            let stop = cancelled || !is_refusal(&error);
            failures.push(FamilyFailure {
                family: *family,
                kind: ApplicationFailureKind::of(Some(&error), cancelled, false),
                error: Some(Box::new(error)),
            });
            if stop {
                return Err(self.failed(failures, outcomes));
            }
        }
        self.finish(failures, outcomes)
    }

    fn report(&self, mut outcomes: Vec<FamilyApplication>) -> MetadataApplicationReport {
        if let Some(acl) = &self.acl_copy
            && let Some(outcome) = outcomes
                .iter_mut()
                .find(|v| v.family == MetadataFamily::Acl)
            && outcome.outcome == ApplicationOutcome::Applied
            && (!acl.issues.is_empty() || acl.conservative_fallback)
        {
            outcome.outcome = if acl.conservative_fallback {
                ApplicationOutcome::AppliedConservativeAcl
            } else {
                ApplicationOutcome::AppliedPartialAcl
            };
        }
        MetadataApplicationReport {
            outcomes,
            mappings: self.mappings.clone(),
            losses: self.losses.clone(),
            skipped: self.skipped.clone(),
            acl_copy: self.acl_copy.clone().map(Box::new),
        }
    }

    /// Reconstruct only a checkpoint-backed report; no target mutations are replayed.
    #[must_use]
    pub fn recovered_acl_report(&self) -> Option<MetadataApplicationReport> {
        if !self.mappings.iter().any(|mapping| {
            mapping.family == MetadataFamily::Acl
                && mapping.decision != MappingDecision::OmittedByPolicy
        }) {
            return None;
        }
        let mut outcomes = self.planned_outcomes();
        for (family, _) in &self.mutations {
            set_outcome(
                &mut outcomes,
                *family,
                ApplicationOutcome::RecoveredFromCheckpoint,
            );
        }
        Some(self.report(outcomes))
    }

    fn planned_outcomes(&self) -> Vec<FamilyApplication> {
        self.mappings
            .iter()
            .map(|mapping| FamilyApplication {
                family: mapping.family,
                outcome: planned_outcome(
                    &mapping.decision,
                    self.mutations
                        .iter()
                        .any(|(family, _)| *family == mapping.family),
                ),
            })
            .collect()
    }
}

/// Records what one failed batch did and returns what still has to be sent, or `None` when
/// application has to stop.
///
/// A family the destination refuses is recorded and everything not yet applied is sent again, so
/// the item fails once with every reason. Resuming rather than reordering is deliberate: the order
/// families are applied in is itself an invariant (a mode written after an ACL rewrites it).
/// Cancellation, a lost session, an unconfirmed mutation, and a whole-batch failure stop
/// application. Later writes either cannot succeed or would not be trustworthy.
fn resume_after<'a>(
    pending: &[&'a (MetadataFamily, MetadataMutation)],
    failure: MetadataBatchFailure,
    outcomes: &mut [FamilyApplication],
    failures: &mut Vec<FamilyFailure>,
    cancel: &CancellationToken,
    target: &BatchApplicationTarget<'_>,
) -> Option<Vec<&'a (MetadataFamily, MetadataMutation)>> {
    // Attribute a whole-batch failure to the last completed family, or the first if none ran.
    let whole_batch = failure.failed_index >= pending.len();
    let failed_index = if whole_batch {
        failure.completed.saturating_sub(1)
    } else {
        failure.failed_index
    }
    .min(pending.len() - 1);
    // A backend may reject the batch before applying any of it, so what precedes the failed index
    // is not necessarily applied: only `completed` says that.
    let completed = failure.completed.min(failed_index);
    let cancelled = cancel.is_cancelled() || failure.error.as_ref().is_none_or(is_cancellation);
    // 暂存批次的屏障失败不能确认属性。已发布目录在取消前完成的写入仍可确认。
    let applied = match (whole_batch, target) {
        (false, _) => completed,
        (true, BatchApplicationTarget::Directory { .. }) if cancelled => {
            failure.completed.min(pending.len())
        }
        (true, _) => 0,
    };
    for (family, _) in &pending[..applied] {
        set_outcome(outcomes, *family, ApplicationOutcome::Applied);
    }
    let family = pending[failed_index].0;
    if !cancelled {
        set_outcome(outcomes, family, ApplicationOutcome::Failed);
    }
    let error = failure.error.as_ref();
    let refusal = error.is_some_and(|error| target.is_refusal(error));
    let write_unconfirmed = !whole_batch
        && (failure.write_unconfirmed
            || (matches!(target, BatchApplicationTarget::Directory { .. })
                && !refusal
                && matches!(error, Some(StorageRoleFailure::Entry(_)))));
    let kind = if !cancelled && write_unconfirmed {
        ApplicationFailureKind::WriteUnconfirmed
    } else {
        ApplicationFailureKind::of(error, cancelled, whole_batch)
    };
    failures.push(FamilyFailure {
        family,
        kind,
        error: failure.error.map(Box::new),
    });
    if cancelled || whole_batch || write_unconfirmed || !refusal {
        return None;
    }
    Some(
        pending[completed..failed_index]
            .iter()
            .chain(&pending[failed_index + 1..])
            .copied()
            .collect(),
    )
}

const fn cancelled_at(family: MetadataFamily) -> FamilyFailure {
    FamilyFailure {
        family,
        kind: ApplicationFailureKind::Cancelled,
        error: None,
    }
}

enum BatchApplicationTarget<'a> {
    Stage {
        target: &'a dyn StagedDestination,
        stage: &'a PreparedStage,
    },
    Directory {
        target: &'a dyn Metadata,
        path: &'a StoragePath,
        expected: EntryIdentityKey,
    },
}

impl BatchApplicationTarget<'_> {
    async fn apply(
        &self,
        mutations: Vec<MetadataMutation>,
        cancel: CancellationToken,
    ) -> Result<(), MetadataBatchFailure> {
        match self {
            Self::Stage { target, stage } => {
                target.apply_metadata_batch(stage, mutations, cancel).await
            }
            Self::Directory {
                target,
                path,
                expected,
            } => {
                target
                    .apply_bound_directory(path, *expected, mutations, cancel)
                    .await
            }
        }
    }

    fn is_refusal(&self, error: &StorageRoleFailure) -> bool {
        match self {
            Self::Stage { .. } => is_refusal(error),
            Self::Directory { .. } => matches!(error,
                StorageRoleFailure::Entry(error)
                    if error.transience() == Transience::Permanent
                    && matches!(error.class(),
                        FailureClass::PermissionDenied | FailureClass::Unsupported | FailureClass::InvalidInput)
            ),
        }
    }
}

enum ApplicationTarget<'a> {
    Version {
        target: &'a dyn Metadata,
        path: &'a StoragePath,
        version_id: &'a str,
    },
    Published {
        target: &'a dyn Metadata,
        path: &'a StoragePath,
    },
}

impl ApplicationTarget<'_> {
    async fn apply(
        &self,
        mutation: MetadataMutation,
        cancel: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        match self {
            Self::Version {
                target,
                path,
                version_id,
            } => {
                target
                    .apply_to_version(path, version_id, mutation, cancel)
                    .await
            }
            Self::Published { target, path } => target.apply(path, mutation, cancel).await,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationOutcome {
    Applied,
    /// The projected ACL was applied with reported omissions.
    AppliedPartialAcl,
    /// A data-denying ACL was applied because no safe projection was available.
    AppliedConservativeAcl,
    /// A matching publication checkpoint proves the mutation preceded publication.
    RecoveredFromCheckpoint,
    PreservedByNativeTransfer,
    OmittedByPolicy,
    NotObserved,
    Unsupported,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FamilyApplication {
    pub family: MetadataFamily,
    pub outcome: ApplicationOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataApplicationReport {
    outcomes: Vec<FamilyApplication>,
    mappings: Vec<FamilyMapping>,
    losses: LossReport,
    skipped: Vec<SkippedFamily>,
    acl_copy: Option<Box<AclCopyReport>>,
}

impl MetadataApplicationReport {
    /// Detailed ACL omissions retained after application.
    #[must_use]
    pub fn acl_copy_report(&self) -> Option<&AclCopyReport> {
        self.acl_copy.as_deref()
    }

    /// Planned preservation or equivalence contract for every metadata family.
    #[must_use]
    pub fn mappings(&self) -> &[FamilyMapping] {
        &self.mappings
    }

    /// Known semantic losses retained from planning, including unmapped principals.
    #[must_use]
    pub const fn loss_report(&self) -> &LossReport {
        &self.losses
    }

    #[must_use]
    pub fn outcomes(&self) -> &[FamilyApplication] {
        &self.outcomes
    }

    /// The families asked for that were not carried because an end cannot do them, each with the
    /// reason. An `Unsupported` outcome always has an entry here.
    #[must_use]
    pub fn skipped(&self) -> &[SkippedFamily] {
        &self.skipped
    }
}

fn planned_outcome(decision: &MappingDecision, has_mutation: bool) -> ApplicationOutcome {
    match decision {
        MappingDecision::Exact | MappingDecision::Equivalent => ApplicationOutcome::NotObserved,
        MappingDecision::Lossy(_) if has_mutation => ApplicationOutcome::NotObserved,
        MappingDecision::Lossy(_) | MappingDecision::OmittedByPolicy => {
            ApplicationOutcome::OmittedByPolicy
        }
        MappingDecision::NotObserved | MappingDecision::NotApplicable => {
            ApplicationOutcome::NotObserved
        }
        MappingDecision::RequiresExternalMapping | MappingDecision::Unsupported => {
            ApplicationOutcome::Unsupported
        }
        MappingDecision::ObservationFailed => ApplicationOutcome::Failed,
    }
}

/// Cancellation stops application at once, whichever family it lands on — including a `Cancelled`
/// a server reports on its own (SMB `STATUS_CANCELLED`): upstream re-enqueues cancelled work, so
/// stopping is what lets it.
fn is_cancellation(error: &StorageRoleFailure) -> bool {
    let class = match error {
        StorageRoleFailure::Entry(error) => error.class(),
        StorageRoleFailure::Session(error) => error.class(),
    };
    class == FailureClass::Cancelled
}

/// Whether a failure is the destination declining this one write — after which the other families
/// are still applied. A lost session is not: it ends every write after it too.
fn is_refusal(error: &StorageRoleFailure) -> bool {
    matches!(error, StorageRoleFailure::Entry(_)) && !is_cancellation(error)
}

fn set_outcome(
    outcomes: &mut [FamilyApplication],
    family: MetadataFamily,
    outcome: ApplicationOutcome,
) {
    if let Some(value) = outcomes.iter_mut().find(|value| value.family == family) {
        value.outcome = outcome;
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
#[path = "tests.rs"]
mod tests;
