//! Deterministic metadata planning, semantic-loss reporting, and application.

use std::fmt;

use tokio_util::sync::CancellationToken;

use crate::model::{
    AclEncoding, FailureClass, MappedOwnership, MetadataObservations, OwnershipMode, StoragePath,
    TimePrecision,
};
use crate::storage::{
    Metadata, MetadataMutation, PreparedStage, StagedDestination, StagedMetadataApplicationFailure,
    StorageRoleFailure,
};

mod compile;
mod errors;

pub(crate) use compile::compile_copied_metadata_plan;
pub use compile::compile_metadata_plan;
pub(crate) use errors::role_failure_text;
pub use errors::{
    ApplicationFailureKind, FamilyFailure, MetadataApplicationFailure, MetadataPlanError,
    MetadataPlanErrorKind, RefusalCause, RefusalSide,
};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MetadataFamily {
    Acl,
    Xattrs,
    Tags,
    OwnershipMode,
    Timestamps,
}

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
pub enum OwnershipTarget {
    Numeric,
    ExternalMapping,
    ModeOnly,
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
    XattrsDropped,
    TagsDropped,
    OwnershipModeDropped,
    OwnerAndGroupDropped,
    /// Owner and group dropped because the source named them and the names could not be mapped
    /// to ids; the mode is kept.
    OwnerAndGroupUnmapped,
    TimestampPrecisionReduced,
    AccessedTimestampDropped,
    ModifiedTimestampDropped,
    CreatedTimestampDropped,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MappingDecision {
    Exact,
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

    #[must_use]
    pub fn has_mutations(&self) -> bool {
        !self.mutations.is_empty()
    }

    /// Applies the immutable plan in family order. A family the target refuses does not stop the
    /// others; the item fails afterwards with every refusal listed.
    ///
    /// # Errors
    /// Returns every failed family and the partial application report on cancellation or target
    /// storage failure.
    pub async fn apply(
        &self,
        target: &dyn Metadata,
        path: &StoragePath,
        cancel: CancellationToken,
    ) -> Result<MetadataApplicationReport, MetadataApplicationFailure> {
        self.apply_to(ApplicationTarget::Published { target, path }, cancel)
            .await
    }

    pub(crate) async fn apply_to_stage(
        &self,
        target: &dyn StagedDestination,
        stage: &PreparedStage,
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
            let Err(failure) = target
                .apply_metadata_batch(stage, mutations, cancel.clone())
                .await
            else {
                for (family, _) in &pending {
                    set_outcome(&mut outcomes, *family, ApplicationOutcome::Applied);
                }
                break;
            };
            let Some(next) = resume_after(&pending, failure, &mut outcomes, &mut failures, &cancel)
            else {
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
            return Ok(MetadataApplicationReport {
                outcomes,
                losses: self.losses.clone(),
            });
        }
        Err(self.failed(failures, outcomes))
    }

    fn failed(
        &self,
        failures: Vec<FamilyFailure>,
        outcomes: Vec<FamilyApplication>,
    ) -> MetadataApplicationFailure {
        MetadataApplicationFailure {
            failures,
            report: MetadataApplicationReport {
                outcomes,
                losses: self.losses.clone(),
            },
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
/// Cancellation, a lost session, and a batch that failed as a whole stop application: later
/// writes either cannot succeed or would not be trustworthy.
fn resume_after<'a>(
    pending: &[&'a (MetadataFamily, MetadataMutation)],
    failure: StagedMetadataApplicationFailure,
    outcomes: &mut [FamilyApplication],
    failures: &mut Vec<FamilyFailure>,
    cancel: &CancellationToken,
) -> Option<Vec<&'a (MetadataFamily, MetadataMutation)>> {
    // A batch that failed as a whole is charged to the last mutation it applied without making
    // durable — the first one when it applied none — which is the family a caller has to look at.
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
    // After a failed barrier nothing in this batch is durable, so none of it is claimed.
    let durably_applied = if whole_batch { 0 } else { completed };
    for (family, _) in &pending[..durably_applied] {
        set_outcome(outcomes, *family, ApplicationOutcome::Applied);
    }
    let family = pending[failed_index].0;
    let cancelled = cancel.is_cancelled() || failure.error.as_ref().is_none_or(is_cancellation);
    if !cancelled {
        set_outcome(outcomes, family, ApplicationOutcome::Failed);
    }
    let stop = cancelled || whole_batch || !failure.error.as_ref().is_some_and(is_refusal);
    failures.push(FamilyFailure {
        family,
        kind: ApplicationFailureKind::of(failure.error.as_ref(), cancelled, whole_batch),
        error: failure.error.map(Box::new),
    });
    if stop {
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

enum ApplicationTarget<'a> {
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
            Self::Published { target, path } => target.apply(path, mutation, cancel).await,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationOutcome {
    Applied,
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
    losses: LossReport,
}

impl MetadataApplicationReport {
    /// Known semantic losses retained from planning, including unmapped principals.
    #[must_use]
    pub const fn loss_report(&self) -> &LossReport {
        &self.losses
    }

    #[must_use]
    pub fn outcomes(&self) -> &[FamilyApplication] {
        &self.outcomes
    }
}

fn planned_outcome(decision: &MappingDecision, has_mutation: bool) -> ApplicationOutcome {
    match decision {
        MappingDecision::Exact => ApplicationOutcome::NotObserved,
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
