//! Deterministic metadata planning, semantic-loss reporting, and application.

use std::fmt;

use tokio_util::sync::CancellationToken;

use crate::model::{
    AclEncoding, FailureClass, MappedOwnership, MetadataObservation, MetadataObservations,
    OwnershipMode, StoragePath, StorageTimestamp, TimePrecision, TimestampMetadata,
};
use crate::storage::{
    Metadata, MetadataMutation, PreparedStage, StagedDestination, StagedMetadataApplicationFailure,
    StorageRoleFailure,
};

mod errors;

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
pub enum SemanticLoss {
    AclDropped,
    XattrsDropped,
    TagsDropped,
    OwnershipModeDropped,
    OwnerAndGroupDropped,
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

/// Compiles all mappings before a target mutation can be issued.
///
/// # Errors
/// Returns the first policy, observation, capability, or principal-mapping refusal.
pub fn compile_metadata_plan(
    request: &MetadataPlanRequest<'_>,
) -> Result<MetadataPlan, MetadataPlanError> {
    let mut plan = MetadataPlan {
        mappings: Vec::with_capacity(5),
        mutations: Vec::with_capacity(5),
        losses: LossReport::default(),
    };
    // Ownership first, and mode with it: writing permission bits recomputes the ACL — the POSIX
    // mask entry, and on most NFSv4 servers (ONTAP among them) the whole ACL. Compiling the ACL
    // first would put it earlier in `mutations`, which is also the order they are applied in, so
    // the copied ACL would be silently overwritten by the mode that follows it and the report
    // would still say it was applied.
    compile_ownership(request, &mut plan)?;
    compile_acl(request, &mut plan)?;
    compile_value(
        MetadataFamily::Xattrs,
        request.observations.xattrs(),
        request.target.xattrs,
        request.policies,
        &mut plan,
        |value| MetadataMutation::Xattrs(value.clone()),
    )?;
    compile_value(
        MetadataFamily::Tags,
        request.observations.tags(),
        request.target.tags,
        request.policies,
        &mut plan,
        |value| MetadataMutation::Tags(value.clone()),
    )?;
    compile_timestamps(request, &mut plan)?;
    Ok(plan)
}

/// Compiles automatic copy facts without fabricating numeric owner/group IDs.
pub(crate) fn compile_copied_metadata_plan(
    request: &MetadataPlanRequest<'_>,
    mode_without_ownership: Option<u32>,
) -> Result<MetadataPlan, MetadataPlanError> {
    let projected = MetadataPlanRequest {
        observations: request.observations,
        target: request.target,
        policies: if mode_without_ownership.is_some() {
            request.policies.with_ownership_mode(MetadataPolicy::Omit)
        } else {
            request.policies
        },
        principal_mapper: request.principal_mapper,
    };
    let mut plan = compile_metadata_plan(&projected)?;
    if let Some(mode) = mode_without_ownership
        .filter(|_| request.policies.get(MetadataFamily::OwnershipMode) != MetadataPolicy::Omit)
    {
        let family = MetadataFamily::OwnershipMode;
        // This alternative observation replaces the absent numeric ownership family.
        plan.mappings.retain(|value| value.family != family);
        plan.mutations.retain(|(value, _)| *value != family);
        plan.losses.0.retain(|(value, _)| *value != family);
        let supported = matches!(
            request.target.ownership_mode,
            OwnershipTarget::Numeric | OwnershipTarget::ModeOnly
        );
        drop_with_loss(
            &mut plan,
            family,
            request.policies.get(family),
            if supported {
                SemanticLoss::OwnerAndGroupDropped
            } else {
                SemanticLoss::OwnershipModeDropped
            },
        )?;
        if supported {
            // The ownership mutation was just retained away, so every mutation left is one that
            // has to observe the new mode: the ACL is recomputed by it, and the timestamps have
            // to be stamped after it. Front of the queue is the only correct place.
            plan.mutations
                .insert(0, (family, MetadataMutation::Mode(mode & 0o7777)));
        }
    }
    Ok(plan)
}

fn compile_acl(
    request: &MetadataPlanRequest<'_>,
    plan: &mut MetadataPlan,
) -> Result<(), MetadataPlanError> {
    let family = MetadataFamily::Acl;
    let policy = request.policies.get(family);
    let Some(value) = observed_value(request.observations.acl(), family, policy, plan)? else {
        return Ok(());
    };
    match request.target.acl {
        AclTarget::Encoding(encoding) if encoding == value.encoding() => {
            exact(plan, family, MetadataMutation::Acl(value.clone()));
            Ok(())
        }
        // Not a downgrade: without an external mapping the destination cannot hold this ACL at
        // all, so `AllowKnownLoss`, which requires the family to be carried, refuses too.
        AclTarget::Encoding(destination) => unavailable(
            plan,
            family,
            policy,
            MappingDecision::RequiresExternalMapping,
            RefusalCause::EncodingsDiffer {
                source: value.encoding(),
                destination,
            },
        ),
        AclTarget::Unsupported => unavailable(
            plan,
            family,
            policy,
            MappingDecision::Unsupported,
            RefusalCause::DestinationCannotStore,
        ),
        AclTarget::NotApplicable => drop_with_loss(plan, family, policy, SemanticLoss::AclDropped),
    }
}

fn compile_value<T>(
    family: MetadataFamily,
    observation: &MetadataObservation<T>,
    target: ValueTarget,
    policies: MetadataPolicies,
    plan: &mut MetadataPlan,
    mutation: impl FnOnce(&T) -> MetadataMutation,
) -> Result<(), MetadataPlanError> {
    let policy = policies.get(family);
    let Some(value) = observed_value(observation, family, policy, plan)? else {
        return Ok(());
    };
    match target {
        ValueTarget::Supported => {
            exact(plan, family, mutation(value));
            Ok(())
        }
        ValueTarget::Unsupported => unavailable(
            plan,
            family,
            policy,
            MappingDecision::Unsupported,
            RefusalCause::DestinationCannotStore,
        ),
        ValueTarget::NotApplicable => {
            let loss = match family {
                MetadataFamily::Xattrs => SemanticLoss::XattrsDropped,
                MetadataFamily::Tags => SemanticLoss::TagsDropped,
                _ => unreachable!("value metadata is xattrs or tags"),
            };
            drop_with_loss(plan, family, policy, loss)
        }
    }
}

fn compile_ownership(
    request: &MetadataPlanRequest<'_>,
    plan: &mut MetadataPlan,
) -> Result<(), MetadataPlanError> {
    let family = MetadataFamily::OwnershipMode;
    let policy = request.policies.get(family);
    let Some(value) = observed_value(request.observations.ownership_mode(), family, policy, plan)?
    else {
        return Ok(());
    };
    match request.target.ownership_mode {
        OwnershipTarget::Numeric => {
            exact(plan, family, MetadataMutation::NumericOwnership(*value));
            Ok(())
        }
        OwnershipTarget::ExternalMapping => {
            let Some(mapper) = request.principal_mapper else {
                return if policy == MetadataPolicy::AllowKnownLoss {
                    drop_with_loss(plan, family, policy, SemanticLoss::OwnershipModeDropped)
                } else {
                    unavailable(
                        plan,
                        family,
                        policy,
                        MappingDecision::RequiresExternalMapping,
                        RefusalCause::PrincipalMapperMissing,
                    )
                };
            };
            let ownership = mapper.map(*value).map_err(|_| {
                MetadataPlanError::new(family, policy, RefusalCause::PrincipalMappingFailed)
            })?;
            exact(plan, family, MetadataMutation::MappedOwnership(ownership));
            Ok(())
        }
        OwnershipTarget::ModeOnly => {
            let losses = vec![SemanticLoss::OwnerAndGroupDropped];
            if policy == MetadataPolicy::RequireExact {
                return Err(MetadataPlanError::new(
                    family,
                    policy,
                    RefusalCause::LossRejected(SemanticLoss::OwnerAndGroupDropped),
                ));
            }
            plan.losses.0.push((family, losses[0]));
            plan.mappings.push(FamilyMapping {
                family,
                decision: MappingDecision::Lossy(losses),
            });
            plan.mutations
                .push((family, MetadataMutation::Mode(value.mode & 0o7777)));
            Ok(())
        }
        OwnershipTarget::Unsupported => unavailable(
            plan,
            family,
            policy,
            MappingDecision::Unsupported,
            RefusalCause::DestinationCannotStore,
        ),
        OwnershipTarget::NotApplicable => {
            drop_with_loss(plan, family, policy, SemanticLoss::OwnershipModeDropped)
        }
    }
}

fn compile_timestamps(
    request: &MetadataPlanRequest<'_>,
    plan: &mut MetadataPlan,
) -> Result<(), MetadataPlanError> {
    let family = MetadataFamily::Timestamps;
    let policy = request.policies.get(family);
    let Some(value) = observed_value(request.observations.timestamps(), family, policy, plan)?
    else {
        return Ok(());
    };
    let target = match request.target.timestamps {
        TimestampTargetCapability::Supported(target) => target,
        TimestampTargetCapability::Unsupported => {
            return unavailable(
                plan,
                family,
                policy,
                MappingDecision::Unsupported,
                RefusalCause::DestinationCannotStore,
            );
        }
        TimestampTargetCapability::NotApplicable => {
            let mut losses = Vec::new();
            if value.accessed.is_some() {
                losses.push(SemanticLoss::AccessedTimestampDropped);
            }
            if value.modified.is_some() {
                losses.push(SemanticLoss::ModifiedTimestampDropped);
            }
            if value.created.is_some() {
                losses.push(SemanticLoss::CreatedTimestampDropped);
            }
            return drop_with_losses(plan, family, policy, losses);
        }
    };
    let mut losses = Vec::new();
    let mapped = TimestampMetadata {
        accessed: map_timestamp(
            value.accessed,
            target.accessed,
            target.precision,
            SemanticLoss::AccessedTimestampDropped,
            &mut losses,
        ),
        modified: map_timestamp(
            value.modified,
            target.modified,
            target.precision,
            SemanticLoss::ModifiedTimestampDropped,
            &mut losses,
        ),
        created: map_timestamp(
            value.created,
            target.created,
            target.precision,
            SemanticLoss::CreatedTimestampDropped,
            &mut losses,
        ),
    };
    if losses.is_empty() {
        exact(plan, family, MetadataMutation::Timestamps(mapped));
        return Ok(());
    }
    if policy == MetadataPolicy::RequireExact {
        return Err(MetadataPlanError::new(
            family,
            policy,
            RefusalCause::LossRejected(losses[0]),
        ));
    }
    for loss in &losses {
        plan.losses.0.push((family, *loss));
    }
    plan.mappings.push(FamilyMapping {
        family,
        decision: MappingDecision::Lossy(losses),
    });
    plan.mutations
        .push((family, MetadataMutation::Timestamps(mapped)));
    Ok(())
}

fn map_timestamp(
    value: Option<StorageTimestamp>,
    supported: bool,
    precision: TimePrecision,
    dropped: SemanticLoss,
    losses: &mut Vec<SemanticLoss>,
) -> Option<StorageTimestamp> {
    let value = value?;
    if !supported {
        losses.push(dropped);
        return None;
    }
    if value.precision() > precision {
        losses.push(SemanticLoss::TimestampPrecisionReduced);
        let step = precision_step(precision);
        let nanos = value.unix_nanos().div_euclid(step) * step;
        return StorageTimestamp::new(nanos, precision).ok();
    }
    Some(value)
}

const fn precision_step(precision: TimePrecision) -> i128 {
    match precision {
        TimePrecision::Seconds => 1_000_000_000,
        TimePrecision::Milliseconds => 1_000_000,
        TimePrecision::Microseconds => 1_000,
        TimePrecision::HundredNanoseconds => 100,
        TimePrecision::Nanoseconds => 1,
    }
}

fn observed_value<'a, T>(
    observation: &'a MetadataObservation<T>,
    family: MetadataFamily,
    policy: MetadataPolicy,
    plan: &mut MetadataPlan,
) -> Result<Option<&'a T>, MetadataPlanError> {
    if policy == MetadataPolicy::Omit {
        plan.mappings.push(FamilyMapping {
            family,
            decision: MappingDecision::OmittedByPolicy,
        });
        return Ok(None);
    }
    match observation {
        MetadataObservation::Value { value, .. } => Ok(Some(value)),
        MetadataObservation::NotRequested => {
            if policy != MetadataPolicy::BestEffort {
                return Err(MetadataPlanError::new(
                    family,
                    policy,
                    RefusalCause::SourceDidNotObserve,
                ));
            }
            plan.mappings.push(FamilyMapping {
                family,
                decision: MappingDecision::NotObserved,
            });
            Ok(None)
        }
        MetadataObservation::NotApplicable => {
            plan.mappings.push(FamilyMapping {
                family,
                decision: MappingDecision::NotApplicable,
            });
            Ok(None)
        }
        MetadataObservation::Unsupported => unavailable(
            plan,
            family,
            policy,
            MappingDecision::Unsupported,
            RefusalCause::SourceCannotObserve,
        )
        .map(|()| None),
        MetadataObservation::Failed { class, transience } => Err(MetadataPlanError::new(
            family,
            policy,
            RefusalCause::SourceObservationFailed {
                class: *class,
                transience: *transience,
            },
        )),
    }
}

fn drop_with_loss(
    plan: &mut MetadataPlan,
    family: MetadataFamily,
    policy: MetadataPolicy,
    loss: SemanticLoss,
) -> Result<(), MetadataPlanError> {
    drop_with_losses(plan, family, policy, vec![loss])
}

fn drop_with_losses(
    plan: &mut MetadataPlan,
    family: MetadataFamily,
    policy: MetadataPolicy,
    losses: Vec<SemanticLoss>,
) -> Result<(), MetadataPlanError> {
    match policy {
        MetadataPolicy::AllowKnownLoss | MetadataPolicy::BestEffort => {
            plan.losses
                .0
                .extend(losses.iter().map(|loss| (family, *loss)));
            plan.mappings.push(FamilyMapping {
                family,
                decision: MappingDecision::Lossy(losses),
            });
            Ok(())
        }
        // Nothing to lose — no value was there to drop — so nothing for a policy to refuse, and
        // "lossy" or "omitted" would both misreport it.
        _ if losses.is_empty() => {
            plan.mappings.push(FamilyMapping {
                family,
                decision: MappingDecision::NotApplicable,
            });
            Ok(())
        }
        MetadataPolicy::RequireExact | MetadataPolicy::Omit => Err(MetadataPlanError::new(
            family,
            policy,
            RefusalCause::LossRejected(losses[0]),
        )),
    }
}

fn exact(plan: &mut MetadataPlan, family: MetadataFamily, mutation: MetadataMutation) {
    plan.mappings.push(FamilyMapping {
        family,
        decision: MappingDecision::Exact,
    });
    plan.mutations.push((family, mutation));
}

fn unavailable(
    plan: &mut MetadataPlan,
    family: MetadataFamily,
    policy: MetadataPolicy,
    decision: MappingDecision,
    cause: RefusalCause,
) -> Result<(), MetadataPlanError> {
    if policy == MetadataPolicy::BestEffort {
        plan.mappings.push(FamilyMapping { family, decision });
        return Ok(());
    }
    Err(MetadataPlanError::new(family, policy, cause))
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
