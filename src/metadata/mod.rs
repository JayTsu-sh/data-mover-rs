//! Deterministic metadata planning, semantic-loss reporting, and application.

use std::fmt;
use std::mem;

use tokio_util::sync::CancellationToken;

use crate::model::{
    AclEncoding, FailureClass, MappedOwnership, MetadataObservation, MetadataObservations,
    OwnershipMode, StoragePath, StorageTimestamp, TimePrecision, TimestampMetadata,
};
use crate::storage::{
    Metadata, MetadataMutation, PreparedStage, StagedDestination, StagedMetadataApplicationFailure,
    StorageRoleFailure,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataPlanErrorKind {
    KnownLossRejected,
    ExternalMappingRequired,
    Unsupported,
    ObservationFailed,
    ObservationRequired,
    PrincipalMappingFailed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataPlanError {
    family: MetadataFamily,
    kind: MetadataPlanErrorKind,
}

impl MetadataPlanError {
    /// Builds one refusal. Callers outside this module only ever read refusals; this exists so
    /// their tests can state the case they are checking instead of provoking it.
    #[cfg(test)]
    pub(crate) const fn new(family: MetadataFamily, kind: MetadataPlanErrorKind) -> Self {
        Self { family, kind }
    }

    #[must_use]
    pub const fn family(self) -> MetadataFamily {
        self.family
    }
    #[must_use]
    pub const fn kind(self) -> MetadataPlanErrorKind {
        self.kind
    }
}

impl fmt::Display for MetadataPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "metadata {:?} planning failed: {:?}",
            self.family, self.kind
        )
    }
}

impl std::error::Error for MetadataPlanError {}

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
    /// Families whose application failure is recorded instead of propagated. A destination can
    /// advertise a capability and still refuse the write — `NFSv4` `SETACL` is the standing example
    /// — and `BestEffort` is the one policy that says that must not fail the transfer.
    tolerant: Vec<MetadataFamily>,
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

    /// Applies the immutable plan in family order and stops at the first failure.
    ///
    /// # Errors
    /// Returns the partial application report on cancellation or target storage failure.
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
        let mut pending = self.mutations.iter().collect::<Vec<_>>();
        if let Some((family, _)) = pending.first()
            && cancel.is_cancelled()
        {
            return Err(self.failure(*family, None, outcomes));
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
            pending = self.resume_after(&pending, failure, &mut outcomes, &cancel)?;
        }
        Ok(MetadataApplicationReport {
            outcomes,
            losses: self.losses.clone(),
        })
    }

    /// Records what one failed batch did and returns what still has to be sent, or the failure
    /// when the family that failed cannot be tolerated.
    ///
    /// A tolerant family that fails is skipped and everything not yet applied is sent again.
    /// Resuming rather than reordering is deliberate: the order families are applied in is itself
    /// an invariant (a mode written after an ACL rewrites it), so moving the tolerant ones to the
    /// end to make one batch would trade one silent corruption for another.
    fn resume_after<'a>(
        &self,
        pending: &[&'a (MetadataFamily, MetadataMutation)],
        failure: StagedMetadataApplicationFailure,
        outcomes: &mut Vec<FamilyApplication>,
        cancel: &CancellationToken,
    ) -> Result<Vec<&'a (MetadataFamily, MetadataMutation)>, MetadataApplicationFailure> {
        let failed_index = failure.failed_index.min(pending.len() - 1);
        // A backend may reject the batch before applying any of it, so what precedes the failed
        // index is not necessarily applied: only `completed` says that.
        let completed = failure.completed.min(failed_index);
        for (family, _) in &pending[..completed] {
            set_outcome(outcomes, *family, ApplicationOutcome::Applied);
        }
        let family = pending[failed_index].0;
        let cancelled = cancel.is_cancelled() || failure.error.as_ref().is_none_or(is_cancellation);
        if cancelled || !self.tolerant.contains(&family) {
            if !cancelled {
                set_outcome(outcomes, family, ApplicationOutcome::Failed);
            }
            return Err(self.failure(family, failure.error, mem::take(outcomes)));
        }
        set_outcome(outcomes, family, ApplicationOutcome::Failed);
        Ok(pending[completed..failed_index]
            .iter()
            .chain(&pending[failed_index + 1..])
            .copied()
            .collect())
    }

    fn failure(
        &self,
        family: MetadataFamily,
        error: Option<StorageRoleFailure>,
        outcomes: Vec<FamilyApplication>,
    ) -> MetadataApplicationFailure {
        MetadataApplicationFailure {
            family,
            error: error.map(Box::new),
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
        for (family, mutation) in &self.mutations {
            if cancel.is_cancelled() {
                return Err(MetadataApplicationFailure {
                    family: *family,
                    error: None,
                    report: MetadataApplicationReport {
                        outcomes,
                        losses: self.losses.clone(),
                    },
                });
            }
            if let Err(error) = target.apply(mutation.clone(), cancel.clone()).await {
                let cancelled = cancel.is_cancelled() || is_cancellation(&error);
                if !cancelled {
                    set_outcome(&mut outcomes, *family, ApplicationOutcome::Failed);
                }
                if !cancelled && self.tolerant.contains(family) {
                    continue;
                }
                return Err(MetadataApplicationFailure {
                    family: *family,
                    error: Some(Box::new(error)),
                    report: MetadataApplicationReport {
                        outcomes,
                        losses: self.losses.clone(),
                    },
                });
            }
            set_outcome(&mut outcomes, *family, ApplicationOutcome::Applied);
        }
        Ok(MetadataApplicationReport {
            outcomes,
            losses: self.losses.clone(),
        })
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

#[derive(Debug)]
pub struct MetadataApplicationFailure {
    family: MetadataFamily,
    /// Boxed so the whole `Result` stays small: the role failure carries paths and diagnostic
    /// strings, and every successful application would otherwise pay for its size.
    error: Option<Box<StorageRoleFailure>>,
    report: MetadataApplicationReport,
}

impl MetadataApplicationFailure {
    #[must_use]
    pub const fn family(&self) -> MetadataFamily {
        self.family
    }
    #[must_use]
    pub fn storage_error(&self) -> Option<&StorageRoleFailure> {
        self.error.as_deref()
    }
    #[must_use]
    pub const fn report(&self) -> &MetadataApplicationReport {
        &self.report
    }
}

impl fmt::Display for MetadataApplicationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "metadata {:?} application failed", self.family)
    }
}

impl std::error::Error for MetadataApplicationFailure {}

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
        tolerant: Vec::new(),
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
    plan.tolerant = plan
        .mutations
        .iter()
        .map(|(family, _)| *family)
        .filter(|family| request.policies.get(*family) == MetadataPolicy::BestEffort)
        .collect();
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
        AclTarget::Encoding(_) => unavailable(
            plan,
            family,
            policy,
            MappingDecision::RequiresExternalMapping,
        ),
        AclTarget::Unsupported => unavailable(plan, family, policy, MappingDecision::Unsupported),
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
        ValueTarget::Unsupported => unavailable(plan, family, policy, MappingDecision::Unsupported),
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
                    )
                };
            };
            let ownership = mapper.map(*value).map_err(|_| MetadataPlanError {
                family,
                kind: MetadataPlanErrorKind::PrincipalMappingFailed,
            })?;
            exact(plan, family, MetadataMutation::MappedOwnership(ownership));
            Ok(())
        }
        OwnershipTarget::ModeOnly => {
            let losses = vec![SemanticLoss::OwnerAndGroupDropped];
            if policy == MetadataPolicy::RequireExact {
                return Err(MetadataPlanError {
                    family,
                    kind: MetadataPlanErrorKind::KnownLossRejected,
                });
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
        OwnershipTarget::Unsupported => {
            unavailable(plan, family, policy, MappingDecision::Unsupported)
        }
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
            return unavailable(plan, family, policy, MappingDecision::Unsupported);
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
        return Err(MetadataPlanError {
            family,
            kind: MetadataPlanErrorKind::KnownLossRejected,
        });
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
                return Err(MetadataPlanError {
                    family,
                    kind: MetadataPlanErrorKind::ObservationRequired,
                });
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
        MetadataObservation::Unsupported => {
            unavailable(plan, family, policy, MappingDecision::Unsupported).map(|()| None)
        }
        MetadataObservation::Failed { .. } if policy == MetadataPolicy::BestEffort => {
            plan.mappings.push(FamilyMapping {
                family,
                decision: MappingDecision::ObservationFailed,
            });
            Ok(None)
        }
        MetadataObservation::Failed { .. } => Err(MetadataPlanError {
            family,
            kind: MetadataPlanErrorKind::ObservationFailed,
        }),
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
        MetadataPolicy::RequireExact | MetadataPolicy::Omit => Err(MetadataPlanError {
            family,
            kind: MetadataPlanErrorKind::KnownLossRejected,
        }),
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
) -> Result<(), MetadataPlanError> {
    if policy == MetadataPolicy::BestEffort {
        plan.mappings.push(FamilyMapping { family, decision });
        return Ok(());
    }
    let kind = match decision {
        MappingDecision::RequiresExternalMapping => MetadataPlanErrorKind::ExternalMappingRequired,
        MappingDecision::Unsupported => MetadataPlanErrorKind::Unsupported,
        _ => MetadataPlanErrorKind::KnownLossRejected,
    };
    Err(MetadataPlanError { family, kind })
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

/// `BestEffort` tolerates a destination refusing a write. Cancellation is not a refusal and is
/// never tolerated, whichever family it lands on — including a `Cancelled` a server reports on its
/// own (SMB `STATUS_CANCELLED`): upstream re-enqueues cancelled work, so stopping is what lets it.
fn is_cancellation(error: &StorageRoleFailure) -> bool {
    let class = match error {
        StorageRoleFailure::Entry(error) => error.class(),
        StorageRoleFailure::Session(error) => error.class(),
    };
    class == FailureClass::Cancelled
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
