//! Deterministic metadata planning, semantic-loss reporting, and application.

use std::fmt;

use tokio_util::sync::CancellationToken;

use crate::model::{
    AclEncoding, MappedOwnership, MetadataObservation, MetadataObservations, OwnershipMode,
    StoragePath, StorageTimestamp, TimePrecision, TimestampMetadata,
};
use crate::storage::{Metadata, MetadataMutation, StorageRoleFailure};

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
        let mut outcomes = self
            .mappings
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
            .collect::<Vec<_>>();
        for (family, mutation) in &self.mutations {
            if cancel.is_cancelled() {
                return Err(MetadataApplicationFailure {
                    family: *family,
                    error: None,
                    report: MetadataApplicationReport { outcomes },
                });
            }
            if let Err(error) = target.apply(path, mutation.clone(), cancel.clone()).await {
                set_outcome(&mut outcomes, *family, ApplicationOutcome::Failed);
                return Err(MetadataApplicationFailure {
                    family: *family,
                    error: Some(error),
                    report: MetadataApplicationReport { outcomes },
                });
            }
            set_outcome(&mut outcomes, *family, ApplicationOutcome::Applied);
        }
        Ok(MetadataApplicationReport { outcomes })
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
}

impl MetadataApplicationReport {
    #[must_use]
    pub fn outcomes(&self) -> &[FamilyApplication] {
        &self.outcomes
    }
}

#[derive(Debug)]
pub struct MetadataApplicationFailure {
    family: MetadataFamily,
    error: Option<StorageRoleFailure>,
    report: MetadataApplicationReport,
}

impl MetadataApplicationFailure {
    #[must_use]
    pub const fn family(&self) -> MetadataFamily {
        self.family
    }
    #[must_use]
    pub const fn storage_error(&self) -> Option<&StorageRoleFailure> {
        self.error.as_ref()
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
    };
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
    compile_ownership(request, &mut plan)?;
    compile_timestamps(request, &mut plan)?;
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
        AclTarget::Encoding(_) if policy == MetadataPolicy::AllowKnownLoss => {
            drop_with_loss(plan, family, policy, SemanticLoss::AclDropped)
        }
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
