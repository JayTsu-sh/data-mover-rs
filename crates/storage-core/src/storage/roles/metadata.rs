//! Metadata observation, permission facts, and application contracts.

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use super::StorageRoleFailure;
use super::version::version_unsupported;
use crate::model::{
    AclMetadata, EntryOperationFailure, ExtendedAttribute, FailureClass, MappedOwnership,
    MetadataCapabilities, MetadataFamily, MetadataObservation, MetadataObservations, ObjectTag,
    ObservationMode, ObservationPlan, ObservationSupport, Operation, OwnershipMode, SourceIdentity,
    SourceVersion, StoragePath, TimestampMetadata, Transience,
};
use crate::storage::{
    CopiedMetadataObservation, PermissionFacts, PrincipalIdentity, SourceDescriptor,
};

/// Metadata observation and application role. It never implicitly refetches omitted facts.
#[async_trait]
pub trait Metadata: Send + Sync {
    /// Instance-specific observation support, queried without backend I/O. Unknown by default
    /// so external roles can continue to determine support while observing individual entries.
    fn observation_capabilities(&self) -> MetadataCapabilities {
        MetadataCapabilities::default()
    }

    /// Removes one named extended attribute. This is distinct from applying `Xattrs`, which
    /// only sets the supplied values. Unsupported backends refuse before I/O; NFS requires a
    /// nonempty UTF-8 name and reports the server's missing-attribute or permission refusal.
    async fn remove_xattr(
        &self,
        path: &StoragePath,
        _name: &[u8],
    ) -> Result<(), StorageRoleFailure> {
        Err(StorageRoleFailure::Entry(
            EntryOperationFailure::new(
                path.clone(),
                Operation::Metadata,
                FailureClass::Unsupported,
                Transience::Permanent,
                "removing extended attributes is unsupported",
            )
            .unwrap_or_else(|_| unreachable!("static metadata diagnostic")),
        ))
    }

    /// Permission facts bound to a description. Reuses inline facts when available; named
    /// principal backends override this instead of fabricating numeric owner identifiers.
    /// Known unsupported or inapplicable ownership returns no facts without backend I/O;
    /// backends with independent permission facts override this method.
    /// Every implementation must honor `descriptor.version()`: a historical descriptor must
    /// never read the current object. Refuse an unavailable version with `Unsupported`.
    async fn permissions(
        &self,
        descriptor: &SourceDescriptor,
    ) -> Result<PermissionFacts, StorageRoleFailure> {
        if matches!(
            self.observation_capabilities()
                .support(MetadataFamily::OwnershipMode),
            ObservationSupport::NotApplicable | ObservationSupport::Unsupported
        ) {
            return Ok(PermissionFacts::default());
        }
        let plan = ObservationPlan::default().with_ownership_mode(ObservationMode::BestEffort);
        let (observations, mode_without_ownership) = match descriptor.version() {
            SourceVersion::Current => (
                match self.observe_listed(descriptor, plan) {
                    Some(observed) => observed?,
                    None => {
                        self.observe_bound(&descriptor.path, &descriptor.source_identity, plan)
                            .await?
                    }
                },
                None,
            ),
            version @ SourceVersion::Id(_) => {
                let bound = self
                    .observe_copy_bound_version(
                        &descriptor.path,
                        &descriptor.source_identity,
                        version,
                        plan,
                    )
                    .await?;
                (bound.observations, bound.mode_without_ownership)
            }
        };
        permission_facts(&descriptor.path, &observations, mode_without_ownership)
    }

    /// Source observations requested by the ordinary transfer entry for baseline metadata copy.
    fn copied_metadata_observation_plan(&self) -> Option<ObservationPlan> {
        None
    }

    async fn observe(
        &self,
        path: &StoragePath,
        plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure>;
    /// The observations of a listed child that its listing already answers under `plan`, with
    /// no I/O: exactly what [`Self::observe`] would report from the same attributes, failures
    /// included. `None` when answering takes a call, which is the default; a traversal then
    /// calls [`Self::observe`] instead.
    fn observe_listed(
        &self,
        descriptor: &SourceDescriptor,
        plan: ObservationPlan,
    ) -> Option<Result<MetadataObservations, StorageRoleFailure>> {
        let _ = (descriptor, plan);
        None
    }
    /// Observes metadata only when the same operation confirms the expected source identity.
    /// Roles that advertise copied metadata should override this method.
    async fn observe_bound(
        &self,
        path: &StoragePath,
        expected: &SourceIdentity,
        plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        let _ = expected;
        self.observe(path, plan).await
    }
    /// Observes baseline copy facts together, bound to the described source version.
    async fn observe_copy_bound(
        &self,
        path: &StoragePath,
        expected: &SourceIdentity,
        plan: ObservationPlan,
    ) -> Result<CopiedMetadataObservation, StorageRoleFailure> {
        Ok(CopiedMetadataObservation {
            observations: self.observe_bound(path, expected, plan).await?,
            mode_without_ownership: None,
            owner_names_unmapped: false,
        })
    }
    /// [`Metadata::observe_copy_bound`] for one pinned source version. The default serves stores
    /// without versions: `Current` observes as usual, and `Id` is `Unsupported`.
    async fn observe_copy_bound_version(
        &self,
        path: &StoragePath,
        expected: &SourceIdentity,
        version: &SourceVersion,
        plan: ObservationPlan,
    ) -> Result<CopiedMetadataObservation, StorageRoleFailure> {
        match version {
            SourceVersion::Current => self.observe_copy_bound(path, expected, plan).await,
            SourceVersion::Id(_) => Err(version_unsupported(path, Operation::Metadata)),
        }
    }
    async fn apply(
        &self,
        path: &StoragePath,
        mutation: MetadataMutation,
        cancel: CancellationToken,
    ) -> Result<(), StorageRoleFailure>;
}

fn permission_facts(
    path: &StoragePath,
    observations: &MetadataObservations,
    mode_without_ownership: Option<u32>,
) -> Result<PermissionFacts, StorageRoleFailure> {
    match observations.ownership_mode() {
        MetadataObservation::Failed { class, transience } => Err(StorageRoleFailure::Entry(
            EntryOperationFailure::new(
                path.clone(),
                Operation::Observe,
                *class,
                *transience,
                "permission observation failed",
            )
            .unwrap_or_else(|_| unreachable!("static diagnostic")),
        )),
        MetadataObservation::Value { value, .. } => Ok(PermissionFacts {
            owner: Some(PrincipalIdentity::Numeric(value.uid)),
            group: Some(PrincipalIdentity::Numeric(value.gid)),
            mode: Some(value.mode & 0o7777),
        }),
        _ => Ok(PermissionFacts {
            mode: mode_without_ownership,
            ..PermissionFacts::default()
        }),
    }
}

/// One backend-neutral metadata mutation compiled before target side effects.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetadataMutation {
    Acl(AclMetadata),
    Xattrs(Vec<ExtendedAttribute>),
    Tags(Vec<ObjectTag>),
    NumericOwnership(OwnershipMode),
    MappedOwnership(MappedOwnership),
    Mode(u32),
    Timestamps(TimestampMetadata),
}
#[cfg(test)]
#[path = "metadata_tests.rs"]
mod tests;
