use std::sync::Arc;
use std::{error::Error, fmt};

use crate::model::{BackendIdentity, BackendKind};

use super::{
    BackendCapabilities, Capability, CapabilityUnavailable, Metadata, Namespace, PreflightPolicy,
    ReadSource, SourceDescriptor, StagedDestination,
};

#[derive(Clone, Default)]
struct BackendRoles {
    read_source: Option<Arc<dyn ReadSource>>,
    staged_destination: Option<Arc<dyn StagedDestination>>,
    namespace: Option<Arc<dyn Namespace>>,
    metadata: Option<Arc<dyn Metadata>>,
}

#[derive(Clone)]
#[allow(dead_code)]
enum ConnectedBackend {
    Local(BackendRoles),
    Nfs(BackendRoles),
    Cifs(BackendRoles),
    S3(BackendRoles),
    Hdfs(BackendRoles),
}

impl ConnectedBackend {
    fn roles(&self) -> &BackendRoles {
        match self {
            Self::Local(roles)
            | Self::Nfs(roles)
            | Self::Cifs(roles)
            | Self::S3(roles)
            | Self::Hdfs(roles) => roles,
        }
    }
}

/// Thin connected storage handle. All operations are lent through deep roles.
#[derive(Clone)]
pub struct Storage {
    identity: BackendIdentity,
    capabilities: BackendCapabilities,
    backend: ConnectedBackend,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StorageBuildError {
    capability: Capability,
}

impl fmt::Display for StorageBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "capability {:?} contradicts connected role",
            self.capability
        )
    }
}

impl Error for StorageBuildError {}

impl Storage {
    #[allow(dead_code)]
    pub(crate) fn connected(
        identity: BackendIdentity,
        capabilities: BackendCapabilities,
        read_source: Option<Arc<dyn ReadSource>>,
        staged_destination: Option<Arc<dyn StagedDestination>>,
        namespace: Option<Arc<dyn Namespace>>,
        metadata: Option<Arc<dyn Metadata>>,
    ) -> Result<Self, StorageBuildError> {
        let roles = BackendRoles {
            read_source,
            staged_destination,
            namespace,
            metadata,
        };
        for (capability, present) in [
            (Capability::ReadSource, roles.read_source.is_some()),
            (
                Capability::StagedDestination,
                roles.staged_destination.is_some(),
            ),
            (Capability::Namespace, roles.namespace.is_some()),
            (Capability::Metadata, roles.metadata.is_some()),
        ] {
            let declared = !matches!(
                capabilities.availability(capability),
                super::CapabilityAvailability::Unsupported(_)
            );
            if declared != present {
                return Err(StorageBuildError { capability });
            }
        }
        let backend = match identity.kind() {
            BackendKind::Local => ConnectedBackend::Local(roles),
            BackendKind::Nfs => ConnectedBackend::Nfs(roles),
            BackendKind::Cifs => ConnectedBackend::Cifs(roles),
            BackendKind::S3 => ConnectedBackend::S3(roles),
            BackendKind::Hdfs => ConnectedBackend::Hdfs(roles),
        };
        Ok(Self {
            identity,
            capabilities,
            backend,
        })
    }

    /// Returns the closed backend kind.
    #[must_use]
    pub const fn kind(&self) -> BackendKind {
        self.identity.kind()
    }
    /// Returns the connected backend identity.
    #[must_use]
    pub const fn identity(&self) -> &BackendIdentity {
        &self.identity
    }
    /// Returns immutable instance-specific capabilities.
    #[must_use]
    pub const fn capabilities(&self) -> &BackendCapabilities {
        &self.capabilities
    }

    /// Lends the source role after typed preflight.
    ///
    /// # Errors
    /// Returns a typed preflight refusal before the role can perform backend I/O.
    pub fn read_source(
        &self,
        policy: &PreflightPolicy,
    ) -> Result<Arc<dyn ReadSource>, CapabilityUnavailable> {
        self.capabilities
            .preflight(Capability::ReadSource, policy)?;
        let role = &self.backend.roles().read_source;
        role.as_ref()
            .map(Arc::clone)
            .ok_or_else(|| CapabilityUnavailable::missing_role(Capability::ReadSource))
    }

    /// Lends the staged destination role after typed preflight.
    ///
    /// # Errors
    /// Returns a typed preflight refusal before the role can perform backend I/O.
    pub fn staged_destination(
        &self,
        policy: &PreflightPolicy,
    ) -> Result<Arc<dyn StagedDestination>, CapabilityUnavailable> {
        self.capabilities
            .preflight(Capability::StagedDestination, policy)?;
        let role = &self.backend.roles().staged_destination;
        role.as_ref()
            .map(Arc::clone)
            .ok_or_else(|| CapabilityUnavailable::missing_role(Capability::StagedDestination))
    }

    /// Lends the namespace role after typed preflight.
    ///
    /// # Errors
    /// Returns a typed preflight refusal before the role can perform backend I/O.
    pub fn namespace(
        &self,
        policy: &PreflightPolicy,
    ) -> Result<Arc<dyn Namespace>, CapabilityUnavailable> {
        self.capabilities.preflight(Capability::Namespace, policy)?;
        let role = &self.backend.roles().namespace;
        role.as_ref()
            .map(Arc::clone)
            .ok_or_else(|| CapabilityUnavailable::missing_role(Capability::Namespace))
    }

    /// Lends the metadata role after typed preflight.
    ///
    /// # Errors
    /// Returns a typed preflight refusal before the role can perform backend I/O.
    pub fn metadata(
        &self,
        policy: &PreflightPolicy,
    ) -> Result<Arc<dyn Metadata>, CapabilityUnavailable> {
        self.capabilities.preflight(Capability::Metadata, policy)?;
        let role = &self.backend.roles().metadata;
        role.as_ref()
            .map(Arc::clone)
            .ok_or_else(|| CapabilityUnavailable::missing_role(Capability::Metadata))
    }
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GuaranteeRequirement {
    Required,
    NotRequired,
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RequiredGuarantees {
    pub independent_verification: GuaranteeRequirement,
    pub preserve_metadata: GuaranteeRequirement,
    pub client_shaped_qos: GuaranteeRequirement,
    pub recoverable: GuaranteeRequirement,
    pub atomic_publication: GuaranteeRequirement,
}

#[allow(dead_code)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum NativePlan {
    Planned { native_requests: u32 },
    NotApplicable,
    Rejected { reason: &'static str },
}

#[allow(dead_code)]
pub(crate) trait NativeTransfer: Send + Sync {
    fn plan(
        &self,
        source: &SourceDescriptor,
        destination: &Storage,
        required: RequiredGuarantees,
    ) -> NativePlan;
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use super::*;
    use crate::storage::{
        ByteStream, CapabilityAvailability, ReadRequest, StorageRoleFailure, UnsupportedReason,
    };

    struct DummyRead;

    struct GuaranteeAwareNativePlanner;

    impl NativeTransfer for GuaranteeAwareNativePlanner {
        fn plan(
            &self,
            _source: &SourceDescriptor,
            _destination: &Storage,
            required: RequiredGuarantees,
        ) -> NativePlan {
            if required.client_shaped_qos == GuaranteeRequirement::Required {
                NativePlan::Rejected {
                    reason: "native path cannot preserve client-shaped QoS",
                }
            } else {
                NativePlan::Planned { native_requests: 1 }
            }
        }
    }

    #[async_trait]
    impl ReadSource for DummyRead {
        async fn describe(
            &self,
            _path: &crate::model::StoragePath,
        ) -> Result<SourceDescriptor, StorageRoleFailure> {
            unreachable!("role lending test does not perform I/O")
        }

        async fn read(&self, _request: ReadRequest) -> Result<ByteStream, StorageRoleFailure> {
            Ok(Box::pin(futures::stream::empty()))
        }
    }

    #[test]
    fn every_backend_lends_only_after_instance_preflight() -> Result<(), Box<dyn std::error::Error>>
    {
        for kind in [
            BackendKind::Local,
            BackendKind::Nfs,
            BackendKind::Cifs,
            BackendKind::S3,
            BackendKind::Hdfs,
        ] {
            let unavailable =
                CapabilityAvailability::Unsupported(UnsupportedReason::new("not supplied")?);
            let capabilities = BackendCapabilities::new(
                CapabilityAvailability::Supported,
                unavailable.clone(),
                unavailable.clone(),
                unavailable,
            );
            let storage = Storage::connected(
                BackendIdentity::new(kind, format!("test-{kind}"))?,
                capabilities,
                Some(Arc::new(DummyRead)),
                None,
                None,
                None,
            )?;
            assert_eq!(storage.kind(), kind);
            assert!(storage.read_source(&PreflightPolicy::production()).is_ok());
            assert!(
                storage
                    .staged_destination(&PreflightPolicy::production())
                    .is_err()
            );
        }
        Ok(())
    }

    #[test]
    fn connected_storage_rejects_capability_role_contradictions()
    -> Result<(), Box<dyn std::error::Error>> {
        let unsupported =
            CapabilityAvailability::Unsupported(UnsupportedReason::new("not supplied")?);
        let capabilities = BackendCapabilities::new(
            CapabilityAvailability::Supported,
            unsupported.clone(),
            unsupported.clone(),
            unsupported,
        );

        let result = Storage::connected(
            BackendIdentity::new(BackendKind::Local, "contradiction")?,
            capabilities,
            None,
            None,
            None,
            None,
        );
        let Err(error) = result else {
            return Err("supported source capability without a role was accepted".into());
        };

        assert_eq!(error.capability, Capability::ReadSource);
        Ok(())
    }

    #[test]
    fn native_planner_must_account_for_required_guarantees()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::model::{EntryKind, IdentityStrength, SourceIdentity, StoragePath};

        let unsupported =
            CapabilityAvailability::Unsupported(UnsupportedReason::new("not supplied")?);
        let destination = Storage::connected(
            BackendIdentity::new(BackendKind::S3, "destination")?,
            BackendCapabilities::new(
                CapabilityAvailability::Supported,
                unsupported.clone(),
                unsupported.clone(),
                unsupported,
            ),
            Some(Arc::new(DummyRead)),
            None,
            None,
            None,
        )?;
        let source_backend = BackendIdentity::new(BackendKind::S3, "source")?;
        let source = SourceDescriptor {
            path: StoragePath::new("bucket/key")?,
            kind: EntryKind::File,
            size: Some(1),
            source_identity: SourceIdentity::new(
                source_backend,
                IdentityStrength::StableWithinBackend,
                b"etag",
            )?,
        };
        let baseline = RequiredGuarantees {
            independent_verification: GuaranteeRequirement::Required,
            preserve_metadata: GuaranteeRequirement::Required,
            client_shaped_qos: GuaranteeRequirement::NotRequired,
            recoverable: GuaranteeRequirement::Required,
            atomic_publication: GuaranteeRequirement::Required,
        };

        assert_eq!(
            GuaranteeAwareNativePlanner.plan(&source, &destination, baseline),
            NativePlan::Planned { native_requests: 1 }
        );
        assert!(matches!(
            GuaranteeAwareNativePlanner.plan(
                &source,
                &destination,
                RequiredGuarantees {
                    client_shaped_qos: GuaranteeRequirement::Required,
                    ..baseline
                }
            ),
            NativePlan::Rejected { .. }
        ));
        Ok(())
    }
}
