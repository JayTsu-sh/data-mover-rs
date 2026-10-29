use std::sync::Arc;
use std::{error::Error, fmt};

use crate::model::{BackendIdentity, BackendKind};

use super::{
    BackendCapabilities, Capability, CapabilityUnavailable, Metadata, Namespace, NativeEndpoint,
    NativePair, ReadSource, StagedDestination,
};

#[derive(Clone, Default)]
struct BackendRoles {
    read_source: Option<Arc<dyn ReadSource>>,
    staged_destination: Option<Arc<dyn StagedDestination>>,
    namespace: Option<Arc<dyn Namespace>>,
    metadata: Option<Arc<dyn Metadata>>,
    native: Option<Arc<dyn NativeEndpoint>>,
}

/// Thin connected storage handle. All operations are lent through deep roles.
#[derive(Clone)]
pub struct Storage {
    identity: BackendIdentity,
    capabilities: BackendCapabilities,
    backend: BackendRoles,
    copy_object_tags: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StorageBuildError {
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

/// Assembles a storage adapter from independently supplied roles.
///
/// Capabilities describe the connected instance. `build` rejects declarations that do not
/// match the supplied roles, before any engine can perform I/O.
pub struct StorageBuilder {
    identity: BackendIdentity,
    capabilities: BackendCapabilities,
    roles: BackendRoles,
    copy_object_tags: bool,
}

impl StorageBuilder {
    #[must_use]
    pub fn new(identity: BackendIdentity, capabilities: BackendCapabilities) -> Self {
        Self {
            identity,
            capabilities,
            roles: BackendRoles::default(),
            copy_object_tags: false,
        }
    }
    /// Supplies the source role.
    #[must_use]
    pub fn read_source(mut self, role: Arc<dyn ReadSource>) -> Self {
        self.roles.read_source = Some(role);
        self
    }

    /// Supplies the destination role.
    #[must_use]
    pub fn staged_destination(mut self, role: Arc<dyn StagedDestination>) -> Self {
        self.roles.staged_destination = Some(role);
        self
    }

    /// Supplies the namespace role.
    #[must_use]
    pub fn namespace(mut self, role: Arc<dyn Namespace>) -> Self {
        self.roles.namespace = Some(role);
        self
    }

    /// Supplies the metadata role.
    #[must_use]
    pub fn metadata(mut self, role: Arc<dyn Metadata>) -> Self {
        self.roles.metadata = Some(role);
        self
    }

    /// Supplies the native transfer role.
    #[must_use]
    pub fn native(mut self, role: Arc<dyn NativeEndpoint>) -> Self {
        self.roles.native = Some(role);
        self
    }

    /// Opts into ordinary tag copying when the other endpoint also opts in.
    #[must_use]
    pub const fn copy_object_tags(mut self, enabled: bool) -> Self {
        self.copy_object_tags = enabled;
        self
    }

    /// Validates role declarations and constructs the handle without performing I/O.
    ///
    /// # Errors
    /// Returns the capability whose declared availability contradicts its supplied role.
    pub fn build(self) -> Result<Storage, StorageBuildError> {
        Storage::connected(
            self.identity,
            self.capabilities,
            self.roles.read_source,
            self.roles.staged_destination,
            self.roles.namespace,
            self.roles.metadata,
            self.roles.native,
        )
        .map(|storage| storage.with_object_tag_copy(self.copy_object_tags))
    }
}

impl StorageBuildError {
    /// The capability whose declaration contradicts the supplied roles.
    #[must_use]
    pub const fn capability(&self) -> Capability {
        self.capability
    }
}

impl Storage {
    /// Constructs an adapter from its connected roles.
    /// Prefer [`StorageBuilder`] when assembling an adapter outside this workspace.
    ///
    /// # Errors
    /// Returns an error if a capability declaration contradicts the supplied roles.
    #[doc(hidden)]
    pub fn connected(
        identity: BackendIdentity,
        capabilities: BackendCapabilities,
        read_source: Option<Arc<dyn ReadSource>>,
        staged_destination: Option<Arc<dyn StagedDestination>>,
        namespace: Option<Arc<dyn Namespace>>,
        metadata: Option<Arc<dyn Metadata>>,
        native: Option<Arc<dyn NativeEndpoint>>,
    ) -> Result<Self, StorageBuildError> {
        let roles = BackendRoles {
            read_source,
            staged_destination,
            namespace,
            metadata,
            native,
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
        Ok(Self {
            identity,
            capabilities,
            backend: roles,
            copy_object_tags: false,
        })
    }

    /// Select the adapter's ordinary object-tag copy policy.
    /// Both endpoints must opt in; metadata capabilities still determine availability.
    #[must_use]
    pub const fn with_object_tag_copy(mut self, enabled: bool) -> Self {
        self.copy_object_tags = enabled;
        self
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
    pub fn read_source(&self) -> Result<Arc<dyn ReadSource>, CapabilityUnavailable> {
        self.capabilities.preflight(Capability::ReadSource)?;
        let role = &self.backend.read_source;
        role.as_ref()
            .map(Arc::clone)
            .ok_or_else(|| CapabilityUnavailable::missing_role(Capability::ReadSource))
    }

    /// Lends the staged destination role after typed preflight.
    ///
    /// # Errors
    /// Returns a typed preflight refusal before the role can perform backend I/O.
    pub fn staged_destination(&self) -> Result<Arc<dyn StagedDestination>, CapabilityUnavailable> {
        self.capabilities.preflight(Capability::StagedDestination)?;
        let role = &self.backend.staged_destination;
        role.as_ref()
            .map(Arc::clone)
            .ok_or_else(|| CapabilityUnavailable::missing_role(Capability::StagedDestination))
    }

    /// Lends the namespace role after typed preflight.
    ///
    /// # Errors
    /// Returns a typed preflight refusal before the role can perform backend I/O.
    pub fn namespace(&self) -> Result<Arc<dyn Namespace>, CapabilityUnavailable> {
        self.capabilities.preflight(Capability::Namespace)?;
        let role = &self.backend.namespace;
        role.as_ref()
            .map(Arc::clone)
            .ok_or_else(|| CapabilityUnavailable::missing_role(Capability::Namespace))
    }

    /// Whether this storage keeps directory objects — files at keys ending in `/` (P7 F5) — as its
    /// namespace role reports ([`super::NamespaceCapabilities::directory_objects`]). A storage
    /// that lends no namespace holds none. No I/O.
    #[must_use]
    pub fn holds_directory_objects(&self) -> bool {
        self.namespace()
            .is_ok_and(|namespace| namespace.capabilities().directory_objects)
    }

    /// Lends the namespace after checking the requested mutation before any I/O.
    ///
    /// # Errors
    /// Returns a typed preflight refusal before the role can perform backend I/O.
    pub fn mutable_namespace(
        &self,
        capability: impl FnOnce(super::NamespaceCapabilities) -> Result<(), &'static str>,
    ) -> Result<Arc<dyn Namespace>, CapabilityUnavailable> {
        let namespace = self.namespace()?;
        match capability(namespace.capabilities()) {
            Err(reason) => Err(CapabilityUnavailable::unsupported(
                Capability::Namespace,
                reason,
            )),
            Ok(()) => Ok(namespace),
        }
    }

    /// Lends the metadata role after typed preflight.
    ///
    /// # Errors
    /// Returns a typed preflight refusal before the role can perform backend I/O.
    pub fn metadata(&self) -> Result<Arc<dyn Metadata>, CapabilityUnavailable> {
        self.capabilities.preflight(Capability::Metadata)?;
        let role = &self.backend.metadata;
        role.as_ref()
            .map(Arc::clone)
            .ok_or_else(|| CapabilityUnavailable::missing_role(Capability::Metadata))
    }

    #[must_use]
    pub fn native_pair(&self, destination: &Self) -> Option<NativePair> {
        NativePair::new(
            Arc::clone(self.backend.native.as_ref()?),
            Arc::clone(destination.backend.native.as_ref()?),
        )
    }

    /// Whether ordinary copy semantics carry object tags for this connected pair.
    #[must_use]
    pub const fn copies_tags_to(&self, destination: &Self) -> bool {
        self.copy_object_tags && destination.copy_object_tags
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use super::*;
    use crate::storage::{
        ByteStream, CapabilityAvailability, ReadRequest, SourceDescriptor, StorageRoleFailure,
        UnsupportedReason,
    };

    struct DummyRead;

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
                None,
            )?;
            assert_eq!(storage.kind(), kind);
            assert!(storage.read_source().is_ok());
            assert!(storage.staged_destination().is_err());
            assert!(storage.namespace().is_err());
            assert!(storage.metadata().is_err());
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
            None,
        );
        let Err(error) = result else {
            return Err("supported source capability without a role was accepted".into());
        };

        assert_eq!(error.capability, Capability::ReadSource);
        Ok(())
    }
}
