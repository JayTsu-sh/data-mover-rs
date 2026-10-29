//! Public comparison must honor a metadata role's independent permission facts.

use super::*;
use crate::model::{
    BackendIdentity, BackendKind, EntryKind, HistoryOrder, IdentityStrength, MetadataCapabilities,
    MetadataFamily, MetadataObservations, ObjectHistory, ObservationPlan, ObservationSupport,
    RecordKind, SourceIdentity, VersionRecord,
};
use crate::storage::{
    BackendCapabilities, ByteStream, CapabilityAvailability, Metadata, MetadataMutation,
    PermissionFacts, PrincipalIdentity, ReadRequest, ReadSource, UnsupportedReason,
};
use async_trait::async_trait;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

struct NamedVersion {
    descriptor: SourceDescriptor,
    owner: &'static str,
    ownership_support: ObservationSupport,
}

#[async_trait]
impl ReadSource for NamedVersion {
    async fn describe(&self, _path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        panic!("a pinned observation must not describe current")
    }

    async fn describe_version(
        &self,
        path: &StoragePath,
        version: &SourceVersion,
    ) -> Result<SourceDescriptor, StorageRoleFailure> {
        assert_eq!(path, &self.descriptor.path);
        assert_eq!(version, self.descriptor.version());
        Ok(self.descriptor.clone())
    }

    async fn read(&self, _request: ReadRequest) -> Result<ByteStream, StorageRoleFailure> {
        panic!("metadata comparison never reads content")
    }
}

#[async_trait]
impl Metadata for NamedVersion {
    fn observation_capabilities(&self) -> MetadataCapabilities {
        MetadataCapabilities::default().with(MetadataFamily::OwnershipMode, self.ownership_support)
    }

    async fn permissions(
        &self,
        descriptor: &SourceDescriptor,
    ) -> Result<PermissionFacts, StorageRoleFailure> {
        assert_eq!(descriptor, &self.descriptor);
        Ok(named_facts(self.owner))
    }

    async fn observe(
        &self,
        _path: &StoragePath,
        _plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        panic!("independent permissions must not be bypassed by metadata observation")
    }

    async fn apply(
        &self,
        _path: &StoragePath,
        _mutation: MetadataMutation,
        _cancel: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        panic!("comparison never applies metadata")
    }
}

fn named_facts(owner: &str) -> PermissionFacts {
    PermissionFacts {
        owner: Some(PrincipalIdentity::Named(owner.into())),
        group: Some(PrincipalIdentity::Named("staff".into())),
        mode: Some(0o640),
    }
}

fn pinned_descriptor(name: &str) -> TestResult<SourceDescriptor> {
    let identity = BackendIdentity::new(BackendKind::S3, name)?;
    let mut descriptor = SourceDescriptor::new(
        StoragePath::new("file")?,
        EntryKind::File,
        Some(4),
        SourceIdentity::new(identity, IdentityStrength::VersionScoped, b"v1")?,
    );
    descriptor.version = SourceVersion::Id("v1".into());
    descriptor = descriptor.with_inline_timestamps(crate::model::TimestampMetadata {
        accessed: None,
        modified: None,
        created: None,
    });
    Ok(descriptor)
}

fn observed(descriptor: &SourceDescriptor) -> TestResult<ObservedEntry> {
    let record = VersionRecord::new(
        Some("v1".into()),
        true,
        RecordKind::Object {
            size: 4,
            etag: String::new(),
            modified: None,
        },
        descriptor.source_identity.clone(),
        None,
    )?;
    Ok(ObservedEntry::new(
        descriptor.path.clone(),
        descriptor.kind,
        descriptor.size,
        None,
        descriptor.source_identity.clone(),
    )?
    .with_history(Some(Box::new(ObjectHistory::whole(
        vec![record],
        HistoryOrder::Server,
    )?))))
}

fn storage(name: &str, owner: &'static str, support: ObservationSupport) -> TestResult<Storage> {
    let descriptor = pinned_descriptor(name)?;
    let identity = descriptor.source_identity.backend().clone();
    let backend = Arc::new(NamedVersion {
        descriptor,
        owner,
        ownership_support: support,
    });
    let supported = CapabilityAvailability::Supported;
    let unsupported = CapabilityAvailability::Unsupported(UnsupportedReason::new("read only")?);
    Ok(Storage::connected(
        identity,
        BackendCapabilities::new(
            supported.clone(),
            unsupported.clone(),
            unsupported,
            supported,
        ),
        Some(backend.clone()),
        None,
        None,
        Some(backend),
        None,
    )?)
}

#[tokio::test]
async fn versioned_compare_uses_named_permission_overrides_independent_of_ownership_capability()
-> TestResult {
    for support in [
        ObservationSupport::Unsupported,
        ObservationSupport::Supported,
    ] {
        let report = compare(IntegrityRequest {
            source: storage("source", "alice", support)?,
            source_path: observed(&pinned_descriptor("source")?)?,
            destination: storage("destination", "bob", support)?,
            destination_path: observed(&pinned_descriptor("destination")?)?,
            plan: IntegrityPlan::default(),
            options: IntegrityOptions::default(),
            cancel: CancellationToken::new(),
        })
        .await?;
        assert_eq!(
            report.differences(),
            &[IntegrityDifference::Permissions {
                source: named_facts("alice"),
                destination: named_facts("bob"),
            }]
        );
        assert!(!report.ignored().contains(&IntegrityIgnored::Owner));
        assert!(!report.ignored().contains(&IntegrityIgnored::Group));
        assert!(!report.ignored().contains(&IntegrityIgnored::Mode));
    }
    Ok(())
}
