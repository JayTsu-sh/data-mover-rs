use super::*;
use crate::model::{
    BackendIdentity, BackendKind, EntryKind, IdentityStrength, MetadataObservation,
    MetadataProvenance, ObservationMode,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

/// A store whose listed/current permissions differ from its immutable version.
struct VersionedMetadata {
    mode_only: bool,
    listed: bool,
}

fn ownership(mode: u32) -> MetadataObservations {
    MetadataObservations::new(
        MetadataObservation::NotRequested,
        MetadataObservation::NotRequested,
        MetadataObservation::NotRequested,
        MetadataObservation::Value {
            value: OwnershipMode {
                uid: 1001,
                gid: 1002,
                mode,
            },
            provenance: MetadataProvenance::Inline,
        },
        MetadataObservation::NotRequested,
    )
    .unwrap_or_else(|error| panic!("{error}"))
}

fn descriptor(version: SourceVersion) -> TestResult<SourceDescriptor> {
    let backend = BackendIdentity::new(BackendKind::S3, "permission-versions")?;
    let mut descriptor = SourceDescriptor::new(
        StoragePath::new("file")?,
        EntryKind::File,
        Some(4),
        SourceIdentity::new(backend, IdentityStrength::VersionScoped, b"v1")?,
    );
    descriptor.version = version;
    Ok(descriptor)
}

#[async_trait]
impl Metadata for VersionedMetadata {
    fn observe_listed(
        &self,
        _descriptor: &SourceDescriptor,
        _plan: ObservationPlan,
    ) -> Option<Result<MetadataObservations, StorageRoleFailure>> {
        self.listed.then(|| Ok(ownership(0o600)))
    }

    async fn observe(
        &self,
        _path: &StoragePath,
        _plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        panic!("listed/current metadata must not be refetched")
    }

    async fn observe_bound(
        &self,
        _path: &StoragePath,
        _expected: &SourceIdentity,
        _plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        Ok(ownership(0o660))
    }

    async fn observe_copy_bound_version(
        &self,
        path: &StoragePath,
        expected: &SourceIdentity,
        version: &SourceVersion,
        plan: ObservationPlan,
    ) -> Result<CopiedMetadataObservation, StorageRoleFailure> {
        let described =
            descriptor(SourceVersion::Id("v1".into())).unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(path, &described.path);
        assert_eq!(expected, &described.source_identity);
        assert_eq!(version, described.version());
        assert_eq!(plan.ownership_mode(), ObservationMode::BestEffort);
        Ok(CopiedMetadataObservation {
            observations: if self.mode_only {
                MetadataObservations::default()
            } else {
                ownership(0o640)
            },
            mode_without_ownership: self.mode_only.then_some(0o640),
            owner_names_unmapped: self.mode_only,
        })
    }

    async fn apply(
        &self,
        _path: &StoragePath,
        _mutation: MetadataMutation,
        _cancel: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        panic!("permission observation never mutates storage")
    }
}

#[tokio::test]
async fn default_permissions_bind_named_versions_and_preserve_mode_without_ownership() -> TestResult
{
    let described = descriptor(SourceVersion::Id("v1".into()))?;
    for mode_only in [false, true] {
        let facts = VersionedMetadata {
            mode_only,
            listed: true,
        }
        .permissions(&described)
        .await?;
        assert_eq!(facts.mode, Some(0o640));
        assert_eq!(
            facts.owner,
            (!mode_only).then_some(PrincipalIdentity::Numeric(1001))
        );
        assert_eq!(
            facts.group,
            (!mode_only).then_some(PrincipalIdentity::Numeric(1002))
        );
    }
    Ok(())
}

#[tokio::test]
async fn current_permissions_keep_listed_observations_without_refetching() -> TestResult {
    let described = descriptor(SourceVersion::Current)?;
    let facts = VersionedMetadata {
        mode_only: false,
        listed: true,
    }
    .permissions(&described)
    .await?;
    assert_eq!(facts.mode, Some(0o600));
    Ok(())
}

#[tokio::test]
async fn current_permissions_keep_the_existing_bound_fallback() -> TestResult {
    let described = descriptor(SourceVersion::Current)?;
    let facts = VersionedMetadata {
        mode_only: false,
        listed: false,
    }
    .permissions(&described)
    .await?;
    assert_eq!(facts.mode, Some(0o660));
    Ok(())
}
