#![cfg(feature = "backend-local")]

use std::num::NonZeroUsize;
use std::path::Path;

use data_mover::model::observation::PrivateBackendEntryFacts;
use data_mover::model::{
    BackendIdentity, BackendKind, EntryKind, HistoryOrder, IdentityStrength, ObjectHistory,
    ObservationPlan, ObservedEntry, RecordKind, SourceIdentity, SourceVersion, StoragePath,
    VersionRecord,
};
use data_mover::storage::{
    BackendCapabilities, BackendConfig, CapabilityAvailability, EntryObservationFailure,
    LocalBackendConfig, SourceDescriptor, Storage, StorageBuilder, UnsupportedReason,
    connect_backend, observe_descriptor,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn local(root: &Path) -> TestResult<Storage> {
    Ok(connect_backend(BackendConfig::Local(LocalBackendConfig {
        root: root.to_owned(),
        rename_context: None,
        read_concurrency: NonZeroUsize::MIN,
        write_concurrency: NonZeroUsize::MIN,
        read_chunk_bytes: None,
        write_chunk_bytes: None,
    }))
    .await?)
}

#[tokio::test]
async fn current_local_descriptor_preserves_facts_and_shared_payload() -> TestResult {
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("file"), b"current observation")?;
    let storage = local(root.path()).await?;
    let descriptor = storage
        .namespace()?
        .stat(&StoragePath::new("file")?)
        .await?;
    let pointer = descriptor
        .backend_fact
        .as_ref()
        .ok_or("missing local facts")?
        .as_ptr();
    let identity = descriptor.source_identity.clone();
    let permissions = descriptor.permissions.clone();
    let observed = observe_descriptor(&storage, descriptor, ObservationPlan::default()).await?;
    assert_eq!(observed.kind(), EntryKind::File);
    assert_eq!(observed.size(), Some(19));
    assert_eq!(observed.source_identity(), &identity);
    assert_eq!(observed.permissions(), permissions.as_deref());
    let PrivateBackendEntryFacts::Local(fact) = observed.backend_facts() else {
        return Err("missing captured facts".into());
    };
    assert_eq!(fact.as_ptr(), pointer);
    assert_eq!(
        ObservedEntry::decode_snapshot(observed.encode_snapshot().as_bytes(), storage.identity())?,
        observed
    );
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn current_symlink_observation_retains_the_exact_target() -> TestResult {
    let root = tempfile::tempdir()?;
    std::os::unix::fs::symlink("missing-target", root.path().join("link"))?;
    let storage = local(root.path()).await?;
    let descriptor = storage
        .namespace()?
        .stat(&StoragePath::new("link")?)
        .await?;
    let observed = observe_descriptor(&storage, descriptor, ObservationPlan::default()).await?;
    assert_eq!(observed.kind(), EntryKind::Symlink);
    assert_eq!(
        observed
            .symlink_target()
            .ok_or("missing target")?
            .as_bytes(),
        b"missing-target"
    );
    assert_eq!(
        ObservedEntry::decode_snapshot(observed.encode_snapshot().as_bytes(), storage.identity())?,
        observed
    );
    Ok(())
}

#[tokio::test]
async fn version_and_listing_descriptors_are_rejected_before_requesting_roles() -> TestResult {
    let identity = BackendIdentity::new(BackendKind::S3, "unsupported-descriptor")?;
    let absent =
        CapabilityAvailability::Unsupported(UnsupportedReason::new("no roles in this test")?);
    let capabilities =
        BackendCapabilities::new(absent.clone(), absent.clone(), absent.clone(), absent);
    let storage = StorageBuilder::new(identity.clone(), capabilities).build()?;
    let source = SourceIdentity::new(identity, IdentityStrength::PathScoped, b"object")?;
    let descriptor = SourceDescriptor::new(
        StoragePath::new("file")?,
        EntryKind::File,
        Some(8),
        source.clone(),
    );
    let mut versioned = descriptor.clone();
    versioned.version = SourceVersion::Id("version-1".into());
    let mut listing_only = descriptor.clone();
    listing_only.listing.listing_only = true;
    let mut marker = descriptor.clone();
    marker.listing.directory_marker = Some(Box::new(descriptor.clone()));
    let record = VersionRecord::new(
        None,
        true,
        RecordKind::Object {
            size: 8,
            etag: String::new(),
            modified: None,
        },
        source,
        None,
    )?;
    let mut history = descriptor;
    history.listing.history = Some(Box::new(ObjectHistory::whole(
        vec![record],
        HistoryOrder::Server,
    )?));
    for value in [versioned, listing_only, marker, history] {
        assert!(matches!(
            observe_descriptor(&storage, value, ObservationPlan::default()).await,
            Err(EntryObservationFailure::UnsupportedDescriptor { .. })
        ));
    }
    Ok(())
}
