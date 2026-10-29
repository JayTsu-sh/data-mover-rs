//! A consumer adapter uses only the SDK-free public interface.
use async_trait::async_trait;
use bytes::Bytes;
use data_mover_model::{
    BackendIdentity, BackendKind, EntryKind, IdentityStrength, SourceIdentity, SourceVersion,
    StoragePath,
};
use data_mover_storage_core::storage::{
    BackendCapabilities, ByteStream, Capability, CapabilityAvailability, ReadRequest, ReadSource,
    SourceDescriptor, StorageBuilder, StorageRoleFailure, UnsupportedReason,
};
use futures::TryStreamExt;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

type TestResult = Result<(), Box<dyn std::error::Error>>;
struct MemorySource {
    identity: SourceIdentity,
}
#[async_trait]
impl ReadSource for MemorySource {
    async fn describe(&self, path: &StoragePath) -> Result<SourceDescriptor, StorageRoleFailure> {
        Ok(SourceDescriptor::new(
            path.clone(),
            EntryKind::File,
            Some(7),
            self.identity.clone(),
        ))
    }
    async fn read(&self, _: ReadRequest) -> Result<ByteStream, StorageRoleFailure> {
        Ok(Box::pin(futures::stream::once(async {
            Ok(Bytes::from_static(b"payload"))
        })))
    }
}
fn capabilities() -> Result<BackendCapabilities, Box<dyn std::error::Error>> {
    let absent =
        CapabilityAvailability::Unsupported(UnsupportedReason::new("read-only test adapter")?);
    Ok(BackendCapabilities::new(
        CapabilityAvailability::Supported,
        absent.clone(),
        absent.clone(),
        absent,
    ))
}
#[tokio::test]
async fn external_adapter_lends_roles_without_a_backend_sdk() -> TestResult {
    let identity = BackendIdentity::new(BackendKind::Local, "memory-contract")?;
    let source = MemorySource {
        identity: SourceIdentity::new(
            identity.clone(),
            IdentityStrength::StableWithinBackend,
            b"one",
        )?,
    };
    let storage = StorageBuilder::new(identity, capabilities()?)
        .read_source(Arc::new(source))
        .build()?;
    assert!(!storage.copies_tags_to(&storage));
    assert!(storage.staged_destination().is_err());
    let reader = storage.read_source()?;
    let path = StoragePath::new("file")?;
    let descriptor = reader.describe(&path).await?;
    let chunks: Vec<Bytes> = reader
        .read(ReadRequest {
            path,
            range: None,
            expected_source: Some(descriptor.source_identity),
            maximum_chunk_bytes: 1024,
            read_inflight: 1,
            read_budget: None,
            cancel: CancellationToken::new(),
            source_qos: None,
            version: SourceVersion::Current,
        })
        .await?
        .try_collect()
        .await?;
    assert_eq!(chunks.concat(), b"payload");
    Ok(())
}
#[test]
fn inconsistent_adapter_is_rejected_before_use() -> TestResult {
    let result = StorageBuilder::new(
        BackendIdentity::new(BackendKind::Local, "invalid")?,
        capabilities()?,
    )
    .build();
    let Err(error) = result else {
        return Err("accepted a missing role".into());
    };
    assert_eq!(error.capability(), Capability::ReadSource);
    Ok(())
}
