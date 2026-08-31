use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream;

use super::staged::{CifsStageFile, CifsStagedDestination, CifsStagedProtocol};
use crate::model::{
    BackendIdentity, BackendKind, EntryKind, EntryOperationFailure, FailureClass, IdentityStrength,
    Operation, SourceIdentity, StoragePath, Transience,
};
use crate::storage::{
    ByteStream, ExistingDestinationPolicy, FinalDestination, PrepareRequest, PublishRequest,
    RecoverRequest, SourceDescriptor, StagedDestination, StorageRoleFailure, VerifyRequest,
};

#[derive(Default)]
struct MemoryProtocol {
    files: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    flushes: Arc<AtomicUsize>,
    closes: Arc<AtomicUsize>,
    writes: Arc<Mutex<Vec<(u64, usize)>>>,
    fail_rename_after_commit: AtomicBool,
}

struct MemoryFile {
    path: String,
    files: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    flushes: Arc<AtomicUsize>,
    closes: Arc<AtomicUsize>,
    writes: Arc<Mutex<Vec<(u64, usize)>>>,
}

#[async_trait]
impl CifsStageFile for MemoryFile {
    fn maximum_read_chunk(&self) -> u32 {
        4
    }

    fn maximum_write_chunk(&self) -> u32 {
        4
    }

    async fn read_at(&self, offset: u64, count: u32) -> smb_domain::Result<Bytes> {
        let files = self
            .files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let value = files
            .get(&self.path)
            .ok_or_else(|| smb_domain::Error::NotFound(self.path.clone()))?;
        let start = usize::try_from(offset)?;
        let end = start
            .checked_add(count as usize)
            .ok_or_else(|| smb_domain::Error::InvalidArgument("read range overflow".into()))?;
        Ok(Bytes::copy_from_slice(value.get(start..end).ok_or_else(
            || smb_domain::Error::InvalidMessage("short staged read".into()),
        )?))
    }

    async fn write_all_at(&self, offset: u64, bytes: Bytes) -> smb_domain::Result<()> {
        self.writes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((offset, bytes.len()));
        let mut files = self
            .files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let value = files
            .get_mut(&self.path)
            .ok_or_else(|| smb_domain::Error::NotFound(self.path.clone()))?;
        let start = usize::try_from(offset)?;
        let end = start
            .checked_add(bytes.len())
            .ok_or_else(|| smb_domain::Error::InvalidArgument("write range overflow".into()))?;
        value.resize(end, 0);
        value[start..end].copy_from_slice(&bytes);
        Ok(())
    }

    async fn flush(&self) -> smb_domain::Result<()> {
        self.flushes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn close(self: Box<Self>) -> smb_domain::Result<()> {
        self.closes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl CifsStagedProtocol for MemoryProtocol {
    async fn create_empty(&self, path: &StoragePath) -> smb_domain::Result<()> {
        let mut files = self
            .files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if files.insert(path.as_str().into(), Vec::new()).is_some() {
            return Err(smb_domain::Error::InvalidState(
                "stage already exists".into(),
            ));
        }
        Ok(())
    }

    async fn open(&self, path: &StoragePath) -> smb_domain::Result<Box<dyn CifsStageFile>> {
        if !self
            .files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(path.as_str())
        {
            return Err(smb_domain::Error::NotFound(path.as_str().into()));
        }
        Ok(Box::new(MemoryFile {
            path: path.as_str().into(),
            files: Arc::clone(&self.files),
            flushes: Arc::clone(&self.flushes),
            closes: Arc::clone(&self.closes),
            writes: Arc::clone(&self.writes),
        }))
    }

    async fn size(&self, path: &StoragePath) -> smb_domain::Result<u64> {
        self.files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(path.as_str())
            .map(|value| value.len() as u64)
            .ok_or_else(|| smb_domain::Error::NotFound(path.as_str().into()))
    }

    async fn rename(
        &self,
        from: &StoragePath,
        to: &StoragePath,
        replace: bool,
    ) -> smb_domain::Result<()> {
        let mut files = self
            .files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !replace && files.contains_key(to.as_str()) {
            return Err(smb_domain::Error::InvalidState("destination exists".into()));
        }
        let value = files
            .remove(from.as_str())
            .ok_or_else(|| smb_domain::Error::NotFound(from.as_str().into()))?;
        files.insert(to.as_str().into(), value);
        if self.fail_rename_after_commit.load(Ordering::SeqCst) {
            return Err(smb_domain::Error::InvalidMessage(
                "scripted lost rename response".into(),
            ));
        }
        Ok(())
    }

    async fn delete(&self, path: &StoragePath) -> smb_domain::Result<()> {
        self.files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(path.as_str())
            .map(|_| ())
            .ok_or_else(|| smb_domain::Error::NotFound(path.as_str().into()))
    }
}

fn identity() -> Result<BackendIdentity, Box<dyn std::error::Error>> {
    Ok(BackendIdentity::new(BackendKind::Cifs, "test-share")?)
}

fn prepare_request(
    identity: &BackendIdentity,
) -> Result<PrepareRequest, Box<dyn std::error::Error>> {
    Ok(PrepareRequest {
        final_destination: FinalDestination::new(StoragePath::new("final.bin")?),
        source: SourceDescriptor::new(
            StoragePath::new("source.bin")?,
            EntryKind::File,
            Some(6),
            SourceIdentity::new(identity.clone(), IdentityStrength::PathScoped, b"source-v1")?,
        ),
        recovery_binding: [7; 32],
    })
}

fn input(parts: &'static [&'static [u8]]) -> ByteStream {
    Box::pin(stream::iter(
        parts.iter().map(|part| Ok(Bytes::from_static(part))),
    ))
}

#[tokio::test]
async fn staged_lifecycle_flushes_verifies_and_atomically_publishes()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryProtocol::default());
    let identity = identity()?;
    let destination = CifsStagedDestination::new(Arc::clone(&protocol), identity.clone());
    let stage = destination.prepare(prepare_request(&identity)?).await?;
    let written = destination.write(&stage, input(&[b"abcdef"])).await?;
    assert_eq!(written.persisted_bytes, 6);
    assert_eq!(
        *protocol
            .writes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        vec![(0, 4), (4, 2)]
    );
    assert_eq!(protocol.flushes.load(Ordering::SeqCst), 1);
    assert_eq!(
        destination.observe_checkpoint(&stage).await?.durable_prefix,
        6
    );
    let hash = *blake3::hash(b"abcdef").as_bytes();
    assert_eq!(
        destination
            .verify(
                &stage,
                VerifyRequest {
                    expected_size: 6,
                    expected_blake3: hash,
                    cancel: tokio_util::sync::CancellationToken::new(),
                },
            )
            .await?
            .blake3,
        hash
    );
    destination
        .publish(
            &stage,
            PublishRequest {
                policy: ExistingDestinationPolicy::Overwrite,
                expected_size: 6,
                expected_blake3: hash,
                cancel: tokio_util::sync::CancellationToken::new(),
            },
        )
        .await
        .map_err(|failure| failure.error)?;
    assert_eq!(
        protocol
            .files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get("final.bin"),
        Some(&b"abcdef".to_vec())
    );
    assert_eq!(protocol.closes.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn recovery_claim_reobserves_durable_prefix_before_resuming()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryProtocol::default());
    let identity = identity()?;
    let first = CifsStagedDestination::new(Arc::clone(&protocol), identity.clone());
    let request = prepare_request(&identity)?;
    let stage = first.prepare(request.clone()).await?;
    first.write(&stage, input(&[b"abc"])).await?;
    let recovery_identity = first.recovery_identity(&stage).await?;

    let resumed = CifsStagedDestination::new(Arc::clone(&protocol), identity);
    let stage = resumed
        .recover(RecoverRequest {
            identity: recovery_identity,
            final_destination: request.final_destination,
            source: request.source,
            recovery_binding: request.recovery_binding,
            claim_token: [9; 32],
        })
        .await?;
    assert_eq!(resumed.observe_checkpoint(&stage).await?.durable_prefix, 3);
    assert_eq!(
        resumed
            .write(&stage, input(&[b"def"]))
            .await?
            .persisted_bytes,
        6
    );
    Ok(())
}

#[tokio::test]
async fn repeated_recovery_reobserves_the_same_claim_after_a_lost_response()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryProtocol::default());
    let identity = identity()?;
    let owner = CifsStagedDestination::new(Arc::clone(&protocol), identity.clone());
    let prepare = prepare_request(&identity)?;
    let stage = owner.prepare(prepare.clone()).await?;
    owner.write(&stage, input(&[b"abc"])).await?;
    let recovery_identity = owner.recovery_identity(&stage).await?;
    let request = RecoverRequest {
        final_destination: stage.final_destination.clone(),
        source: prepare.source,
        identity: recovery_identity,
        recovery_binding: stage.recovery_binding,
        claim_token: [9; 32],
    };
    let first = CifsStagedDestination::new(Arc::clone(&protocol), identity.clone())
        .recover(request.clone())
        .await?;
    let repeated = CifsStagedDestination::new(protocol, identity)
        .recover(request)
        .await?;
    assert_eq!(first.token, repeated.token);
    assert_eq!(repeated.write_offset, 3);
    Ok(())
}

#[tokio::test]
async fn verify_or_skip_retains_equivalent_final_and_discards_stage()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryProtocol::default());
    protocol
        .files
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert("final.bin".into(), b"abcdef".to_vec());
    let identity = identity()?;
    let destination = CifsStagedDestination::new(Arc::clone(&protocol), identity.clone());
    let stage = destination.prepare(prepare_request(&identity)?).await?;
    destination.write(&stage, input(&[b"abcdef"])).await?;
    let hash = *blake3::hash(b"abcdef").as_bytes();
    let published = destination
        .publish(
            &stage,
            PublishRequest {
                policy: ExistingDestinationPolicy::VerifyOrSkip,
                expected_size: 6,
                expected_blake3: hash,
                cancel: tokio_util::sync::CancellationToken::new(),
            },
        )
        .await
        .map_err(|failure| failure.error)?;
    assert_eq!(
        published.disposition,
        crate::storage::PublicationDisposition::ExistingEquivalent
    );
    assert_eq!(
        protocol
            .files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn lost_rename_response_reconciles_committed_final_content()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryProtocol::default());
    let identity = identity()?;
    let destination = CifsStagedDestination::new(Arc::clone(&protocol), identity.clone());
    let stage = destination.prepare(prepare_request(&identity)?).await?;
    destination.write(&stage, input(&[b"abcdef"])).await?;
    protocol
        .fail_rename_after_commit
        .store(true, Ordering::SeqCst);
    let hash = *blake3::hash(b"abcdef").as_bytes();
    let published = destination
        .publish(
            &stage,
            PublishRequest {
                policy: ExistingDestinationPolicy::Overwrite,
                expected_size: 6,
                expected_blake3: hash,
                cancel: tokio_util::sync::CancellationToken::new(),
            },
        )
        .await
        .map_err(|failure| failure.error)?;
    assert_eq!(
        published.disposition,
        crate::storage::PublicationDisposition::Published
    );
    Ok(())
}

#[tokio::test]
async fn cancelled_publication_leaves_the_stage_and_final_unchanged()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryProtocol::default());
    let identity = identity()?;
    let destination = CifsStagedDestination::new(Arc::clone(&protocol), identity.clone());
    let stage = destination.prepare(prepare_request(&identity)?).await?;
    destination.write(&stage, input(&[b"abcdef"])).await?;
    let cancel = tokio_util::sync::CancellationToken::new();
    cancel.cancel();
    let result = destination
        .publish(
            &stage,
            PublishRequest {
                policy: ExistingDestinationPolicy::Overwrite,
                expected_size: 6,
                expected_blake3: *blake3::hash(b"abcdef").as_bytes(),
                cancel,
            },
        )
        .await;
    let Err(failure) = result else {
        return Err("cancelled publication unexpectedly succeeded".into());
    };
    assert!(!failure.final_destination_changed);
    assert!(
        !protocol
            .files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key("final.bin")
    );
    assert_eq!(
        destination.observe_checkpoint(&stage).await?.durable_prefix,
        6
    );
    Ok(())
}

#[tokio::test]
async fn cancelled_verification_fast_fails_and_preserves_the_checkpoint()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryProtocol::default());
    let identity = identity()?;
    let destination = CifsStagedDestination::new(Arc::clone(&protocol), identity.clone());
    let stage = destination.prepare(prepare_request(&identity)?).await?;
    destination.write(&stage, input(&[b"abcdef"])).await?;
    let cancel = tokio_util::sync::CancellationToken::new();
    cancel.cancel();
    let result = destination
        .verify(
            &stage,
            VerifyRequest {
                expected_size: 6,
                expected_blake3: *blake3::hash(b"abcdef").as_bytes(),
                cancel,
            },
        )
        .await;
    assert!(result.is_err());
    assert_eq!(
        destination.observe_checkpoint(&stage).await?.durable_prefix,
        6
    );
    Ok(())
}

#[tokio::test]
async fn cancelled_input_stops_inflight_write_without_flushing()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryProtocol::default());
    let identity = identity()?;
    let destination = CifsStagedDestination::new(Arc::clone(&protocol), identity.clone());
    let stage = destination.prepare(prepare_request(&identity)?).await?;
    let cancelled = StorageRoleFailure::Entry(EntryOperationFailure::new(
        StoragePath::new("source.bin")?,
        Operation::Read,
        FailureClass::Cancelled,
        Transience::Transient,
        "source cancelled",
    )?);
    let input: ByteStream = Box::pin(stream::iter(vec![
        Ok(Bytes::from_static(b"abcd")),
        Err(cancelled),
    ]));
    assert!(destination.write(&stage, input).await.is_err());
    assert_eq!(protocol.flushes.load(Ordering::SeqCst), 0);
    assert_eq!(protocol.closes.load(Ordering::SeqCst), 1);
    Ok(())
}
