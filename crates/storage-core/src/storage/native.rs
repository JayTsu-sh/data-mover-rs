use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use super::{DestinationPrepareRequest, PreparedStage, StorageRoleFailure, WriteEvidence};

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct NativeAffinity([u8; 32]);

impl NativeAffinity {
    #[must_use]
    pub fn derive(facts: &[&[u8]]) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"data-mover/native-affinity/v1\0");
        for fact in facts {
            hasher.update(&(fact.len() as u64).to_le_bytes());
            hasher.update(fact);
        }
        Self(*hasher.finalize().as_bytes())
    }
}

impl fmt::Debug for NativeAffinity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("NativeAffinity(<opaque>)")
    }
}

pub struct NativeSourceBinding {
    pub affinity: NativeAffinity,
    pub token: Bytes,
    pub size: u64,
}

pub struct NativeStageEvidence {
    pub write: WriteEvidence,
    pub native_bytes: u64,
    pub native_requests: u64,
    /// Whether the copy is one server-side operation sent at publication (S3: one `CopyObject`
    /// up to 64 MiB): no intermediate state it could recover from, whatever the policy.
    pub single_operation: bool,
}

pub struct NativeStageFailure {
    pub error: StorageRoleFailure,
    pub native_bytes: u64,
    pub native_requests: u64,
}

#[async_trait]
pub trait NativeEndpoint: Send + Sync {
    fn affinity(&self) -> NativeAffinity;

    /// Whether this exact source is prepared as a resumable multipart operation.
    fn supports_hash_checkpoint(&self, _source: &super::SourceDescriptor) -> bool {
        false
    }

    async fn bind_source(
        &self,
        source: &super::SourceDescriptor,
    ) -> Result<NativeSourceBinding, StorageRoleFailure>;

    /// Prepares the stage this endpoint's native copy fills, as a destination that keeps its
    /// recovery state beside the final file (ADR-0006 C18): what is found there is resumed or
    /// cleaned up, and the stage reports that fact.
    async fn prepare_native(
        &self,
        request: DestinationPrepareRequest,
    ) -> Result<PreparedStage, StorageRoleFailure>;

    /// Copies `source` into `stage` using the native backend's own bounded request window.
    async fn copy_into_stage(
        &self,
        source: NativeSourceBinding,
        stage: &PreparedStage,
        cancel: CancellationToken,
    ) -> Result<NativeStageEvidence, NativeStageFailure>;
}

pub struct NativePair {
    source: Arc<dyn NativeEndpoint>,
    destination: Arc<dyn NativeEndpoint>,
}

impl NativePair {
    pub fn supports_hash_checkpoint(&self, source: &super::SourceDescriptor) -> bool {
        self.destination.supports_hash_checkpoint(source)
    }
    pub fn new(
        source: Arc<dyn NativeEndpoint>,
        destination: Arc<dyn NativeEndpoint>,
    ) -> Option<Self> {
        (source.affinity() == destination.affinity()).then_some(Self {
            source,
            destination,
        })
    }

    ///
    /// # Errors
    /// Propagates the source adapter's identity or access failure.
    pub async fn bind_source(
        &self,
        source: &super::SourceDescriptor,
    ) -> Result<NativeSourceBinding, StorageRoleFailure> {
        self.source.bind_source(source).await
    }

    /// The destination's [`NativeEndpoint::prepare_native`].
    ///
    /// # Errors
    /// Propagates the destination adapter's preparation or recovery failure.
    pub async fn prepare_native(
        &self,
        request: DestinationPrepareRequest,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        self.destination.prepare_native(request).await
    }

    ///
    /// # Errors
    /// Returns the destination failure together with the native progress already made.
    pub async fn copy_into_stage(
        &self,
        source: NativeSourceBinding,
        stage: &PreparedStage,
        cancel: CancellationToken,
    ) -> Result<NativeStageEvidence, NativeStageFailure> {
        self.destination
            .copy_into_stage(source, stage, cancel)
            .await
    }
}
