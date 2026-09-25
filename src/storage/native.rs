use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use super::{DestinationPrepareRequest, PreparedStage, StorageRoleFailure, WriteEvidence};

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) struct NativeAffinity([u8; 32]);

impl NativeAffinity {
    pub(crate) fn derive(facts: &[&[u8]]) -> Self {
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

pub(crate) struct NativeSourceBinding {
    pub affinity: NativeAffinity,
    pub token: Bytes,
    pub size: u64,
}

pub(crate) struct NativeStageEvidence {
    pub write: WriteEvidence,
    pub native_bytes: u64,
    pub native_requests: u64,
}

pub(crate) struct NativeStageFailure {
    pub error: StorageRoleFailure,
    pub native_bytes: u64,
    pub native_requests: u64,
}

#[async_trait]
pub(crate) trait NativeEndpoint: Send + Sync {
    fn affinity(&self) -> NativeAffinity;

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

    /// Copies `source` into `stage`, with at most `operations` copy requests in flight (the
    /// transfer's `InflightLimits` operation bound).
    async fn copy_into_stage(
        &self,
        source: NativeSourceBinding,
        stage: &PreparedStage,
        cancel: CancellationToken,
        operations: usize,
    ) -> Result<NativeStageEvidence, NativeStageFailure>;
}

pub(crate) struct NativePair {
    source: Arc<dyn NativeEndpoint>,
    destination: Arc<dyn NativeEndpoint>,
}

impl NativePair {
    pub(crate) fn new(
        source: Arc<dyn NativeEndpoint>,
        destination: Arc<dyn NativeEndpoint>,
    ) -> Option<Self> {
        (source.affinity() == destination.affinity()).then_some(Self {
            source,
            destination,
        })
    }

    pub(crate) async fn bind_source(
        &self,
        source: &super::SourceDescriptor,
    ) -> Result<NativeSourceBinding, StorageRoleFailure> {
        self.source.bind_source(source).await
    }

    /// The destination's [`NativeEndpoint::prepare_native`].
    pub(crate) async fn prepare_native(
        &self,
        request: DestinationPrepareRequest,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        self.destination.prepare_native(request).await
    }

    pub(crate) async fn copy_into_stage(
        &self,
        source: NativeSourceBinding,
        stage: &PreparedStage,
        cancel: CancellationToken,
        operations: usize,
    ) -> Result<NativeStageEvidence, NativeStageFailure> {
        self.destination
            .copy_into_stage(source, stage, cancel, operations)
            .await
    }
}
