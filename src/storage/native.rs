use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use super::{PrepareRequest, PreparedStage, StorageRoleFailure, WriteEvidence};

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
    pub stage: PreparedStage,
    pub write: WriteEvidence,
    pub native_bytes: u64,
    pub native_requests: u64,
}

pub(crate) struct NativeStageFailure {
    pub error: StorageRoleFailure,
    pub stage: Option<PreparedStage>,
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

    async fn copy_to_stage(
        &self,
        source: NativeSourceBinding,
        request: PrepareRequest,
        cancel: CancellationToken,
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

    pub(crate) async fn copy_to_stage(
        &self,
        source: NativeSourceBinding,
        request: PrepareRequest,
        cancel: CancellationToken,
    ) -> Result<NativeStageEvidence, NativeStageFailure> {
        self.destination
            .copy_to_stage(source, request, cancel)
            .await
    }
}
