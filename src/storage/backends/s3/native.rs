use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::sync::CancellationToken;

use crate::model::{BackendIdentity, IdentityStrength, Operation, SourceIdentity};
use crate::storage::{
    NativeAffinity, NativeEndpoint, NativeRecoveryMode, NativeSourceBinding, NativeStageEvidence,
    NativeStageFailure, SourceDescriptor, StorageRoleFailure,
};

use super::source::{entry, role_failure};
use super::staged::S3StagedDestination;
use super::{S3NativeCopySource, S3Protocol};

#[derive(Clone)]
pub(crate) struct S3NativeContext {
    affinity: NativeAffinity,
    bucket: String,
    prefix: Option<String>,
}

impl S3NativeContext {
    pub(crate) fn new(
        endpoint: &str,
        compatibility: &str,
        bucket: String,
        prefix: Option<String>,
    ) -> Self {
        Self {
            affinity: NativeAffinity::derive(&[endpoint.as_bytes(), compatibility.as_bytes()]),
            bucket,
            prefix,
        }
    }

    fn full_key(&self, path: &str) -> String {
        self.prefix.as_ref().map_or_else(
            || path.to_string(),
            |prefix| format!("{}/{path}", prefix.trim_end_matches('/')),
        )
    }
}

pub(crate) struct S3NativeEndpoint<P> {
    protocol: Arc<P>,
    staged: Arc<S3StagedDestination<P>>,
    identity: BackendIdentity,
    context: S3NativeContext,
}

impl<P> S3NativeEndpoint<P> {
    pub(crate) const fn new(
        protocol: Arc<P>,
        staged: Arc<S3StagedDestination<P>>,
        identity: BackendIdentity,
        context: S3NativeContext,
    ) -> Self {
        Self {
            protocol,
            staged,
            identity,
            context,
        }
    }
}

#[async_trait]
impl<P: S3Protocol + 'static> NativeEndpoint for S3NativeEndpoint<P> {
    fn affinity(&self) -> NativeAffinity {
        self.context.affinity
    }

    fn recovery_mode(&self, source_size: u64) -> NativeRecoveryMode {
        if source_size > super::S3_NATIVE_COPY_SINGLE_MAX {
            NativeRecoveryMode::Checkpointed
        } else {
            NativeRecoveryMode::Atomic
        }
    }

    async fn bind_source(
        &self,
        source: &SourceDescriptor,
    ) -> Result<NativeSourceBinding, StorageRoleFailure> {
        let facts = self
            .protocol
            .head(source.path.as_str())
            .await
            .map_err(|failure| role_failure(&source.path, Operation::Read, failure))?;
        validate_identity(
            source,
            &self.identity,
            &facts.etag,
            facts.version_id.as_deref(),
        )?;
        let native = S3NativeCopySource {
            bucket: self.context.bucket.clone(),
            key: self.context.full_key(source.path.as_str()),
            etag: facts.etag,
            version_id: facts.version_id,
            size: facts.size,
        };
        Ok(NativeSourceBinding {
            affinity: self.context.affinity,
            token: encode_source(&native).map_err(|()| {
                entry(
                    &source.path,
                    Operation::Read,
                    "native source binding is too large",
                )
            })?,
            size: native.size,
        })
    }

    async fn copy_into_stage(
        &self,
        source: NativeSourceBinding,
        stage: &crate::storage::PreparedStage,
        cancel: CancellationToken,
    ) -> Result<NativeStageEvidence, NativeStageFailure> {
        if source.affinity != self.context.affinity {
            return Err(stage_failure(stage, "native pair affinity changed"));
        }
        let native = decode_source(source.token)
            .map_err(|()| stage_failure(stage, "invalid native source binding"))?;
        if native.size != source.size {
            return Err(stage_failure(stage, "native source size changed"));
        }
        self.staged.fill_native(stage, native, cancel).await
    }
}

fn validate_identity(
    source: &SourceDescriptor,
    backend: &BackendIdentity,
    etag: &str,
    version: Option<&str>,
) -> Result<(), StorageRoleFailure> {
    let identity = SourceIdentity::new(
        backend.clone(),
        if version.is_some() {
            IdentityStrength::VersionScoped
        } else {
            IdentityStrength::PathScoped
        },
        version.unwrap_or(etag),
    )
    .map_err(|error| entry(&source.path, Operation::Read, error.to_string()))?;
    if identity == source.source_identity {
        Ok(())
    } else {
        Err(entry(
            &source.path,
            Operation::Read,
            "S3 source identity changed",
        ))
    }
}

fn encode_source(source: &S3NativeCopySource) -> Result<Bytes, ()> {
    let mut output = BytesMut::new();
    for value in [&source.bucket, &source.key, &source.etag] {
        put_string(&mut output, value)?;
    }
    let version = source.version_id.as_deref().unwrap_or("");
    put_string(&mut output, version)?;
    output.put_u64(source.size);
    Ok(output.freeze())
}

fn put_string(output: &mut BytesMut, value: &str) -> Result<(), ()> {
    let length = u32::try_from(value.len()).map_err(|_| ())?;
    output.put_u32(length);
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn decode_source(mut token: Bytes) -> Result<S3NativeCopySource, ()> {
    let bucket = take_string(&mut token)?;
    let key = take_string(&mut token)?;
    let etag = take_string(&mut token)?;
    let version = take_string(&mut token)?;
    if token.remaining() != 8 {
        return Err(());
    }
    let size = token.get_u64();
    Ok(S3NativeCopySource {
        bucket,
        key,
        etag,
        version_id: (!version.is_empty()).then_some(version),
        size,
    })
}

fn take_string(token: &mut Bytes) -> Result<String, ()> {
    if token.remaining() < 4 {
        return Err(());
    }
    let length = token.get_u32() as usize;
    if length > 4096 || token.remaining() < length {
        return Err(());
    }
    String::from_utf8(token.copy_to_bytes(length).to_vec()).map_err(|_| ())
}

fn stage_failure(stage: &crate::storage::PreparedStage, diagnostic: &str) -> NativeStageFailure {
    NativeStageFailure {
        error: entry(
            stage.final_destination.path(),
            Operation::Prepare,
            diagnostic,
        ),
        native_bytes: 0,
        native_requests: 0,
    }
}
