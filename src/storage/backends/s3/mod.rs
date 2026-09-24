//! Standard S3 roles for the `ArchitectureReady` storage seam.

mod metadata;
mod native;
mod protocol;
#[cfg(test)]
mod recovery_tests;
mod source;
mod staged;

use std::sync::Arc;

use crate::model::{BackendIdentity, BackendKind};
use crate::storage::{BackendCapabilities, CapabilityAvailability, Storage};

pub(crate) use protocol::{
    S3_NATIVE_COPY_SINGLE_MAX, S3NativeCopyEvidence, S3NativeCopyFailure, S3NativeCopyResult,
    S3NativeCopySource, S3ObjectFacts, S3PartFacts, S3Protocol, S3ProtocolFailure, S3Result,
    S3WriteFacts, is_real_version_id,
};

pub(crate) use metadata::S3TagSupport;
pub(crate) use native::S3NativeContext;

#[cfg(test)]
pub(crate) fn connect<P>(
    protocol: Arc<P>,
    identity: BackendIdentity,
    native_context: Option<S3NativeContext>,
) -> Result<Storage, Box<dyn std::error::Error>>
where
    P: S3Protocol + 'static,
{
    connect_with_tag_support(protocol, identity, native_context, S3TagSupport::Supported)
}

pub(crate) fn connect_with_tag_support<P>(
    protocol: Arc<P>,
    identity: BackendIdentity,
    native_context: Option<S3NativeContext>,
    tag_support: S3TagSupport,
) -> Result<Storage, Box<dyn std::error::Error>>
where
    P: S3Protocol + 'static,
{
    if identity.kind() != BackendKind::S3 {
        return Err("S3 roles require an S3 backend identity".into());
    }
    let source = Arc::new(source::S3ReadSource::new(
        protocol.clone(),
        identity.clone(),
    ));
    let metadata: Arc<dyn crate::storage::Metadata> = Arc::new(metadata::S3Metadata::new(
        protocol.clone(),
        identity.clone(),
        tag_support,
    ));
    let staged = Arc::new(
        staged::S3StagedDestination::new(protocol.clone(), identity.clone())
            .with_metadata(Arc::clone(&metadata)),
    );
    let native = native_context.map(|context| {
        Arc::new(native::S3NativeEndpoint::new(
            protocol.clone(),
            staged.clone(),
            identity.clone(),
            context,
        )) as Arc<dyn crate::storage::NativeEndpoint>
    });
    drop(protocol);
    Storage::connected(
        identity,
        BackendCapabilities::new(
            CapabilityAvailability::Supported,
            CapabilityAvailability::Supported,
            CapabilityAvailability::Unsupported(crate::storage::UnsupportedReason::new(
                "namespace role is delivered by traversal",
            )?),
            CapabilityAvailability::Supported,
        ),
        Some(source),
        Some(staged),
        None,
        Some(metadata),
        native,
    )
    .map_err(Into::into)
}

#[cfg(test)]
pub(crate) mod tests;
