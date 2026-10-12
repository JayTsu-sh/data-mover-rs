//! Standard S3 roles for the `ArchitectureReady` storage seam.

mod broken_body;
mod capabilities;
mod current;
mod histories;
mod history;
mod listing;
mod metadata;
mod namespace;
mod native;
mod paging;
mod protocol;
mod same_object;
mod source;
mod staged;

use std::sync::Arc;

use crate::model::{
    BackendIdentity, BackendKind, BackendSessionFailure, FailureClass, Operation, Transience,
};
use crate::storage::{BackendCapabilities, CapabilityAvailability, Namespace, Storage};

pub(crate) use protocol::{
    S3BucketVersioning, S3DeleteReply, S3ListedObject, S3ListedVersion, S3NativeCopyEvidence,
    S3NativeCopySource, S3ObjectFacts, S3ObjectPage, S3PartFacts, S3Protocol, S3ProtocolFailure,
    S3RejectedEntry, S3Result, S3StreamedPut, S3VersionFacts, S3VersionMarker, S3VersionPage,
    S3VersioningAtConnect, S3WriteFacts, composite_etag, is_real_version_id,
};

pub(crate) use broken_body::broken_body_failure;
pub(crate) use capabilities::S3BucketCapabilities;
pub(crate) use metadata::S3TagSupport;
pub(crate) use native::S3NativeContext;
pub(crate) use same_object::{S3ObjectChange, S3SeenObject, object_change};
pub(crate) use staged::DEFAULT_SINGLE_PUT_THRESHOLD;
pub(crate) use staged::single_put_threshold;

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

#[cfg(test)]
pub(crate) fn connect_with_tag_support<P>(
    protocol: Arc<P>,
    identity: BackendIdentity,
    native_context: Option<S3NativeContext>,
    tag_support: S3TagSupport,
) -> Result<Storage, Box<dyn std::error::Error>>
where
    P: S3Protocol + 'static,
{
    connect_configured(
        protocol,
        identity,
        native_context,
        tag_support,
        Some(DEFAULT_SINGLE_PUT_THRESHOLD),
    )
}

/// Connects the S3 roles. Sources of at most `single_put_threshold` bytes are written as one
/// `PutObject` (ADR-0006 C14b; a configured value is checked by [`single_put_threshold`]); `None`
/// writes every object through a multipart upload.
pub(crate) fn connect_configured<P>(
    protocol: Arc<P>,
    identity: BackendIdentity,
    native_context: Option<S3NativeContext>,
    tag_support: S3TagSupport,
    single_put_threshold: Option<u64>,
) -> Result<Storage, Box<dyn std::error::Error>>
where
    P: S3Protocol + 'static,
{
    connect_roles(
        protocol,
        identity,
        native_context,
        tag_support,
        single_put_threshold,
        |staged| staged,
    )
}

/// S3 roles with `checkpoint_interval` as the destination's automatic interval instead of 64 MiB,
/// so engine tests of recovery at the destination (on for every connection since ADR-0006 C15c)
/// need not move that much.
#[cfg(test)]
pub(crate) fn connect_at_destination<P>(
    protocol: Arc<P>,
    identity: BackendIdentity,
    checkpoint_interval: u64,
) -> Result<Storage, Box<dyn std::error::Error>>
where
    P: S3Protocol + 'static,
{
    connect_roles(
        protocol,
        identity,
        None,
        S3TagSupport::Supported,
        Some(DEFAULT_SINGLE_PUT_THRESHOLD),
        |staged| staged.with_checkpoint_interval(checkpoint_interval),
    )
}

/// S3 roles able to copy natively (the in-memory native context) with `checkpoint_interval` as the
/// automatic interval and a native copy split as `native` says — (single-copy limit, part size) —
/// instead of 64 MiB each (ADR-0006 C18).
#[cfg(test)]
pub(crate) fn connect_native_at_destination<P>(
    protocol: Arc<P>,
    identity: BackendIdentity,
    checkpoint_interval: u64,
    native: (u64, u64),
) -> Result<Storage, Box<dyn std::error::Error>>
where
    P: S3Protocol + 'static,
{
    connect_roles(
        protocol,
        identity,
        Some(tests::native_context()),
        S3TagSupport::Supported,
        Some(DEFAULT_SINGLE_PUT_THRESHOLD),
        |staged| {
            staged
                .with_checkpoint_interval(checkpoint_interval)
                .with_native_sizing(native.0, native.1)
        },
    )
}

fn connect_roles<P>(
    protocol: Arc<P>,
    identity: BackendIdentity,
    native_context: Option<S3NativeContext>,
    tag_support: S3TagSupport,
    single_put_threshold: Option<u64>,
    configure: impl FnOnce(staged::S3StagedDestination<P>) -> staged::S3StagedDestination<P>,
) -> Result<Storage, Box<dyn std::error::Error>>
where
    P: S3Protocol + 'static,
{
    if identity.kind() != BackendKind::S3 {
        return Err("S3 roles require an S3 backend identity".into());
    }
    refuse_unknown_versioning(protocol.as_ref())?;
    let source = Arc::new(source::S3ReadSource::new(
        protocol.clone(),
        identity.clone(),
    ));
    let metadata: Arc<dyn crate::storage::Metadata> = Arc::new(metadata::S3Metadata::new(
        protocol.clone(),
        identity.clone(),
        tag_support,
    ));
    let staged = Arc::new(configure(
        staged::S3StagedDestination::new(protocol.clone(), identity.clone())
            .with_metadata(Arc::clone(&metadata))
            .with_tag_support(matches!(tag_support, S3TagSupport::Supported))
            .with_single_put_threshold(single_put_threshold),
    ));
    let namespace: Arc<dyn Namespace> = Arc::new(namespace::S3Namespace::new(
        protocol.clone(),
        identity.clone(),
    ));
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
            CapabilityAvailability::Supported,
            CapabilityAvailability::Supported,
        ),
        Some(source),
        Some(staged),
        Some(namespace),
        Some(metadata),
        native,
    )
    .map_err(Into::into)
}

/// A bucket whose profile keeps versions but whose `GetBucketVersioning` got no answer at
/// connection (P7 Q-A): whether its listings carry histories is unknown, and guessing would make
/// two scans of one bucket disagree (a `"null"` record per key, or none). A transient
/// `Connectivity` session failure, so the caller connects again; a refusal to answer is no such
/// failure (it lists histories, Q26), and a NAS bucket never lists them.
fn refuse_unknown_versioning<P: S3Protocol + ?Sized>(
    protocol: &P,
) -> Result<(), Box<dyn std::error::Error>> {
    let S3VersioningAtConnect::Unreachable { status } = protocol.versioning_at_connect() else {
        return Ok(());
    };
    if !protocol.bucket_capabilities().versions {
        return Ok(());
    }
    let answer = status.map_or_else(
        || "no response (timeout or lost connection)".to_owned(),
        |status| format!("HTTP {status}"),
    );
    Err(Box::new(BackendSessionFailure::new(
        Operation::Connect,
        FailureClass::Connectivity,
        Transience::Transient,
        format!(
            "GetBucketVersioning got no answer when the bucket was connected ({answer}): whether \
             its listings carry every version is unknown; connect again"
        ),
    )?))
}

#[cfg(test)]
pub(crate) mod tests;
