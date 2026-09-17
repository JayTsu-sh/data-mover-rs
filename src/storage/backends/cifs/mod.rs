//! CIFS adapter facade.

mod checkpoint;
mod metadata;
mod namespace;
mod positioned_writer;
mod protocol;
mod read_pipeline;
mod source;
mod staged;
mod writer;

use std::sync::Arc;

pub(crate) use protocol::ensure_root;

use crate::model::{BackendIdentity, BackendKind};
use crate::storage::{BackendCapabilities, CapabilityAvailability, Storage};

pub(crate) fn connect(
    share: smb_domain::Share,
    root: Option<String>,
    identity: BackendIdentity,
    read_inflight: std::num::NonZeroUsize,
    write_inflight: std::num::NonZeroUsize,
) -> Result<Storage, Box<dyn std::error::Error>> {
    if identity.kind() != BackendKind::Cifs {
        return Err("CIFS roles require a CIFS backend identity".into());
    }
    let capabilities = BackendCapabilities::new(
        CapabilityAvailability::Supported,
        CapabilityAvailability::Supported,
        CapabilityAvailability::Supported,
        CapabilityAvailability::Supported,
    );
    let protocol = Arc::new(protocol::SmbDomainProtocol::new(share, root));
    let source = Arc::new(
        source::CifsReadSource::new(Arc::clone(&protocol), identity.clone())
            .with_read_inflight(read_inflight),
    );
    let namespace = Arc::new(namespace::CifsNamespace::new(
        Arc::clone(&protocol),
        identity.clone(),
    ));
    let metadata: Arc<dyn crate::storage::Metadata> = Arc::new(metadata::CifsMetadata::new(
        Arc::clone(&protocol),
        identity.clone(),
    ));
    let staged = Arc::new(
        staged::CifsStagedDestination::new(Arc::clone(&protocol), identity.clone())
            .with_metadata(Arc::clone(&metadata))
            .with_write_inflight(write_inflight),
    );
    Ok(Storage::connected(
        identity,
        capabilities,
        Some(source),
        Some(staged),
        Some(namespace),
        Some(metadata),
        None,
    )?)
}

#[cfg(test)]
mod source_tests;
#[cfg(test)]
mod staged_tests;

#[cfg(test)]
mod namespace_tests;
#[cfg(test)]
mod pipeline_tests;
