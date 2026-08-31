//! CIFS adapter facade.

mod metadata;
mod namespace;
mod protocol;
mod source;
mod staged;

use std::sync::Arc;

use crate::model::{BackendIdentity, BackendKind};
use crate::storage::{BackendCapabilities, CapabilityAvailability, Storage};

pub(crate) fn connect(
    share: smb_domain::Share,
    root: Option<String>,
    identity: BackendIdentity,
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
    let source = Arc::new(source::CifsReadSource::new(
        Arc::clone(&protocol),
        identity.clone(),
    ));
    let namespace = Arc::new(namespace::CifsNamespace::new(
        Arc::clone(&protocol),
        identity.clone(),
    ));
    let staged = Arc::new(staged::CifsStagedDestination::new(
        Arc::clone(&protocol),
        identity.clone(),
    ));
    let metadata = Arc::new(metadata::CifsMetadata::new(protocol));
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
