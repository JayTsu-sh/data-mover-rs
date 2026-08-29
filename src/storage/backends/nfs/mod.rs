//! NFS adapter facade.

mod acl;
pub(crate) mod common;
#[allow(dead_code)]
pub(crate) mod metadata;
#[allow(dead_code)]
pub(crate) mod namespace;
pub(crate) mod protocol;
mod recovery;
#[allow(dead_code)]
pub(crate) mod source;
#[allow(dead_code)]
pub(crate) mod staged;

use std::sync::Arc;

use crate::model::{BackendIdentity, BackendKind};
use crate::storage::{BackendCapabilities, CapabilityAvailability, Storage};

#[allow(dead_code)]
pub(crate) fn connect<P>(
    protocol: Arc<P>,
    identity: BackendIdentity,
) -> Result<Storage, Box<dyn std::error::Error>>
where
    P: source::NfsSourceProtocol
        + staged::NfsStagedProtocol
        + namespace::NfsNamespaceProtocol
        + metadata::NfsMetadataProtocol
        + common::NfsFactsProvider
        + 'static,
{
    validate_identity(&identity)?;
    protocol.instance_facts()?;
    let capabilities = capabilities();
    let source = Arc::new(source::NfsReadSourceAdapter::new(
        protocol.clone(),
        identity.clone(),
    ));
    let destination = Arc::new(staged::NfsStagedDestinationAdapter::new(
        protocol.clone(),
        identity.clone(),
    ));
    let namespace = Arc::new(namespace::NfsNamespaceAdapter::new(
        protocol.clone(),
        identity.clone(),
    ));
    let metadata = Arc::new(metadata::NfsMetadataAdapter::new(protocol));
    Ok(Storage::connected(
        identity,
        capabilities,
        Some(source),
        Some(destination),
        Some(namespace),
        Some(metadata),
        None,
    )?)
}

fn validate_identity(identity: &BackendIdentity) -> Result<(), NfsConnectError> {
    if identity.kind() == BackendKind::Nfs {
        Ok(())
    } else {
        Err(NfsConnectError)
    }
}

#[derive(Clone, Copy, Debug)]
struct NfsConnectError;

impl std::fmt::Display for NfsConnectError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("NFS roles require an NFS backend identity")
    }
}

impl std::error::Error for NfsConnectError {}

fn capabilities() -> BackendCapabilities {
    BackendCapabilities::new(
        CapabilityAvailability::Supported,
        CapabilityAvailability::Supported,
        CapabilityAvailability::Supported,
        CapabilityAvailability::Supported,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{Capability, CapabilityAvailability};

    #[test]
    fn certified_nfs_roles_are_supported() {
        let values = capabilities();
        assert_eq!(
            values.availability(Capability::ReadSource),
            &CapabilityAvailability::Supported,
        );
    }

    #[test]
    fn nfs_roles_reject_a_non_nfs_identity() {
        let identity = BackendIdentity::new(BackendKind::Local, "wrong-kind")
            .unwrap_or_else(|error| panic!("{error}"));
        assert!(validate_identity(&identity).is_err());
    }
}
