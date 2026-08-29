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

use crate::model::{BackendIdentity, BackendKind, NfsVersion};
use crate::storage::{BackendCapabilities, CapabilityAvailability, Storage, ValidationGate};
use common::NfsInstanceFacts;

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
    let facts = protocol.instance_facts()?;
    let capabilities = capabilities(facts)?;
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

fn capabilities(
    facts: NfsInstanceFacts,
) -> Result<BackendCapabilities, crate::storage::CapabilityValueError> {
    let gate = match facts.dialect {
        NfsVersion::V3 | NfsVersion::V4_0 => {
            return Ok(BackendCapabilities::new(
                CapabilityAvailability::Supported,
                CapabilityAvailability::Supported,
                CapabilityAvailability::Supported,
                CapabilityAvailability::Supported,
            ));
        }
        NfsVersion::V4_1 => "DM-NFS41-CONTRACT",
    };
    let availability = CapabilityAvailability::Uncertified(ValidationGate::new(gate)?);
    Ok(BackendCapabilities::new(
        availability.clone(),
        availability.clone(),
        availability.clone(),
        availability,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{Capability, CapabilityAvailability};

    #[test]
    fn certified_v3_and_v40_are_supported_while_v41_keeps_its_independent_gate() {
        for (dialect, gate) in [
            (NfsVersion::V3, "DM-NFS3-CONTRACT"),
            (NfsVersion::V4_0, "DM-NFS40-CONTRACT"),
            (NfsVersion::V4_1, "DM-NFS41-CONTRACT"),
        ] {
            let values = capabilities(NfsInstanceFacts {
                dialect,
                max_read_size: 65_536,
                max_write_size: 65_536,
                acl: dialect != NfsVersion::V3,
                xattrs: dialect != NfsVersion::V3,
                stable_writes: true,
            })
            .unwrap_or_else(|error| panic!("{error}"));
            if matches!(dialect, NfsVersion::V3 | NfsVersion::V4_0) {
                assert_eq!(
                    values.availability(Capability::ReadSource),
                    &CapabilityAvailability::Supported
                );
            } else {
                assert!(matches!(
                    values.availability(Capability::ReadSource),
                    CapabilityAvailability::Uncertified(required) if required.as_str() == gate
                ));
            }
        }
    }

    #[test]
    fn nfs_roles_reject_a_non_nfs_identity() {
        let identity = BackendIdentity::new(BackendKind::Local, "wrong-kind")
            .unwrap_or_else(|error| panic!("{error}"));
        assert!(validate_identity(&identity).is_err());
    }
}
