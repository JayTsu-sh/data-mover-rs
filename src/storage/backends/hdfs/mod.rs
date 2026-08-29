//! HDFS adapter facade.

#[cfg(test)]
pub(crate) mod contract_tests;
mod metadata;
mod namespace;
pub(crate) mod protocol;
mod source;
mod staged;

use std::sync::Arc;

use crate::model::{BackendIdentity, BackendKind};
use crate::storage::{BackendCapabilities, CapabilityAvailability, Storage};

/// Connects an already validated HDFS client to the architecture-ready roles.
pub(crate) fn connect<P>(
    storage: Arc<P>,
    identity: BackendIdentity,
) -> Result<Storage, Box<dyn std::error::Error>>
where
    P: protocol::HdfsProtocol + 'static,
{
    if identity.kind() != BackendKind::Hdfs {
        return Err(HdfsConnectError.into());
    }
    let source = Arc::new(source::HdfsReadSource::new(
        Arc::clone(&storage),
        identity.clone(),
    ));
    let staged = Arc::new(staged::HdfsStagedDestination::new(
        Arc::clone(&storage),
        identity.clone(),
    ));
    let namespace = Arc::new(namespace::HdfsNamespace::new(
        Arc::clone(&storage),
        identity.clone(),
    ));
    let metadata = Arc::new(metadata::HdfsMetadata::new(storage));
    Ok(Storage::connected(
        identity,
        capabilities(),
        Some(source),
        Some(staged),
        Some(namespace),
        Some(metadata),
        None,
    )?)
}

fn capabilities() -> BackendCapabilities {
    BackendCapabilities::new(
        CapabilityAvailability::Supported,
        CapabilityAvailability::Supported,
        CapabilityAvailability::Supported,
        CapabilityAvailability::Supported,
    )
}

#[derive(Clone, Copy, Debug)]
struct HdfsConnectError;

impl std::fmt::Display for HdfsConnectError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("HDFS roles require an HDFS backend identity")
    }
}

impl std::error::Error for HdfsConnectError {}

#[cfg(test)]
pub(crate) fn test_identity(name: &str) -> Result<BackendIdentity, crate::model::ModelValueError> {
    BackendIdentity::new(BackendKind::Hdfs, name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hdfs_capabilities_are_truthful_for_connected_roles() {
        let values = capabilities();
        for capability in [
            crate::storage::Capability::ReadSource,
            crate::storage::Capability::StagedDestination,
            crate::storage::Capability::Namespace,
            crate::storage::Capability::Metadata,
        ] {
            assert_eq!(
                values.availability(capability),
                &CapabilityAvailability::Supported
            );
        }
    }
}
