//! Local-filesystem adapter facade.

use std::sync::Arc;

use crate::storage::{
    BackendCapabilities, CapabilityAvailability, LocalTransferConfig, LocalTransferConnectError,
    Storage, UnsupportedReason,
};

#[allow(dead_code)]
pub(crate) mod observation;

#[allow(dead_code)]
pub(crate) mod source;

#[allow(dead_code)]
pub(crate) mod staged;

pub(crate) fn connect_transfer(
    config: LocalTransferConfig,
) -> Result<Storage, LocalTransferConnectError> {
    let source = Arc::new(
        source::LocalReadSource::new(&config.root, config.identity.clone())
            .map_err(LocalTransferConnectError::Source)?,
    );
    let destination = Arc::new(
        staged::LocalStagedDestination::new(
            config.root,
            config.identity.clone(),
            config.write_concurrency.get(),
        )
        .map_err(LocalTransferConnectError::Destination)?,
    );
    let unsupported = CapabilityAvailability::Unsupported(
        UnsupportedReason::new("role is not part of the Local transfer endpoint")
            .map_err(LocalTransferConnectError::Capability)?,
    );
    let capabilities = BackendCapabilities::new(
        CapabilityAvailability::Supported,
        CapabilityAvailability::Supported,
        unsupported.clone(),
        unsupported,
    );
    Storage::connected(
        config.identity,
        capabilities,
        Some(source),
        Some(destination),
        None,
        None,
        None,
    )
    .map_err(|_| LocalTransferConnectError::Invariant)
}

#[cfg(test)]
pub(crate) fn test_identity(name: &str) -> crate::model::BackendIdentity {
    crate::model::BackendIdentity::new(crate::model::BackendKind::Local, name)
        .unwrap_or_else(|error| panic!("{error}"))
}

#[cfg(test)]
pub(crate) fn test_source_storage(
    root: &std::path::Path,
    name: &str,
) -> Result<
    (
        crate::storage::Storage,
        std::sync::Arc<source::LocalReadSource>,
    ),
    Box<dyn std::error::Error>,
> {
    let identity = test_identity(name);
    let source = std::sync::Arc::new(source::LocalReadSource::new(root, identity.clone())?);
    let storage = crate::storage::Storage::connected(
        identity,
        test_capabilities(true, false)?,
        Some(source.clone()),
        None,
        None,
        None,
        None,
    )?;
    Ok((storage, source))
}

#[cfg(test)]
pub(crate) fn test_destination_storage(
    root: &std::path::Path,
    name: &str,
) -> Result<crate::storage::Storage, Box<dyn std::error::Error>> {
    test_destination_storage_with_role(root, name).map(|(storage, _)| storage)
}

#[cfg(test)]
pub(crate) fn test_destination_storage_with_role(
    root: &std::path::Path,
    name: &str,
) -> Result<
    (
        crate::storage::Storage,
        std::sync::Arc<staged::LocalStagedDestination>,
    ),
    Box<dyn std::error::Error>,
> {
    let identity = test_identity(name);
    let destination = std::sync::Arc::new(staged::LocalStagedDestination::new(
        root,
        identity.clone(),
        2,
    )?);
    let storage = crate::storage::Storage::connected(
        identity,
        test_capabilities(false, true)?,
        None,
        Some(destination.clone()),
        None,
        None,
        None,
    )?;
    Ok((storage, destination))
}

#[cfg(test)]
pub(crate) fn test_unsupported_storage(
    name: &str,
) -> Result<crate::storage::Storage, Box<dyn std::error::Error>> {
    Ok(crate::storage::Storage::connected(
        test_identity(name),
        test_capabilities(false, false)?,
        None,
        None,
        None,
        None,
        None,
    )?)
}

#[cfg(test)]
fn test_capabilities(
    read: bool,
    staged: bool,
) -> Result<crate::storage::BackendCapabilities, Box<dyn std::error::Error>> {
    use crate::storage::{CapabilityAvailability, UnsupportedReason};
    let unavailable = CapabilityAvailability::Unsupported(UnsupportedReason::new("not supplied")?);
    Ok(crate::storage::BackendCapabilities::new(
        if read {
            CapabilityAvailability::Supported
        } else {
            unavailable.clone()
        },
        if staged {
            CapabilityAvailability::Supported
        } else {
            unavailable.clone()
        },
        unavailable.clone(),
        unavailable,
    ))
}
