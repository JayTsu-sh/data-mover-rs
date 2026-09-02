//! Local-filesystem adapter facade.

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

use crate::model::{BackendIdentity, BackendKind};
use crate::storage::{BackendCapabilities, CapabilityAvailability, Storage, UnsupportedReason};

#[allow(dead_code)]
pub(crate) mod observation;

#[allow(dead_code)]
pub(crate) mod source;

#[allow(dead_code)]
pub(crate) mod staged;

pub(crate) fn connect_transfer(
    root: PathBuf,
    identity: BackendIdentity,
    read_concurrency: NonZeroUsize,
    write_concurrency: NonZeroUsize,
) -> Result<Storage, Box<dyn std::error::Error>> {
    if identity.kind() != BackendKind::Local {
        return Err("Local roles require a Local backend identity".into());
    }
    let source = Arc::new(source::LocalReadSource::new(
        &root,
        identity.clone(),
        read_concurrency.get(),
    )?);
    let staged = Arc::new(staged::LocalStagedDestination::new(
        root,
        identity.clone(),
        write_concurrency.get(),
    )?);
    let unsupported = CapabilityAvailability::Unsupported(UnsupportedReason::new(
        "role is not supplied by the Local transfer-only endpoint",
    )?);
    Ok(Storage::connected(
        identity,
        BackendCapabilities::new(
            CapabilityAvailability::Supported,
            CapabilityAvailability::Supported,
            unsupported.clone(),
            unsupported,
        ),
        Some(source),
        Some(staged),
        None,
        None,
        None,
    )?)
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
    let source = std::sync::Arc::new(source::LocalReadSource::new(root, identity.clone(), 4)?);
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
