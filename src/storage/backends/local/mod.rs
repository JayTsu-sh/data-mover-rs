//! Local-filesystem adapter facade.

use std::io;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cap_std::ambient_authority;
use cap_std::fs::Dir;

use crate::model::{BackendIdentity, BackendKind};
use crate::storage::{BackendCapabilities, CapabilityAvailability, Storage};

pub(crate) mod namespace;

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
    let sandbox = open_root(&root)?;
    let metadata = Arc::new(observation::LocalObservationAdapter::from_root(
        Arc::clone(&sandbox),
        identity.clone(),
    ));
    let namespace = Arc::new(namespace::LocalNamespace::from_root(
        sandbox,
        identity.clone(),
    ));
    let staged = Arc::new(staged::LocalStagedDestination::new(
        root,
        identity.clone(),
        write_concurrency.get(),
    )?);
    Ok(Storage::connected(
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
        None,
    )?)
}

/// Opens the root directory capability shared by the observation and namespace roles.
fn open_root(root: &Path) -> io::Result<Arc<Dir>> {
    let canonical = std::fs::canonicalize(root)?;
    Dir::open_ambient_dir(canonical, ambient_authority()).map(Arc::new)
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
    let metadata = std::sync::Arc::new(observation::LocalObservationAdapter::new(
        root,
        identity.clone(),
    )?);
    let storage = crate::storage::Storage::connected(
        identity,
        test_capabilities(true, false, true)?,
        Some(source.clone()),
        None,
        None,
        Some(metadata),
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
    let metadata = std::sync::Arc::new(observation::LocalObservationAdapter::new(
        root,
        identity.clone(),
    )?);
    let storage = crate::storage::Storage::connected(
        identity,
        test_capabilities(false, true, true)?,
        None,
        Some(destination.clone()),
        None,
        Some(metadata),
        None,
    )?;
    Ok((storage, destination))
}

/// A traversal-only storage plus the two roles it lends, so tests can drive their probes.
#[cfg(test)]
pub(crate) type TraversalStorage = (
    Storage,
    Arc<namespace::LocalNamespace>,
    Arc<observation::LocalObservationAdapter>,
);

/// Storage lending only the namespace and metadata roles, which is what traversal borrows.
#[cfg(test)]
pub(crate) fn test_traversal_storage(
    root: &Path,
    name: &str,
) -> Result<TraversalStorage, Box<dyn std::error::Error>> {
    let identity = test_identity(name);
    let sandbox = open_root(root)?;
    let namespace = Arc::new(namespace::LocalNamespace::from_root(
        Arc::clone(&sandbox),
        identity.clone(),
    ));
    let metadata = Arc::new(observation::LocalObservationAdapter::from_root(
        sandbox,
        identity.clone(),
    ));
    let unavailable = CapabilityAvailability::Unsupported(crate::storage::UnsupportedReason::new(
        "not supplied",
    )?);
    let storage = Storage::connected(
        identity,
        BackendCapabilities::new(
            unavailable.clone(),
            unavailable,
            CapabilityAvailability::Supported,
            CapabilityAvailability::Supported,
        ),
        None,
        None,
        Some(namespace.clone()),
        Some(metadata.clone()),
        None,
    )?;
    Ok((storage, namespace, metadata))
}

#[cfg(test)]
pub(crate) fn test_unsupported_storage(
    name: &str,
) -> Result<crate::storage::Storage, Box<dyn std::error::Error>> {
    Ok(crate::storage::Storage::connected(
        test_identity(name),
        test_capabilities(false, false, false)?,
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
    metadata: bool,
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
        if metadata {
            CapabilityAvailability::Supported
        } else {
            unavailable
        },
    ))
}
