//! Local-filesystem adapter facade.

#[allow(dead_code)]
pub(crate) mod observation;

#[allow(dead_code)]
pub(crate) mod source;

#[allow(dead_code)]
pub(crate) mod staged;

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
    )?;
    Ok((storage, source))
}

#[cfg(test)]
pub(crate) fn test_destination_storage(
    root: &std::path::Path,
    name: &str,
) -> Result<crate::storage::Storage, Box<dyn std::error::Error>> {
    let identity = test_identity(name);
    let destination = std::sync::Arc::new(staged::LocalStagedDestination::new(
        root,
        identity.clone(),
        2,
    )?);
    Ok(crate::storage::Storage::connected(
        identity,
        test_capabilities(false, true)?,
        None,
        Some(destination),
        None,
        None,
    )?)
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
