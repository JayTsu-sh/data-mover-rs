use crate::{
    metadata::MetadataApplicationFailure,
    model::BackendKind,
    storage::{CapabilityUnavailable, PublicationFailure, StorageRoleFailure},
};
use std::{io, num::TryFromIntError};
use tokio::task::JoinError;
#[derive(Debug, thiserror::Error)]
pub enum DirectPatchError {
    #[error("{0:?} backend does not support Direct sparse patch sessions")]
    UnsupportedDestination(BackendKind),
    #[error("Invalid Direct patch input: {0}")]
    InvalidInput(&'static str),
    #[error("Destination basis changed before Direct patch: {0}")]
    BasisChanged(String),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Join(#[from] JoinError),
    #[error(transparent)]
    Integer(#[from] TryFromIntError),
    #[error(transparent)]
    Role(#[from] StorageRoleFailure),
    #[error(transparent)]
    Capability(#[from] CapabilityUnavailable),
    #[error(transparent)]
    Metadata(#[from] MetadataApplicationFailure),
    #[error("Direct publication failed (destination changed={changed}): {source}")]
    Publication {
        #[source]
        source: StorageRoleFailure,
        changed: bool,
    },
}
impl From<PublicationFailure> for DirectPatchError {
    fn from(value: PublicationFailure) -> Self {
        Self::Publication {
            source: value.error,
            changed: value.final_destination_changed,
        }
    }
}
