use crate::{
    metadata::{ApplicationFailureKind, MetadataApplicationFailure},
    model::{BackendKind, FailureClass},
    storage::{CapabilityUnavailable, PublicationFailure, StorageRoleFailure},
};
use std::{io, num::TryFromIntError};
use tokio::task::JoinError;
#[derive(Debug, thiserror::Error)]
pub enum DirectPatchError {
    #[error(
        "Direct patch cancelled; an in-progress patch may have partially updated the destination"
    )]
    Cancelled,
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
impl DirectPatchError {
    /// 保留原阶段错误及 source，同时统一识别输入、角色、元数据和发布阶段的取消。
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        match self {
            Self::Cancelled => true,
            Self::Role(source) | Self::Publication { source, .. } => match source {
                StorageRoleFailure::Entry(error) => error.class() == FailureClass::Cancelled,
                StorageRoleFailure::Session(error) => error.class() == FailureClass::Cancelled,
            },
            Self::Metadata(error) => error.kind() == ApplicationFailureKind::Cancelled,
            _ => false,
        }
    }
}
impl From<PublicationFailure> for DirectPatchError {
    fn from(value: PublicationFailure) -> Self {
        Self::Publication {
            source: value.error,
            changed: value.final_destination_changed,
        }
    }
}
