//! CIFS has no POSIX 0600 or security descriptor on `FileOpenOptions::create_new`.
//! A v2 temporary inherits the staging directory ACL while empty. Before any source
//! fragments are written, its DACL is replaced with the payload DACL, protected from
//! additional inheritance, and read back. Failure refuses the checkpoint. This needs
//! `READ_CONTROL`/`WRITE_DAC`; servers unable to preserve that DACL are unsupported for
//! hash checkpoints. Legacy pointers do not issue these security operations.
//! Payload ACL tightening is applied to the retained pointer first. The two files
//! cannot change ACL atomically; callers still require exclusive ownership of the key.

use super::{at_destination, metadata::CifsMetadataProtocol};
use super::{source::classify, staged::CifsStagedDestination};
use crate::model::{Operation, StoragePath};
use crate::storage::artifacts::ArtifactKind;
use crate::storage::{Metadata, MetadataMutation, PreparedStage, StorageRoleFailure};

pub(super) async fn copy<P: CifsMetadataProtocol + ?Sized>(
    protocol: &P,
    pointer: &StoragePath,
    payload: &StoragePath,
) -> smb_domain::Result<()> {
    let mut expected = CifsMetadataProtocol::get_acl(protocol, payload).await?;
    expected.control = expected.control.with_dacl_protected(true);
    CifsMetadataProtocol::set_acl(protocol, pointer, expected.clone()).await?;
    let actual = CifsMetadataProtocol::get_acl(protocol, pointer).await?;
    if !actual.control.dacl_protected() || actual.dacl != expected.dacl {
        return Err(smb_domain::Error::UnsupportedOperation(
            "server did not preserve the protected checkpoint DACL".into(),
        ));
    }
    Ok(())
}

pub(super) async fn before_payload_acl(
    adapter: &CifsStagedDestination,
    stage: &PreparedStage,
    metadata: &dyn Metadata,
    mutation: &MetadataMutation,
    cancel: tokio_util::sync::CancellationToken,
) -> Result<(), StorageRoleFailure> {
    let _guard = stage.checkpoint_guard().await;
    at_destination::before_publication(adapter, stage).await?;
    let pointer = at_destination::artifact_path(
        stage.final_destination.path(),
        ArtifactKind::Pointer,
        false,
    )?;
    metadata.apply(&pointer, mutation.clone(), cancel).await
}

pub(super) async fn refresh(
    adapter: &CifsStagedDestination,
    stage: &PreparedStage,
    payload: &StoragePath,
) -> Result<(), StorageRoleFailure> {
    let _guard = stage.checkpoint_guard().await;
    at_destination::before_publication(adapter, stage).await?;
    let pointer = at_destination::artifact_path(
        stage.final_destination.path(),
        ArtifactKind::Pointer,
        false,
    )?;
    adapter
        .protocol
        .protect_checkpoint(&pointer, payload)
        .await
        .map_err(|error| classify(stage.final_destination.path(), Operation::Metadata, &error))
}

#[cfg(test)]
#[path = "checkpoint_acl_tests.rs"]
mod tests;
