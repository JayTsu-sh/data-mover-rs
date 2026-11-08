//! In-place Local targets: shared payload/metadata I/O, no stage artifacts or rename.
#[cfg(unix)]
use std::io::ErrorKind;

#[cfg(unix)]
use super::super::creation::create_file;

use super::{
    FailureClass, LocalStagedDestination, Operation, PrepareRequest, PreparedStage,
    PublicationEvidence, PublicationFailure, PublishRequest, StorageRoleFailure, failure, io,
    io_failure,
};

#[cfg(unix)]
use super::direct_durability::sync as sync_direct_publication;
#[cfg(unix)]
use super::{Arc, AtomicBool, Bytes, LocalStageState, OpenOptions, Path};
#[cfg(unix)]
use crate::model::{BackendKind, IdentityStrength, SourceIdentity};
use crate::model::{EntryOperationFailure, StoragePath};
use crate::storage::SourceDescriptor;

impl LocalStagedDestination {
    pub(super) async fn check_direct_inputs(
        &self,
        stage: &PreparedStage,
        inputs: Box<dyn Iterator<Item = SourceDescriptor> + Send + '_>,
    ) -> Result<(), StorageRoleFailure> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;
            let path = stage.final_destination().path();
            let file = self
                .open_stage_file_for(stage, Operation::Prepare, false)
                .await?;
            let metadata = tokio::task::spawn_blocking(move || file.metadata())
                .await
                .map_err(|_| failure(path, Operation::Prepare, FailureClass::Internal))?
                .map_err(|error| io_failure(path, Operation::Prepare, &error))?;
            for source in inputs {
                if source.source_identity.backend().kind() == BackendKind::Local
                    && source.source_identity
                        == SourceIdentity::from_u64_pair_le(
                            source.source_identity.backend().clone(),
                            IdentityStrength::StableWithinBackend,
                            metadata.dev(),
                            metadata.ino(),
                        )
                {
                    return Err(failure(path, Operation::Prepare, FailureClass::Conflict));
                }
            }
            Ok(())
        }
        #[cfg(not(unix))]
        {
            let _ = inputs;
            Err(failure(
                stage.final_destination().path(),
                Operation::Prepare,
                FailureClass::Unsupported,
            ))
        }
    }
    #[cfg(unix)]
    fn is_held_elsewhere(error: &StorageRoleFailure) -> bool {
        matches!(
            error,
            StorageRoleFailure::Entry(entry)
                if entry.class() == FailureClass::Conflict
                    && entry.transience() == crate::model::Transience::Transient
        )
    }

    #[cfg(unix)]
    pub(super) async fn open_direct(
        &self,
        request: PrepareRequest,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        use cap_std::fs::OpenOptionsExt as _;
        Self::validate_prepare_request(&request)?;
        let fact = match super::at_destination::clear_before_direct(self, &request).await {
            Ok(fact) => fact,
            // Another process's live stage of this file: refuse rather than race it.
            Err(error) if Self::is_held_elsewhere(&error) => return Err(error),
            // Anything else (a directory the caller may not write, a reserved name that is not a
            // file) must not stop an in-place write that needs neither; the leftovers stay.
            Err(error) => {
                tracing::warn!(
                    path = %request.final_destination.path().as_str(),
                    ?error,
                    "could not clear transfer artifacts before a direct write; leaving them"
                );
                crate::storage::PrepareFact::Fresh
            }
        };
        let path = request.final_destination.path().clone();
        let relative = Self::checked_relative(&path, Operation::Prepare)?;
        let name = relative
            .file_name()
            .ok_or_else(|| failure(&path, Operation::Prepare, FailureClass::InvalidInput))?
            .to_owned();
        let parent = relative
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
            .to_owned();
        let root = Arc::clone(&self.root_dir);
        let private_creation = request.final_destination.private_creation();
        let source = request.source.source_identity;
        let open_path = path.clone();
        let (directory, file, cleanup) = tokio::task::spawn_blocking(move || {
            let fail = |class| failure(&open_path, Operation::Prepare, class);
            if cancel.is_cancelled() {
                return Err(fail(FailureClass::Cancelled));
            }
            let directory = Self::open_or_create_parent(&root, &parent)
                .map_err(|e| io_failure(&open_path, Operation::Prepare, &e))?;
            let mut options = OpenOptions::new();
            // No truncate at open: compare the opened inode before changing any content.
            // NONBLOCK prevents opening a FIFO from hanging before the type check.
            options
                .read(true)
                .write(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
            let (file, created) =
                match create_file(&directory, &name, &options, private_creation) {
                    Err(error) if error.kind() == ErrorKind::AlreadyExists => directory
                        .open_with(&name, &options)
                        .map(|file| (file, false)),
                    other => other.map(|file| (file, true)),
                }
                .map_err(|e| io_failure(&open_path, Operation::Prepare, &e))?;
            let metadata = file
                .metadata()
                .map_err(|e| io_failure(&open_path, Operation::Prepare, &e))?;
            if !metadata.is_file() {
                return Err(fail(FailureClass::Unsupported));
            }
            if source.backend().kind() == crate::model::BackendKind::Local
                && super::super::observation::source_identity(
                    source.backend(),
                    &open_path,
                    &metadata,
                )? == source
            {
                return Err(fail(FailureClass::Conflict));
            }
            // Truncation is deferred to the writer so subsequent errors carry the direct handle.
            let file = Arc::new(file.into_std());
            let cleanup = super::direct_cleanup::MutationEvidence::new(created, &metadata);
            Ok((Arc::new(directory), file, Arc::new(cleanup)))
        })
        .await
        .map_err(|_| failure(&path, Operation::Prepare, FailureClass::Internal))??;
        let name = relative
            .file_name()
            .ok_or_else(|| failure(&path, Operation::Prepare, FailureClass::InvalidInput))?
            .to_owned();
        let mut target = PreparedStage::new(
            self.identity.clone(),
            request.final_destination,
            Bytes::new(),
            [0; 32],
            0,
            None,
        )
        .disable_recovery();
        target.set_direct(true);
        target.set_durable_publication(false);
        target.set_prepare_fact(fact);
        target.set_backend_state(Some(Arc::new(LocalStageState {
            directory,
            file: std::sync::Mutex::new(Some(file)),
            token: Bytes::new(),
            final_path: path,
            relative,
            name,
            first_write: AtomicBool::new(true),
            direct_cleanup: Some(cleanup),
            transfer_identity: None,
        })));
        Ok(target)
    }

    #[cfg(not(unix))]
    pub(super) async fn open_direct(
        &self,
        request: PrepareRequest,
        _cancel: tokio_util::sync::CancellationToken,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        Err(failure(
            request.final_destination.path(),
            Operation::Prepare,
            FailureClass::Unsupported,
        ))
    }

    pub(super) async fn finish_direct(
        &self,
        target: &PreparedStage,
        request: PublishRequest,
    ) -> Result<PublicationEvidence, PublicationFailure> {
        let map = |error| PublicationFailure {
            error,
            final_destination_changed: true,
        };
        if request.cancel.is_cancelled() {
            return Err(map(failure(
                target.final_destination().path(),
                Operation::Publish,
                FailureClass::Cancelled,
            )));
        }
        let file = self
            .open_stage_file_for(target, Operation::Publish, false)
            .await
            .map_err(map)?;
        #[cfg(unix)]
        let directory = self
            .stage_directory(target, Operation::Publish)
            .await
            .map_err(map)?;
        #[cfg(unix)]
        let name = self.stage_name(target, Operation::Publish).map_err(map)?;
        let path = target.final_destination().path().clone();
        #[cfg(unix)]
        let durability = target.durable_publication().then(|| {
            (
                Arc::clone(&self.root_dir),
                Self::stage_relative(target, Operation::Publish),
                Arc::clone(&self.directory_sync),
                Arc::clone(&self.write_probe),
            )
        });
        tokio::task::spawn_blocking(move || {
            if request.cancel.is_cancelled() {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "direct completion cancelled",
                ));
            }
            let metadata = file.metadata()?;
            if metadata.len() != request.expected_size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "direct target length mismatch",
                ));
            }
            #[cfg(unix)]
            {
                use cap_std::fs::MetadataExt as _;
                use std::os::unix::fs::MetadataExt as _;
                let visible = directory.symlink_metadata(name)?;
                if !visible.is_file()
                    || metadata.dev() != visible.dev()
                    || metadata.ino() != visible.ino()
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "direct target was replaced",
                    ));
                }
                if let Some((root, relative, sync, probe)) = durability {
                    sync_direct_publication(
                        &root,
                        &file,
                        &directory,
                        &relative.map_err(io::Error::other)?,
                        &sync,
                        &probe,
                        &request.cancel,
                    )?;
                }
            }
            Ok(())
        })
        .await
        .map_err(|_| map(failure(&path, Operation::Publish, FailureClass::Internal)))?
        .map_err(|e| map(publication_io_failure(&path, &e)))?;
        Ok(PublicationEvidence::new(
            path,
            crate::storage::PublicationDisposition::Published,
            None,
        ))
    }
}

fn publication_io_failure(path: &StoragePath, error: &io::Error) -> StorageRoleFailure {
    let original = io_failure(path, Operation::Publish, error);
    let StorageRoleFailure::Entry(cause) = &original else {
        return original;
    };
    // 角色边界保留操作分类和具体阶段原因；模型拒绝不合法诊断时仍返回原始分类。
    EntryOperationFailure::new(
        path.clone(),
        Operation::Publish,
        cause.class(),
        cause.transience(),
        format!("Direct target publication failed: {error}"),
    )
    .map(StorageRoleFailure::Entry)
    .unwrap_or(original)
}
