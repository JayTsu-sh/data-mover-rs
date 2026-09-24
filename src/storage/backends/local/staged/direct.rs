//! In-place Local targets: shared payload/metadata I/O, no stage artifacts or rename.
use super::{
    FailureClass, LocalStagedDestination, Operation, PrepareRequest, PreparedStage,
    PublicationEvidence, PublicationFailure, PublishRequest, StorageRoleFailure, failure, io,
    io_failure,
};

#[cfg(unix)]
use super::{Arc, AtomicBool, Bytes, LocalStageState, OpenOptions, Path};

impl LocalStagedDestination {
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
        let source = request.source.source_identity;
        let open_path = path.clone();
        let (directory, file) = tokio::task::spawn_blocking(move || {
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
                .create(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
            let file = directory
                .open_with(&name, &options)
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
            Ok((Arc::new(directory), Arc::new(file.into_std())))
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
        target.direct = true;
        target.durable_publication = false;
        target.prepare_fact = fact;
        target.backend_state = Some(Arc::new(LocalStageState {
            directory,
            file: std::sync::Mutex::new(Some(file)),
            token: Bytes::new(),
            final_path: path,
            relative,
            name,
            first_write: AtomicBool::new(true),
            transfer_identity: None,
        }));
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
                target.final_destination.path(),
                Operation::Publish,
                FailureClass::Cancelled,
            )));
        }
        let file = self
            .open_stage_file_for(target, Operation::Publish)
            .await
            .map_err(map)?;
        #[cfg(unix)]
        let directory = self
            .stage_directory(target, Operation::Publish)
            .await
            .map_err(map)?;
        #[cfg(unix)]
        let name = self.stage_name(target, Operation::Publish).map_err(map)?;
        let path = target.final_destination.path().clone();
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
            }
            Ok(())
        })
        .await
        .map_err(|_| map(failure(&path, Operation::Publish, FailureClass::Internal)))?
        .map_err(|e| map(io_failure(&path, Operation::Publish, &e)))?;
        Ok(PublicationEvidence {
            final_destination: path,
            disposition: crate::storage::PublicationDisposition::Published,
        })
    }
}
