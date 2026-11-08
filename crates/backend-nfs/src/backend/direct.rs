//! NFS Direct 使用最终名称；通过打开时的 FH 核对归属，不创建或恢复 checkpoint。

use std::sync::{
    Arc, Mutex, PoisonError,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use bytes::Bytes;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    NfsProtocolFailure, NfsStageFile, NfsStageState, NfsStagedDestinationAdapter, checked_final,
    entry_failure, role_failure,
};
use crate::DEFAULT_FILE_MODE;
use crate::model::{
    BackendKind, EntryKind, EntryOperationFailure, FailureClass, IdentityStrength, Operation,
    SourceIdentity, Transience,
};
use crate::storage::{
    PrepareFact, PrepareRequest, PreparedStage, PublicationDisposition, PublicationEvidence,
    ReadSource, SourceDescriptor, StorageRoleFailure,
};

struct OpenedDirect {
    file: Box<dyn NfsStageFile>,
    handle: Bytes,
    initial: Option<SourceDescriptor>,
}

struct InitialChange {
    size: Option<u64>,
    content_version: Option<Bytes>,
}

pub(in crate::backend) struct DirectState {
    identity: SourceIdentity,
    initial: Option<InitialChange>,
    expected_size: u64,
    mutated: AtomicBool,
    metadata_started: AtomicBool,
    file: Mutex<Option<Arc<dyn NfsStageFile>>>,
    finalizing: AtomicBool,
}

pub(super) fn state(stage: &PreparedStage) -> Result<&DirectState, StorageRoleFailure> {
    stage
        .backend_state()
        .as_ref()
        .and_then(|owned| owned.downcast_ref::<NfsStageState>())
        .and_then(|owned| match owned {
            NfsStageState::Direct(direct) => Some(direct),
            NfsStageState::Staged { .. } => None,
        })
        .ok_or_else(|| {
            entry_failure(
                stage.final_destination().path(),
                Operation::Prepare,
                FailureClass::Conflict,
                Transience::Permanent,
            )
        })
}

pub(super) fn mark_mutation(stage: &PreparedStage) {
    if !stage.direct() {
        return;
    }
    if let Ok(state) = state(stage) {
        state.mutated.store(true, Ordering::Release);
    }
}

pub(super) fn mark_metadata(stage: &PreparedStage) {
    if !stage.direct() {
        return;
    }
    if let Ok(owned) = state(stage) {
        owned.metadata_started.store(true, Ordering::Release);
    }
}

pub(super) fn mark_finalizing(stage: &PreparedStage) {
    if !stage.direct() {
        return;
    }
    if let Ok(owned) = state(stage) {
        owned.finalizing.store(true, Ordering::Release);
    }
}

pub(super) fn release_file(stage: &PreparedStage) {
    if !stage.direct() {
        return;
    }
    if let Ok(owned) = state(stage) {
        owned
            .file
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
    }
}

// 失败或取消后的 CLOSE 只收束当前句柄，不重新开始一轮协议恢复。
const FAILED_CLOSE_BUDGET: Duration = Duration::from_secs(3);

pub(super) async fn close_failed(
    stage: &PreparedStage,
    file: &Arc<dyn NfsStageFile>,
    original: Option<&StorageRoleFailure>,
) -> Result<(), StorageRoleFailure> {
    let result = timeout(FAILED_CLOSE_BUDGET, file.close_uncommitted()).await;
    let result = result.unwrap_or_else(|_| {
        let context = original.map(|error| match error {
            StorageRoleFailure::Entry(error) => error.diagnostic(),
            StorageRoleFailure::Session(error) => error.diagnostic(),
        });
        let diagnostic = format!(
            "NFS Direct failed CLOSE exceeded 3 seconds; close result is unknown; preceding failure: {}",
            context.unwrap_or("caller abandoned the write"),
        );
        Err(NfsProtocolFailure::new(FailureClass::Timeout, Transience::Unknown)
            .with_status(diagnostic))
    });
    result.map_err(|error| role_failure(stage.final_destination().path(), Operation::Write, error))
}

fn refused(
    stage: &PreparedStage,
    operation: Operation,
    message: &'static str,
) -> StorageRoleFailure {
    StorageRoleFailure::Entry(
        EntryOperationFailure::new(
            stage.final_destination().path().clone(),
            operation,
            FailureClass::Conflict,
            Transience::Permanent,
            message,
        )
        .unwrap_or_else(|_| unreachable!("static Direct diagnostic is valid")),
    )
}

fn cancelled(request: &PrepareRequest) -> StorageRoleFailure {
    entry_failure(
        request.final_destination.path(),
        Operation::Prepare,
        FailureClass::Cancelled,
        Transience::Transient,
    )
}

impl NfsStagedDestinationAdapter {
    pub(super) async fn open_direct(
        &self,
        request: PrepareRequest,
        cancel: CancellationToken,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        checked_final(request.final_destination.path())?;
        let expected_size = request.source.size.ok_or_else(|| {
            entry_failure(
                request.final_destination.path(),
                Operation::Prepare,
                FailureClass::InvalidInput,
                Transience::Permanent,
            )
        })?;
        if cancel.is_cancelled() {
            return Err(cancelled(&request));
        }
        let fact = super::super::at_destination::clear_before_direct(self, &request).await?;
        if cancel.is_cancelled() {
            return Err(cancelled(&request));
        }
        let opened = self.open_direct_file(&request).await?;
        self.claim_direct(request, opened, expected_size, fact)
    }

    async fn open_direct_file(
        &self,
        request: &PrepareRequest,
    ) -> Result<OpenedDirect, StorageRoleFailure> {
        match self.source.describe(request.final_destination.path()).await {
            Ok(initial) => return self.open_existing_direct(request, initial).await,
            Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::NotFound => {}
            Err(error) => return Err(error),
        }
        let mode = if request.final_destination.private_creation() {
            0o600
        } else {
            DEFAULT_FILE_MODE
        };
        let (file, handle) = self
            .protocol
            .create_empty_open(request.final_destination.path(), mode)
            .await
            .map_err(|error| {
                role_failure(request.final_destination.path(), Operation::Prepare, error)
            })?;
        Ok(OpenedDirect {
            file,
            handle,
            initial: None,
        })
    }

    fn claim_direct(
        &self,
        request: PrepareRequest,
        opened: OpenedDirect,
        expected_size: u64,
        fact: PrepareFact,
    ) -> Result<PreparedStage, StorageRoleFailure> {
        let OpenedDirect {
            file,
            handle,
            initial,
        } = opened;
        let identity = SourceIdentity::new(
            self.identity.clone(),
            IdentityStrength::StableWithinBackend,
            handle,
        )
        .map_err(|_| {
            entry_failure(
                request.final_destination.path(),
                Operation::Prepare,
                FailureClass::Protocol,
                Transience::Permanent,
            )
        })?;
        let token = Bytes::copy_from_slice(Uuid::new_v4().as_bytes());
        let file: Arc<dyn NfsStageFile> = Arc::from(file);
        let mut stage = PreparedStage::new(
            self.identity.clone(),
            request.final_destination,
            token.clone(),
            request.recovery_binding,
            0,
            None,
        );
        stage.set_direct(true);
        stage.set_recovery_enabled(false);
        stage.mark_at_destination(fact);
        stage.set_backend_state(Some(Arc::new(NfsStageState::Direct(DirectState {
            identity,
            initial: initial.map(|initial| InitialChange {
                size: initial.size,
                content_version: initial.content_version,
            }),
            expected_size,
            mutated: AtomicBool::new(false),
            metadata_started: AtomicBool::new(false),
            file: Mutex::new(Some(file.clone())),
            finalizing: AtomicBool::new(false),
        }))));
        self.cache_prepared_handle(token.clone(), file);
        self.claim_authority(token);
        // CREATE 已经成功时仍返回本次拥有的 stage，让引擎在取消路径按显式权限清理。
        Ok(stage)
    }

    async fn open_existing_direct(
        &self,
        request: &PrepareRequest,
        initial: SourceDescriptor,
    ) -> Result<OpenedDirect, StorageRoleFailure> {
        let path = request.final_destination.path();
        if request.source.source_identity.backend().kind() == BackendKind::Nfs
            && request.source.source_identity.stable_bytes()
                == initial.source_identity.stable_bytes()
        {
            return Err(entry_failure(
                path,
                Operation::Prepare,
                FailureClass::Conflict,
                Transience::Permanent,
            ));
        }
        if initial.kind != EntryKind::File {
            return Err(entry_failure(
                path,
                Operation::Prepare,
                FailureClass::Conflict,
                Transience::Permanent,
            ));
        }
        let (file, handle) = self
            .protocol
            .open_write(path)
            .await
            .map_err(|error| role_failure(path, Operation::Prepare, error))?;
        if initial.source_identity.stable_bytes() != Some(handle.as_ref()) {
            file.close_uncommitted()
                .await
                .map_err(|error| role_failure(path, Operation::Prepare, error))?;
            return Err(entry_failure(
                path,
                Operation::Prepare,
                FailureClass::Conflict,
                Transience::Permanent,
            ));
        }
        Ok(OpenedDirect {
            file,
            handle,
            initial: Some(initial),
        })
    }

    pub(super) async fn confirm_direct(
        &self,
        stage: &PreparedStage,
    ) -> Result<SourceDescriptor, StorageRoleFailure> {
        let expected = state(stage)?;
        let actual = self
            .source
            .describe(stage.final_destination().path())
            .await?;
        if actual.kind != EntryKind::File || actual.source_identity != expected.identity {
            return Err(refused(
                stage,
                Operation::Verify,
                "NFS Direct target no longer names the opened file",
            ));
        }
        Ok(actual)
    }

    pub(super) async fn finish_direct(
        &self,
        stage: &PreparedStage,
    ) -> Result<PublicationEvidence, StorageRoleFailure> {
        let actual = self.confirm_direct(stage).await?;
        if actual.size != Some(state(stage)?.expected_size) {
            return Err(entry_failure(
                stage.final_destination().path(),
                Operation::Publish,
                FailureClass::Corruption,
                Transience::Permanent,
            ));
        }
        self.release_authority(stage);
        Ok(PublicationEvidence::new(
            stage.final_destination().path().clone(),
            PublicationDisposition::Published,
            None,
        ))
    }

    pub(super) async fn cleanup_direct(
        &self,
        stage: PreparedStage,
    ) -> Result<(), StorageRoleFailure> {
        self.validate(&stage)?;
        let result = async {
            self.close_direct(&stage).await?;
            self.remove_owned_direct(&stage).await
        }
        .await;
        self.release_authority(&stage);
        result
    }

    pub(super) async fn close_direct(
        &self,
        stage: &PreparedStage,
    ) -> Result<(), StorageRoleFailure> {
        self.take_prepared_handle(stage.token());
        let owned = state(stage)?;
        let file = owned
            .file
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(file) = file {
            if owned.finalizing.load(Ordering::Acquire) {
                return Err(refused(
                    stage,
                    Operation::Write,
                    "NFS Direct finalization was interrupted; close result is unknown",
                ));
            }
            mark_finalizing(stage);
            close_failed(stage, &file, None).await?;
        }
        Ok(())
    }

    async fn remove_owned_direct(&self, stage: &PreparedStage) -> Result<(), StorageRoleFailure> {
        let expected = state(stage)?;
        if expected.metadata_started.load(Ordering::Acquire) {
            return Err(refused(
                stage,
                Operation::Namespace,
                "NFS Direct metadata work cannot authorize data cleanup",
            ));
        }
        let actual = match self.source.describe(stage.final_destination().path()).await {
            Ok(actual) => actual,
            Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::NotFound => {
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        if actual.kind != EntryKind::File || actual.source_identity != expected.identity {
            return Err(refused(
                stage,
                Operation::Namespace,
                "NFS Direct cleanup refused a replacement target",
            ));
        }
        if let Some(initial) = &expected.initial {
            if !expected.mutated.load(Ordering::Acquire) {
                return Ok(());
            }
            if initial.size == actual.size && initial.content_version == actual.content_version {
                return Err(refused(
                    stage,
                    Operation::Namespace,
                    "NFS Direct write was issued but unchanged attributes cannot confirm target mutation or cleanup",
                ));
            }
        }
        self.protocol
            .delete(stage.final_destination().path())
            .await
            .map_err(|error| {
                role_failure(
                    stage.final_destination().path(),
                    Operation::Namespace,
                    error,
                )
            })
    }
}
