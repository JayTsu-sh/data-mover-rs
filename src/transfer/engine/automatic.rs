use super::{RecoveryContext, StoragePath, StorageRoleFailure};
use crate::model::{EntryOperationFailure, FailureClass, Operation, Transience};
use crate::storage::{CheckpointRegistration, RecoveryIdentity};

/// Holds the registration lease for the lifetime of a deferred stage, after its first checkpoint.
pub(super) struct Registration {
    binding: [u8; 32],
    path: StoragePath,
    context: tokio::sync::Mutex<Option<RecoveryContext>>,
}

impl Registration {
    pub(super) fn new(binding: [u8; 32], path: StoragePath) -> Self {
        Self {
            binding,
            path,
            context: tokio::sync::Mutex::new(None),
        }
    }
}

#[async_trait::async_trait]
impl CheckpointRegistration for Registration {
    async fn register(
        &self,
        stage: &crate::storage::PreparedStage,
        identity: RecoveryIdentity,
    ) -> Result<(), StorageRoleFailure> {
        let failed = |_| registration_failure(&self.path, FailureClass::Internal);
        let mut context = self.context.lock().await;
        let opened = super::super::recovery_store::open(self.binding)
            .await
            .map_err(failed)?;
        if opened.identity.is_some() {
            return Err(registration_failure(&self.path, FailureClass::Conflict));
        }
        opened.registrar.register(identity).await.map_err(failed)?;
        stage.retain_recovery_lease(std::sync::Arc::clone(&opened.lease));
        stage
            .registration_owned
            .store(true, std::sync::atomic::Ordering::Release);
        *context = Some(opened);
        Ok(())
    }
}

fn registration_failure(path: &StoragePath, class: FailureClass) -> StorageRoleFailure {
    StorageRoleFailure::Entry(
        EntryOperationFailure::new(
            path.clone(),
            Operation::Prepare,
            class,
            Transience::Permanent,
            "automatic recovery registration failed",
        )
        .unwrap_or_else(|_| unreachable!("static diagnostic is valid")),
    )
}
