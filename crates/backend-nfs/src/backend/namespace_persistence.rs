//! 已发布文件、目录和链接的持久化确认；不创建目标、stage 或修改属性。
use std::{sync::Arc, time::Duration};

use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use super::{NfsNamespaceAdapter, checked};
use crate::model::{EntryKind, FailureClass, Operation, SourceVersion, StoragePath, Transience};
use crate::storage::backends::nfs::source::{NfsProtocolFailure, entry_failure, role_failure};
use crate::storage::backends::nfs::staged::NfsStageFile;
use crate::storage::{SourceDescriptor, StorageRoleFailure};

impl NfsNamespaceAdapter {
    pub(super) async fn persist_entry(
        &self,
        expected: &SourceDescriptor,
        cancel: CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        tokio::select! {
            biased;
            () = cancel.cancelled() => Err(entry_failure(
                &expected.path, Operation::Publish, FailureClass::Cancelled, Transience::Transient,
            )),
            result = self.persist_verified(expected) => result,
        }
    }

    async fn persist_verified(
        &self,
        expected: &SourceDescriptor,
    ) -> Result<(), StorageRoleFailure> {
        self.validate_expected(expected)?;
        let protocol = if expected.kind == EntryKind::File {
            Some(self.persistence.as_ref().ok_or_else(|| {
                rejected(
                    &expected.path,
                    FailureClass::Unsupported,
                    "NFS file persistence protocol is unavailable",
                )
            })?)
        } else {
            None
        };
        self.confirm_binding(expected).await?;
        if let Some(protocol) = protocol {
            self.persist_file(expected, protocol.as_ref()).await?;
        }
        // NFS 目录和链接的 namespace/SETATTR 修改按协议同步完成，不向它们发送文件 COMMIT。
        // 已核验描述符来自最后一次修改之后；前后完整路径复核仍不可省略。
        self.confirm_binding(expected).await
    }

    fn validate_expected(&self, expected: &SourceDescriptor) -> Result<(), StorageRoleFailure> {
        checked(&expected.path)?;
        if !matches!(
            expected.kind,
            EntryKind::File | EntryKind::Directory | EntryKind::Symlink
        ) || expected.version != SourceVersion::Current
            || expected.source_identity.backend() != &self.identity
            || expected.source_identity.stable_bytes().is_none()
            || (expected.kind == EntryKind::File && expected.size.is_none())
            || expected
                .inline_timestamps()
                .is_none_or(|times| times.modified.is_none() || times.created.is_none())
        {
            return Err(rejected(
                &expected.path,
                FailureClass::Unsupported,
                "NFS persistence requires a supported current entry with bound identity, mtime and ctime; files also require size",
            ));
        }
        Ok(())
    }

    async fn persist_file(
        &self,
        expected: &SourceDescriptor,
        protocol: &dyn super::NfsStagedProtocol,
    ) -> Result<(), StorageRoleFailure> {
        let (file, handle) = protocol
            .open_read(&expected.path)
            .await
            .map_err(|error| role_failure(&expected.path, Operation::Publish, error))?;
        let barrier = if expected.source_identity.stable_bytes() == Some(handle.as_ref()) {
            file.checkpoint(true).await
        } else {
            Err(
                NfsProtocolFailure::new(FailureClass::Conflict, Transience::Permanent)
                    .with_status("NFS persistence opened a replacement file"),
            )
        };
        settle_close(file.as_ref(), barrier)
            .await
            .map_err(|error| role_failure(&expected.path, Operation::Publish, error))
    }

    async fn confirm_binding(&self, expected: &SourceDescriptor) -> Result<(), StorageRoleFailure> {
        // 独立重新解析端点前缀及全部祖先；不把被 rename 的缓存目录当作当前路径。
        let actual = self.describe(&expected.path, true).await?;
        let times = expected.inline_timestamps();
        let current = actual.inline_timestamps();
        if actual.kind != expected.kind
            || actual.size != expected.size
            || actual.source_identity != expected.source_identity
            || times.map(|times| (times.modified, times.created))
                != current.map(|times| (times.modified, times.created))
            || actual.inline_mode() != expected.inline_mode()
            || actual.inline_ownership != expected.inline_ownership
            || actual.permissions != expected.permissions
        {
            return Err(rejected(
                &expected.path,
                FailureClass::Conflict,
                "NFS persistence target identity or verified attributes changed",
            ));
        }
        Ok(())
    }
}

async fn settle_close(
    file: &dyn NfsStageFile,
    barrier: Result<(), NfsProtocolFailure>,
) -> Result<(), NfsProtocolFailure> {
    // CLOSE 是确认的一部分：nfs-rs 还在这里收束当前 mount 的 pNFS DS/LAYOUTCOMMIT。
    // COMMIT 已失败时，不能再等待一整轮 CLOSE 恢复；未知结果保持可见。
    let closed = timeout(Duration::from_secs(3), file.close_uncommitted())
        .await
        .unwrap_or_else(|_| {
            Err(
                NfsProtocolFailure::new(FailureClass::Timeout, Transience::Unknown).with_status(
                    "NFS persistence CLOSE exceeded 3 seconds; close result is unknown",
                ),
            )
        });
    match (barrier, closed) {
        (Err(mut primary), Err(secondary)) => {
            primary.status = Some(Arc::from(format!(
                "NFS persistence COMMIT failed ({:?}, {:?}): {}; CLOSE failed ({:?}, {:?}): {}",
                primary.class,
                primary.transience,
                primary.status.as_deref().unwrap_or("no server status"),
                secondary.class,
                secondary.transience,
                secondary.status.as_deref().unwrap_or("no server status"),
            )));
            Err(primary)
        }
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    }
}

fn rejected(
    path: &StoragePath,
    class: FailureClass,
    diagnostic: &'static str,
) -> StorageRoleFailure {
    role_failure(
        path,
        Operation::Publish,
        NfsProtocolFailure::new(class, Transience::Permanent).with_status(diagnostic),
    )
}
