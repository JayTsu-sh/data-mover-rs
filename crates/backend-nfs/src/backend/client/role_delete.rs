//! NFS 删除：私有产物清理与基于原观察的非递归源清理。
use super::{
    FType3, MAX_STALE_RETRIES, NFSStorage, classify_role_error, is_retryable_with_invalidation,
    is_server_busy,
};
use crate::model::{EntryKind, FailureClass, ObservedEntry, StoragePath, Transience};
use crate::storage::artifacts::is_artifact_path;
use crate::storage::backends::nfs::namespace::NfsNamespaceObservation;
use crate::storage::backends::nfs::namespace::purge::{refused, verify_current};
use crate::storage::backends::nfs::protocol::{classify_error, classify_persistence_error};
use crate::storage::backends::nfs::source::NfsProtocolFailure;
use bytes::Bytes;
use nfs_rs::{DirectoryCookie, NfsError, ReaddirplusPage};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

type Result<T> = std::result::Result<T, NfsProtocolFailure>;

impl NFSStorage {
    pub(super) async fn role_purge_observed(
        &self,
        path: &StoragePath,
        kind: EntryKind,
        expected: &ObservedEntry,
    ) -> Result<()> {
        if expected.path() != path || expected.kind() != kind {
            return Err(refused(
                FailureClass::InvalidInput,
                "NFS source purge path/kind do not match its observation",
            ));
        }
        let native = Path::new(path.as_str());
        let name = native
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(NfsProtocolFailure::protocol)?;
        let (parent, current) = self.role_fresh_purge_child(path).await?;
        verify_current(expected, &current)?;
        // RFC 1813 §§3.3.12–13 / RFC 8881 §18.25：只删除此名称或空目录。
        // 协议没有 compare-and-unlink；调用方仍须排除并发源修改。
        let result = if kind == EntryKind::Directory {
            self.mount.rmdir(parent, name).await
        } else {
            self.mount.remove(parent, name).await
        };
        // 不复用旧 delete_file 的 stale/name 重试，也不清理目录中的隐藏产物。
        // 删除确认与 COMMIT 一样，须同时保留网络原因与未知结果。
        match result {
            Ok(()) => {
                self.forget_subtree(native);
                Ok(())
            }
            Err(error) => Err(classify_persistence_error(error)),
        }
    }

    async fn role_fresh_purge_child(
        &self,
        path: &StoragePath,
    ) -> Result<(Bytes, NfsNamespaceObservation)> {
        let native = Path::new(path.as_str());
        let parent = native.parent().ok_or_else(NfsProtocolFailure::protocol)?;
        let name = native
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(NfsProtocolFailure::protocol)?;
        // 从端点前缀重新解析父目录；不能借用 rename 后仍有效的旧缓存句柄。
        let relative = format!("{}/{}", self.root, parent.display());
        let directory = self
            .mount
            .lookup_path(&relative)
            .await
            .map_err(classify_error)?;
        let object = self
            .mount
            .lookup(directory.fh.clone(), name)
            .await
            .map_err(classify_error)?;
        let attrs = match object.attr {
            Some(attrs) => attrs,
            None => self
                .mount
                .getattr(object.fh.clone())
                .await
                .map_err(attributes_failure)?,
        };
        if !super::attributes::present(&attrs, 1) {
            return Err(refused(
                FailureClass::Protocol,
                "NFS source recheck omitted file type",
            ));
        }
        let current = self.role_namespace_observation(path, object.fh, &attrs);
        Ok((directory.fh, current))
    }

    pub(super) async fn role_cleanup_artifacts(
        &self,
        directory: &Path,
        require_empty: bool,
    ) -> Result<()> {
        let mut handle = self.role_lookup_fh(directory).await?.fh;
        let mut cookie = DirectoryCookie::default();
        let mut reached = HashSet::new();
        let mut artifacts = Vec::new();
        loop {
            let page = self.cleanup_page(directory, &mut handle, cookie).await?;
            for entry in page.entries {
                let path = child(directory, &entry.file_name)?;
                if is_artifact_path(&path.to_string_lossy()) {
                    artifacts.push((path, entry));
                } else if require_empty {
                    return Err(NfsProtocolFailure::new(
                        FailureClass::Conflict,
                        Transience::Permanent,
                    ));
                }
            }
            if page.eof {
                break;
            }
            if !reached.insert(page.next.cookie) {
                return Err(NfsProtocolFailure::protocol());
            }
            cookie = page.next;
        }
        // The full directory was checked before any artifact is removed.
        let mut pending = artifacts;
        let mut directories = Vec::new();
        while let Some((path, entry)) = pending.pop() {
            let mut handle = if entry.handle.is_empty() {
                self.role_lookup_fh(&path).await?.fh
            } else {
                entry.handle
            };
            let attrs = match entry.attr {
                Some(attrs) => attrs,
                None => self
                    .mount
                    .getattr(handle.clone())
                    .await
                    .map_err(classify_error)?,
            };
            if attrs.type_ != FType3::NF3DIR as u32 {
                self.delete_file(&path).await.map_err(classify_role_error)?;
                continue;
            }
            let mut cookie = DirectoryCookie::default();
            let mut reached = HashSet::new();
            loop {
                let page = self.cleanup_page(&path, &mut handle, cookie).await?;
                for entry in page.entries {
                    pending.push((child(&path, &entry.file_name)?, entry));
                }
                if page.eof {
                    break;
                }
                if !reached.insert(page.next.cookie) {
                    return Err(NfsProtocolFailure::protocol());
                }
                cookie = page.next;
            }
            directories.push(path);
        }
        for path in directories.into_iter().rev() {
            self.role_delete_directory(&path).await?;
        }
        Ok(())
    }

    async fn cleanup_page(
        &self,
        path: &Path,
        handle: &mut Bytes,
        cookie: DirectoryCookie,
    ) -> Result<ReaddirplusPage> {
        for attempt in 0..=MAX_STALE_RETRIES {
            match self.mount.readdirplus_page(handle.clone(), cookie).await {
                Ok(page) => return Ok(page),
                Err(error)
                    if cookie == DirectoryCookie::default()
                        && attempt < MAX_STALE_RETRIES
                        && (is_retryable_with_invalidation(&error) || is_server_busy(&error)) =>
                {
                    if is_retryable_with_invalidation(&error) {
                        let relative = StoragePath::new(path.to_string_lossy())
                            .map_err(|_| NfsProtocolFailure::protocol())?;
                        self.forget_role_directory(&relative)
                            .await
                            .map_err(classify_role_error)?;
                        *handle = self.role_lookup_fh(path).await?.fh;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                Err(error) => return Err(classify_error(error)),
            }
        }
        unreachable!("bounded NFS cleanup retry loop always returns")
    }
}

fn attributes_failure(error: NfsError) -> NfsProtocolFailure {
    let mut failure = classify_error(error);
    if failure.class == FailureClass::NotFound {
        let reason = format!(
            "NFS source attribute read failed after successful lookup; path absence is unconfirmed: {}",
            failure.status.as_deref().unwrap_or("NotFound"),
        );
        failure.class = FailureClass::Protocol;
        failure = failure.with_status(reason);
    }
    failure
}

fn child(parent: &Path, name: &str) -> Result<PathBuf> {
    if matches!(name, "" | "." | "..")
        || name.contains('/')
        || name.contains(char::REPLACEMENT_CHARACTER)
    {
        return Err(NfsProtocolFailure::protocol());
    }
    Ok(parent.join(name))
}

#[cfg(test)]
mod tests {
    use super::{attributes_failure, child};
    use crate::model::FailureClass;
    use nfs_rs::{Nfs3ErrorCode, Nfs4ErrorCode, NfsError};

    #[test]
    fn missing_attributes_after_lookup_do_not_confirm_path_absence() {
        for error in [
            NfsError::Nfs3(Nfs3ErrorCode::NFS3ERR_NOENT),
            NfsError::Nfs4(Nfs4ErrorCode::NFS4ERR_NOENT),
        ] {
            let failure = attributes_failure(error);
            assert_eq!(failure.class, FailureClass::Protocol);
            assert!(
                failure
                    .status
                    .as_deref()
                    .is_some_and(|message| message.contains("after successful lookup")
                        && message.contains("NOENT"))
            );
        }
    }
    use std::path::Path;

    #[test]
    fn cleanup_rejects_directory_navigation_names() {
        for name in [".", ".."] {
            assert!(child(Path::new("artifact"), name).is_err(), "{name}");
        }
        assert_eq!(
            child(Path::new("artifact"), "file").ok().as_deref(),
            Some(Path::new("artifact/file"))
        );
    }
}
