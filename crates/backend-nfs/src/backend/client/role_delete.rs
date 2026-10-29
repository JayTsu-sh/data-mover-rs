//! Private artifact cleanup below a namespace directory. Visible children are never followed.
use super::{
    FType3, MAX_STALE_RETRIES, NFSStorage, classify_role_error, is_retryable_with_invalidation,
    is_server_busy,
};
use crate::model::{FailureClass, StoragePath, Transience};
use crate::storage::artifacts::is_artifact_path;
use crate::storage::backends::nfs::protocol::classify_error;
use crate::storage::backends::nfs::source::NfsProtocolFailure;
use bytes::Bytes;
use nfs_rs::{DirectoryCookie, ReaddirplusPage};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

type Result<T> = std::result::Result<T, NfsProtocolFailure>;

impl NFSStorage {
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
    use super::child;
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
