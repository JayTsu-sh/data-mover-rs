//! An open smb-rs directory handle as the listing reads it (P7 C9).

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{StreamExt as _, TryStreamExt as _, future};

use super::metadata::CifsInlineMetadata;
use super::namespace::{CifsDirectory, CifsListedChild, CifsUnspelled};
use super::protocol::{close_directory, identity_bytes};
use super::source::CifsSourceFacts;
use crate::model::{EntryKind, StoragePath};

/// One open directory, with the path it was opened at (its children are spelled under it) and
/// whether this session's identities use file ids.
pub(super) struct DomainDirectory {
    directory: smb_domain::Directory,
    path: StoragePath,
    use_file_ids: bool,
}

impl DomainDirectory {
    pub(super) const fn new(
        directory: smb_domain::Directory,
        path: StoragePath,
        use_file_ids: bool,
    ) -> Self {
        Self {
            directory,
            path,
            use_file_ids,
        }
    }

    /// One `QUERY_DIRECTORY` record as a listed child.
    fn listed(&self, entry: &smb_domain::DirectoryEntry) -> CifsListedChild {
        let child = CifsUnspelled::spell(&self.path, entry.name())?;
        // Reparse points stay `File`: the facade cannot read link targets, and `Symlink` would
        // route traversal into an unsupported `ReadLink`.
        let kind = if entry.is_directory() {
            EntryKind::Directory
        } else {
            EntryKind::File
        };
        Ok((
            child,
            CifsInlineMetadata {
                facts: CifsSourceFacts {
                    kind,
                    size: entry.len(),
                    identity: identity_bytes(kind, entry.len(), entry.written(), entry.changed()),
                    file_id: entry.file_id().filter(|_| self.use_file_ids),
                    maximum_read_chunk: u32::MAX,
                },
                accessed: entry.accessed(),
                modified: entry.written(),
                created: entry.created(),
                readonly: Some(entry.is_readonly()),
                reparse_point: entry.is_reparse_point(),
            },
        ))
    }
}

#[async_trait]
impl CifsDirectory for DomainDirectory {
    fn entries(&self) -> BoxStream<'_, smb_domain::Result<CifsListedChild>> {
        self.directory
            .entries("*")
            .try_filter(|entry| future::ready(!matches!(entry.name(), "." | "..")))
            .map_ok(|entry| self.listed(&entry))
            .boxed()
    }

    async fn close(self: Box<Self>) -> smb_domain::Result<()> {
        close_directory(self.directory).await
    }
}
