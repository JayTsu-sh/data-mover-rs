use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use futures::TryStreamExt as _;

use super::metadata::{CifsInlineMetadata, CifsMetadataProtocol};
use super::namespace::CifsNamespaceProtocol;
use super::source::{CifsReadCursor, CifsSourceFacts, CifsSourceProtocol};
use super::staged::{CifsStageFile, CifsStagedProtocol};
use crate::model::{EntryKind, StoragePath};

pub(super) struct SmbDomainProtocol {
    share: smb_domain::Share,
    root: Option<String>,
}

impl SmbDomainProtocol {
    pub(super) fn new(share: smb_domain::Share, root: Option<String>) -> Self {
        Self { share, root }
    }

    fn share_path(&self, path: &StoragePath) -> smb_domain::Result<smb_domain::SharePath> {
        let path = path.as_str().replace('/', "\\");
        let value = match (self.root.as_deref(), path.is_empty()) {
            (Some(root), false) if !root.is_empty() => {
                format!("{}\\{path}", root.replace('/', "\\"))
            }
            (Some(root), _) if !root.is_empty() => root.replace('/', "\\"),
            (_, false) => path,
            _ => ".".to_owned(),
        };
        smb_domain::SharePath::new(value)
    }
}

struct DomainReadCursor {
    file: smb_domain::File,
}

struct DomainStageFile {
    file: smb_domain::File,
}

#[async_trait]
impl CifsStageFile for DomainStageFile {
    fn maximum_read_chunk(&self) -> u32 {
        self.file.io_capabilities().maximum_read_chunk()
    }

    fn maximum_write_chunk(&self) -> u32 {
        self.file.io_capabilities().maximum_write_chunk()
    }

    async fn read_at(&self, offset: u64, count: u32) -> smb_domain::Result<Bytes> {
        self.file.read_exact_at(offset, count).await
    }

    async fn write_all_at(&self, offset: u64, bytes: Bytes) -> smb_domain::Result<()> {
        self.file.write_all_at(offset, bytes).await
    }

    async fn flush(&self) -> smb_domain::Result<()> {
        self.file.flush().await
    }

    async fn close(self: Box<Self>) -> smb_domain::Result<()> {
        close_file(self.file).await
    }
}

#[async_trait]
impl CifsReadCursor for DomainReadCursor {
    fn maximum_read_chunk(&self) -> u32 {
        self.file.io_capabilities().maximum_read_chunk()
    }

    async fn read_at(&mut self, offset: u64, count: u32) -> smb_domain::Result<Bytes> {
        self.file.read_exact_at(offset, count).await
    }

    async fn close(self: Box<Self>) -> smb_domain::Result<()> {
        close_file(self.file).await
    }
}

#[async_trait]
impl CifsSourceProtocol for SmbDomainProtocol {
    async fn describe(&self, path: &StoragePath) -> smb_domain::Result<CifsSourceFacts> {
        let share_path = self.share_path(path)?;
        let resource = self.share.open(&share_path).await?;
        match resource {
            smb_domain::Resource::File(file) => {
                let metadata = file.metadata().await;
                let close = close_file(*file).await;
                finish_facts(metadata, close, EntryKind::File)
            }
            smb_domain::Resource::Directory(directory) => {
                let metadata = directory.metadata().await;
                let close = close_directory(directory).await;
                finish_facts(metadata, close, EntryKind::Directory)
            }
            smb_domain::Resource::Pipe(pipe) => {
                let _ = close_pipe(pipe).await;
                Err(smb_domain::Error::UnsupportedOperation(
                    "named pipes are not storage entries".into(),
                ))
            }
        }
    }

    async fn open(
        &self,
        path: &StoragePath,
    ) -> smb_domain::Result<(Box<dyn CifsReadCursor>, CifsSourceFacts)> {
        let share_path = self.share_path(path)?;
        let file = self
            .share
            .open_file(&share_path, smb_domain::FileOpenOptions::open_existing())
            .await?;
        let metadata = match file.metadata().await {
            Ok(metadata) => metadata,
            Err(error) => {
                let _ = close_file(file).await;
                return Err(error);
            }
        };
        let facts = facts(EntryKind::File, &metadata);
        Ok((Box::new(DomainReadCursor { file }), facts))
    }
}

#[async_trait]
impl CifsNamespaceProtocol for SmbDomainProtocol {
    async fn list(
        &self,
        path: &StoragePath,
    ) -> smb_domain::Result<Vec<(StoragePath, CifsSourceFacts)>> {
        let share_path = self.share_path(path)?;
        let directory = self
            .share
            .open_directory(
                &share_path,
                smb_domain::DirectoryOpenOptions::open_existing(),
            )
            .await?;
        let entries = directory.entries("*").try_collect::<Vec<_>>().await;
        let close = close_directory(directory).await;
        let entries = entries?;
        close?;
        entries
            .into_iter()
            .filter(|entry| !matches!(entry.name(), "." | ".."))
            .map(|entry| {
                let child = child_path(path, entry.name())?;
                let kind = if entry.is_directory() {
                    EntryKind::Directory
                } else {
                    EntryKind::File
                };
                let mut identity = BytesMut::with_capacity(16);
                identity.extend_from_slice(b"cifs-dir-entry-v1\0");
                identity.put_u64(entry.len());
                Ok((
                    child,
                    CifsSourceFacts {
                        kind,
                        size: entry.len(),
                        identity: identity.freeze(),
                    },
                ))
            })
            .collect()
    }
}

#[async_trait]
impl CifsStagedProtocol for SmbDomainProtocol {
    async fn create_empty(&self, path: &StoragePath) -> smb_domain::Result<()> {
        let staging = StoragePath::new(".data-mover-staging")
            .map_err(|_| smb_domain::Error::InvalidArgument("invalid staging path".into()))?;
        let staging = self.share_path(&staging)?;
        match self
            .share
            .open_directory(&staging, smb_domain::DirectoryOpenOptions::open_existing())
            .await
        {
            Ok(directory) => close_directory(directory).await?,
            Err(error) if is_not_found(&error) => {
                match self
                    .share
                    .open_directory(&staging, smb_domain::DirectoryOpenOptions::create_new())
                    .await
                {
                    Ok(directory) => close_directory(directory).await?,
                    Err(error) if is_name_collision(&error) => {
                        let directory = self
                            .share
                            .open_directory(
                                &staging,
                                smb_domain::DirectoryOpenOptions::open_existing(),
                            )
                            .await?;
                        close_directory(directory).await?;
                    }
                    Err(error) => return Err(error),
                }
            }
            Err(error) => return Err(error),
        }
        let path = self.share_path(path)?;
        let file = self
            .share
            .open_file(&path, smb_domain::FileOpenOptions::create_new())
            .await?;
        close_file(file).await
    }

    async fn open(&self, path: &StoragePath) -> smb_domain::Result<Box<dyn CifsStageFile>> {
        let path = self.share_path(path)?;
        let file = self
            .share
            .open_file(&path, smb_domain::FileOpenOptions::open_existing())
            .await?;
        Ok(Box::new(DomainStageFile { file }))
    }

    async fn size(&self, path: &StoragePath) -> smb_domain::Result<u64> {
        let path = self.share_path(path)?;
        let file = self
            .share
            .open_file(&path, smb_domain::FileOpenOptions::open_existing())
            .await?;
        let metadata = file.metadata().await;
        let close = close_file(file).await;
        let metadata = metadata?;
        close?;
        Ok(metadata.len())
    }

    async fn rename(
        &self,
        from: &StoragePath,
        to: &StoragePath,
        replace: bool,
    ) -> smb_domain::Result<()> {
        let from = self.share_path(from)?;
        let to = self.share_path(to)?;
        let file = self
            .share
            .open_file(&from, smb_domain::FileOpenOptions::open_existing())
            .await?;
        let rename = if replace {
            file.rename_replace(&to).await
        } else {
            file.rename(&to).await
        };
        let close = close_file(file).await;
        rename?;
        close
    }

    async fn delete(&self, path: &StoragePath) -> smb_domain::Result<()> {
        let path = self.share_path(path)?;
        let file = self
            .share
            .open_file(&path, smb_domain::FileOpenOptions::open_existing())
            .await?;
        let delete = file.delete().await;
        let close = close_file(file).await;
        delete?;
        close
    }
}

#[async_trait]
impl CifsMetadataProtocol for SmbDomainProtocol {
    async fn metadata(&self, path: &StoragePath) -> smb_domain::Result<CifsInlineMetadata> {
        let path = self.share_path(path)?;
        let resource = self.share.open(&path).await?;
        let metadata = resource.metadata().await;
        let close = close_resource(resource).await;
        let metadata = metadata?;
        close?;
        Ok(CifsInlineMetadata {
            accessed: metadata.accessed(),
            modified: metadata.written(),
            created: metadata.created(),
        })
    }

    async fn get_acl(
        &self,
        path: &StoragePath,
    ) -> smb_domain::Result<smb_domain::SecurityDescriptor> {
        let path = self.share_path(path)?;
        let resource = self
            .share
            .open_security(&path, smb_domain::SecurityOpenOptions::default())
            .await?;
        let descriptor = resource
            .query_security(smb_domain::SecuritySelection::default().dacl(true))
            .await;
        let close = close_resource(resource).await;
        let descriptor = descriptor?;
        close?;
        Ok(descriptor)
    }

    async fn set_acl(
        &self,
        path: &StoragePath,
        descriptor: smb_domain::SecurityDescriptor,
    ) -> smb_domain::Result<()> {
        let path = self.share_path(path)?;
        let resource = self
            .share
            .open_security(
                &path,
                smb_domain::SecurityOpenOptions::default().write_dacl(true),
            )
            .await?;
        let applied = resource
            .set_security(
                descriptor,
                smb_domain::SecuritySelection::default().dacl(true),
            )
            .await;
        let close = close_resource(resource).await;
        applied?;
        close
    }
}

fn finish_facts(
    metadata: smb_domain::Result<smb_domain::ResourceMetadata>,
    close: smb_domain::Result<()>,
    kind: EntryKind,
) -> smb_domain::Result<CifsSourceFacts> {
    let metadata = metadata?;
    close?;
    Ok(facts(kind, &metadata))
}

fn require_confirmed_close(outcome: smb_domain::CloseOutcome) -> smb_domain::Result<()> {
    match outcome {
        smb_domain::CloseOutcome::Confirmed | smb_domain::CloseOutcome::AlreadyClosed => Ok(()),
        smb_domain::CloseOutcome::OutcomeUnknown => Err(smb_domain::Error::OutcomeUnknown),
    }
}

fn child_path(parent: &StoragePath, name: &str) -> smb_domain::Result<StoragePath> {
    let value = if parent.as_str().is_empty() {
        name.to_owned()
    } else {
        format!("{}/{name}", parent.as_str())
    };
    StoragePath::new(value)
        .map_err(|_| smb_domain::Error::InvalidArgument("invalid CIFS child path".into()))
}

fn is_not_found(error: &smb_domain::Error) -> bool {
    match error {
        smb_domain::Error::NotFound(_) => true,
        smb_domain::Error::ReceivedErrorMessage(status, _)
        | smb_domain::Error::UnexpectedMessageStatus(status) => matches!(
            smb_domain::protocol::Status::try_from(*status),
            Ok(smb_domain::protocol::Status::ObjectNameNotFound
                | smb_domain::protocol::Status::ObjectPathNotFound)
        ),
        _ => false,
    }
}

fn is_name_collision(error: &smb_domain::Error) -> bool {
    match error {
        smb_domain::Error::ReceivedErrorMessage(status, _)
        | smb_domain::Error::UnexpectedMessageStatus(status) => matches!(
            smb_domain::protocol::Status::try_from(*status),
            Ok(smb_domain::protocol::Status::ObjectNameCollision)
        ),
        _ => false,
    }
}

async fn close_resource(resource: smb_domain::Resource) -> smb_domain::Result<()> {
    match resource {
        smb_domain::Resource::File(file) => close_file(*file).await,
        smb_domain::Resource::Directory(directory) => close_directory(directory).await,
        smb_domain::Resource::Pipe(pipe) => close_pipe(pipe).await,
    }
}

async fn close_file(file: smb_domain::File) -> smb_domain::Result<()> {
    require_confirmed_close(file.close().await?)
}

async fn close_directory(directory: smb_domain::Directory) -> smb_domain::Result<()> {
    require_confirmed_close(directory.close().await?)
}

async fn close_pipe(pipe: smb_domain::Pipe) -> smb_domain::Result<()> {
    require_confirmed_close(pipe.close().await?)
}

fn facts(kind: EntryKind, metadata: &smb_domain::ResourceMetadata) -> CifsSourceFacts {
    let mut identity = BytesMut::with_capacity(40);
    identity.extend_from_slice(b"data-mover:cifs-path-identity:v1\0");
    identity.put_u64(metadata.len());
    put_time(&mut identity, metadata.written());
    put_time(&mut identity, metadata.changed());
    CifsSourceFacts {
        kind,
        size: metadata.len(),
        identity: identity.freeze(),
    }
}

fn put_time(output: &mut BytesMut, value: std::time::SystemTime) {
    let nanos = value
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    output.extend_from_slice(&nanos.to_be_bytes());
}
