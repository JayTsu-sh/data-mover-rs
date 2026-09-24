use std::fmt;
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use futures::{StreamExt as _, TryStreamExt as _};

use super::metadata::{CifsInlineMetadata, CifsMetadataProtocol};
use super::namespace::CifsNamespaceProtocol;
use super::source::{CifsReadCursor, CifsSourceFacts, CifsSourceProtocol};
use super::staged::{CifsStageFile, CifsStagedProtocol};
use crate::model::{EntryKind, StoragePath};

const STATUS_OBJECT_NAME_NOT_FOUND: u32 = 0xC000_0034;
const STATUS_OBJECT_PATH_NOT_FOUND: u32 = 0xC000_003A;
const STATUS_OBJECT_NAME_COLLISION: u32 = 0xC000_0035;

pub(super) struct SmbDomainProtocol {
    share: smb_domain::Share,
    root: Option<String>,
    /// Whether file ids take part in identity for this session.
    ///
    /// Listings get their id from a wide `QUERY_DIRECTORY` class and opens from the `QFid`
    /// create context — two independent server features. A server that offers only one would
    /// make `List` and `Stat` disagree on identity for every entry (`Conflict` on each
    /// transfer), so [`probe_identity_mode`] checks both at connect time and this flag turns
    /// file ids off on **both** paths when either is missing.
    use_file_ids: bool,
}

impl SmbDomainProtocol {
    pub(super) fn new(share: smb_domain::Share, root: Option<String>, use_file_ids: bool) -> Self {
        Self {
            share,
            root,
            use_file_ids,
        }
    }

    fn file_id(&self, file_id: Option<u64>) -> Option<u64> {
        file_id.filter(|_| self.use_file_ids)
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

    /// Entry kind through one `GENERIC_READ` open, without the two `QUERY_INFO` round trips
    /// that `describe` adds.
    async fn kind_of(&self, path: &StoragePath) -> smb_domain::Result<EntryKind> {
        let share_path = self.share_path(path)?;
        let resource = self.share.open(&share_path).await?;
        let kind = resource_kind(&resource);
        close_resource(resource).await?;
        kind.ok_or_else(|| {
            smb_domain::Error::UnsupportedOperation("named pipes are not storage entries".into())
        })
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

    async fn read_at(&self, offset: u64, count: u32) -> smb_domain::Result<Bytes> {
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
            // The CREATE response already carried everything a describe needs, so no
            // QUERY_INFO round trip: CREATE + CLOSE instead of CREATE + 2×QUERY_INFO + CLOSE.
            smb_domain::Resource::File(file) => {
                let maximum_read_chunk = file.io_capabilities().maximum_read_chunk();
                let metadata = file.opened_metadata();
                close_file(*file).await?;
                Ok(self.facts(EntryKind::File, &metadata, maximum_read_chunk))
            }
            smb_domain::Resource::Directory(directory) => {
                let metadata = directory.opened_metadata();
                close_directory(directory).await?;
                Ok(self.facts(EntryKind::Directory, &metadata, u32::MAX))
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
        let metadata = file.opened_metadata();
        let facts = self.facts(
            EntryKind::File,
            &metadata,
            file.io_capabilities().maximum_read_chunk(),
        );
        Ok((Box::new(DomainReadCursor { file }), facts))
    }
}

#[async_trait]
impl CifsNamespaceProtocol for SmbDomainProtocol {
    async fn stat(&self, path: &StoragePath) -> smb_domain::Result<CifsSourceFacts> {
        CifsSourceProtocol::describe(self, path).await
    }

    async fn create_directory(&self, path: &StoragePath) -> smb_domain::Result<()> {
        let path = self.share_path(path)?;
        let directory = self
            .share
            .open_directory(&path, smb_domain::DirectoryOpenOptions::create_new())
            .await?;
        close_directory(directory).await
    }

    async fn remove(&self, path: &StoragePath) -> smb_domain::Result<()> {
        let kind = self.kind_of(path).await?;
        let path = self.share_path(path)?;
        if kind == EntryKind::Directory {
            let directory = self
                .share
                .open_directory(&path, smb_domain::DirectoryOpenOptions::open_existing())
                .await?;
            let deleted = directory.delete().await;
            let close = close_directory(directory).await;
            deleted?;
            close
        } else {
            let file = self
                .share
                .open_file(&path, smb_domain::FileOpenOptions::open_existing())
                .await?;
            let deleted = file.delete().await;
            let close = close_file(file).await;
            deleted?;
            close
        }
    }

    async fn rename_entry(&self, from: &StoragePath, to: &StoragePath) -> smb_domain::Result<()> {
        let kind = self.kind_of(from).await?;
        let from = self.share_path(from)?;
        let to = self.share_path(to)?;
        if kind == EntryKind::Directory {
            let directory = self
                .share
                .open_directory(&from, smb_domain::DirectoryOpenOptions::open_existing())
                .await?;
            let renamed = directory.rename_replace(&to).await;
            let close = close_directory(directory).await;
            renamed?;
            return close;
        }
        let file = self
            .share
            .open_file(&from, smb_domain::FileOpenOptions::open_existing())
            .await?;
        let renamed = file.rename_replace(&to).await;
        let close = close_file(file).await;
        renamed?;
        close
    }

    async fn list(
        &self,
        path: &StoragePath,
    ) -> smb_domain::Result<Vec<(StoragePath, CifsInlineMetadata)>> {
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
                // Reparse points stay `File`: the facade cannot read link targets, and
                // `Symlink` would route traversal into an unsupported `ReadLink`.
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
                            identity: identity_bytes(
                                kind,
                                entry.len(),
                                entry.written(),
                                entry.changed(),
                            ),
                            file_id: self.file_id(entry.file_id()),
                            maximum_read_chunk: u32::MAX,
                        },
                        accessed: entry.accessed(),
                        modified: entry.written(),
                        created: entry.created(),
                        readonly: Some(entry.is_readonly()),
                        reparse_point: entry.is_reparse_point(),
                    },
                ))
            })
            .collect()
    }
}

/// Error surfaced by [`ensure_root`] at connect time.
#[derive(Debug)]
pub(crate) enum CifsRootError {
    /// A component of the configured root exists but is not a directory.
    NotADirectory(String),
    Protocol(smb_domain::Error),
}

impl fmt::Display for CifsRootError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotADirectory(component) => write!(
                formatter,
                "CIFS root component '{component}' exists but is not a directory"
            ),
            Self::Protocol(error) => write!(formatter, "CIFS root check failed: {error}"),
        }
    }
}

impl std::error::Error for CifsRootError {}

impl From<smb_domain::Error> for CifsRootError {
    fn from(error: smb_domain::Error) -> Self {
        Self::Protocol(error)
    }
}

/// Minimal namespace verbs [`ensure_root`] needs, so the walk is unit-testable without a share.
#[async_trait]
pub(crate) trait RootProtocol: Send + Sync {
    /// `Ok(None)` when the path does not exist.
    async fn kind(&self, share_path: &str) -> smb_domain::Result<Option<EntryKind>>;
    async fn create_directory(&self, share_path: &str) -> smb_domain::Result<()>;
}

#[async_trait]
impl RootProtocol for smb_domain::Share {
    async fn kind(&self, share_path: &str) -> smb_domain::Result<Option<EntryKind>> {
        let path = smb_domain::SharePath::new(share_path)?;
        match self.open(&path).await {
            Ok(resource) => {
                let kind = resource_kind(&resource);
                close_resource(resource).await?;
                Ok(kind)
            }
            Err(error) if is_not_found(&error) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn create_directory(&self, share_path: &str) -> smb_domain::Result<()> {
        let path = smb_domain::SharePath::new(share_path)?;
        let directory = self
            .open_directory(&path, smb_domain::DirectoryOpenOptions::create_new())
            .await?;
        close_directory(directory).await
    }
}

/// Share-relative prefixes of `root` (`"a/b/c"` → `["a", "a\\b", "a\\b\\c"]`), ignoring empty
/// components so trailing or doubled separators do not produce invalid share paths.
pub(crate) fn root_prefixes(root: &str) -> Vec<String> {
    let mut prefixes = Vec::new();
    let mut accumulated = String::new();
    for component in root
        .split(['/', '\\'])
        .filter(|component| !component.is_empty())
    {
        if !accumulated.is_empty() {
            accumulated.push('\\');
        }
        accumulated.push_str(component);
        prefixes.push(accumulated.clone());
    }
    prefixes
}

/// Creates the missing components of the configured root sub-path (legacy
/// `ensure_root_exists`). An existing root costs one open; only a missing one walks the prefixes.
///
/// # Errors
/// Returns [`CifsRootError::NotADirectory`] when a component exists as a file or pipe and
/// [`CifsRootError::Protocol`] for any other server failure.
pub(crate) async fn ensure_root(
    protocol: &(impl RootProtocol + ?Sized),
    root: &str,
) -> Result<(), CifsRootError> {
    let prefixes = root_prefixes(root);
    let Some(full) = prefixes.last() else {
        return Ok(());
    };
    match protocol.kind(full).await? {
        Some(EntryKind::Directory) => return Ok(()),
        Some(_) => return Err(CifsRootError::NotADirectory(full.clone())),
        None => {}
    }
    for prefix in &prefixes {
        match protocol.kind(prefix).await? {
            Some(EntryKind::Directory) => continue,
            Some(_) => return Err(CifsRootError::NotADirectory(prefix.clone())),
            None => {}
        }
        match protocol.create_directory(prefix).await {
            Ok(()) => {}
            // Lost a race with another creator: accept it if it is a directory.
            Err(error) if is_collision(&error) => match protocol.kind(prefix).await? {
                Some(EntryKind::Directory) => {}
                _ => return Err(CifsRootError::NotADirectory(prefix.clone())),
            },
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn error_status(error: &smb_domain::Error) -> Option<u32> {
    match error {
        smb_domain::Error::ReceivedErrorMessage(status, _)
        | smb_domain::Error::UnexpectedMessageStatus(status) => Some(*status),
        _ => None,
    }
}

fn is_not_found(error: &smb_domain::Error) -> bool {
    matches!(error, smb_domain::Error::NotFound(_))
        || matches!(
            error_status(error),
            Some(STATUS_OBJECT_NAME_NOT_FOUND | STATUS_OBJECT_PATH_NOT_FOUND)
        )
}

fn is_collision(error: &smb_domain::Error) -> bool {
    error_status(error) == Some(STATUS_OBJECT_NAME_COLLISION)
}

const fn resource_kind(resource: &smb_domain::Resource) -> Option<EntryKind> {
    match resource {
        smb_domain::Resource::File(_) => Some(EntryKind::File),
        smb_domain::Resource::Directory(_) => Some(EntryKind::Directory),
        smb_domain::Resource::Pipe(_) => None,
    }
}

#[async_trait]
impl CifsStagedProtocol for SmbDomainProtocol {
    async fn create_empty(&self, path: &StoragePath) -> smb_domain::Result<()> {
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
        let metadata = file.opened_metadata();
        close_file(file).await?;
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
        let kind = resource_kind(&resource).unwrap_or(EntryKind::File);
        let metadata = resource.opened_metadata();
        close_resource(resource).await?;
        let metadata = metadata?;
        Ok(CifsInlineMetadata {
            facts: self.facts(kind, &metadata, u32::MAX),
            accessed: metadata.accessed(),
            modified: metadata.written(),
            created: metadata.created(),
            readonly: Some(metadata.is_readonly()),
            reparse_point: metadata.is_reparse_point(),
        })
    }

    async fn set_timestamps(
        &self,
        path: &StoragePath,
        value: crate::model::TimestampMetadata,
    ) -> smb_domain::Result<()> {
        let update = super::metadata::timestamp_update(value)?;
        if update == smb_domain::MetadataUpdate::default() {
            return Ok(());
        }
        let path = self.share_path(path)?;
        let resource = self
            .share
            .open_metadata(
                &path,
                smb_domain::MetadataOpenOptions::default().write_attributes(true),
            )
            .await?;
        let applied = resource.set_metadata(update).await;
        let close = close_resource(resource).await;
        applied?;
        close
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

impl SmbDomainProtocol {
    fn facts(
        &self,
        kind: EntryKind,
        metadata: &smb_domain::ResourceMetadata,
        maximum_read_chunk: u32,
    ) -> CifsSourceFacts {
        CifsSourceFacts {
            kind,
            size: metadata.len(),
            identity: identity_bytes(kind, metadata.len(), metadata.written(), metadata.changed()),
            file_id: self.file_id(metadata.file_id()),
            maximum_read_chunk,
        }
    }
}

/// Decides once per session whether file ids can be identity.
///
/// Lists the root once and opens it once (its `.` record answers for the listing path, so an
/// empty root is judged like any other). A session uses file ids only when the wide directory
/// class **and** the `QFid` create context both answered, because identity must be the same
/// whichever verb observed an entry; a server offering only one would make `List` and `Stat`
/// disagree on every entry.
///
/// This is a capability check, so it never fails the connect: any error (a root that is
/// write-only or does not exist yet, nothing the caller may open) and a listing with nothing
/// to judge (no `.` record and no entries) both switch file ids **off** on both paths, each
/// with a warning. Off is the conservative answer: defaulting to on would split identities
/// later on a server that accepts the wide class but not `QFid`.
pub(crate) async fn probe_identity_mode(share: &smb_domain::Share, root: Option<&str>) -> bool {
    let base = SmbDomainProtocol::new(share.clone(), root.map(str::to_owned), true);
    match probe_both_paths(&base).await {
        Ok(Some((listing, open))) if listing && open => true,
        Ok(Some((listing, open))) => {
            tracing::warn!(
                listing,
                open,
                "CIFS identity probe: file id available on only one path; identities fall back \
                 to path scope for this session"
            );
            false
        }
        Ok(None) => {
            tracing::warn!(
                "CIFS identity probe: root listing had nothing to judge; identities stay \
                 path-scoped"
            );
            false
        }
        Err(error) => {
            tracing::warn!(
                %error,
                "CIFS identity probe failed; identities fall back to path scope for this session"
            );
            false
        }
    }
}

/// `(listing_has_id, open_has_id)` for the root, or `None` when the root gave nothing to judge.
///
/// The listing half comes from the root's own `.` record when the server returns one, so an
/// empty root still gets a verdict; otherwise from its first entry. The open half comes from
/// opening the root itself, falling back to the first [`PROBE_CANDIDATES`] children only if the
/// root refuses (a listable but not openable root is unusual, a first child that is
/// `$RECYCLE.BIN` or another user's directory is not, and a single `ACCESS_DENIED` must not
/// switch rename detection off for the whole session).
async fn probe_both_paths(base: &SmbDomainProtocol) -> smb_domain::Result<Option<(bool, bool)>> {
    let root = StoragePath::root();
    let directory = base
        .share
        .open_directory(
            &base.share_path(&root)?,
            smb_domain::DirectoryOpenOptions::open_existing(),
        )
        .await?;
    let listing = probe_listing(&directory).await;
    let close = close_directory(directory).await;
    let listing = listing?;
    close?;
    let Some(listing_has_id) = listing
        .dot_has_id
        .or(listing.candidates.first().map(|c| c.1))
    else {
        return Ok(None);
    };
    let mut last_error = match CifsSourceProtocol::describe(base, &root).await {
        Ok(facts) => return Ok(Some((listing_has_id, facts.file_id.is_some()))),
        Err(error) => error,
    };
    for (child, _) in listing.candidates {
        match CifsSourceProtocol::describe(base, &child).await {
            Ok(facts) => return Ok(Some((listing_has_id, facts.file_id.is_some()))),
            Err(error) => {
                tracing::debug!(
                    %error,
                    path = child.as_str(),
                    "CIFS identity probe: candidate did not open"
                );
                last_error = error;
            }
        }
    }
    Err(last_error)
}

/// How many root entries the identity probe will try to open when the root itself refuses.
const PROBE_CANDIDATES: usize = 3;

/// What one listing of the root tells the identity probe.
struct ProbeListing {
    /// Whether the `.` record carried a file id; `None` when the server returned no `.` record.
    dot_has_id: Option<bool>,
    /// The first few real children, each with whether its record carried a file id.
    candidates: Vec<(StoragePath, bool)>,
}

/// Lists the root once for the identity probe.
///
/// The stream is drained to its end even though only a few records are kept. Dropping a
/// `QueryDirectoryStream` mid-way used to cancel a `QUERY_DIRECTORY` the facade had already
/// sent for the next page; the server answered it after the CLOSE with `STATUS_FILE_CLOSED`,
/// which the runtime treated as a fatal wire fault, and the next operation on the fresh
/// session failed in recovery (3/16 connects on FAS2750, 2026-09-18). smb-rs #76 fixed both
/// ends (pinned `8b10f35`); draining stays because it also spares one round trip nobody
/// would collect, at the price of the root's full listing once per connect, which `list`
/// pays for the same directory anyway.
async fn probe_listing(directory: &smb_domain::Directory) -> smb_domain::Result<ProbeListing> {
    let mut entries = directory.entries("*");
    let mut listing = ProbeListing {
        dot_has_id: None,
        candidates: Vec::with_capacity(PROBE_CANDIDATES),
    };
    while let Some(entry) = entries.next().await {
        let entry = entry?;
        match entry.name() {
            "." => listing.dot_has_id = Some(entry.file_id().is_some()),
            ".." => {}
            name if listing.candidates.len() < PROBE_CANDIDATES => {
                let child = child_path(&StoragePath::root(), name)?;
                listing.candidates.push((child, entry.file_id().is_some()));
            }
            _ => {}
        }
    }
    Ok(listing)
}

/// Content-version payload shared by `describe`, `open`, `metadata`, and `list`, so the same
/// entry hashes identically whichever verb observed it. It is also the path-scoped identity
/// when the server offers no file id.
///
/// The `cifs-path-identity:v2` prefix predates the file-id identity; it is kept because the
/// bytes are persisted in recovery bindings and snapshots, and renaming it would invalidate
/// them for no behavioural gain.
///
/// Directories hash a zero length: `FILE_DIRECTORY_INFORMATION` reports 0 for a subdirectory
/// while a directory handle's `EndOfFile` reports index allocation, so the real length would
/// make listing and stat disagree.
///
/// `v2` because the payload changed: `v1` listings hashed a different prefix and length only, and
/// `v1` directories hashed their real length. The bump keeps a snapshot persisted by an older
/// build distinguishable instead of silently comparing unequal.
pub(super) fn identity_bytes(
    kind: EntryKind,
    len: u64,
    written: SystemTime,
    changed: SystemTime,
) -> Bytes {
    let mut identity = BytesMut::with_capacity(40);
    identity.extend_from_slice(b"data-mover:cifs-path-identity:v2\0");
    identity.put_u64(if kind == EntryKind::Directory { 0 } else { len });
    put_time(&mut identity, written);
    put_time(&mut identity, changed);
    identity.freeze()
}

fn put_time(output: &mut BytesMut, value: std::time::SystemTime) {
    let nanos = value
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    output.extend_from_slice(&nanos.to_be_bytes());
}
