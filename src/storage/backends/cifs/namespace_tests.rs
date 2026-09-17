use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;

use super::metadata::CifsInlineMetadata;
use super::namespace::{CifsNamespace, CifsNamespaceProtocol};
use super::protocol::{CifsRootError, RootProtocol, ensure_root, identity_bytes, root_prefixes};
use super::source::CifsSourceFacts;
use crate::model::{BackendIdentity, BackendKind, EntryKind, FailureClass, StoragePath};
use crate::storage::{Namespace, NamespaceRequest, NamespaceResult, StorageRoleFailure};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

const STATUS_OBJECT_NAME_NOT_FOUND: u32 = 0xC000_0034;
const STATUS_OBJECT_NAME_COLLISION: u32 = 0xC000_0035;
const STATUS_DIRECTORY_NOT_EMPTY: u32 = 0xC000_0101;

struct RecordingProtocol {
    calls: Mutex<Vec<String>>,
    fail_status: Option<u32>,
}

impl RecordingProtocol {
    fn new(fail_status: Option<u32>) -> Arc<Self> {
        Arc::new(Self {
            calls: Mutex::new(Vec::new()),
            fail_status,
        })
    }

    fn record(&self, call: String) -> smb_domain::Result<()> {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(call);
        match self.fail_status {
            Some(status) => Err(smb_domain::Error::UnexpectedMessageStatus(status)),
            None => Ok(()),
        }
    }

    fn calls(&self) -> Vec<String> {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

#[async_trait]
impl CifsNamespaceProtocol for RecordingProtocol {
    async fn stat(&self, path: &StoragePath) -> smb_domain::Result<CifsSourceFacts> {
        self.record(format!("stat {}", path.as_str()))?;
        Ok(CifsSourceFacts {
            kind: if path.as_str().starts_with("file") {
                EntryKind::File
            } else {
                EntryKind::Directory
            },
            size: 7,
            identity: Bytes::from_static(b"stat-identity"),
            maximum_read_chunk: 4,
        })
    }

    async fn list(
        &self,
        path: &StoragePath,
    ) -> smb_domain::Result<Vec<(StoragePath, CifsInlineMetadata)>> {
        self.record(format!("list {}", path.as_str()))?;
        Ok(Vec::new())
    }

    async fn create_directory(&self, path: &StoragePath) -> smb_domain::Result<()> {
        self.record(format!("mkdir {}", path.as_str()))
    }

    async fn remove(&self, path: &StoragePath) -> smb_domain::Result<()> {
        self.record(format!("remove {}", path.as_str()))
    }

    async fn rename_entry(&self, from: &StoragePath, to: &StoragePath) -> smb_domain::Result<()> {
        self.record(format!("rename {} -> {}", from.as_str(), to.as_str()))
    }
}

fn namespace(protocol: &Arc<RecordingProtocol>) -> Result<CifsNamespace> {
    Ok(CifsNamespace::new(
        Arc::clone(protocol),
        BackendIdentity::new(BackendKind::Cifs, "cifs-namespace-test")?,
    ))
}

fn path(value: &str) -> Result<StoragePath> {
    Ok(StoragePath::new(value)?)
}

fn class(failure: &StorageRoleFailure) -> FailureClass {
    match failure {
        StorageRoleFailure::Entry(failure) => failure.class(),
        StorageRoleFailure::Session(failure) => failure.class(),
    }
}

fn failure_class(
    outcome: std::result::Result<NamespaceResult, StorageRoleFailure>,
) -> Result<FailureClass> {
    match outcome {
        Err(failure) => Ok(class(&failure)),
        Ok(result) => Err(format!("expected a failure, got {result:?}").into()),
    }
}

#[tokio::test]
async fn stat_returns_one_descriptor_for_the_requested_path() -> Result {
    let protocol = RecordingProtocol::new(None);
    let result = namespace(&protocol)?
        .execute(NamespaceRequest::Stat(path("file.bin")?))
        .await?;
    let NamespaceResult::Entries(entries) = result else {
        return Err("stat must return entries".into());
    };
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].path.as_str(), "file.bin");
    assert_eq!(entries[0].kind, EntryKind::File);
    assert_eq!(entries[0].size, Some(7));
    assert_eq!(protocol.calls(), vec!["stat file.bin"]);
    Ok(())
}

#[tokio::test]
async fn mutating_verbs_complete_and_forward_paths() -> Result {
    let protocol = RecordingProtocol::new(None);
    let namespace = namespace(&protocol)?;
    for request in [
        NamespaceRequest::CreateDirectory(path("a/b")?),
        NamespaceRequest::Rename {
            from: path("a/b/x.bin")?,
            to: path("a/b/y.bin")?,
        },
        NamespaceRequest::Delete(path("a/b/y.bin")?),
        NamespaceRequest::Delete(path("a/b")?),
    ] {
        assert_eq!(
            namespace.execute(request).await?,
            NamespaceResult::Completed
        );
    }
    assert_eq!(
        protocol.calls(),
        vec![
            "mkdir a/b",
            "rename a/b/x.bin -> a/b/y.bin",
            "remove a/b/y.bin",
            "remove a/b",
        ]
    );
    Ok(())
}

#[tokio::test]
async fn read_link_is_a_typed_unsupported_refusal_without_protocol_calls() -> Result {
    let protocol = RecordingProtocol::new(None);
    let outcome = namespace(&protocol)?
        .execute(NamespaceRequest::ReadLink(path("link")?))
        .await;
    assert_eq!(failure_class(outcome)?, FailureClass::Unsupported);
    assert!(protocol.calls().is_empty());
    Ok(())
}

#[tokio::test]
async fn object_name_not_found_maps_to_not_found_for_stat_and_delete() -> Result {
    let protocol = RecordingProtocol::new(Some(STATUS_OBJECT_NAME_NOT_FOUND));
    let namespace = namespace(&protocol)?;
    for request in [
        NamespaceRequest::Stat(path("missing")?),
        NamespaceRequest::Delete(path("missing")?),
    ] {
        let outcome = namespace.execute(request).await;
        assert_eq!(failure_class(outcome)?, FailureClass::NotFound);
    }
    Ok(())
}

#[tokio::test]
async fn name_collision_on_create_directory_maps_to_conflict() -> Result {
    let protocol = RecordingProtocol::new(Some(STATUS_OBJECT_NAME_COLLISION));
    let outcome = namespace(&protocol)?
        .execute(NamespaceRequest::CreateDirectory(path("exists")?))
        .await;
    assert_eq!(failure_class(outcome)?, FailureClass::Conflict);
    Ok(())
}

#[tokio::test]
async fn deleting_a_populated_directory_maps_to_conflict() -> Result {
    let protocol = RecordingProtocol::new(Some(STATUS_DIRECTORY_NOT_EMPTY));
    let outcome = namespace(&protocol)?
        .execute(NamespaceRequest::Delete(path("populated")?))
        .await;
    assert_eq!(failure_class(outcome)?, FailureClass::Conflict);
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// ensure_root walk over an in-memory share
// ---------------------------------------------------------------------------------------------

struct MemoryShare {
    entries: Mutex<Vec<(String, EntryKind)>>,
    calls: Mutex<Vec<String>>,
    collide_once: Mutex<Option<String>>,
}

impl MemoryShare {
    fn new(existing: &[(&str, EntryKind)]) -> Self {
        Self {
            entries: Mutex::new(
                existing
                    .iter()
                    .map(|(path, kind)| ((*path).to_owned(), *kind))
                    .collect(),
            ),
            calls: Mutex::new(Vec::new()),
            collide_once: Mutex::new(None),
        }
    }

    fn calls(&self) -> Vec<String> {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn record(&self, call: String) {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(call);
    }
}

#[async_trait]
impl RootProtocol for MemoryShare {
    async fn kind(&self, share_path: &str) -> smb_domain::Result<Option<EntryKind>> {
        self.record(format!("kind {share_path}"));
        Ok(self
            .entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .find(|(path, _)| path == share_path)
            .map(|(_, kind)| *kind))
    }

    async fn create_directory(&self, share_path: &str) -> smb_domain::Result<()> {
        self.record(format!("mkdir {share_path}"));
        let collide = self
            .collide_once
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take_if(|pending| pending == share_path);
        if collide.is_some() {
            // Another client created it first.
            self.entries
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push((share_path.to_owned(), EntryKind::Directory));
            return Err(smb_domain::Error::UnexpectedMessageStatus(
                STATUS_OBJECT_NAME_COLLISION,
            ));
        }
        self.entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((share_path.to_owned(), EntryKind::Directory));
        Ok(())
    }
}

#[test]
fn root_prefixes_normalise_separators_and_drop_empty_components() {
    assert_eq!(root_prefixes("a/b/c"), ["a", "a\\b", "a\\b\\c"]);
    assert_eq!(root_prefixes("/a//b\\c/"), ["a", "a\\b", "a\\b\\c"]);
    assert!(root_prefixes("").is_empty());
    assert!(root_prefixes("///").is_empty());
}

#[tokio::test]
async fn ensure_root_costs_one_probe_when_the_root_exists() -> Result {
    let share = MemoryShare::new(&[("a\\b", EntryKind::Directory)]);
    ensure_root(&share, "a/b").await?;
    assert_eq!(share.calls(), ["kind a\\b"]);
    Ok(())
}

#[tokio::test]
async fn ensure_root_creates_only_the_missing_components() -> Result {
    let share = MemoryShare::new(&[("a", EntryKind::Directory)]);
    ensure_root(&share, "a/b/c").await?;
    assert_eq!(
        share.calls(),
        [
            "kind a\\b\\c",
            "kind a",
            "kind a\\b",
            "mkdir a\\b",
            "kind a\\b\\c",
            "mkdir a\\b\\c"
        ]
    );
    Ok(())
}

#[tokio::test]
async fn ensure_root_accepts_a_lost_creation_race() -> Result {
    let share = MemoryShare::new(&[]);
    *share
        .collide_once
        .lock()
        .unwrap_or_else(PoisonError::into_inner) = Some("a".to_owned());
    ensure_root(&share, "a").await?;
    assert_eq!(share.calls(), ["kind a", "kind a", "mkdir a", "kind a"]);
    Ok(())
}

#[tokio::test]
async fn ensure_root_rejects_a_file_in_the_path_without_creating_anything() -> Result {
    let share = MemoryShare::new(&[("a", EntryKind::File)]);
    let Err(error) = ensure_root(&share, "a/b").await else {
        return Err("a file component must be rejected".into());
    };
    assert!(matches!(error, CifsRootError::NotADirectory(ref component) if component == "a"));
    assert!(share.calls().iter().all(|call| !call.starts_with("mkdir")));
    Ok(())
}

#[tokio::test]
async fn ensure_root_with_an_empty_root_is_a_no_op() -> Result {
    let share = MemoryShare::new(&[]);
    ensure_root(&share, "").await?;
    assert!(share.calls().is_empty());
    Ok(())
}

#[test]
fn identity_bytes_track_content_change_and_ignore_directory_length() {
    let base = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
    let later = base + std::time::Duration::from_secs(1);
    let file = identity_bytes(EntryKind::File, 10, base, base);
    assert_eq!(file, identity_bytes(EntryKind::File, 10, base, base));
    assert_ne!(file, identity_bytes(EntryKind::File, 11, base, base));
    assert_ne!(file, identity_bytes(EntryKind::File, 10, later, base));
    assert_ne!(file, identity_bytes(EntryKind::File, 10, base, later));
    assert_eq!(
        identity_bytes(EntryKind::Directory, 0, base, base),
        identity_bytes(EntryKind::Directory, 4096, base, base),
        "listing reports 0 while a directory handle reports index allocation"
    );
}

/// Listing protocol whose records carry the read-only attribute, as `QUERY_DIRECTORY` does.
struct ListingProtocol {
    entries: Vec<(String, EntryKind, Option<bool>)>,
}

#[async_trait]
impl CifsNamespaceProtocol for ListingProtocol {
    async fn stat(&self, _path: &StoragePath) -> smb_domain::Result<CifsSourceFacts> {
        Ok(CifsSourceFacts {
            kind: EntryKind::File,
            size: 7,
            identity: Bytes::from_static(b"stat-identity"),
            maximum_read_chunk: 4,
        })
    }

    async fn list(
        &self,
        path: &StoragePath,
    ) -> smb_domain::Result<Vec<(StoragePath, CifsInlineMetadata)>> {
        let stamp = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
        self.entries
            .iter()
            .map(|(name, kind, readonly)| {
                let child = StoragePath::new(format!("{}/{name}", path.as_str()))
                    .map_err(|_| smb_domain::Error::InvalidArgument("invalid path".into()))?;
                Ok((
                    child,
                    CifsInlineMetadata {
                        facts: CifsSourceFacts {
                            kind: *kind,
                            size: 3,
                            identity: Bytes::from_static(b"listed"),
                            maximum_read_chunk: u32::MAX,
                        },
                        accessed: stamp,
                        modified: stamp,
                        created: stamp,
                        readonly: *readonly,
                    },
                ))
            })
            .collect()
    }

    async fn create_directory(&self, _path: &StoragePath) -> smb_domain::Result<()> {
        Ok(())
    }

    async fn remove(&self, _path: &StoragePath) -> smb_domain::Result<()> {
        Ok(())
    }

    async fn rename_entry(&self, _from: &StoragePath, _to: &StoragePath) -> smb_domain::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn listing_derives_mode_from_the_read_only_attribute_per_entry() -> Result {
    let protocol = Arc::new(ListingProtocol {
        entries: vec![
            ("locked.bin".to_owned(), EntryKind::File, Some(true)),
            ("plain.bin".to_owned(), EntryKind::File, Some(false)),
            ("locked.dir".to_owned(), EntryKind::Directory, Some(true)),
            ("plain.dir".to_owned(), EntryKind::Directory, Some(false)),
            ("unknown.bin".to_owned(), EntryKind::File, None),
        ],
    });
    let namespace = CifsNamespace::new(
        protocol,
        BackendIdentity::new(BackendKind::Cifs, "cifs-readonly-test")?,
    );
    let NamespaceResult::Entries(entries) = namespace
        .execute(NamespaceRequest::List(path("dir")?))
        .await?
    else {
        return Err("list must return entries".into());
    };
    let observed: Vec<Option<u32>> = entries
        .iter()
        .map(crate::storage::SourceDescriptor::inline_mode)
        .collect();
    assert_eq!(
        observed,
        vec![Some(0o444), Some(0o644), Some(0o555), Some(0o755), None,],
        "a record without the attribute stays None instead of claiming the entry is writable"
    );
    Ok(())
}

#[tokio::test]
async fn stat_never_claims_permission_bits() -> Result {
    let protocol = RecordingProtocol::new(None);
    let NamespaceResult::Entries(entries) = namespace(&protocol)?
        .execute(NamespaceRequest::Stat(path("file.bin")?))
        .await?
    else {
        return Err("stat must return entries".into());
    };
    assert_eq!(
        entries[0].inline_mode(),
        None,
        "a metadata open carries no attribute bits, so stat must not invent permission bits"
    );
    Ok(())
}
