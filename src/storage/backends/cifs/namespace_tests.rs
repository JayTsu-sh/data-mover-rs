use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;

use super::namespace::{CifsNamespace, CifsNamespaceProtocol};
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
    ) -> smb_domain::Result<Vec<(StoragePath, CifsSourceFacts)>> {
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
