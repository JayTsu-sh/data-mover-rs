//! `create_directory_all` driving the NFS namespace adapter level by level (the adapter's
//! `create_directory` creates one level, `src/nfs/role_namespace.rs`), with the server refusals
//! classified as the NFS role classifies them.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;
use nfs_rs::{DirectoryCookie, Nfs3ErrorCode as V3, Nfs4ErrorCode as V4, NfsError};

use super::namespace::{
    NfsListingPages, NfsNamespaceAdapter, NfsNamespaceObservation, NfsNamespaceProtocol,
    NfsReadEnd, NfsReadFailure,
};
use super::protocol::classify_error;
use super::source::NfsProtocolFailure;
use crate::model::{
    BackendIdentity, BackendKind, EntryKind, FailureClass, StoragePath, Transience,
};
use crate::storage::create_dir::create_directory_all_with_namespace;
use crate::storage::{CreateDirectoryAllFailure, StorageRoleFailure};

/// A server that checks write access on the parent before the name, the way the permission
/// scenario below needs: `ro` is 0555 (below the root, also read-only), `ro/w` is 0777.
struct AccessFirst {
    directories: Mutex<HashSet<String>>,
    /// Parents in which every MKDIR is refused, existing name or not.
    read_only: HashSet<&'static str>,
    refusal: fn() -> NfsError,
    calls: Mutex<Vec<String>>,
}

impl AccessFirst {
    fn new(refusal: fn() -> NfsError, read_only: &[&'static str]) -> Arc<Self> {
        Arc::new(Self {
            directories: Mutex::new(["ro", "ro/w"].map(str::to_owned).into_iter().collect()),
            read_only: read_only.iter().copied().collect(),
            refusal,
            calls: Mutex::new(Vec::new()),
        })
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

fn unused() -> NfsProtocolFailure {
    NfsProtocolFailure::new(FailureClass::Unsupported, Transience::Permanent)
}

#[async_trait]
impl NfsNamespaceProtocol for AccessFirst {
    async fn stat(
        &self,
        path: &StoragePath,
    ) -> Result<NfsNamespaceObservation, NfsProtocolFailure> {
        self.record(format!("stat {}", path.as_str()));
        let directories = self
            .directories
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if !directories.contains(path.as_str()) {
            return Err(classify_error(NfsError::Nfs3(V3::NFS3ERR_NOENT)));
        }
        Ok(NfsNamespaceObservation {
            path: path.clone(),
            kind: EntryKind::Directory,
            size: None,
            file_handle: Bytes::from(path.as_str().to_owned()),
            timestamps: None,
            mode: None,
            ownership: None,
        })
    }

    async fn directory_handle(&self, _path: &StoragePath) -> Result<Bytes, NfsProtocolFailure> {
        Err(unused())
    }

    async fn read_directory(
        &self,
        _path: &StoragePath,
        _handle: Bytes,
        _from: DirectoryCookie,
        _pages: &mut dyn NfsListingPages,
    ) -> Result<NfsReadEnd, NfsReadFailure> {
        Ok(NfsReadEnd::Complete)
    }

    async fn forget_directory(&self, _path: &StoragePath) -> Result<(), NfsProtocolFailure> {
        Ok(())
    }

    async fn read_link(&self, _path: &StoragePath) -> Result<Bytes, NfsProtocolFailure> {
        Err(unused())
    }

    async fn create_directory(&self, path: &StoragePath) -> Result<(), NfsProtocolFailure> {
        self.record(format!("mkdir {}", path.as_str()));
        let parent = path
            .as_str()
            .rsplit_once('/')
            .map_or("", |(parent, _)| parent);
        if self.read_only.contains(parent) {
            return Err(classify_error((self.refusal)()));
        }
        self.directories
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(path.as_str().to_owned());
        Ok(())
    }

    async fn delete(
        &self,
        _path: &StoragePath,
        _kind: EntryKind,
    ) -> Result<(), NfsProtocolFailure> {
        Err(unused())
    }

    async fn rename(
        &self,
        _from: &StoragePath,
        _to: &StoragePath,
    ) -> Result<(), NfsProtocolFailure> {
        Err(unused())
    }
}

fn adapter(protocol: Arc<AccessFirst>) -> NfsNamespaceAdapter {
    let identity = BackendIdentity::new(BackendKind::Nfs, "nfs-create-dir-test")
        .unwrap_or_else(|error| panic!("{error}"));
    NfsNamespaceAdapter::new(protocol, identity)
}

fn path(value: &str) -> StoragePath {
    StoragePath::new(value).unwrap_or_else(|error| panic!("{error}"))
}

/// A refusal's status name, the refusal, and the parents in which every MKDIR gets it.
type Refusal = (&'static str, fn() -> NfsError, &'static [&'static str]);

/// The refusals an NFS server gives a MKDIR it will not do: a mode that denies the caller, and
/// a read-only export (every parent refuses).
const REFUSALS: [Refusal; 4] = [
    (
        "NFS3ERR_ACCES",
        || NfsError::Nfs3(V3::NFS3ERR_ACCES),
        &["", "ro"],
    ),
    (
        "NFS4ERR_ACCESS",
        || NfsError::Nfs4(V4::NFS4ERR_ACCESS),
        &["", "ro"],
    ),
    (
        "NFS3ERR_ROFS",
        || NfsError::Nfs3(V3::NFS3ERR_ROFS),
        &["", "ro", "ro/w"],
    ),
    (
        "NFS4ERR_ROFS",
        || NfsError::Nfs4(V4::NFS4ERR_ROFS),
        &["", "ro", "ro/w"],
    ),
];

/// Every NFS level now goes through `create_directory_all`'s rule: a refused level that is
/// already a directory is passed, intermediate or leaf.
#[tokio::test]
async fn refused_existing_levels_are_passed_on_nfs() {
    for (status, refusal, read_only) in REFUSALS {
        let protocol = AccessFirst::new(refusal, read_only);
        let namespace = adapter(Arc::clone(&protocol));
        create_directory_all_with_namespace(&namespace, &path("ro/w"))
            .await
            .unwrap_or_else(|error| panic!("{status}: {error:?}"));
        assert_eq!(
            protocol.calls(),
            ["mkdir ro", "stat ro", "mkdir ro/w", "stat ro/w"],
            "{status}"
        );
    }
}

/// The scenario the real-server contract reproduces: the new level below the writable `ro/w`
/// is created, and a new level in the read-only `ro` is refused there, permanently.
#[tokio::test]
async fn a_new_level_below_a_writable_directory_is_created_and_one_in_a_read_only_one_refused() {
    for (status, refusal, read_only) in REFUSALS {
        let protocol = AccessFirst::new(refusal, read_only);
        let namespace = adapter(Arc::clone(&protocol));
        let writable = create_directory_all_with_namespace(&namespace, &path("ro/w/new")).await;
        if read_only.contains(&"ro/w") {
            // A read-only export: nothing new can be made anywhere.
            assert!(writable.is_err(), "{status}");
        } else {
            writable.unwrap_or_else(|error| panic!("{status}: {error:?}"));
        }
        let refused = create_directory_all_with_namespace(&namespace, &path("ro/new")).await;
        let Err(CreateDirectoryAllFailure::Role(StorageRoleFailure::Entry(failure))) = refused
        else {
            panic!("{status}: expected an entry failure, got {refused:?}");
        };
        assert_eq!(failure.class(), FailureClass::PermissionDenied, "{status}");
        assert_eq!(failure.transience(), Transience::Permanent, "{status}");
        assert_eq!(failure.path().as_str(), "ro/new", "{status}");
        assert!(
            failure.diagnostic().contains(status),
            "{status}: {}",
            failure.diagnostic()
        );
    }
}
