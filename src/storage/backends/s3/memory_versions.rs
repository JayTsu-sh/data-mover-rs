//! The versioned mode of [`MemoryS3`] (ADR-0006 C17): a bucket whose versioning is enabled or
//! suspended keeps every key's versions and delete markers in order, as S3 does.
//!
//! - A write (`PutObject`, a completion) adds a new current version: a minted id when enabled,
//!   the one `"null"` version (replacing the previous `"null"` entry) when suspended.
//! - An unversioned `DeleteObject` adds a delete marker the same way and is logged, so a test can
//!   prove none was sent to a final key.
//! - `delete_version` removes one entry for good (none is fine); the newest remaining entry becomes
//!   current. A key under a legal hold ([`MemoryS3::lock_versions`]) refuses it with
//!   `PermissionDenied`, as Object Lock does — an unversioned delete (a marker) is still allowed.
//! - `list_versions` reports a key's entries newest first.
//!
//! With versioning off (the default) the fake behaves as before, and lists an existing object as
//! version `"null"`.

use std::collections::{HashMap, HashSet};
use std::sync::{MutexGuard, PoisonError};

use bytes::Bytes;

use super::{MemoryS3, not_found};
use crate::model::{FailureClass, Transience};
use crate::storage::artifacts::is_artifact_path;
use crate::storage::backends::s3::{S3ObjectFacts, S3ProtocolFailure, S3Result, S3VersionFacts};

/// What [`MemoryS3::forget_version`] did.
enum Forgotten {
    /// The key has versions, none of them this one.
    Nothing,
    /// The key has no recorded versions.
    Unrecorded,
    /// Removed; the id of the newest remaining entry, if any.
    Removed(Option<String>),
}

/// A bucket's versioning state.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum Versioning {
    #[default]
    Off,
    Enabled,
    Suspended,
}

/// What the versioned mode keeps beside the fake's objects and version contents.
#[derive(Default)]
pub(crate) struct VersionedBucket {
    mode: Versioning,
    /// Version ids of each key, oldest first.
    order: HashMap<String, Vec<String>>,
    /// Keys whose versions refuse deletion (a legal hold on every version).
    locked: HashSet<String>,
    /// Single versions that refuse deletion (a legal hold on one version).
    locked_versions: HashSet<(String, String)>,
    /// Keys an unversioned `DeleteObject` was sent to.
    plain_deletes: Vec<String>,
    /// (key, version) of every `delete_version` that removed an entry.
    version_deletes: Vec<(String, String)>,
    next: u64,
}

impl VersionedBucket {
    /// The id the next write or marker gets, and the id it replaces (suspended: the `"null"` one).
    fn mint(&mut self, key: &str) -> String {
        if self.mode == Versioning::Suspended {
            if let Some(ids) = self.order.get_mut(key) {
                ids.retain(|id| id != "null");
            }
            return "null".to_string();
        }
        self.next += 1;
        format!("v{:04}", self.next)
    }

    fn push(&mut self, key: &str, id: &str) {
        self.order
            .entry(key.to_string())
            .or_default()
            .push(id.to_string());
    }

    fn newest(&self, key: &str) -> Option<String> {
        self.order.get(key)?.last().cloned()
    }
}

impl MemoryS3 {
    fn bucket(&self) -> MutexGuard<'_, VersionedBucket> {
        self.bucket.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn versioning(&self) -> Versioning {
        self.bucket().mode
    }

    /// Switches the bucket's versioning (enabling or suspending keeps what is stored).
    pub(crate) fn set_versioning(&self, mode: Versioning) {
        self.bucket().mode = mode;
    }

    /// Places a legal hold on every version of `key`: deleting one is refused.
    pub(crate) fn lock_versions(&self, key: &str) {
        self.bucket().locked.insert(key.to_string());
    }

    /// Places a legal hold on version `id` of `key` only.
    pub(crate) fn lock_version(&self, key: &str, id: &str) {
        self.bucket()
            .locked_versions
            .insert((key.to_string(), id.to_string()));
    }

    /// Keys an unversioned `DeleteObject` was sent to that are not transfer artifacts: final keys,
    /// which ADR-0006 C17 never deletes so.
    pub(crate) fn plain_deletes_of_final_keys(&self) -> Vec<String> {
        self.bucket()
            .plain_deletes
            .iter()
            .filter(|key| !is_artifact_path(key))
            .cloned()
            .collect()
    }

    /// Every key that holds a version or a delete marker, sorted.
    pub(crate) fn keys_with_versions(&self) -> Vec<String> {
        let mut keys: Vec<String> = self
            .bucket()
            .order
            .iter()
            .filter(|(_, ids)| !ids.is_empty())
            .map(|(key, _)| key.clone())
            .collect();
        keys.sort();
        keys
    }

    /// (key, version) of every version deleted for good.
    pub(crate) fn version_deletes(&self) -> Vec<(String, String)> {
        self.bucket().version_deletes.clone()
    }

    /// Records `id` as the newest entry of `key` when the bucket is versioned.
    pub(super) fn record_version(&self, key: &str, id: &str) {
        let mut bucket = self.bucket();
        if bucket.mode != Versioning::Off {
            if let Some(ids) = bucket.order.get_mut(key) {
                ids.retain(|existing| existing != id);
            }
            bucket.push(key, id);
        }
    }

    /// A write in a versioned bucket: stores `bytes` as a new current version and returns its id;
    /// `None` when versioning is off.
    pub(super) async fn versioned_store(&self, key: &str, bytes: &Bytes) -> Option<String> {
        let id = {
            let mut bucket = self.bucket();
            if bucket.mode == Versioning::Off {
                return None;
            }
            let id = bucket.mint(key);
            bucket.push(key, &id);
            id
        };
        self.versions
            .lock()
            .await
            .insert((key.to_string(), id.clone()), Some(bytes.clone()));
        self.objects
            .lock()
            .await
            .insert(key.to_string(), bytes.clone());
        Some(id)
    }

    /// An unversioned `DeleteObject` in a versioned bucket: adds a delete marker. `false` when
    /// versioning is off (the caller removes the object).
    pub(super) async fn versioned_delete(&self, key: &str) -> bool {
        let id = {
            let mut bucket = self.bucket();
            bucket.plain_deletes.push(key.to_string());
            if bucket.mode == Versioning::Off {
                return false;
            }
            let id = bucket.mint(key);
            bucket.push(key, &id);
            id
        };
        self.versions
            .lock()
            .await
            .insert((key.to_string(), id), None);
        self.objects.lock().await.remove(key);
        true
    }

    /// HEAD of the current object in a versioned bucket; `None` when versioning is off.
    pub(super) async fn versioned_head(&self, key: &str) -> Option<S3Result<S3ObjectFacts>> {
        if self.versioning() == Versioning::Off {
            return None;
        }
        let newest = self.bucket().newest(key);
        let Some(id) = newest else {
            // An object written before versioning was enabled is version "null".
            let object = self.objects.lock().await.get(key).cloned();
            return Some(object.ok_or_else(not_found).map(|bytes| S3ObjectFacts {
                size: bytes.len() as u64,
                etag: self.etag_for(&bytes),
                version_id: Some("null".to_string()),
                last_modified: None,
            }));
        };
        let stored = self
            .versions
            .lock()
            .await
            .get(&(key.to_string(), id.clone()))
            .cloned()
            .flatten();
        let last_modified = *self.last_modified.lock().await;
        Some(stored.ok_or_else(not_found).map(|bytes| S3ObjectFacts {
            size: bytes.len() as u64,
            etag: self.etag_for(&bytes),
            version_id: Some(id),
            last_modified,
        }))
    }

    pub(super) async fn delete_one_version(&self, key: &str, version_id: &str) -> S3Result<()> {
        let current = match self.forget_version(key, version_id)? {
            Forgotten::Nothing => return Ok(()),
            Forgotten::Unrecorded => return self.delete_unversioned_null(key, version_id).await,
            Forgotten::Removed(current) => current,
        };
        let mut versions = self.versions.lock().await;
        versions.remove(&(key.to_string(), version_id.to_string()));
        let body = current.and_then(|id| versions.get(&(key.to_string(), id)).cloned().flatten());
        drop(versions);
        let mut objects = self.objects.lock().await;
        match body {
            Some(body) => objects.insert(key.to_string(), body),
            None => objects.remove(key),
        };
        Ok(())
    }

    /// Removes `version_id` from the order of `key`, unless Object Lock holds the key.
    fn forget_version(&self, key: &str, version_id: &str) -> S3Result<Forgotten> {
        let mut bucket = self.bucket();
        let held = (key.to_string(), version_id.to_string());
        if bucket.locked.contains(key) || bucket.locked_versions.contains(&held) {
            return Err(S3ProtocolFailure::entry(
                FailureClass::PermissionDenied,
                Transience::Permanent,
                "the S3 version is protected by Object Lock",
            ));
        }
        let Some(ids) = bucket.order.get_mut(key) else {
            return Ok(Forgotten::Unrecorded);
        };
        let before = ids.len();
        ids.retain(|id| id != version_id);
        if ids.len() == before {
            return Ok(Forgotten::Nothing);
        }
        let newest = ids.last().cloned();
        bucket
            .version_deletes
            .push((key.to_string(), version_id.to_string()));
        Ok(Forgotten::Removed(newest))
    }

    /// A key with no recorded versions (versioning off): `?versionId=null` removes the object
    /// (`MinIO` accepts it in an unversioned bucket too), and so does the id of a version the older
    /// single-version model stored (`put_version`, or a write while its current version is real).
    async fn delete_unversioned_null(&self, key: &str, version_id: &str) -> S3Result<()> {
        let stored = self
            .versions
            .lock()
            .await
            .remove(&(key.to_string(), version_id.to_string()));
        let mut objects = self.objects.lock().await;
        let removed = match stored {
            Some(body) if objects.get(key) == body.as_ref() => objects.remove(key).is_some(),
            Some(_) => true,
            None => version_id == "null" && objects.remove(key).is_some(),
        };
        drop(objects);
        if removed {
            self.bucket()
                .version_deletes
                .push((key.to_string(), version_id.to_string()));
        }
        Ok(())
    }

    /// Every entry of `key`, newest first; an unversioned bucket lists its object as `"null"`.
    pub(super) async fn versions_of(&self, key: &str) -> S3Result<Vec<S3VersionFacts>> {
        let ids = self.bucket().order.get(key).cloned().unwrap_or_default();
        if self.versioning() == Versioning::Off && ids.is_empty() {
            let object = self.objects.lock().await.get(key).cloned();
            return Ok(object
                .map(|bytes| self.version_facts("null", Some(bytes), true))
                .into_iter()
                .collect());
        }
        let versions = self.versions.lock().await;
        let count = ids.len();
        Ok(ids
            .iter()
            .enumerate()
            .rev()
            .map(|(index, id)| {
                let body = versions
                    .get(&(key.to_string(), id.clone()))
                    .cloned()
                    .flatten();
                self.version_facts(id, body, index + 1 == count)
            })
            .collect())
    }

    fn version_facts(&self, id: &str, body: Option<Bytes>, is_latest: bool) -> S3VersionFacts {
        S3VersionFacts {
            version_id: id.to_string(),
            is_latest,
            delete_marker: body.is_none(),
            size: body.as_ref().map_or(0, |bytes| bytes.len() as u64),
            etag: body.map(|bytes| self.etag_for(&bytes)).unwrap_or_default(),
        }
    }
}
