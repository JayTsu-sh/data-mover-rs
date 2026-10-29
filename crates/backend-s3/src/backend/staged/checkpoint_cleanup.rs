//! Strict cleanup of owned hash-pointer history, including its initial v1 placeholder.
//! Version deletion never creates a marker or removes final content. Legacy stages retain
//! their separate warning-only deletion path.

use super::super::paging::{ListingLimits, PageRetry, Paging};
use super::super::source::{classified_entry, role_failure};
use super::super::{S3ObjectFacts, S3Protocol, S3VersionFacts, S3VersionMarker, S3VersionPage};
use super::final_upload::FinalUpload;
use super::upload_pointer::{self, NONCE_BYTES, UploadRecord};
use super::{S3StagedDestination, cleanup_result};
use crate::model::{FailureClass, Operation, StoragePath, Transience};
use crate::storage::pointer::{DestinationPointer, MAX_POINTER_BYTES};
use crate::storage::{PreparedStage, StorageRoleFailure};

fn conflict(stage: &PreparedStage) -> StorageRoleFailure {
    classified_entry(
        stage.final_destination().path(),
        Operation::Namespace,
        FailureClass::Conflict,
        Transience::Permanent,
        "S3 checkpoint cleanup cannot prove ownership of the current root or marker",
    )
}

struct Ownership {
    binding: [u8; 32],
    transfer: [u8; 32],
    upload_id: String,
    part_size: u64,
    nonce: [u8; NONCE_BYTES],
}

impl Ownership {
    fn new(stage: &PreparedStage, upload: &FinalUpload) -> Self {
        Self {
            binding: stage.recovery_binding(),
            transfer: upload.transfer_identity,
            upload_id: upload.upload_id.clone(),
            part_size: upload.part_size as u64,
            nonce: upload.nonce,
        }
    }

    /// Only this attempt's nonce fences the current root, never an earlier incarnation's.
    fn owns_current(&self, bytes: &[u8]) -> bool {
        self.same_upload(bytes)
            .is_some_and(|record| record.nonce == self.nonce)
    }

    /// Once fenced by our current root, all bound incarnations of the exact same multipart
    /// upload are owned history. An arbitrarily old nonce need not survive in process memory.
    fn same_upload(&self, bytes: &[u8]) -> Option<UploadRecord> {
        let pointer = DestinationPointer::decode(bytes).ok()?;
        if pointer.binding != self.binding || pointer.transfer_identity != self.transfer {
            return None;
        }
        let record = upload_pointer::accepted(&pointer)?;
        (record.upload_id == self.upload_id && record.part_size == self.part_size).then_some(record)
    }
}

/// At most two service pages of work are held. Budget exhaustion is a typed failure, never
/// successful truncation, and leaves the current root available for another cleanup attempt.
struct VersionWalk {
    paging: Paging<S3VersionMarker>,
    retry: PageRetry,
    marker: Option<S3VersionMarker>,
    done: bool,
}

impl VersionWalk {
    fn new() -> Self {
        let limits = ListingLimits {
            max_entries: 2048,
            max_continuations: 16_384,
            ..ListingLimits::PRODUCTION
        };
        Self {
            paging: Paging::new(limits),
            retry: PageRetry::new(limits),
            marker: None,
            done: false,
        }
    }

    async fn next<P: S3Protocol>(
        &mut self,
        protocol: &P,
        path: &StoragePath,
    ) -> Result<Option<S3VersionPage>, StorageRoleFailure> {
        if self.done {
            return Ok(None);
        }
        let page = self
            .retry
            .fetch(|| protocol.list_versions_page(path.as_str(), self.marker.as_ref()))
            .await
            .map_err(|error| role_failure(path, Operation::Namespace, error))?;
        let listed = page.versions.len() + page.markers.len() + page.prefixes.len();
        self.marker = self.paging.advance_holding(
            path,
            listed,
            listed.saturating_mul(2),
            page.next.clone(),
        )?;
        self.done = self.marker.is_none();
        if !page.rejected.is_empty() {
            return Err(classified_entry(
                path,
                Operation::Namespace,
                FailureClass::Protocol,
                Transience::Permanent,
                "S3 checkpoint history contains an undecodable entry",
            ));
        }
        Ok(Some(page))
    }
}

impl<P: S3Protocol + 'static> S3StagedDestination<P> {
    /// Caller holds `checkpoint_guard`. Fence first, then stream old owned generations while
    /// retaining the current root. Unknown markers are never inferred to belong to this stage.
    pub(super) async fn cleanup_hash_pointer(
        &self,
        stage: &PreparedStage,
        upload: &FinalUpload,
    ) -> Result<(), StorageRoleFailure> {
        if upload_pointer::pointer_path(stage.final_destination().path())? != upload.pointer {
            return Err(conflict(stage));
        }
        if !self.protocol.versioning_enabled() && !upload.has_pointer_history() {
            let found =
                upload_pointer::read(&*self.protocol, &upload.pointer, MAX_POINTER_BYTES + 1)
                    .await?;
            let Some(found) = found else {
                return Ok(());
            };
            if !Ownership::new(stage, upload).owns_current(&found.bytes) {
                return Err(conflict(stage));
            }
            if found.version.is_none() {
                return cleanup_result(
                    &upload.pointer,
                    self.protocol.delete_object(upload.pointer.as_str()).await,
                );
            }
        }
        let owner = Ownership::new(stage, upload);
        let Some(anchor) = self.hash_cleanup_anchor(stage, upload, &owner).await? else {
            return Ok(());
        };
        self.sweep_hash_history(stage, upload, &owner, &anchor)
            .await?;
        // A second bounded scan refuses success if an incomplete/changing listing left old H.
        self.verify_hash_history(stage, upload, &owner, &anchor)
            .await?;
        self.check_hash_anchor(stage, upload, &owner, &anchor)
            .await?;
        cleanup_result(
            &upload.pointer,
            self.protocol
                .delete_version(upload.pointer.as_str(), &anchor)
                .await,
        )
    }

    async fn hash_cleanup_anchor(
        &self,
        stage: &PreparedStage,
        upload: &FinalUpload,
        owner: &Ownership,
    ) -> Result<Option<String>, StorageRoleFailure> {
        if let Some(found) =
            upload_pointer::read(&*self.protocol, &upload.pointer, MAX_POINTER_BYTES + 1).await?
        {
            if !owner.owns_current(&found.bytes) {
                return Err(conflict(stage));
            }
            return found.version.map(Some).ok_or_else(|| conflict(stage));
        }
        // Missing HEAD may mean an unknown delete marker, not successful cleanup. Do not
        // delete anything until the bounded listing proves the exact artifact has no history.
        let mut walk = VersionWalk::new();
        while let Some(page) = walk.next(&*self.protocol, &upload.pointer).await? {
            for entry in page.versions.iter().chain(&page.markers) {
                if entry.key != upload.pointer.as_str() {
                    continue;
                }
                if entry.facts.is_latest
                    && entry.facts.delete_marker
                    && upload
                        .lock()
                        .checkpoint_cleanup_markers
                        .contains(&entry.facts.version_id)
                {
                    return Ok(Some(entry.facts.version_id.clone()));
                }
                return Err(conflict(stage));
            }
        }
        Ok(None)
    }

    async fn check_hash_anchor(
        &self,
        stage: &PreparedStage,
        upload: &FinalUpload,
        owner: &Ownership,
        anchor: &str,
    ) -> Result<(), StorageRoleFailure> {
        if self
            .hash_cleanup_anchor(stage, upload, owner)
            .await?
            .as_deref()
            == Some(anchor)
        {
            Ok(())
        } else {
            Err(conflict(stage))
        }
    }

    async fn sweep_hash_history(
        &self,
        stage: &PreparedStage,
        upload: &FinalUpload,
        owner: &Ownership,
        anchor: &str,
    ) -> Result<(), StorageRoleFailure> {
        let mut walk = VersionWalk::new();
        let mut pending = Vec::new();
        while let Some(page) = walk.next(&*self.protocol, &upload.pointer).await? {
            let mut current = Vec::new();
            for entry in page.versions.into_iter().chain(page.markers) {
                if entry.key != upload.pointer.as_str() {
                    continue;
                }
                if entry.facts.is_latest && entry.facts.version_id != anchor {
                    return Err(conflict(stage));
                }
                if entry.facts.version_id != anchor
                    && self.owned_hash_entry(upload, owner, &entry.facts).await?
                {
                    current.push(entry.facts.version_id);
                }
            }
            self.check_hash_anchor(stage, upload, owner, anchor).await?;
            self.delete_hash_versions(upload, pending).await?;
            // The previous page's continuation has now been consumed. Keep this page's
            // cursor version alive until the next fetch, even on stores requiring it to exist.
            pending = current;
        }
        self.check_hash_anchor(stage, upload, owner, anchor).await?;
        self.delete_hash_versions(upload, pending).await
    }

    async fn verify_hash_history(
        &self,
        stage: &PreparedStage,
        upload: &FinalUpload,
        owner: &Ownership,
        anchor: &str,
    ) -> Result<(), StorageRoleFailure> {
        let mut walk = VersionWalk::new();
        while let Some(page) = walk.next(&*self.protocol, &upload.pointer).await? {
            for entry in page.versions.into_iter().chain(page.markers) {
                if entry.key != upload.pointer.as_str() {
                    continue;
                }
                if entry.facts.is_latest && entry.facts.version_id != anchor {
                    return Err(conflict(stage));
                }
                if entry.facts.version_id != anchor
                    && self.owned_hash_entry(upload, owner, &entry.facts).await?
                {
                    return Err(conflict(stage));
                }
            }
        }
        Ok(())
    }

    async fn delete_hash_versions(
        &self,
        upload: &FinalUpload,
        versions: Vec<String>,
    ) -> Result<(), StorageRoleFailure> {
        for version in versions {
            cleanup_result(
                &upload.pointer,
                self.protocol
                    .delete_version(upload.pointer.as_str(), &version)
                    .await,
            )?;
        }
        Ok(())
    }

    async fn owned_hash_entry(
        &self,
        upload: &FinalUpload,
        owner: &Ownership,
        version: &S3VersionFacts,
    ) -> Result<bool, StorageRoleFailure> {
        if version.delete_marker {
            return Ok(upload
                .lock()
                .checkpoint_cleanup_markers
                .contains(&version.version_id));
        }
        self.owns_hash_version(upload, owner, version).await
    }

    async fn owns_hash_version(
        &self,
        upload: &FinalUpload,
        owner: &Ownership,
        version: &S3VersionFacts,
    ) -> Result<bool, StorageRoleFailure> {
        if version.size > MAX_POINTER_BYTES as u64 {
            return Ok(false);
        }
        let pin = S3ObjectFacts {
            size: version.size,
            etag: version.etag.clone(),
            version_id: Some(version.version_id.clone()),
            last_modified: None,
        };
        let bytes = self
            .protocol
            .get_range(upload.pointer.as_str(), 0..version.size, &pin)
            .await
            .map_err(|error| role_failure(&upload.pointer, Operation::Namespace, error))?;
        Ok(owner.same_upload(&bytes).is_some())
    }
}
