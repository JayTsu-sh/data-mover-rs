//! The ONTAP NAS bucket mode of [`MemoryS3`] (P7 §14.2, `compat=ontap&bucket_type=nas`), as
//! measured on ONTAP 9.19.1 (N0):
//!
//! - A `HEAD`, `GET`, listing or `CopyObject` reply carries an opaque `ETag` derived from the
//!   file's mtime and size — never the MD5 — so every write changes it and it is stable between
//!   reads; a `PutObject` reply carries the body's MD5. The fake mints the opaque value from a write
//!   generation and the size ([`MemoryS3::nas_rewrite_keeping_mtime`] keeps it, as a rewrite that
//!   restores the mtime does).
//! - No conditional reads: a range read is checked after the fact, as the real protocol checks the
//!   response — another `ETag` is a permanent `Conflict`. A replacement can be injected after the
//!   n-th range read of a key ([`NasBucket::replace_after_ranges`]), and the next range read cut
//!   short ([`NasBucket::cut_next_range`], the `Conflict` a short body is).
//! - `CreateMultipartUpload`, `ListObjectVersions` and `GetBucketVersioning` answer 501
//!   (`Unsupported`); `ListMultipartUploads` and `ListParts` 403 (`PermissionDenied`).
//! - Listings keep no order (the fake's `unordered` listing).

use std::collections::HashMap;
use std::sync::{MutexGuard, PoisonError};

use bytes::Bytes;

use super::MemoryS3;
use crate::model::{FailureClass, Transience};
use crate::storage::backends::s3::{S3BucketCapabilities, S3ProtocolFailure, S3Result};

/// What the NAS mode keeps beside the fake's objects.
#[derive(Default)]
pub(crate) struct NasBucket {
    enabled: bool,
    /// The opaque `ETag` of each key written (or first read) in NAS mode.
    etags: HashMap<String, String>,
    generation: u64,
    /// After this many more range reads of the key, its object is replaced by these bytes (a
    /// writer over S3 or NFS).
    pub(crate) replace_after_ranges: Option<(String, u32, Bytes)>,
    /// The next range read fails as a body the server cut short (a replacement committed).
    pub(crate) cut_next_range: bool,
    /// Range reads served (NAS mode), `HEAD`s answered (either mode), and `GetBucketVersioning`
    /// requests answered (NAS mode).
    pub(crate) ranges: u32,
    pub(crate) heads: u32,
    pub(crate) versioning_requests: u32,
}

impl NasBucket {
    fn mint(&mut self, key: &str, len: usize) -> String {
        self.generation += 1;
        let etag = format!("\"nas-{:06}-{len}\"", self.generation);
        self.etags.insert(key.to_string(), etag.clone());
        etag
    }
}

/// 501 `NotImplemented`, as the real protocol maps it.
fn not_implemented() -> S3ProtocolFailure {
    S3ProtocolFailure::entry(
        FailureClass::Unsupported,
        Transience::Permanent,
        "NotImplemented (ONTAP NAS bucket)",
    )
}

/// 403 `AccessDenied`, as the real protocol maps it.
fn access_denied() -> S3ProtocolFailure {
    S3ProtocolFailure::entry(
        FailureClass::PermissionDenied,
        Transience::Permanent,
        "AccessDenied (ONTAP NAS bucket)",
    )
}

/// A request a NAS bucket refuses.
#[derive(Clone, Copy)]
pub(super) enum NasRefused {
    /// 501: `CreateMultipartUpload`, `ListObjectVersions`, `GetBucketVersioning`.
    NotImplemented,
    /// 403: `ListMultipartUploads`, `ListParts`.
    Denied,
}

impl MemoryS3 {
    /// An in-memory ONTAP NAS bucket.
    pub(crate) fn nas_bucket() -> Self {
        let memory = Self::default();
        memory.nas().enabled = true;
        memory.listing().unordered = true;
        memory
    }

    pub(crate) fn nas(&self) -> MutexGuard<'_, NasBucket> {
        self.nas.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(super) fn is_nas(&self) -> bool {
        self.nas().enabled
    }

    pub(super) fn capabilities(&self) -> S3BucketCapabilities {
        if self.is_nas() {
            S3BucketCapabilities::NAS
        } else {
            S3BucketCapabilities::OBJECT
        }
    }

    /// The `ETag` a read of `key`, holding `bytes`, reports: the opaque one in NAS mode (minted
    /// on first sight for an object a test stored directly), otherwise the content's.
    pub(super) fn object_etag(&self, key: &str, bytes: &Bytes) -> String {
        let mut nas = self.nas();
        if !nas.enabled {
            drop(nas);
            return self.etag_for(bytes);
        }
        match nas.etags.get(key) {
            Some(etag) => etag.clone(),
            None => nas.mint(key, bytes.len()),
        }
    }

    /// A write of `key` in NAS mode: a new mtime, so a new opaque `ETag`.
    pub(super) fn nas_written(&self, key: &str, len: usize) {
        let mut nas = self.nas();
        if nas.enabled {
            nas.mint(key, len);
        }
    }

    /// The failure a NAS bucket answers `request` with; nothing on an object bucket.
    pub(super) fn nas_refusal(&self, request: NasRefused) -> S3Result<()> {
        if !self.is_nas() {
            return Ok(());
        }
        Err(match request {
            NasRefused::NotImplemented => not_implemented(),
            NasRefused::Denied => access_denied(),
        })
    }

    /// Counts a range read of `key` in NAS mode; a cut read fails, and a due replacement is
    /// stored after this read was served.
    pub(super) async fn nas_range_served(&self, key: &str) -> S3Result<()> {
        let replacement = {
            let mut nas = self.nas();
            if !nas.enabled {
                return Ok(());
            }
            nas.ranges += 1;
            if std::mem::take(&mut nas.cut_next_range) {
                return Err(S3ProtocolFailure::entry(
                    FailureClass::Conflict,
                    Transience::Permanent,
                    "the S3 object's body ended early: it was replaced during the read",
                ));
            }
            match nas.replace_after_ranges.take() {
                Some((target, 1, bytes)) if target == key => Some(bytes),
                Some((target, left, bytes)) if target == key => {
                    nas.replace_after_ranges = Some((target, left - 1, bytes));
                    None
                }
                other => {
                    nas.replace_after_ranges = other;
                    None
                }
            }
        };
        if let Some(bytes) = replacement {
            self.nas_written(key, bytes.len());
            self.objects.lock().await.insert(key.to_string(), bytes);
        }
        Ok(())
    }

    /// Counts a `HEAD` (in either mode).
    pub(super) fn nas_head_served(&self) {
        self.nas().heads += 1;
    }

    /// Rewrites `key` over NFS keeping its mtime (`rsync -t`, `touch -d`): a NAS bucket reports
    /// the same opaque `ETag` for the new content (N0).
    pub(crate) async fn nas_rewrite_keeping_mtime(&self, key: &str, bytes: Bytes) {
        self.objects.lock().await.insert(key.to_string(), bytes);
    }
}
