//! Transfer identity and recovery binding (ADR-0006 identity layers 2 and 3).
//!
//! The transfer identity names "this source file goes to that destination file". data-mover derives
//! it from the two canonical endpoints and paths, so a process with no local state computes the same
//! value for the same pair. The recovery binding adds what the source looked like when a stage was
//! written; a stored binding that differs from the current one means restart, not resume.
//!
//! Every variable-length field is length-prefixed, and each hash has its own domain prefix: a derived
//! identity, a label override and a binding can never collide.

use std::fmt;

use crate::model::{BackendIdentity, EntryIdentityKey, StoragePath};

use super::model::TransferValueError;

const MAX_LABEL_BYTES: usize = 1024;
const DERIVED_DOMAIN: &[u8] = b"data-mover/transfer-identity/v1\0";
const LABEL_DOMAIN: &[u8] = b"data-mover/transfer-identity/override/v1\0";
const BINDING_DOMAIN: &[u8] = b"data-mover/recovery-binding/v3\0";

/// Which version of the source a transfer copies. Only the current version exists until source
/// version selection lands (ADR-0006 C6), which adds a selector with its own, non-zero tag.
const CURRENT_VERSION_TAG: u8 = 0x00;

/// A stable 32-byte name for one logical transfer: which source file goes to which destination
/// file. It does not change across attempts, reschedules or source updates.
///
/// It is not secret — it hashes endpoints and paths, never credentials — and displays as 64
/// lowercase hex digits.
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct TransferIdentity([u8; 32]);

impl TransferIdentity {
    /// Derives the identity of copying the current version of `source_path` on `source` to
    /// `final_path` on `destination`. The endpoints are the canonical ones data-mover derives from
    /// the backend configuration; the paths are taken literally.
    #[must_use]
    pub fn derive(
        source: &BackendIdentity,
        source_path: &StoragePath,
        destination: &BackendIdentity,
        final_path: &StoragePath,
    ) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(DERIVED_DOMAIN);
        update_endpoint(&mut hasher, source);
        update_field(&mut hasher, source_path.as_str().as_bytes());
        hasher.update(&[CURRENT_VERSION_TAG]);
        update_endpoint(&mut hasher, destination);
        update_field(&mut hasher, final_path.as_str().as_bytes());
        Self(*hasher.finalize().as_bytes())
    }

    /// An identity named by the caller instead of derived. Two transfers with the same label share
    /// an identity whatever they copy, so a label opts out of finding a transfer from its files.
    ///
    /// # Errors
    /// Returns an error for blank, NUL-containing, or unbounded labels.
    pub fn new(label: impl Into<String>) -> Result<Self, TransferValueError> {
        let label = label.into();
        if label.trim().is_empty() || label.contains('\0') || label.len() > MAX_LABEL_BYTES {
            return Err(TransferValueError::new(
                "transfer identity must be non-blank and bounded",
            ));
        }
        let mut hasher = blake3::Hasher::new();
        hasher.update(LABEL_DOMAIN);
        update_field(&mut hasher, label.as_bytes());
        Ok(Self(*hasher.finalize().as_bytes()))
    }

    /// The identity bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for TransferIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0
            .iter()
            .try_for_each(|byte| write!(formatter, "{byte:02x}"))
    }
}

impl fmt::Debug for TransferIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "TransferIdentity({self})")
    }
}

/// What a stage was written from, as stored in its recovery record.
pub(crate) struct BindingSource<'a> {
    pub(crate) path: &'a StoragePath,
    pub(crate) identity_key: EntryIdentityKey,
    pub(crate) size: Option<u64>,
    pub(crate) content_version: Option<&'a [u8]>,
}

/// The recovery binding: the transfer identity, which source entry and what it looked like, and
/// where the stage lands. The source path and the destination are repeated although a derived
/// identity already names them: a label override names neither, and the source identity key does
/// not always name the entry (an S3 object's is its `versionId` or `ETag`, not its key). One label
/// reused for two sources or two destinations must not share a binding.
pub(crate) fn binding_hash(
    identity: &TransferIdentity,
    source: &BindingSource<'_>,
    destination: &BackendIdentity,
    final_path: &StoragePath,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(BINDING_DOMAIN);
    hasher.update(identity.as_bytes());
    update_field(&mut hasher, source.path.as_str().as_bytes());
    hasher.update(source.identity_key.as_bytes());
    let size = source.size.map(u64::to_le_bytes);
    update_optional(&mut hasher, size.as_ref().map(<[u8; 8]>::as_slice));
    update_optional(&mut hasher, source.content_version);
    update_endpoint(&mut hasher, destination);
    update_field(&mut hasher, final_path.as_str().as_bytes());
    *hasher.finalize().as_bytes()
}

fn update_endpoint(hasher: &mut blake3::Hasher, endpoint: &BackendIdentity) {
    update_field(hasher, endpoint.kind().as_str().as_bytes());
    update_field(hasher, endpoint.stable_id().as_bytes());
}

fn update_optional(hasher: &mut blake3::Hasher, value: Option<&[u8]>) {
    match value {
        None => {
            hasher.update(&[0]);
        }
        Some(value) => {
            hasher.update(&[1]);
            update_field(hasher, value);
        }
    }
}

fn update_field(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value);
}

#[cfg(test)]
#[path = "identity_tests.rs"]
mod tests;
