use std::path::PathBuf;

use crate::hdfs::{HdfsSourceFingerprint, HdfsStableSourceFact, HdfsTransferRequest};
use crate::{EntryEnum, Result, StorageEnum};

pub(crate) fn hdfs_default_transfer_identity(source: &StorageEnum, entry: &EntryEnum) -> String {
    let (source_kind, namespace) = match source {
        StorageEnum::Local(storage) => (b"local".as_slice(), storage.root_path.to_string_lossy()),
        StorageEnum::NFS(storage) => (b"nfs".as_slice(), storage.transfer_namespace().into()),
        StorageEnum::S3(storage) => (b"s3".as_slice(), storage.transfer_namespace().into()),
        StorageEnum::CIFS(storage) => (b"cifs".as_slice(), storage.transfer_namespace().into()),
        StorageEnum::HDFS(storage) => (b"hdfs".as_slice(), storage.transfer_namespace().into()),
    };
    hdfs_default_transfer_identity_for(source_kind, &namespace, entry)
}

fn hdfs_default_transfer_identity_for(
    source_kind: &[u8],
    namespace: &str,
    entry: &EntryEnum,
) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"data-mover:hdfs-default-copy-identity:v1\0");
    hasher.update(source_kind);
    hasher.update(b"\0");
    hasher.update(namespace.as_bytes());
    hasher.update(b"\0");
    hasher.update(entry.get_relative_path().to_string_lossy().as_bytes());
    let digest = hasher.finalize().to_hex();
    format!("default-copy-{}", &digest[..32])
}

pub(crate) fn hdfs_write_options(entry: &EntryEnum) -> (u32, Option<u32>) {
    match entry {
        EntryEnum::NAS(entry) => (entry.mode & 0o7777, None),
        EntryEnum::S3(_) => (0o644, None),
        EntryEnum::HDFS(entry) => (entry.mode & 0o7777, entry.replication),
    }
}

/// Map common source facts into the fingerprint used by stable HDFS transfers.
#[must_use]
pub fn hdfs_source_fingerprint(entry: &EntryEnum) -> HdfsSourceFingerprint {
    let size = entry.get_size();
    let mtime = entry.get_mtime();
    match entry {
        EntryEnum::NAS(entry) if entry.file_handle.is_some() => {
            let fact = entry
                .file_handle
                .as_deref()
                .map(HdfsStableSourceFact::FileIdentity);
            HdfsSourceFingerprint::new(size, mtime, fact)
        }
        EntryEnum::NAS(entry) if entry.ino.is_some() => {
            let fact = entry.ino.unwrap_or_default().to_le_bytes();
            HdfsSourceFingerprint::new(size, mtime, Some(HdfsStableSourceFact::FileIdentity(&fact)))
        }
        EntryEnum::S3(entry) => HdfsSourceFingerprint::new(
            size,
            mtime,
            entry
                .version_id
                .as_deref()
                .map(HdfsStableSourceFact::ObjectVersion),
        ),
        EntryEnum::NAS(_) | EntryEnum::HDFS(_) => HdfsSourceFingerprint::new(size, mtime, None),
    }
}

/// Build an HDFS-owned stable request after explicitly mapping common entry facts.
///
/// # Errors
///
/// Returns an error when the opaque identity, destination path, or source size
/// cannot form a valid HDFS transfer request.
pub fn hdfs_transfer_request(
    entry: &EntryEnum,
    transfer_identity: &str,
    final_path: PathBuf,
) -> Result<HdfsTransferRequest> {
    let (mode, replication) = hdfs_write_options(entry);
    HdfsTransferRequest::new(
        transfer_identity,
        hdfs_source_fingerprint(entry),
        final_path,
        entry.get_size(),
        mode,
        replication,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::S3Entry;

    #[test]
    fn default_copy_identity_is_stable_and_source_scoped() {
        let source = s3_entry(16, 123, Some("v1"));
        let same_source = s3_entry(17, 124, Some("v2"));
        let other_path = EntryEnum::S3(S3Entry {
            relative_path: "other.bin".to_string(),
            ..match s3_entry(16, 123, Some("v1")) {
                EntryEnum::S3(entry) => entry,
                _ => unreachable!(),
            }
        });

        assert_eq!(
            hdfs_default_transfer_identity_for(b"s3", "endpoint/bucket/prefix", &source),
            hdfs_default_transfer_identity_for(b"s3", "endpoint/bucket/prefix", &same_source),
            "source content changes must not create a new logical transfer identity"
        );
        assert_ne!(
            hdfs_default_transfer_identity_for(b"s3", "endpoint/bucket/prefix", &source),
            hdfs_default_transfer_identity_for(b"s3", "endpoint/bucket/prefix", &other_path)
        );
        assert_ne!(
            hdfs_default_transfer_identity_for(b"s3", "endpoint-a/bucket", &source),
            hdfs_default_transfer_identity_for(b"s3", "endpoint-b/bucket", &source),
            "different source namespaces must never reuse one HDFS partial"
        );
    }

    fn s3_entry(size: u64, mtime: i64, version: Option<&str>) -> EntryEnum {
        EntryEnum::S3(S3Entry {
            name: "file.bin".to_string(),
            relative_path: "file.bin".to_string(),
            extension: Some("bin".to_string()),
            size,
            mtime,
            tags: None,
            version_id: version.map(str::to_string),
            is_latest: true,
            is_delete_marker: false,
            version_count: None,
            is_dir: false,
        })
    }

    #[test]
    fn maps_available_s3_version_facts_into_hdfs_fingerprint() {
        let baseline = hdfs_source_fingerprint(&s3_entry(16, 123, Some("v1")));
        assert_eq!(
            baseline,
            hdfs_source_fingerprint(&s3_entry(16, 123, Some("v1")))
        );
        assert_ne!(
            baseline,
            hdfs_source_fingerprint(&s3_entry(17, 123, Some("v1")))
        );
        assert_ne!(
            baseline,
            hdfs_source_fingerprint(&s3_entry(16, 124, Some("v1")))
        );
        assert_ne!(
            baseline,
            hdfs_source_fingerprint(&s3_entry(16, 123, Some("v2")))
        );

        let request = hdfs_transfer_request(
            &s3_entry(16, 123, Some("v1")),
            "transfer",
            PathBuf::from("file.bin"),
        )
        .unwrap_or_else(|error| panic!("valid HDFS request was rejected: {error}"));
        assert_eq!(request.expected_size(), 16);
        assert_eq!(request.mode(), 0o644);
        assert_eq!(request.replication(), None);
    }
}
