//! Public wire contract for the compatibility resume APIs.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Byte-resume destination handle produced by `StorageEnum::resume_prepare`
/// and consumed by the write/commit compatibility APIs.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum StreamHandle {
    /// NAS (Local/NFS/CIFS) destination using a staged partial file.
    Nas { part_path: PathBuf },
    /// S3 destination using an in-progress multipart upload.
    S3 {
        upload_id: String,
        part_size: u64,
        dst_key: String,
    },
    /// HDFS tail-only resume state transferable to a separate receiver process.
    Hdfs {
        part_path: PathBuf,
        prefix_len: u64,
        expected_size: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AssertTestValue;

    #[test]
    fn hdfs_resume_handle_preserves_legacy_wire_representation() {
        let handle = StreamHandle::Hdfs {
            part_path: PathBuf::from("目录/文件.bin.part"),
            prefix_len: 123,
            expected_size: 456,
        };
        let encoded = serde_json::to_vec(&handle).assert_value("serialize HDFS handle");
        let decoded: StreamHandle =
            serde_json::from_slice(&encoded).assert_value("deserialize HDFS handle");
        assert_eq!(decoded, handle);

        let legacy_fixture =
            r#"{"Hdfs":{"part_path":"目录/文件.bin.part","prefix_len":123,"expected_size":456}}"#;
        let decoded_fixture: StreamHandle = serde_json::from_slice(legacy_fixture.as_bytes())
            .assert_value("deserialize legacy HDFS handle fixture");
        assert_eq!(decoded_fixture, handle);
    }

    #[test]
    fn public_export_paths_remain_compatible() {
        let handle = crate::storage_enum::StreamHandle::Nas {
            part_path: PathBuf::from("file.part"),
        };
        let root_handle: crate::StreamHandle = handle;
        assert_eq!(
            root_handle,
            StreamHandle::Nas {
                part_path: PathBuf::from("file.part")
            }
        );
    }
}
