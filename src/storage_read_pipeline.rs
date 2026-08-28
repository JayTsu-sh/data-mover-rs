//! Protocol-neutral streaming source-read dispatch.

use tokio::sync::mpsc;

use crate::checksum::HashCalculator;
use crate::error::StorageError;
use crate::qos::QosManager;
use crate::{DataChunk, EntryEnum, Result, StorageEnum};

/// 从源端分块读取文件数据到 channel（内部辅助方法）。
pub(crate) async fn read_data_from(
    from: &StorageEnum,
    entry: &EntryEnum,
    tx: mpsc::Sender<DataChunk>,
    size: u64,
    enable_integrity_check: bool,
    qos: Option<QosManager>,
) -> Result<Option<HashCalculator>> {
    match (from, entry) {
        (StorageEnum::Local(s), EntryEnum::NAS(e)) => {
            s.read_data(tx, &e.relative_path, size, enable_integrity_check, qos)
                .await
        }
        (StorageEnum::NFS(s), EntryEnum::NAS(e)) => {
            s.read_data(tx, &e.relative_path, size, enable_integrity_check, qos)
                .await
        }
        (StorageEnum::CIFS(s), EntryEnum::NAS(e)) => {
            s.read_data(tx, &e.relative_path, size, enable_integrity_check, qos)
                .await
        }
        (StorageEnum::S3(s), EntryEnum::S3(e)) => {
            s.read_data(tx, &e.relative_path, size, enable_integrity_check, qos)
                .await
        }
        (StorageEnum::HDFS(s), EntryEnum::HDFS(e)) => {
            s.read_data(tx, &e.relative_path, size, enable_integrity_check, qos)
                .await
        }
        _ => Err(StorageError::OperationError(format!(
            "unsupported source/entry combination for tar read_data: {entry:?}"
        ))),
    }
}
