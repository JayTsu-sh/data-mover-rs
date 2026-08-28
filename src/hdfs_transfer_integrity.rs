//! HDFS staged-file integrity comparison shared by current and compatibility flows.

use std::path::Path;

use crate::{EntryEnum, Result, StorageEnum};

pub(crate) async fn partial_matches(
    from: &StorageEnum,
    to: &StorageEnum,
    entry: &EntryEnum,
    part_path: &Path,
    enabled: bool,
) -> Result<bool> {
    if !enabled {
        return Ok(true);
    }
    let size = entry.get_size();
    let source_hash = StorageEnum::compute_entry_hash(from, entry).await?;
    let destination_hash = to.compute_hash(part_path, size).await?;
    Ok(source_hash == destination_hash)
}
