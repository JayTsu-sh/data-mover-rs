//! 每次目录列举的绝对统计；调用方按目录替换贡献，不能跨重放直接相加。
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as DeserializeError};

use super::ListingOutcome;
use crate::model::{EntryKind, SpecialFileKind, StoragePath};

/// 逻辑子项的类型分布；无法取得类型的失败进入 unknown。
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct EntryKindCounts {
    pub directories: u64,
    pub regular_files: u64,
    pub symlinks: u64,
    pub block_devices: u64,
    pub character_devices: u64,
    pub fifos: u64,
    pub sockets: u64,
    pub unknown: u64,
}

impl EntryKindCounts {
    #[must_use]
    pub const fn total(self) -> u64 {
        self.directories
            .saturating_add(self.regular_files)
            .saturating_add(self.symlinks)
            .saturating_add(self.block_devices)
            .saturating_add(self.character_devices)
            .saturating_add(self.fifos)
            .saturating_add(self.sockets)
            .saturating_add(self.unknown)
    }

    #[must_use]
    pub fn saturating_add(self, other: Self) -> Self {
        Self {
            directories: self.directories.saturating_add(other.directories),
            regular_files: self.regular_files.saturating_add(other.regular_files),
            symlinks: self.symlinks.saturating_add(other.symlinks),
            block_devices: self.block_devices.saturating_add(other.block_devices),
            character_devices: self
                .character_devices
                .saturating_add(other.character_devices),
            fifos: self.fifos.saturating_add(other.fifos),
            sockets: self.sockets.saturating_add(other.sockets),
            unknown: self.unknown.saturating_add(other.unknown),
        }
    }

    #[must_use]
    pub fn saturating_sub(self, other: Self) -> Self {
        Self {
            directories: self.directories.saturating_sub(other.directories),
            regular_files: self.regular_files.saturating_sub(other.regular_files),
            symlinks: self.symlinks.saturating_sub(other.symlinks),
            block_devices: self.block_devices.saturating_sub(other.block_devices),
            character_devices: self
                .character_devices
                .saturating_sub(other.character_devices),
            fifos: self.fifos.saturating_sub(other.fifos),
            sockets: self.sockets.saturating_sub(other.sockets),
            unknown: self.unknown.saturating_sub(other.unknown),
        }
    }

    pub(crate) fn count(&mut self, kind: Option<EntryKind>) {
        let value = match kind {
            Some(EntryKind::Directory) => &mut self.directories,
            Some(EntryKind::File) => &mut self.regular_files,
            Some(EntryKind::Symlink) => &mut self.symlinks,
            Some(EntryKind::Special(SpecialFileKind::BlockDevice)) => &mut self.block_devices,
            Some(EntryKind::Special(SpecialFileKind::CharacterDevice)) => {
                &mut self.character_devices
            }
            Some(EntryKind::Special(SpecialFileKind::Fifo)) => &mut self.fifos,
            Some(EntryKind::Special(SpecialFileKind::Socket)) => &mut self.sockets,
            None => &mut self.unknown,
        };
        *value = value.saturating_add(1);
    }
}

/// 一次实际 listing pass 的直属子项累计快照，不包含子目录的孩子或遍历根本身。
///
/// 首个快照 `pass_start=true`，`revision` 从 1 递增；再次列举同一路径会开启新 pass。
/// `outcome=None` 是逐页 partial，`Some` 表示列举已经结束；只有 `Complete` 确认完整。
/// 快照在该页所有条目（包括一个 key 的全部历史段）之后交付。预取尚未交付的页不计入。
///
/// 隐藏内部产物及 `emit=false`、`descend=false` 的剪枝目录不在五类计数中，其子树不补扫。
/// `emit=true`、`descend=false` 的目录仍计入 retained。metadata family 的补充失败不替代 retained。
/// `scanned = filtered + retained + failed + vanished`（逐类型成立）。
///
/// 这些是绝对贡献；恢复可能重新列举目录，消费者必须按目录替换而非直接累加。
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DirectoryStatistics {
    #[serde(
        serialize_with = "serialize_path",
        deserialize_with = "deserialize_path"
    )]
    pub path: StoragePath,
    pub pass_start: bool,
    pub revision: u64,
    pub scanned: EntryKindCounts,
    pub filtered: EntryKindCounts,
    pub retained: EntryKindCounts,
    pub failed: EntryKindCounts,
    pub vanished: EntryKindCounts,
    pub pruned_dirs: u64,
    pub truncated_dirs: u64,
    /// retained 普通文件的当前大小；`unknown_file_sizes` 不以 0 冒充已知大小。
    pub regular_file_bytes: u64,
    pub unknown_file_sizes: u64,
    /// retained key 的完整历史记录数，与 key 的单次类型计数独立。
    pub versions: u64,
    pub delete_markers: u64,
    pub all_versions_bytes: u64,
    pub outcome: Option<ListingOutcome>,
    pub vanished_listing: bool,
}

fn serialize_path<S: Serializer>(path: &StoragePath, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(path.as_str())
}

fn deserialize_path<'de, D: Deserializer<'de>>(deserializer: D) -> Result<StoragePath, D::Error> {
    StoragePath::new(String::deserialize(deserializer)?).map_err(DeserializeError::custom)
}
