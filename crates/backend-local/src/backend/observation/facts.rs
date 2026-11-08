//! Local 私有观察事实；所有捕获路径复用同一 metadata，不追加属性读取。
use cap_std::fs::Metadata;
#[cfg(unix)]
use cap_std::fs::MetadataExt as _;

#[cfg(unix)]
use super::content_version;

const VERSION: u8 = 2;
#[cfg(unix)]
const UNIX_ATTRIBUTE_BYTES: usize = 1 + 7 * 8;
#[cfg(unix)]
const UNIX_VERSION_BYTES: usize = 4 * 8;

#[cfg(unix)]
pub(crate) fn backend_facts(metadata: &Metadata) -> Vec<u8> {
    let mut facts = Vec::with_capacity(UNIX_ATTRIBUTE_BYTES + UNIX_VERSION_BYTES);
    facts.push(VERSION);
    for value in [
        metadata.dev(),
        metadata.ino(),
        metadata.mode().into(),
        metadata.uid().into(),
        metadata.gid().into(),
        metadata.nlink(),
        metadata.rdev(),
    ] {
        facts.extend_from_slice(&value.to_le_bytes());
    }
    facts.extend_from_slice(&content_version(metadata));
    facts
}

#[cfg(not(unix))]
pub(crate) fn backend_facts(metadata: &Metadata) -> Vec<u8> {
    let mut facts = Vec::with_capacity(17);
    facts.push(VERSION);
    facts.extend_from_slice(&metadata.len().to_le_bytes());
    facts.extend_from_slice(&u64::from(metadata.permissions().readonly()).to_le_bytes());
    facts
}

/// 只接受当前完整格式；缺失、损坏和无变化证据的旧事实均不能授权删源。
#[cfg(unix)]
pub(crate) fn purge_content_version(facts: &[u8]) -> Option<&[u8]> {
    (facts.len() == UNIX_ATTRIBUTE_BYTES + UNIX_VERSION_BYTES && facts.first() == Some(&VERSION))
        .then(|| &facts[UNIX_ATTRIBUTE_BYTES..])
}
