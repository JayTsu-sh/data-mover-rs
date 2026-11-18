//! Settling a `CompleteMultipartUpload` whose reply was lost (ADR-0006 C17): is the object at the
//! final key the one the completion would make, and which version is it?

use super::super::{S3Protocol, S3VersionFacts, S3WriteFacts, is_real_version_id};
use super::single::same_etag;
use crate::model::StoragePath;

/// 重读得到的版本只作读取锚点，不能冒充本次修改响应中的创建版本回执。
pub(super) fn observed_facts(etag: String, version_id: Option<String>) -> S3WriteFacts {
    S3WriteFacts {
        etag,
        version_id: version_id.filter(|version| is_real_version_id(version)),
        reported_version: None,
    }
}

/// 查找与本次期望大小／分片 `ETag` 一致的最终内容，不证明是哪次请求创建的。
/// `pin_latest` 时按版本列表固定最新匹配版本；否则只用 HEAD 核对，不增加版本推断。
/// `NoSuchUpload` 也可能来自中止或生命周期清理，因此推断结果始终没有创建版本回执。
pub(super) async fn completed_object<P: S3Protocol>(
    protocol: &P,
    path: &StoragePath,
    size: u64,
    expected: &str,
    pin_latest: bool,
) -> Option<S3WriteFacts> {
    if pin_latest {
        match protocol.list_versions(path.as_str()).await {
            Ok(versions) if !versions.is_empty() => {
                return latest_match(&versions, size, expected);
            }
            Ok(_) => {}
            Err(error) => {
                tracing::debug!(path = %path.as_str(), ?error, "could not list the S3 key's versions; reconciling by HEAD");
            }
        }
    }
    let facts = protocol.head(path.as_str()).await.ok()?;
    (facts.size == size && same_etag(&facts.etag, expected))
        .then(|| observed_facts(facts.etag, None))
}

/// The latest entry of a key's versions, when it is a version (not a delete marker) of `size`
/// bytes with the `ETag` `expected`.
fn latest_match(versions: &[S3VersionFacts], size: u64, expected: &str) -> Option<S3WriteFacts> {
    let latest = versions.iter().find(|version| version.is_latest)?;
    (!latest.delete_marker && latest.size == size && same_etag(&latest.etag, expected))
        .then(|| observed_facts(latest.etag.clone(), Some(latest.version_id.clone())))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: &str, latest: bool, marker: bool, size: u64, etag: &str) -> S3VersionFacts {
        S3VersionFacts {
            version_id: id.into(),
            is_latest: latest,
            delete_marker: marker,
            size,
            etag: etag.into(),
        }
    }

    /// Only the latest entry counts, and only a version with our size and `ETag`; `"null"` (an
    /// unversioned bucket) is no version.
    #[test]
    fn only_a_latest_matching_version_can_pin_reads() {
        let ours = "\"abc-3\"";
        assert!(
            latest_match(&[entry("v2", true, false, 10, ours)], 10, ours)
                .is_some_and(|facts| facts.reported_version.is_none())
        );
        let claimed = |versions: &[S3VersionFacts]| {
            latest_match(versions, 10, ours).map(|facts| facts.version_id)
        };
        assert_eq!(
            claimed(&[
                entry("v2", true, false, 10, ours),
                entry("v1", false, false, 10, ours)
            ]),
            Some(Some("v2".into()))
        );
        assert_eq!(claimed(&[entry("null", true, false, 10, ours)]), Some(None));
        assert_eq!(claimed(&[entry("m", true, true, 0, "")]), None);
        assert_eq!(
            claimed(&[
                entry("m", true, true, 0, ""),
                entry("v1", false, false, 10, ours)
            ]),
            None
        );
        assert_eq!(claimed(&[entry("v3", true, false, 11, ours)]), None);
        assert_eq!(
            claimed(&[entry("v3", true, false, 10, "\"other-3\"")]),
            None
        );
        assert_eq!(claimed(&[]), None);
    }
}
