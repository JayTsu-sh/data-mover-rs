//! Settling a `CompleteMultipartUpload` whose reply was lost (ADR-0006 C17): is the object at the
//! final key the one the completion would make, and which version is it?

use super::super::{S3Protocol, S3VersionFacts, S3WriteFacts};
use super::single::same_etag;
use crate::model::StoragePath;

/// The object an ambiguous completion made at `path`: one of `size` bytes whose `ETag` is the
/// composite `expected`.
///
/// With `claim_version` — the upload is known gone (`NoSuchUpload`), so under the caller contract
/// (one writer per key) the completion committed and nothing was written after it — the key's
/// versions are listed and the **latest** entry must be that object; its version, when real, is
/// reported as ours. In an unversioned bucket the listing shows it as `"null"`, which claims no
/// version. A store that cannot list versions, or lists none for the key, falls back to a HEAD,
/// claiming no version.
///
/// Residual risk: anything that aborts our upload before it completes — a bucket lifecycle rule,
/// another writer's prepare (`abort_uploads`) — also leaves `NoSuchUpload`; if an identical earlier
/// object is then the latest version, that older version is claimed. Likewise a later identical
/// write by another writer, landing before the listing, is claimed. The bytes are ours either way;
/// only the version may not be the one our completion made.
///
/// Without `claim_version` (the completion may not have committed: an identical earlier object
/// would match too) only a HEAD is made and no version is claimed.
pub(super) async fn completed_object<P: S3Protocol>(
    protocol: &P,
    path: &StoragePath,
    size: u64,
    expected: &str,
    claim_version: bool,
) -> Option<S3WriteFacts> {
    if claim_version {
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
        .then(|| S3WriteFacts::new(facts.etag, None))
}

/// The latest entry of a key's versions, when it is a version (not a delete marker) of `size`
/// bytes with the `ETag` `expected`.
fn latest_match(versions: &[S3VersionFacts], size: u64, expected: &str) -> Option<S3WriteFacts> {
    let latest = versions.iter().find(|version| version.is_latest)?;
    (!latest.delete_marker && latest.size == size && same_etag(&latest.etag, expected))
        .then(|| S3WriteFacts::new(latest.etag.clone(), Some(latest.version_id.clone())))
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
    fn only_a_latest_matching_version_is_ours() {
        let ours = "\"abc-3\"";
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
