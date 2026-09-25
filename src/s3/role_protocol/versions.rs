//! Version requests of the role protocol (ADR-0006 C17): the versions of exactly one key, and a
//! delete of one stored version that adds no delete marker.

use aws_sdk_s3::operation::list_object_versions::ListObjectVersionsOutput;

use super::{
    ProvideErrorMetadata, S3ProtocolFailure, S3Result, S3Storage, s3_role_entry,
    s3_role_remote_failure, s3_role_transport_failure, versioned_failure,
};
use crate::model::FailureClass;
use crate::storage::backends::s3::S3VersionFacts;

/// Where the next page of a truncated `ListObjectVersions` starts: (key marker, version id
/// marker).
type VersionsMarker = (Option<String>, Option<String>);

impl S3Storage {
    /// `DeleteObject` naming one version: removes that version for good.
    pub(super) async fn role_delete_version(&self, key: &str, version_id: &str) -> S3Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket_name)
            .key(self.build_full_key(key))
            .version_id(version_id)
            .send()
            .await
            .map(|_| ())
            .map_err(|error| {
                let status = error.raw_response().map(|r| r.status().as_u16());
                let code = error
                    .as_service_error()
                    .and_then(ProvideErrorMetadata::code)
                    .map(str::to_string);
                let failure = classify_sdk!(error, "S3 DeleteObject (version) request failed");
                delete_version_failure(status, code.as_deref(), failure)
            })
    }

    /// `ListObjectVersions` with the key as prefix, keeping only the entries of exactly that key.
    /// The listing is sorted by key and the exact key sorts first among the keys it prefixes, so
    /// the first page that shows another key holds the last of ours.
    pub(super) async fn role_list_versions(&self, key: &str) -> S3Result<Vec<S3VersionFacts>> {
        let full_key = self.build_full_key(key);
        let mut marker: VersionsMarker = (None, None);
        let mut versions = Vec::new();
        loop {
            let response = self
                .client
                .list_object_versions()
                .bucket(&self.bucket_name)
                .prefix(&full_key)
                .set_key_marker(marker.0.clone())
                .set_version_id_marker(marker.1.clone())
                .send()
                .await
                .map_err(|error| classify_sdk!(error, "S3 ListObjectVersions request failed"))?;
            let (page, passed_key) = versions_of_key(&response, &full_key)?;
            versions.extend(page);
            if passed_key {
                return Ok(versions);
            }
            let Some(next) = next_versions_marker(&response, &marker)? else {
                return Ok(versions);
            };
            marker = next;
        }
    }
}

/// The failure of a delete that named a version. A version Object Lock protects is refused with
/// 403 `AccessDenied` by AWS (already `PermissionDenied`) and with 400 `InvalidRequest` ("Object is
/// WORM protected") by `MinIO` `RELEASE.2023-03-20`; both concern this one version, not the session.
/// A 405 is a store that does not delete by version (DXN / `StorageGRID` answer 405 to operations
/// they lack) — `Unsupported`, never the "delete marker" `NotFound` a versioned read gets, which a
/// clean-up would take as done. A malformed version id stays `InvalidInput`
/// ([`versioned_failure`]).
fn delete_version_failure(
    status: Option<u16>,
    code: Option<&str>,
    failure: S3ProtocolFailure,
) -> S3ProtocolFailure {
    match (status, code) {
        (Some(400), Some("InvalidRequest")) | (_, Some("ObjectLocked")) => s3_role_entry(
            FailureClass::PermissionDenied,
            &format!(
                "the S3 version is protected by Object Lock ({} {})",
                status.unwrap_or_default(),
                code.unwrap_or_default()
            ),
        ),
        (Some(405), _) => s3_role_entry(
            FailureClass::Unsupported,
            "the S3 store does not delete by version",
        ),
        _ => versioned_failure(status, code, failure),
    }
}

/// The entries of `response` on exactly `full_key`, and whether the page reached a key sorting
/// after it (a prefix listing sorts every longer key after the exact one).
fn versions_of_key(
    response: &ListObjectVersionsOutput,
    full_key: &str,
) -> S3Result<(Vec<S3VersionFacts>, bool)> {
    let mut entries = Vec::new();
    let mut passed_key = false;
    for version in response.versions() {
        if version.key() != Some(full_key) {
            passed_key |= version.key().is_some_and(|key| key > full_key);
            continue;
        }
        entries.push(S3VersionFacts {
            version_id: listed_id(version.version_id())?,
            is_latest: version.is_latest() == Some(true),
            delete_marker: false,
            size: version
                .size()
                .and_then(|size| u64::try_from(size).ok())
                .ok_or_else(|| S3ProtocolFailure::protocol("S3 version has invalid size"))?,
            etag: version.e_tag().unwrap_or_default().to_string(),
        });
    }
    for marker in response.delete_markers() {
        if marker.key() != Some(full_key) {
            passed_key |= marker.key().is_some_and(|key| key > full_key);
            continue;
        }
        entries.push(S3VersionFacts {
            version_id: listed_id(marker.version_id())?,
            is_latest: marker.is_latest() == Some(true),
            delete_marker: true,
            size: 0,
            etag: String::new(),
        });
    }
    Ok((entries, passed_key))
}

fn listed_id(id: Option<&str>) -> S3Result<String> {
    id.filter(|id| !id.is_empty())
        .map(str::to_string)
        .ok_or_else(|| S3ProtocolFailure::protocol("S3 version listing omitted a version id"))
}

fn next_versions_marker(
    response: &ListObjectVersionsOutput,
    previous: &VersionsMarker,
) -> S3Result<Option<VersionsMarker>> {
    if response.is_truncated() != Some(true) {
        return Ok(None);
    }
    let next = (
        response.next_key_marker().map(str::to_string),
        response.next_version_id_marker().map(str::to_string),
    );
    if next.0.is_none() || &next == previous {
        return Err(S3ProtocolFailure::protocol(
            "S3 ListObjectVersions continuation marker did not advance",
        ));
    }
    Ok(Some(next))
}

#[cfg(test)]
mod tests {
    use aws_sdk_s3::types::{DeleteMarkerEntry, ObjectVersion};

    use super::*;
    use crate::model::Transience;

    /// A version Object Lock protects is a permanent `PermissionDenied` of the entry under either
    /// store's answer; a malformed id stays `InvalidInput`, anything else the generic mapping.
    #[test]
    fn a_locked_version_is_a_refusal_of_the_entry() {
        let generic = |status| s3_role_remote_failure(status, None, "delete");
        let locked = S3ProtocolFailure::entry(
            FailureClass::PermissionDenied,
            Transience::Permanent,
            "the S3 version is protected by Object Lock (400 InvalidRequest)",
        );
        assert_eq!(
            delete_version_failure(Some(400), Some("InvalidRequest"), generic(Some(400))),
            locked
        );
        assert!(matches!(
            s3_role_remote_failure(Some(403), Some("AccessDenied"), "delete"),
            S3ProtocolFailure::Entry {
                class: FailureClass::PermissionDenied,
                ..
            }
        ));
        assert!(matches!(
            delete_version_failure(Some(400), Some("InvalidArgument"), generic(Some(400))),
            S3ProtocolFailure::Entry {
                class: FailureClass::InvalidInput,
                ..
            }
        ));
        // A store without delete-by-version: not "already gone", which a clean-up takes as done.
        assert!(matches!(
            delete_version_failure(Some(405), None, generic(Some(405))),
            S3ProtocolFailure::Entry {
                class: FailureClass::Unsupported,
                ..
            }
        ));
        assert!(matches!(
            delete_version_failure(Some(503), None, generic(Some(503))),
            S3ProtocolFailure::Session {
                class: FailureClass::Connectivity,
                ..
            }
        ));
    }

    fn listing(truncated: bool) -> ListObjectVersionsOutput {
        let version = |key: &str, id: &str, latest| {
            ObjectVersion::builder()
                .key(key)
                .version_id(id)
                .is_latest(latest)
                .size(3)
                .e_tag("\"e\"")
                .build()
        };
        let builder = ListObjectVersionsOutput::builder()
            .versions(version("p/a", "v2", false))
            .versions(version("p/a", "v1", false))
            .versions(version("p/a.bak", "v9", true))
            .delete_markers(
                DeleteMarkerEntry::builder()
                    .key("p/a")
                    .version_id("m1")
                    .is_latest(true)
                    .build(),
            )
            .is_truncated(truncated);
        if truncated {
            builder
                .next_key_marker("k")
                .next_version_id_marker("v")
                .build()
        } else {
            builder.build()
        }
    }

    /// A prefix listing also returns longer keys: only the exact key's versions and markers count,
    /// and a page that reached another key ends the listing.
    #[test]
    fn only_the_exact_keys_versions_are_kept() -> Result<(), String> {
        let (entries, passed) =
            versions_of_key(&listing(false), "p/a").map_err(|e| format!("{e:?}"))?;
        let ids: Vec<(&str, bool, bool)> = entries
            .iter()
            .map(|entry| {
                (
                    entry.version_id.as_str(),
                    entry.is_latest,
                    entry.delete_marker,
                )
            })
            .collect();
        assert_eq!(
            ids,
            [
                ("v2", false, false),
                ("v1", false, false),
                ("m1", true, true)
            ]
        );
        assert!(passed);
        Ok(())
    }

    #[test]
    fn a_truncated_version_listing_must_advance() {
        let start = (None, None);
        assert_eq!(next_versions_marker(&listing(false), &start), Ok(None));
        assert_eq!(
            next_versions_marker(&listing(true), &start),
            Ok(Some((Some("k".into()), Some("v".into()))))
        );
        let repeated = (Some("k".to_string()), Some("v".to_string()));
        assert!(next_versions_marker(&listing(true), &repeated).is_err());
    }
}
