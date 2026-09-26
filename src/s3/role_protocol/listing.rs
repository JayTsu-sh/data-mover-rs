//! Delimiter listings of the role protocol (ADR-0006 C22): one page of `ListObjectsV2` or of
//! `ListObjectVersions` under one prefix, with keys and common prefixes made relative to the
//! storage root. Continuation tokens and markers are passed back exactly as the server sent them.

use aws_sdk_s3::operation::list_object_versions::ListObjectVersionsOutput;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
use aws_sdk_s3::primitives::DateTime;
use aws_sdk_s3::types::{CommonPrefix, DeleteMarkerEntry, Object, ObjectVersion};

use super::{
    ProvideErrorMetadata, S3ProtocolFailure, S3Result, S3Storage, s3_role_remote_failure,
    s3_role_transport_failure,
};
use crate::model::{FailureClass, StorageTimestamp, Transience};
use crate::s3::version_order::{DocumentOrder, send_in_document_order};
use crate::storage::backends::s3::{
    S3ListedObject, S3ListedVersion, S3ObjectPage, S3RejectedEntry, S3VersionFacts,
    S3VersionMarker, S3VersionPage,
};
use crate::time_util::s3_listing_time;

impl S3Storage {
    /// The storage root every listed key must start with: the URL's prefix (ending in `/`), or
    /// nothing.
    fn listing_root(&self) -> &str {
        self.prefix.as_deref().unwrap_or_default()
    }

    pub(super) async fn role_list_objects_page(
        &self,
        prefix: &str,
        token: Option<&str>,
    ) -> S3Result<S3ObjectPage> {
        let response = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket_name)
            .prefix(self.build_full_key(prefix))
            .delimiter("/")
            .set_continuation_token(token.map(str::to_string))
            .send()
            .await
            .map_err(|error| {
                listing_failure(classify_sdk!(error, "S3 ListObjectsV2 request failed"))
            })?;
        object_page(&response, self.listing_root())
    }

    pub(super) async fn role_list_versions_page(
        &self,
        prefix: &str,
        marker: Option<&S3VersionMarker>,
    ) -> S3Result<S3VersionPage> {
        let request = self
            .client
            .list_object_versions()
            .bucket(&self.bucket_name)
            .prefix(self.build_full_key(prefix))
            .delimiter("/")
            .set_key_marker(marker.map(|marker| marker.key_marker.clone()))
            .set_version_id_marker(marker.and_then(|marker| marker.version_id_marker.clone()));
        let response = send_in_document_order(request).await.map_err(|error| {
            listing_failure(classify_sdk!(
                *error,
                "S3 ListObjectVersions request failed"
            ))
        })?;
        version_page(
            &response.output,
            response.order.as_ref(),
            self.listing_root(),
        )
    }
}

/// A listing names a prefix, which never 404s: only a missing bucket does, and that ends the
/// session rather than one directory.
fn listing_failure(failure: S3ProtocolFailure) -> S3ProtocolFailure {
    match failure {
        S3ProtocolFailure::Entry {
            class: FailureClass::NotFound,
            ..
        } => S3ProtocolFailure::session(
            FailureClass::NotFound,
            Transience::Permanent,
            "the S3 bucket does not exist",
        ),
        other => other,
    }
}

/// `key` relative to the storage root. A missing key, or one outside the root (the server ignored
/// the prefix), is that one element's rejection, never the page's failure.
fn relative(key: Option<&str>, root: &str) -> Result<String, S3RejectedEntry> {
    key.and_then(|key| key.strip_prefix(root))
        .map(str::to_string)
        .ok_or_else(|| S3RejectedEntry {
            key: None,
            raw_key: key.unwrap_or_default().to_string(),
            reason: if key.is_none() {
                "the S3 listing returned an entry without a key"
            } else {
                "the S3 listing returned a key outside its prefix"
            },
        })
}

fn listed_time(time: Option<&DateTime>) -> Option<StorageTimestamp> {
    time.and_then(|time| s3_listing_time(time.secs(), time.subsec_nanos()))
}

/// The size of the listed object `key`; a missing or negative one rejects that object alone.
fn listed_size(
    size: Option<i64>,
    key: &str,
    raw_key: Option<&str>,
) -> Result<u64, S3RejectedEntry> {
    size.and_then(|size| u64::try_from(size).ok())
        .ok_or_else(|| S3RejectedEntry {
            key: Some(key.to_string()),
            raw_key: raw_key.unwrap_or_default().to_string(),
            reason: "the S3 listing gave an object a missing or negative size",
        })
}

/// Decoded `items`, with every element that could not be decoded moved to `rejected`.
fn decoded<T, U>(
    items: impl IntoIterator<Item = T>,
    decode: impl Fn(T) -> Result<U, S3RejectedEntry>,
    rejected: &mut Vec<S3RejectedEntry>,
) -> Vec<U> {
    let mut kept = Vec::new();
    for item in items {
        match decode(item) {
            Ok(value) => kept.push(value),
            Err(rejection) => rejected.push(rejection),
        }
    }
    kept
}

fn prefixes(
    prefixes: &[CommonPrefix],
    root: &str,
    rejected: &mut Vec<S3RejectedEntry>,
) -> Vec<String> {
    decoded(prefixes, |prefix| relative(prefix.prefix(), root), rejected)
}

fn listed_object(object: &Object, root: &str) -> Result<S3ListedObject, S3RejectedEntry> {
    let key = relative(object.key(), root)?;
    Ok(S3ListedObject {
        size: listed_size(object.size(), &key, object.key())?,
        key,
        etag: object.e_tag().unwrap_or_default().to_string(),
        last_modified: listed_time(object.last_modified()),
    })
}

/// Where the listing continues: a truncated page must name it; a page that leaves `IsTruncated`
/// out but names a continuation is followed (some stores omit the flag, and the legacy walk
/// follows the token alone); an explicitly complete page ends the listing.
fn continuation<'a>(
    truncated: Option<bool>,
    next: Option<&'a str>,
    what: &'static str,
) -> S3Result<Option<&'a str>> {
    match (truncated, next) {
        (Some(true), None) => Err(S3ProtocolFailure::protocol(what)),
        (Some(true) | None, next) => Ok(next),
        (Some(false), _) => Ok(None),
    }
}

fn object_page(response: &ListObjectsV2Output, root: &str) -> S3Result<S3ObjectPage> {
    let next = continuation(
        response.is_truncated(),
        response.next_continuation_token(),
        "truncated S3 ListObjectsV2 page has no token",
    )?
    .map(str::to_string);
    let mut rejected = Vec::new();
    let objects = decoded(
        response.contents(),
        |object| listed_object(object, root),
        &mut rejected,
    );
    let prefixes = prefixes(response.common_prefixes(), root, &mut rejected);
    Ok(S3ObjectPage {
        objects,
        prefixes,
        rejected,
        next,
    })
}

/// A listed entry without a version id is the `"null"` version: stores without versioning may
/// leave the id out, and one such entry must not end the whole listing.
fn listed_version_id(id: Option<&str>) -> String {
    id.filter(|id| !id.is_empty()).unwrap_or("null").to_string()
}

fn listed_version(
    version: &ObjectVersion,
    root: &str,
    position: Option<u64>,
) -> Result<S3ListedVersion, S3RejectedEntry> {
    let key = relative(version.key(), root)?;
    Ok(S3ListedVersion {
        facts: S3VersionFacts {
            version_id: listed_version_id(version.version_id()),
            is_latest: version.is_latest() == Some(true),
            delete_marker: false,
            size: listed_size(version.size(), &key, version.key())?,
            etag: version.e_tag().unwrap_or_default().to_string(),
        },
        key,
        last_modified: listed_time(version.last_modified()),
        position,
    })
}

fn listed_marker(
    marker: &DeleteMarkerEntry,
    root: &str,
    position: Option<u64>,
) -> Result<S3ListedVersion, S3RejectedEntry> {
    Ok(S3ListedVersion {
        key: relative(marker.key(), root)?,
        facts: S3VersionFacts {
            version_id: listed_version_id(marker.version_id()),
            is_latest: marker.is_latest() == Some(true),
            delete_marker: true,
            size: 0,
            etag: String::new(),
        },
        last_modified: listed_time(marker.last_modified()),
        position,
    })
}

/// One page of versions; `order` (the response's document order, when it could be read) gives
/// every entry its position.
fn version_page(
    response: &ListObjectVersionsOutput,
    order: Option<&DocumentOrder>,
    root: &str,
) -> S3Result<S3VersionPage> {
    let next = continuation(
        response.is_truncated(),
        response.next_key_marker(),
        "truncated S3 ListObjectVersions page has no key marker",
    )?
    .map(|key_marker| S3VersionMarker {
        key_marker: key_marker.to_string(),
        version_id_marker: response.next_version_id_marker().map(str::to_string),
    });
    let mut rejected = Vec::new();
    let versions = decoded(
        response.versions().iter().enumerate(),
        |(index, version)| {
            listed_version(version, root, order.and_then(|order| order.version(index)))
        },
        &mut rejected,
    );
    let markers = decoded(
        response.delete_markers().iter().enumerate(),
        |(index, marker)| listed_marker(marker, root, order.and_then(|order| order.marker(index))),
        &mut rejected,
    );
    let prefixes = prefixes(response.common_prefixes(), root, &mut rejected);
    Ok(S3VersionPage {
        versions,
        markers,
        prefixes,
        rejected,
        next,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn common(prefix: &str) -> CommonPrefix {
        CommonPrefix::builder().prefix(prefix).build()
    }

    /// Keys and prefixes lose the storage root; a MinIO-style marker is kept exactly as sent,
    /// root included, because it is a token, not a key.
    #[test]
    fn keys_lose_the_root_and_markers_are_kept_verbatim() -> Result<(), String> {
        let response = ListObjectVersionsOutput::builder()
            .versions(
                ObjectVersion::builder()
                    .key("root/d/k")
                    .version_id("v1")
                    .is_latest(true)
                    .size(3)
                    .e_tag("\"e\"")
                    .last_modified(DateTime::from_millis(1_500))
                    .build(),
            )
            .delete_markers(
                DeleteMarkerEntry::builder()
                    .key("root/d/gone")
                    .version_id("m1")
                    .is_latest(true)
                    .build(),
            )
            .common_prefixes(common("root/d/sub/"))
            .is_truncated(true)
            .next_key_marker("root/d/k[minio_cache:v2,return:]")
            .build();
        let page = version_page(&response, None, "root/").map_err(|e| format!("{e:?}"))?;
        assert_eq!(page.versions[0].key, "d/k");
        assert_eq!(
            page.versions[0]
                .last_modified
                .map(StorageTimestamp::unix_nanos),
            Some(1_500_000_000)
        );
        assert!(page.markers[0].facts.delete_marker && page.markers[0].key == "d/gone");
        assert_eq!(page.prefixes, ["d/sub/"]);
        assert_eq!(
            page.next,
            Some(S3VersionMarker {
                key_marker: "root/d/k[minio_cache:v2,return:]".into(),
                version_id_marker: None,
            })
        );
        Ok(())
    }

    #[test]
    fn a_key_outside_the_root_or_a_truncated_page_without_a_token_is_refused() {
        let outside = ListObjectsV2Output::builder()
            .contents(Object::builder().key("other/a").size(1).build())
            .contents(Object::builder().key("root/b").size(1).build())
            .build();
        let page = object_page(&outside, "root/").unwrap_or_else(|e| panic!("{e:?}"));
        assert_eq!(page.objects.len(), 1, "the sibling inside the root is kept");
        assert_eq!(page.rejected.len(), 1);
        assert_eq!(page.rejected[0].key, None);
        assert_eq!(page.rejected[0].raw_key, "other/a");
        let tokenless = ListObjectsV2Output::builder().is_truncated(true).build();
        assert!(object_page(&tokenless, "").is_err());
        let flagless = ListObjectsV2Output::builder()
            .next_continuation_token("t")
            .build();
        let page = object_page(&flagless, "").unwrap_or_else(|e| panic!("{e:?}"));
        assert_eq!(page.next.as_deref(), Some("t"));
        let complete = ListObjectsV2Output::builder()
            .is_truncated(false)
            .next_continuation_token("t")
            .build();
        let page = object_page(&complete, "").unwrap_or_else(|e| panic!("{e:?}"));
        assert_eq!(page.next, None);
        let idless = ListObjectVersionsOutput::builder()
            .versions(ObjectVersion::builder().key("k").size(1).build())
            .build();
        let page = version_page(&idless, None, "").unwrap_or_else(|e| panic!("{e:?}"));
        assert_eq!(page.versions[0].facts.version_id, "null");
        let markerless = ListObjectVersionsOutput::builder()
            .is_truncated(true)
            .build();
        assert!(version_page(&markerless, None, "").is_err());
        let token = ListObjectsV2Output::builder()
            .contents(Object::builder().key("a").size(1).build())
            .common_prefixes(common("b/"))
            .is_truncated(true)
            .next_continuation_token("t")
            .build();
        let page = object_page(&token, "").unwrap_or_else(|e| panic!("{e:?}"));
        assert_eq!(page.next.as_deref(), Some("t"));
        assert_eq!(page.prefixes, ["b/"]);
    }

    /// An object without a valid size, and a key outside the root, reject that one element: the
    /// page and its siblings stand (P7 C3; this used to end the whole session).
    #[test]
    fn an_undecodable_element_is_rejected_alone() {
        let objects = ListObjectsV2Output::builder()
            .contents(Object::builder().key("root/d/negative").size(-1).build())
            .contents(Object::builder().key("root/d/sizeless").build())
            .contents(Object::builder().key("root/d/fine").size(2).build())
            .common_prefixes(common("elsewhere/"))
            .build();
        let page = object_page(&objects, "root/").unwrap_or_else(|e| panic!("{e:?}"));
        assert_eq!(page.objects.len(), 1);
        assert_eq!(page.objects[0].key, "d/fine");
        let keys: Vec<_> = page.rejected.iter().map(|r| r.key.as_deref()).collect();
        assert_eq!(keys, [Some("d/negative"), Some("d/sizeless"), None]);
        assert_eq!(page.rejected[2].raw_key, "elsewhere/");
        let versions = ListObjectVersionsOutput::builder()
            .versions(ObjectVersion::builder().key("k").version_id("v2").build())
            .versions(
                ObjectVersion::builder()
                    .key("k")
                    .version_id("v1")
                    .size(1)
                    .build(),
            )
            .build();
        let page = version_page(&versions, None, "").unwrap_or_else(|e| panic!("{e:?}"));
        assert_eq!(page.versions.len(), 1);
        assert_eq!(page.rejected.len(), 1);
        assert_eq!(page.rejected[0].key.as_deref(), Some("k"));
    }

    /// A missing bucket ends the session; any other listing failure keeps its scope.
    #[test]
    fn only_a_missing_bucket_becomes_a_session_failure() {
        let missing = s3_role_remote_failure(Some(404), Some("NoSuchBucket"), "list");
        assert!(matches!(
            listing_failure(missing),
            S3ProtocolFailure::Session {
                class: FailureClass::NotFound,
                ..
            }
        ));
        let denied = s3_role_remote_failure(Some(403), None, "list");
        assert!(matches!(
            listing_failure(denied),
            S3ProtocolFailure::Entry {
                class: FailureClass::PermissionDenied,
                ..
            }
        ));
    }
}
