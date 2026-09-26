//! `compat=hcp`: an object's tags are its HCP REST `BPM` annotation, read as the legacy path reads
//! them, not S3 object tagging.

use super::{s3_role_remote_failure, s3_role_transport_failure};
use crate::model::{FailureClass, ObjectTag, Transience};
use crate::s3::S3Storage;
use crate::storage::backends::s3::{S3ProtocolFailure, S3Result};
use crate::third_party::hcp::client::HcpAnnotationError;

impl S3Storage {
    /// The tags of the object at `key` (of one version when `version_id` is given); none when the
    /// object has no `BPM` annotation. A directory object (`d/`) has none: it is not asked for.
    pub(super) async fn hcp_tags(
        &self,
        key: &str,
        version_id: Option<&str>,
    ) -> S3Result<Vec<ObjectTag>> {
        if key.ends_with('/') {
            return Ok(Vec::new());
        }
        let client = self.hcp_client.as_ref().ok_or_else(|| {
            S3ProtocolFailure::protocol("a compat=hcp storage has no HCP REST client")
        })?;
        let tags = client
            .annotation_tags(&self.build_full_key(key), version_id)
            .await
            .map_err(|error| annotation_failure(error, version_id.is_some()))?;
        tags.unwrap_or_default()
            .into_iter()
            .map(|tag| {
                ObjectTag::new(tag.key, tag.value).map_err(|error| {
                    S3ProtocolFailure::entry(
                        FailureClass::Corruption,
                        Transience::Permanent,
                        error.to_string(),
                    )
                })
            })
            .collect()
    }
}

/// The failure of an annotation read. HCP names no error code: a status alone classifies it, and a
/// 400 for a pinned version is the version query being refused (unverified on a real HCP).
fn annotation_failure(error: HcpAnnotationError, pinned: bool) -> S3ProtocolFailure {
    const DIAGNOSTIC: &str = "HCP REST annotation request failed";
    match error {
        HcpAnnotationError::Transport => s3_role_transport_failure(DIAGNOSTIC),
        HcpAnnotationError::Status(400) if pinned => S3ProtocolFailure::entry(
            FailureClass::Unsupported,
            Transience::Permanent,
            "HCP REST refused the annotation of a pinned object version",
        ),
        HcpAnnotationError::Status(status) => {
            s3_role_remote_failure(Some(status), None, DIAGNOSTIC)
        }
        HcpAnnotationError::Malformed => S3ProtocolFailure::entry(
            FailureClass::Protocol,
            Transience::Permanent,
            "the HCP BPM annotation is not the expected XML",
        ),
        HcpAnnotationError::UnaddressableKey => S3ProtocolFailure::entry(
            FailureClass::InvalidInput,
            Transience::Permanent,
            "an object key with a `.` or `..` segment cannot be named in an HCP REST path",
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::PoisonError;

    use crate::AssertTestValue;
    use crate::model::{FailureClass, Transience};
    use crate::s3::S3Compatibility;
    use crate::s3::tests::make_test_storage;
    use crate::storage::backends::s3::{S3Protocol, S3ProtocolFailure};
    use crate::third_party::hcp::client::tests::{ANNOTATION, Seen, client, endpoint};

    /// A `compat=hcp` storage under `prefix/` whose namespace answers at `address`.
    fn hcp_storage(address: SocketAddr) -> crate::s3::S3Storage {
        let mut storage = make_test_storage(Some("prefix/"));
        storage.options.compat = S3Compatibility::Hcp;
        storage.hcp_client = Some(client(address));
        storage
    }

    fn requests(seen: &Seen) -> Vec<String> {
        seen.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }

    /// The failure a namespace answering `status` gives.
    async fn failure(status: &'static str, version: Option<&str>) -> Option<S3ProtocolFailure> {
        let (address, _) = endpoint(status, "").await;
        hcp_storage(address).get_tags("k", version).await.err()
    }

    #[tokio::test]
    async fn tags_come_from_the_rest_annotation() {
        let (address, seen) = endpoint("200 OK", ANNOTATION).await;
        let tags = hcp_storage(address)
            .get_tags("dir/a b.txt", None)
            .await
            .assert_value("tags");
        let pairs: Vec<(&str, &str)> = tags.iter().map(|tag| (tag.key(), tag.value())).collect();
        assert_eq!(
            pairs,
            [("bpdName", "flow"), ("btNo", "42"), ("uploader", "alice")]
        );
        let head = requests(&seen).concat();
        assert!(
            head.starts_with(
                "GET /rest/prefix/dir/a%20b.txt?type=custom-metadata&annotation=BPM HTTP/1.1"
            ),
            "{head}"
        );
    }

    /// Every byte a key may hold reaches HCP as that key's path; `.` / `..` segments, which a
    /// path cannot carry, are refused before any request, and a directory object is not asked for.
    #[tokio::test]
    async fn keys_are_encoded_or_refused() {
        let (address, seen) = endpoint("200 OK", ANNOTATION).await;
        let storage = hcp_storage(address);
        for (key, path) in [
            ("a#b?c%d", "/rest/prefix/a%23b%3Fc%25d?"),
            ("é/ü", "/rest/prefix/%C3%A9/%C3%BC?"),
            ("/lead", "/rest/prefix//lead?"),
        ] {
            storage.get_tags(key, None).await.assert_value("tags");
            let head = requests(&seen).pop().assert_value("a request");
            assert!(head.starts_with(&format!("GET {path}")), "{key}: {head}");
        }
        let asked = requests(&seen).len();
        assert!(
            storage
                .get_tags("dir/", None)
                .await
                .assert_value("directory")
                .is_empty()
        );
        for key in ["a/../b", "./a"] {
            let refused = storage.get_tags(key, None).await.err();
            assert!(
                matches!(
                    refused,
                    Some(S3ProtocolFailure::Entry {
                        class: FailureClass::InvalidInput,
                        transience: Transience::Permanent,
                        ..
                    })
                ),
                "{key}: {refused:?}"
            );
        }
        assert_eq!(requests(&seen).len(), asked, "no request for these keys");
    }

    #[tokio::test]
    async fn a_pinned_version_is_asked_for() {
        let (address, seen) = endpoint("200 OK", ANNOTATION).await;
        hcp_storage(address)
            .get_tags("k", Some("93781022374785"))
            .await
            .assert_value("tags");
        let head = requests(&seen).concat();
        assert!(
            head.starts_with(
                "GET /rest/prefix/k?type=custom-metadata&annotation=BPM&version=93781022374785 "
            ),
            "{head}"
        );
    }

    #[tokio::test]
    async fn no_annotation_is_no_tags() {
        for status in ["404 Not Found", "204 No Content", "200 OK"] {
            let (address, _) = endpoint(status, "").await;
            let tags = hcp_storage(address)
                .get_tags("k", None)
                .await
                .assert_value("tags");
            assert!(tags.is_empty(), "{status}");
        }
    }

    #[tokio::test]
    async fn statuses_are_classified() {
        for (status, class, transience) in [
            (
                "403 Forbidden",
                FailureClass::PermissionDenied,
                Transience::Permanent,
            ),
            (
                "503 Service Unavailable",
                FailureClass::Throttled,
                Transience::Transient,
            ),
        ] {
            let failure = failure(status, None).await;
            assert!(
                matches!(
                    &failure,
                    Some(S3ProtocolFailure::Entry { class: c, transience: t, .. }
                        | S3ProtocolFailure::Session { class: c, transience: t, .. })
                        if *c == class && *t == transience
                ),
                "{status}: {failure:?}"
            );
        }
        let refused_version = failure("400 Bad Request", Some("1")).await;
        assert!(
            matches!(
                refused_version,
                Some(S3ProtocolFailure::Entry {
                    class: FailureClass::Unsupported,
                    ..
                })
            ),
            "{refused_version:?}"
        );
    }

    #[tokio::test]
    async fn an_unexpected_annotation_is_a_protocol_failure() {
        let (address, _) = endpoint("200 OK", "<not-bpm/>").await;
        let malformed = hcp_storage(address).get_tags("k", None).await.err();
        assert!(
            matches!(
                malformed,
                Some(S3ProtocolFailure::Entry {
                    class: FailureClass::Protocol,
                    transience: Transience::Permanent,
                    ..
                })
            ),
            "{malformed:?}"
        );
    }

    #[tokio::test]
    async fn an_unreachable_namespace_is_a_lost_connection() {
        // Nothing listens on the port the name resolves to.
        let closed = SocketAddr::from(([127, 0, 0, 1], 1));
        let unreachable = hcp_storage(closed).get_tags("k", None).await.err();
        assert!(
            matches!(
                unreachable,
                Some(S3ProtocolFailure::Session {
                    class: FailureClass::Connectivity,
                    transience: Transience::Transient,
                    ..
                })
            ),
            "{unreachable:?}"
        );
    }
}
