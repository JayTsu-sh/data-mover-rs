//! What one S3 listing does with a throttling service and with elements it cannot decode (P7 C3):
//! it backs off within a budget instead of ending the session, and an undecodable object, or a
//! key outside the listed prefix, fails that one child beside its siblings.

use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::*;
use crate::model::{FailureClass, Transience};
use crate::storage::backends::s3::S3RejectedEntry;
use crate::storage::backends::s3::namespace::S3Namespace;
use crate::storage::backends::s3::paging::ListingLimits;
use crate::storage::listing::testing::listed;
use crate::storage::throttle::ThrottleBackoff;
use crate::storage::{CollectLimits, ListRequest, Namespace, StorageRoleFailure, collect_listing};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn path(value: &str) -> StoragePath {
    StoragePath::new(value).unwrap_or_else(|error| panic!("{error}"))
}

async fn bucket(keys: &[&str]) -> TestResult<Arc<MemoryS3>> {
    let protocol = Arc::new(MemoryS3::default());
    for key in keys {
        let body = Bytes::from_static(b"x");
        protocol
            .put_object(key, body.clone().into(), &content_md5(&body))
            .await
            .map_err(|e| format!("{e:?}"))?;
    }
    Ok(protocol)
}

/// A namespace that backs off for at most `budget` in total, in delays of a few milliseconds.
fn backing_off(protocol: &Arc<MemoryS3>, budget: Duration) -> S3Namespace<MemoryS3> {
    S3Namespace::new(protocol.clone(), identity()).with_limits(ListingLimits {
        throttle: ThrottleBackoff {
            first: Duration::from_millis(2),
            max_delay: Duration::from_millis(8),
            budget,
        },
        ..ListingLimits::PRODUCTION
    })
}

/// What the role protocol maps a 503 `SlowDown` to (`crate::storage::backends::s3::client::role_protocol`, whose own test
/// pins that mapping; role tests use only the protocol seam).
fn slow_down() -> S3ProtocolFailure {
    S3ProtocolFailure::session(
        FailureClass::Throttled,
        Transience::Transient,
        "S3 ListObjectsV2 request failed",
    )
}

/// Pages answered 503 `SlowDown` are requested again after a backoff, and the listing completes
/// with every child. Fault injection: stop treating `Throttled` as a reason to back off and this
/// listing ends the session instead — red (and the mapping test in `role_protocol` goes red if
/// `SlowDown` stops mapping to `Throttled`).
#[tokio::test]
async fn a_throttled_page_is_requested_again_within_the_budget() -> TestResult {
    let protocol = bucket(&["d/a", "d/b", "d/c"]).await?;
    protocol.listing().page_size = 1;
    protocol.listing().throttled = Some((3, slow_down()));
    let namespace = backing_off(&protocol, Duration::from_secs(5));
    let (entries, failures) = listed(&namespace, &path("d")).await?;
    let paths: Vec<&str> = entries.iter().map(|entry| entry.path.as_str()).collect();
    assert_eq!(paths, ["d/a", "d/b", "d/c"]);
    assert!(failures.is_empty(), "{failures:?}");
    // Three pages, plus the three throttled attempts at the first one.
    assert_eq!(protocol.listing().calls.len(), 6);
    Ok(())
}

/// Still throttled once the budget is spent: the listing fails — as this directory's failure,
/// `Throttled` and `Transient` — never as the session's.
#[tokio::test]
async fn throttling_past_the_budget_fails_only_the_listing() -> TestResult {
    let protocol = bucket(&["d/a"]).await?;
    protocol.listing().throttled = Some((usize::MAX, slow_down()));
    let namespace = backing_off(&protocol, Duration::from_millis(30));
    let failure = listed(&namespace, &path("d")).await.err();
    let Some(StorageRoleFailure::Entry(failure)) = failure else {
        panic!("expected the listing's own failure, got {failure:?}")
    };
    assert_eq!(failure.path(), &path("d"));
    assert_eq!(
        (failure.class(), failure.transience()),
        (FailureClass::Throttled, Transience::Transient)
    );
    assert!(protocol.listing().calls.len() > 1, "it did back off first");
    Ok(())
}

fn rejected(key: Option<&str>, raw_key: &str) -> S3RejectedEntry {
    S3RejectedEntry {
        key: key.map(str::to_owned),
        raw_key: raw_key.to_owned(),
        reason: "the S3 listing gave an object a missing or negative size",
    }
}

/// An object listed without a valid size fails alone, at its own path, and a key outside the
/// storage root or outside the listed directory fails alone at the directory with its raw key.
/// All of these used to end the session.
#[tokio::test]
async fn an_undecodable_element_fails_that_child_alone() -> TestResult {
    let protocol = bucket(&["d/good"]).await?;
    protocol.listing().rejected = vec![
        ("d/".into(), rejected(Some("d/bad"), "d/bad")),
        ("d/".into(), rejected(None, "other-root/x")),
        // Below a deeper `/`: only rolls its subdirectory up; that listing reports it.
        ("d/".into(), rejected(Some("d/sub/deep"), "d/sub/deep")),
    ];
    protocol.listing().stray = vec![(
        "d/".into(),
        S3ListedObject {
            key: "e/stray".into(),
            size: 1,
            etag: "\"e\"".into(),
            last_modified: None,
        },
    )];
    let namespace = S3Namespace::new(protocol.clone(), identity());
    let request = ListRequest::new(path("d"), CancellationToken::new());
    let page = collect_listing(namespace.list(request), CollectLimits::UNBOUNDED).await?;
    let (entries, failures) = page.clone().into_descriptors(&path("d"));
    let paths: Vec<&str> = entries.iter().map(|entry| entry.path.as_str()).collect();
    assert_eq!(paths, ["d/good", "d/sub"]);
    let blamed: Vec<(&str, FailureClass)> = failures
        .iter()
        .map(|failure| (failure.path().as_str(), failure.class()))
        .collect();
    assert_eq!(
        blamed,
        [
            ("d/bad", FailureClass::Protocol),
            ("d", FailureClass::Protocol),
            ("d", FailureClass::Protocol),
        ]
    );
    let raw: Vec<Option<&[u8]>> = page
        .failures
        .iter()
        .map(|failure| failure.raw_name.as_deref())
        .collect();
    assert_eq!(
        raw,
        [None, Some(&b"e/stray"[..]), Some(&b"other-root/x"[..])]
    );
    Ok(())
}

/// A key with one version the listing could not decode has no whole history: the key fails, and
/// none of its versions is listed as if it were all of them.
#[tokio::test]
async fn a_key_with_an_undecodable_version_fails_whole() -> TestResult {
    let protocol = bucket(&["d/k", "d/other"]).await?;
    protocol.listing().rejected = vec![("d/".into(), rejected(Some("d/k"), "d/k"))];
    // Never versioned, but refusing to say: its keys are listed with their histories (Q26).
    protocol.answer_at_connect(S3VersioningAtConnect::Refused);
    let namespace = S3Namespace::new(protocol.clone(), identity());
    let (entries, failures) = listed(&namespace, &path("d")).await?;
    let paths: Vec<&str> = entries.iter().map(|entry| entry.path.as_str()).collect();
    assert_eq!(paths, ["d/other"]);
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].path(), &path("d/k"));
    Ok(())
}
