//! The document order of `ListObjectVersions`, read from real `MinIO` responses (P7 review,
//! existing issue 8).
//!
//! Fixtures (`tests/data/s3/`, captured on `MinIO` RELEASE.2023-03-20, VM 102, in a temporary
//! versioned bucket that was deleted afterwards): one key `race` written by 12 concurrent PUTs and
//! 4 concurrent DELETEs, three rounds — 44 entries (36 versions, 8 delete markers), 20 of which
//! share a `LastModified` millisecond with another. `minio-concurrent-versions.xml` is the one-page listing; `…-paged.xml` the same
//! listing with `max-keys=2` (22 pages, `<!--PAGE-END-->` between them), so the key straddles every
//! page boundary. The server lists the key's entries in reverse write order, versions and delete
//! markers interleaved, the latest first — the order every assertion here expects.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, PoisonError};

use aws_sdk_s3::Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_smithy_runtime_api::client::http::{
    HttpConnector, HttpConnectorFuture, SharedHttpConnector, http_client_fn,
};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::http::StatusCode;
use aws_smithy_types::body::SdkBody;
use bytes::Bytes;

use super::*;
use crate::model::{HistoryOrder, ObjectHistory, StoragePath, VersionRecord};
use crate::s3::S3Storage;
use crate::s3::tests::make_test_storage;
use crate::s3::version_listing::{VersionOrDeleteMarker, VersionPages, oldest_first_by_key};
use crate::storage::backends::s3::{S3Protocol, S3VersioningAtConnect, connect, tests::identity};
use crate::storage::listing::testing::listed;
use crate::storage::{PreflightPolicy, SourceDescriptor};

const FULL: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/data/s3/minio-concurrent-versions.xml"
));
const PAGED: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/data/s3/minio-concurrent-versions-paged.xml"
));

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

/// A connector that answers every request with the next canned (status, XML body).
#[derive(Clone, Debug)]
struct Replay(Arc<Mutex<VecDeque<(u16, Bytes)>>>);

impl HttpConnector for Replay {
    fn call(&self, _request: HttpRequest) -> HttpConnectorFuture {
        let (status, body) = self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop_front()
            .unwrap_or((200, Bytes::new()));
        HttpConnectorFuture::new(async move {
            let status = StatusCode::try_from(status).unwrap_or_else(|e| panic!("{e}"));
            Ok(HttpResponse::new(status, SdkBody::from(body)))
        })
    }
}

/// An SDK client whose requests are answered with `bodies` (200), in turn.
fn replay_client(bodies: Vec<String>) -> Client {
    replay_responses(bodies.into_iter().map(|body| (200, body)).collect())
}

/// An SDK client whose requests (and retries) are answered with `responses`, in turn.
fn replay_responses(responses: Vec<(u16, String)>) -> Client {
    let replay = Replay(Arc::new(Mutex::new(
        responses
            .into_iter()
            .map(|(status, body)| (status, Bytes::from(body)))
            .collect(),
    )));
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url("http://replay.invalid")
        .credentials_provider(Credentials::new("ak", "sk", None, None, "replay"))
        .force_path_style(true)
        .http_client(http_client_fn(move |_, _| {
            SharedHttpConnector::new(replay.clone())
        }))
        .build();
    Client::from_conf(config)
}

/// A legacy `S3Storage` (bucket `test-bucket`, no prefix) whose requests get `bodies`.
pub(crate) fn replay_storage(bodies: Vec<String>) -> S3Storage {
    let mut storage = make_test_storage(None);
    storage.client = replay_client(bodies);
    storage
}

fn pages() -> Vec<String> {
    PAGED
        .split("<!--PAGE-END-->")
        .map(str::trim)
        .filter(|page| !page.is_empty())
        .map(str::to_string)
        .collect()
}

/// The version ids of `xml`'s entries in document order.
fn document_ids(xml: &str) -> Vec<String> {
    xml.split("<VersionId>")
        .skip(1)
        .filter_map(|rest| rest.split_once("</VersionId>"))
        .map(|(id, _)| id.to_string())
        .collect()
}

/// The key's true history, oldest first: the server's order reversed.
fn written_order() -> Vec<String> {
    let mut ids = document_ids(FULL);
    ids.reverse();
    ids
}

/// Every record of the listed keys' histories.
fn records(entries: &[SourceDescriptor]) -> Vec<&VersionRecord> {
    entries
        .iter()
        .filter_map(|entry| entry.listing.history.as_deref())
        .flat_map(ObjectHistory::records)
        .collect()
}

fn listed_ids(entries: &[SourceDescriptor]) -> Vec<String> {
    records(entries)
        .into_iter()
        .map(|record| record.version_id().unwrap_or("null").to_string())
        .collect()
}

/// The role listing of a versioned bucket answered by `storage`.
async fn list_all(mut storage: S3Storage) -> TestResult<Vec<SourceDescriptor>> {
    storage.versioning_at_connect = S3VersioningAtConnect::Enabled;
    let roles = connect(Arc::new(storage), identity(), None)?;
    let namespace = roles.namespace(&PreflightPolicy::production())?;
    let (entries, failures) = listed(&*namespace, &StoragePath::root()).await?;
    assert!(failures.is_empty(), "{failures:?}");
    Ok(entries)
}

#[test]
fn the_fixture_is_the_concurrent_write_listing() {
    let ids = document_ids(FULL);
    assert_eq!(ids.len(), 44);
    // 12 DELETEs made 8 markers: MinIO adds none when the latest entry already is one.
    assert_eq!(FULL.matches("<DeleteMarker>").count(), 8);
    let paged: Vec<String> = pages().iter().flat_map(|page| document_ids(page)).collect();
    assert_eq!(paged, ids);
}

/// The document order aligns with the SDK's two lists: every entry of the fixture gets its place.
#[tokio::test]
async fn document_positions_follow_the_raw_response() -> TestResult {
    let client = replay_client(vec![FULL.to_string()]);
    let listed = send_in_document_order(client.list_object_versions().bucket("b")).await?;
    let order = listed.order.ok_or("no document order")?;
    let ids = document_ids(FULL);
    for (index, version) in listed.output.versions().iter().enumerate() {
        let position = order.version(index).ok_or("version without position")?;
        let at = usize::try_from(position)?;
        assert_eq!(Some(ids[at].as_str()), version.version_id());
    }
    for (index, marker) in listed.output.delete_markers().iter().enumerate() {
        let position = order.marker(index).ok_or("marker without position")?;
        let at = usize::try_from(position)?;
        assert_eq!(Some(ids[at].as_str()), marker.version_id());
    }
    Ok(())
}

/// A retried request keeps the body of the attempt that succeeded, not the throttled one.
#[tokio::test]
async fn a_retried_request_reads_the_successful_attempts_order() -> TestResult {
    let throttled = "<Error><Code>SlowDown</Code><Message>slow down</Message></Error>";
    let client = replay_responses(vec![(503, throttled.to_string()), (200, FULL.to_string())]);
    let listed = send_in_document_order(client.list_object_versions().bucket("b")).await?;
    assert_eq!(listed.output.versions().len(), 36);
    let order = listed.order.ok_or("no document order after a retry")?;
    assert_eq!(order.version(0), Some(1));
    assert_eq!(order.marker(0), Some(0));
    Ok(())
}

/// Role-based (C11b: one child per key with its history): the concurrent-write history comes out
/// in true write order, from the server's order; ordering the same entries by `LastModified` (the
/// order before this fix) does not.
#[tokio::test]
async fn the_role_listing_orders_concurrent_writes_as_the_server_did() -> TestResult {
    let entries = list_all(replay_storage(vec![FULL.to_string()])).await?;
    assert_eq!(entries.len(), 1, "one key, one child");
    assert_eq!(listed_ids(&entries), written_order());
    assert!(entries.iter().all(|entry| {
        entry
            .listing
            .history
            .as_deref()
            .is_some_and(|history| history.order() == HistoryOrder::Server)
    }));
    let latest: Vec<bool> = records(&entries)
        .into_iter()
        .map(VersionRecord::is_latest)
        .collect();
    assert_eq!(latest.iter().filter(|latest| **latest).count(), 1);
    assert_eq!(latest.last(), Some(&true));

    // Without the document order: a time merge, marked inferred, and wrong on this listing.
    let client = replay_client(vec![FULL.to_string()]);
    let output = client.list_object_versions().bucket("b").send().await?;
    let page = VersionsInOrder {
        output,
        order: None,
    };
    let mut pages = VersionPages::default();
    let entries = pages.positioned(&page, |_| true);
    let entries = pages.complete_keys(entries, false);
    let histories = oldest_first_by_key(entries);
    assert_eq!(histories.len(), 1);
    assert_eq!(histories[0].order, HistoryOrder::Inferred);
    let by_time: Vec<String> = histories[0]
        .entries
        .iter()
        .map(|entry| match entry {
            VersionOrDeleteMarker::Version(v) => v.version_id(),
            VersionOrDeleteMarker::DeleteMarker(m) => m.version_id(),
        })
        .map(|id| id.unwrap_or_default().to_string())
        .collect();
    assert_ne!(by_time, written_order());
    Ok(())
}

/// The same history over 22 pages of two entries: the key straddles every page boundary and still
/// comes out whole, in write order.
#[tokio::test]
async fn a_key_straddling_every_page_keeps_the_servers_order() -> TestResult {
    let entries = list_all(replay_storage(pages())).await?;
    assert_eq!(listed_ids(&entries), written_order());
    Ok(())
}

/// The protocol page of the role listing carries the positions too.
#[tokio::test]
async fn a_role_protocol_page_carries_positions() -> TestResult {
    let storage = replay_storage(vec![FULL.to_string()]);
    let page = storage
        .list_versions_page("", None)
        .await
        .map_err(|e| format!("{e:?}"))?;
    let mut positions: Vec<u64> = page
        .versions
        .iter()
        .chain(&page.markers)
        .map(|entry| entry.position.ok_or("no position"))
        .collect::<Result<_, _>>()?;
    positions.sort_unstable();
    assert_eq!(positions, (0..44).collect::<Vec<u64>>());
    Ok(())
}

fn output_of(entries: &[(&str, &str, bool)]) -> ListObjectVersionsOutput {
    let mut builder = ListObjectVersionsOutput::builder();
    for (key, id, marker) in entries {
        if *marker {
            builder = builder.delete_markers(
                aws_sdk_s3::types::DeleteMarkerEntry::builder()
                    .key(*key)
                    .version_id(*id)
                    .build(),
            );
        } else {
            builder = builder.versions(
                aws_sdk_s3::types::ObjectVersion::builder()
                    .key(*key)
                    .version_id(*id)
                    .build(),
            );
        }
    }
    builder.build()
}

/// Keys are compared as text: entity and character references resolve, and elements of other
/// names (the owner, common prefixes) do not count.
#[test]
fn escaped_keys_are_read_as_text() {
    let body = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>b</Name>
<DeleteMarker><Key>a &amp; &#34;b&#34;</Key><VersionId>m1</VersionId><Owner><ID>o</ID></Owner></DeleteMarker>
<Version><Key>a &amp; &#34;b&#34;</Key><VersionId>v1</VersionId><ETag>&#34;e&#34;</ETag></Version>
<CommonPrefixes><Prefix>d/</Prefix></CommonPrefixes>
<Version><Key><![CDATA[c<d]]></Key><VersionId>null</VersionId></Version>
</ListVersionsResult>"#;
    let output = output_of(&[
        ("a & \"b\"", "m1", true),
        ("a & \"b\"", "v1", false),
        ("c<d", "null", false),
    ]);
    let order = DocumentOrder::read(body.as_bytes(), &output);
    assert_eq!(
        order,
        Some(DocumentOrder {
            versions: vec![1, 2],
            markers: vec![0],
        })
    );
}

/// A body that names other entries than the SDK parsed, or is not well formed, gives no order.
#[test]
fn a_body_that_disagrees_with_the_sdk_gives_no_order() {
    let body = "<ListVersionsResult><Version><Key>k</Key><VersionId>v2</VersionId></Version>\
                <DeleteMarker><Key>k</Key><VersionId>m</VersionId></DeleteMarker>\
                </ListVersionsResult>";
    let parsed = output_of(&[("k", "v2", false), ("k", "m", true)]);
    assert!(DocumentOrder::read(body.as_bytes(), &parsed).is_some());
    let other_id = output_of(&[("k", "v1", false), ("k", "m", true)]);
    assert_eq!(DocumentOrder::read(body.as_bytes(), &other_id), None);
    let extra = output_of(&[("k", "v2", false), ("k", "m", true), ("k", "v1", false)]);
    assert_eq!(DocumentOrder::read(body.as_bytes(), &extra), None);
    let truncated = &body[..body.len() - 10];
    assert_eq!(DocumentOrder::read(truncated.as_bytes(), &parsed), None);
}

/// Empty elements and a prefixed namespace are read too: `<Version/>` names no key or id, an empty
/// `<Key/>` an empty key.
#[test]
fn empty_elements_and_prefixed_names_are_read() {
    let body = "<s3:ListVersionsResult xmlns:s3=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                <s3:Version/>\
                <s3:DeleteMarker><s3:Key/><s3:VersionId>m</s3:VersionId></s3:DeleteMarker>\
                <s3:Version><s3:Key>k</s3:Key><s3:VersionId>v</s3:VersionId></s3:Version>\
                </s3:ListVersionsResult>";
    let output = ListObjectVersionsOutput::builder()
        .versions(aws_sdk_s3::types::ObjectVersion::builder().build())
        .versions(
            aws_sdk_s3::types::ObjectVersion::builder()
                .key("k")
                .version_id("v")
                .build(),
        )
        .delete_markers(
            aws_sdk_s3::types::DeleteMarkerEntry::builder()
                .key("")
                .version_id("m")
                .build(),
        )
        .build();
    assert_eq!(
        DocumentOrder::read(body.as_bytes(), &output),
        Some(DocumentOrder {
            versions: vec![0, 2],
            markers: vec![1],
        })
    );
}
