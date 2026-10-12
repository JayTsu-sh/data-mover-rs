//! ONTAP's `ListObjectVersions` with delimiter `/` (P7 existing issue 10, F4), replayed from real
//! responses through the SDK and the document-order interceptor.
//!
//! ONTAP 9.19.1 can end a truncated page inside a key and name that key as `NextKeyMarker` without
//! a `NextVersionIdMarker`; the next page, asked with the key marker alone, starts after the whole
//! key, and the rest of its versions are silently lost. It can also repeat a common prefix on the
//! next page, and end a listing with an empty page.
//!
//! Fixtures (`tests/data/s3/`, captured 2026-09-26 on ONTAP 9.19.1, SVM `lizy`, bucket
//! `data-mover-test-ver`, prefix `f4-1790409358/`, deleted afterwards), pages separated by
//! `<!--PAGE-END-->`; each page is the answer to the request its echoed `Prefix` / `KeyMarker` /
//! `VersionIdMarker` name — both the continuation a listing without the guard asks for and the one
//! with it:
//! - `ontap-versions-delimiter-small.xml` (`max-keys=3`): `mks/a`, `mks/x/中/y` and seven versions
//!   of `mks/z`. The first page holds `a`, the newest `z` and the prefix `x/`, and names `z` with no
//!   version marker: the key marker alone answers an empty, complete page (six versions lost); the
//!   guarded listing reads them over two more pages and an empty last one.
//! - `ontap-versions-delimiter-markers.xml` (`max-keys=7`): `mkd/a` (two versions under a latest
//!   delete marker), `mkd/x/中/y`, and `mkd/z` written put, delete, put, put, delete, put, put. The
//!   first page ends on a delete marker of `z` that follows two of its versions (both kinds on
//!   the page: the last entry comes from the document order); the key marker alone loses four
//!   entries.
//! - `ontap-versions-delimiter-mixed.xml` (`max-keys=11`): 80 random names (`é`, `中`, `.`, `-`…)
//!   with one to three versions each (128 entries in `rnd/`, 18 subdirectories): pages that end
//!   inside a key without a version marker, and common prefixes repeated on the next page.
//!
//! The replay answers a request no fixture page echoes with an empty, complete listing: a
//! subdirectory's listing, or a continuation neither walk asked for.

use std::collections::{BTreeSet, HashMap};
use std::fmt::Debug;
use std::sync::{Arc, Mutex, PoisonError};

use aws_sdk_s3::Client;
use aws_sdk_s3::Config as SdkConfig;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::types::{DeleteMarkerEntry, ObjectVersion};
use aws_smithy_runtime_api::client::http::{
    HttpConnector, HttpConnectorFuture, SharedHttpConnector, http_client_fn,
};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::http::StatusCode;
use aws_smithy_types::body::SdkBody;
use bytes::Bytes;

use super::*;
use crate::model::{EntryKind, FailureClass, StoragePath, Transience};
use crate::s3::S3Storage;
use crate::s3::tests::make_test_storage;
use crate::storage::backends::s3::{S3VersioningAtConnect, connect, tests::identity};
use crate::storage::listing::testing::listed;
use crate::storage::{PreflightPolicy, StorageRoleFailure};
use crate::{StorageEntryMessage, WalkOptions};

const SMALL: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/data/s3/ontap-versions-delimiter-small.xml"
));
const MARKERS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/data/s3/ontap-versions-delimiter-markers.xml"
));
const MIXED: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/data/s3/ontap-versions-delimiter-mixed.xml"
));
const ROOT: &str = "f4-1790409358/";
const FIXTURES: [(&str, &str); 3] = [(SMALL, "mks"), (MARKERS, "mkd"), (MIXED, "rnd")];

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

/// (prefix, key marker, version id marker) as a request asks them, an absent one empty.
type Asked = (String, String, String);

/// The text of the first `<name>` element of `xml`, or empty.
fn element(xml: &str, name: &str) -> String {
    let open = format!("<{name}>");
    xml.split_once(&open)
        .and_then(|(_, rest)| rest.split_once(&format!("</{name}>")))
        .map_or_else(String::new, |(text, _)| text.to_string())
}

/// Every text of a `<name>` element in `xml`.
fn elements(xml: &str, name: &str) -> Vec<String> {
    let (open, close) = (format!("<{name}>"), format!("</{name}>"));
    xml.split(&open)
        .skip(1)
        .filter_map(|rest| rest.split_once(&close))
        .map(|(text, _)| text.to_string())
        .collect()
}

fn pages(fixture: &str) -> Vec<(Asked, Bytes)> {
    fixture
        .split("<!--PAGE-END-->")
        .map(str::trim)
        .filter(|page| !page.is_empty())
        .map(|page| {
            let asked = (
                element(page, "Prefix"),
                element(page, "KeyMarker"),
                element(page, "VersionIdMarker"),
            );
            (asked, Bytes::from(page.to_string()))
        })
        .collect()
}

/// Answers each request with the fixture page that echoes what it asks, and records the asks.
#[derive(Clone, Debug)]
struct MarkerReplay {
    pages: Arc<Vec<(Asked, Bytes)>>,
    asked: Arc<Mutex<Vec<Asked>>>,
    /// Each request's query string as sent: whether a marker was there at all.
    queries: Arc<Mutex<Vec<String>>>,
}

impl MarkerReplay {
    fn new(fixture: &str) -> Self {
        Self {
            pages: Arc::new(pages(fixture)),
            asked: Arc::default(),
            queries: Arc::default(),
        }
    }

    fn asked(&self) -> Vec<Asked> {
        self.asked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn queries(&self) -> Vec<String> {
        self.queries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

fn empty_listing(prefix: &str) -> Bytes {
    let prefix = prefix.replace('&', "&amp;").replace('<', "&lt;");
    Bytes::from(format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListVersionsResult \
         xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>test-bucket</Name>\
         <Prefix>{prefix}</Prefix><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>\
         </ListVersionsResult>"
    ))
}

impl HttpConnector for MarkerReplay {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let query = request.uri().split_once('?').map_or("", |(_, query)| query);
        self.queries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(query.to_string());
        let params: HashMap<String, String> = url::form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .collect();
        let param = |name: &str| params.get(name).cloned().unwrap_or_default();
        let asked = (
            param("prefix"),
            param("key-marker"),
            param("version-id-marker"),
        );
        let body = self
            .pages
            .iter()
            .find(|(page, _)| *page == asked)
            .map_or_else(|| empty_listing(&asked.0), |(_, body)| body.clone());
        self.asked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(asked);
        HttpConnectorFuture::new(async move {
            let ok = StatusCode::try_from(200_u16).unwrap_or_else(|e| panic!("{e}"));
            Ok(HttpResponse::new(ok, SdkBody::from(body)))
        })
    }
}

/// A versioned legacy `S3Storage` (bucket `test-bucket`, no prefix) answered by `replay`.
fn storage(replay: &MarkerReplay) -> S3Storage {
    let replay = replay.clone();
    let config = SdkConfig::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url("http://replay.invalid")
        .credentials_provider(Credentials::new("ak", "sk", None, None, "replay"))
        .force_path_style(true)
        .http_client(http_client_fn(move |_, _| {
            SharedHttpConnector::new(replay.clone())
        }))
        .build();
    let mut storage = make_test_storage(None);
    storage.client = Client::from_conf(config);
    storage.is_bucket_versioned = true;
    storage.versioning_at_connect = S3VersioningAtConnect::Enabled;
    storage
}

/// What a fixture holds: every (key, version id) of every page — each listed by at least one of
/// the two walks — those of them that are delete markers, and every common prefix.
struct Truth {
    entries: BTreeSet<(String, String)>,
    markers: BTreeSet<(String, String)>,
    prefixes: BTreeSet<String>,
}

impl Truth {
    fn of(fixture: &str) -> Self {
        let mut truth = Self {
            entries: BTreeSet::new(),
            markers: BTreeSet::new(),
            prefixes: BTreeSet::new(),
        };
        for (_, page) in pages(fixture) {
            let page = String::from_utf8_lossy(&page).to_string();
            for (tag, marker) in [("Version", false), ("DeleteMarker", true)] {
                for entry in elements(&page, tag) {
                    let named = (element(&entry, "Key"), element(&entry, "VersionId"));
                    if marker {
                        truth.markers.insert(named.clone());
                    }
                    truth.entries.insert(named);
                }
            }
            for common in elements(&page, "CommonPrefixes") {
                truth.prefixes.insert(element(&common, "Prefix"));
            }
        }
        truth
    }
}

fn unique<T: Ord + Clone + Debug>(listed: &[T], what: &str) -> BTreeSet<T> {
    let set: BTreeSet<T> = listed.iter().cloned().collect();
    assert_eq!(set.len(), listed.len(), "{what} listed twice: {listed:?}");
    set
}

/// The role listing of `<ROOT><dir>` over `replay`, a versioned bucket: every record of its keys'
/// histories as (key, version id) and its subdirectories as prefixes.
async fn role_all(
    replay: &MarkerReplay,
    dir: &str,
) -> Result<(Vec<(String, String)>, Vec<String>), StorageRoleFailure> {
    let roles =
        connect(Arc::new(storage(replay)), identity(), None).unwrap_or_else(|e| panic!("{e:?}"));
    let namespace = roles
        .namespace(&PreflightPolicy::production())
        .unwrap_or_else(|e| panic!("{e:?}"));
    let directory = StoragePath::new(format!("{ROOT}{dir}")).unwrap_or_else(|e| panic!("{e}"));
    let (entries, failures) = listed(&*namespace, &directory).await?;
    assert!(failures.is_empty(), "{failures:?}");
    let files = entries
        .iter()
        .filter(|entry| entry.kind == EntryKind::File)
        .flat_map(|entry| {
            let history = entry.listing.history.as_deref();
            history.into_iter().flat_map(|history| {
                history.records().iter().map(|record| {
                    let id = record.version_id().unwrap_or("null").to_string();
                    (entry.path.as_str().to_string(), id)
                })
            })
        })
        .collect();
    let directories = entries
        .iter()
        .filter(|entry| entry.kind == EntryKind::Directory)
        .map(|entry| format!("{}/", entry.path.as_str()))
        .collect();
    Ok((files, directories))
}

/// The role listing of every version: all of them (delete markers too), each once, and each
/// subdirectory once; never a continuation with the key marker alone on a key the page ended in.
#[tokio::test]
async fn the_role_listing_lists_every_version_and_each_prefix_once() -> TestResult {
    for (fixture, dir) in FIXTURES {
        let replay = MarkerReplay::new(fixture);
        let (files, directories) = role_all(&replay, dir).await?;
        let truth = Truth::of(fixture);
        assert_eq!(unique(&files, "version"), truth.entries, "{dir}");
        assert_eq!(unique(&directories, "directory"), truth.prefixes, "{dir}");
    }
    let replay = MarkerReplay::new(SMALL);
    role_all(&replay, "mks").await?;
    let z = format!("{ROOT}mks/z");
    let asked = replay.asked();
    assert!(asked.iter().all(|(_, key, id)| key != &z || !id.is_empty()));
    assert_eq!(asked.len(), 4, "{asked:?}");
    Ok(())
}

/// Every message of a legacy `walkdir` of `sub` over `replay`: files as (key, version id),
/// directories as prefixes, and error reasons.
async fn walked(
    replay: &MarkerReplay,
    sub: &str,
) -> TestResult<(Vec<(String, String)>, Vec<String>, Vec<String>)> {
    let walk = storage(replay).walkdir(Some(sub), WalkOptions::default())?;
    let (mut files, mut dirs, mut errors) = (Vec::new(), Vec::new(), Vec::new());
    while let Some(message) = walk.next().await {
        match message {
            StorageEntryMessage::Scanned(entry) if entry.get_is_dir() => {
                dirs.push(format!("{}/", entry.get_relative_path().to_string_lossy()));
            }
            StorageEntryMessage::Scanned(entry) => files.push((
                entry.get_relative_path().to_string_lossy().to_string(),
                entry.get_version_id().unwrap_or_default().to_string(),
            )),
            StorageEntryMessage::Error { reason, .. } => errors.push(reason),
            _ => {}
        }
    }
    Ok((files, dirs, errors))
}

/// Legacy `walkdir`: every version and delete marker once, each subdirectory sent once and listed
/// once (a repeated prefix used to walk its whole subtree again).
#[tokio::test]
async fn the_legacy_walk_lists_every_version_and_walks_each_prefix_once() -> TestResult {
    for (fixture, dir) in FIXTURES {
        let replay = MarkerReplay::new(fixture);
        let (files, dirs, errors) = walked(&replay, &format!("{ROOT}{dir}")).await?;
        assert!(errors.is_empty(), "{errors:?}");
        let truth = Truth::of(fixture);
        assert_eq!(unique(&files, "version"), truth.entries, "{dir}");
        assert_eq!(unique(&dirs, "directory"), truth.prefixes, "{dir}");
        let subtree_listings: Vec<String> = (replay.asked().into_iter())
            .map(|(prefix, _, _)| prefix)
            .filter(|prefix| truth.prefixes.contains(prefix))
            .collect();
        assert_eq!(unique(&subtree_listings, "subtree"), truth.prefixes);
    }
    Ok(())
}

/// A page shaped as `RustFS` lists (every delete marker, then every version), truncated inside
/// `<ROOT>g/k` without a version marker: the key's entries are not the page's final run, so where it
/// continues is unknown.
const GROUPED: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListVersionsResult \
    xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>test-bucket</Name><Prefix>f4-1790409358/g/</Prefix>\
    <KeyMarker></KeyMarker><VersionIdMarker></VersionIdMarker><NextKeyMarker>f4-1790409358/g/k</NextKeyMarker>\
    <MaxKeys>4</MaxKeys><IsTruncated>true</IsTruncated>\
    <DeleteMarker><Key>f4-1790409358/g/k</Key><VersionId>k3</VersionId><IsLatest>false</IsLatest>\
    <LastModified>2026-09-26T00:00:03.000Z</LastModified></DeleteMarker>\
    <Version><Key>f4-1790409358/g/a</Key><VersionId>a1</VersionId><IsLatest>true</IsLatest>\
    <LastModified>2026-09-26T00:00:01.000Z</LastModified><ETag>\"e\"</ETag><Size>1</Size></Version>\
    <Version><Key>f4-1790409358/g/k</Key><VersionId>k4</VersionId><IsLatest>true</IsLatest>\
    <LastModified>2026-09-26T00:00:04.000Z</LastModified><ETag>\"e\"</ETag><Size>1</Size></Version>\
    <Version><Key>f4-1790409358/g/k</Key><VersionId>k2</VersionId><IsLatest>false</IsLatest>\
    <LastModified>2026-09-26T00:00:02.000Z</LastModified><ETag>\"e\"</ETag><Size>1</Size></Version>\
    </ListVersionsResult>";

/// An unknown resume point is never guessed: the role listing fails the directory (`Protocol`,
/// permanent); the legacy walks list the page's complete keys, drop the key it ended in and
/// report the error.
#[tokio::test]
async fn an_unknown_resume_point_is_reported_not_guessed() -> TestResult {
    let replay = MarkerReplay::new(GROUPED);
    let Err(StorageRoleFailure::Entry(failure)) = role_all(&replay, "g").await.map(|_| ()) else {
        panic!("the role listing must fail the directory");
    };
    assert_eq!(failure.class(), FailureClass::Protocol);
    assert_eq!(failure.transience(), Transience::Permanent);
    assert_eq!(failure.path().as_str(), format!("{ROOT}g"));

    let complete = [(format!("{ROOT}g/a"), "a1".to_string())];

    let (files, _, errors) = walked(&MarkerReplay::new(GROUPED), &format!("{ROOT}g")).await?;
    assert_eq!(errors, [UnknownResumePoint::DIAGNOSTIC]);
    assert_eq!(files, complete);
    Ok(())
}

/// Pages shaped as `StorageGRID` 11.5 answers (written by hand, not captured): the first truncated
/// at the common prefix `<ROOT>s/x/` with an empty `<NextVersionIdMarker>` element — the next page
/// is asked with the key marker alone — then the last page.
const EMPTY_VERSION_MARKER: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListVersionsResult \
    xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>test-bucket</Name><Prefix>f4-1790409358/s/</Prefix>\
    <KeyMarker></KeyMarker><VersionIdMarker></VersionIdMarker><NextKeyMarker>f4-1790409358/s/x/</NextKeyMarker>\
    <NextVersionIdMarker></NextVersionIdMarker><MaxKeys>2</MaxKeys><Delimiter>/</Delimiter>\
    <IsTruncated>true</IsTruncated>\
    <Version><Key>f4-1790409358/s/a</Key><VersionId>a1</VersionId><IsLatest>true</IsLatest>\
    <LastModified>2026-09-26T00:00:01.000Z</LastModified><ETag>\"e\"</ETag><Size>1</Size></Version>\
    <CommonPrefixes><Prefix>f4-1790409358/s/x/</Prefix></CommonPrefixes>\
    </ListVersionsResult><!--PAGE-END--><?xml version=\"1.0\" encoding=\"UTF-8\"?><ListVersionsResult \
    xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>test-bucket</Name><Prefix>f4-1790409358/s/</Prefix>\
    <KeyMarker>f4-1790409358/s/x/</KeyMarker><VersionIdMarker></VersionIdMarker><MaxKeys>2</MaxKeys>\
    <Delimiter>/</Delimiter><IsTruncated>false</IsTruncated>\
    <Version><Key>f4-1790409358/s/z</Key><VersionId>z1</VersionId><IsLatest>true</IsLatest>\
    <LastModified>2026-09-26T00:00:03.000Z</LastModified><ETag>\"e\"</ETag><Size>1</Size></Version>\
    </ListVersionsResult>";

/// Every `name=value` of a raw query string, undecoded.
fn query_params(query: &str) -> HashMap<&str, &str> {
    (query.split('&'))
        .map(|pair| pair.split_once('=').unwrap_or((pair, "")))
        .collect()
}

/// `StorageGRID` answers an empty `version-id-marker` with 400 (`InvalidArgument`): no walk sends
/// one, nor one without a key marker; the first page asks no marker at all.
#[tokio::test]
async fn no_listing_sends_an_empty_or_unpaired_version_marker() -> TestResult {
    let fixtures = FIXTURES.into_iter().chain([(EMPTY_VERSION_MARKER, "s")]);
    for (fixture, dir) in fixtures {
        let (role, paged_replay, walk) = (
            MarkerReplay::new(fixture),
            MarkerReplay::new(fixture),
            MarkerReplay::new(fixture),
        );
        role_all(&role, dir).await?;
        paged(&paged_replay, &format!("{ROOT}{dir}/")).await?;
        walked(&walk, &format!("{ROOT}{dir}")).await?;
        for replay in [&role, &paged_replay, &walk] {
            let queries = replay.queries();
            let first = queries.first().map(|query| query_params(query));
            assert!(
                first.is_some_and(|first| !first.contains_key("key-marker")
                    && !first.contains_key("version-id-marker")),
                "{dir}: {queries:?}"
            );
            assert!(queries.len() > 1, "{dir}: {queries:?}");
            for query in &queries {
                let params = query_params(query);
                if let Some(id) = params.get("version-id-marker") {
                    assert!(!id.is_empty(), "{dir}: empty version marker in {query}");
                    let key = params.get("key-marker");
                    assert!(key.is_some_and(|key| !key.is_empty()), "{dir}: {query}");
                }
            }
        }
    }
    // Every walk reads both pages of that listing.
    let both = [
        (format!("{ROOT}s/a"), "a1".to_string()),
        (format!("{ROOT}s/z"), "z1".to_string()),
    ];
    let (files, directories) = role_all(&MarkerReplay::new(EMPTY_VERSION_MARKER), "s").await?;
    assert_eq!(
        (files, directories),
        (both.to_vec(), vec![format!("{ROOT}s/x/")])
    );
    let (files, _, errors) = walked(
        &MarkerReplay::new(EMPTY_VERSION_MARKER),
        &format!("{ROOT}s"),
    )
    .await?;
    assert!(errors.is_empty(), "{errors:?}");
    assert_eq!(files, both);
    Ok(())
}

fn version(key: &str, id: &str) -> ObjectVersion {
    ObjectVersion::builder().key(key).version_id(id).build()
}

fn marker(key: &str, id: &str) -> DeleteMarkerEntry {
    DeleteMarkerEntry::builder().key(key).version_id(id).build()
}

/// A truncated page naming `key_marker` (and `version_marker`, when given).
fn page(
    versions: Vec<ObjectVersion>,
    markers: Vec<DeleteMarkerEntry>,
    key_marker: &str,
    version_marker: Option<&str>,
    order: Option<(Vec<u64>, Vec<u64>)>,
) -> VersionsInOrder {
    let output = ListObjectVersionsOutput::builder()
        .set_versions(Some(versions))
        .set_delete_markers(Some(markers))
        .is_truncated(true)
        .next_key_marker(key_marker)
        .set_next_version_id_marker(version_marker.map(str::to_string))
        .build();
    VersionsInOrder {
        output,
        order: order.map(|(versions, markers)| DocumentOrder { versions, markers }),
    }
}

#[test]
fn the_version_marker_is_the_servers_or_the_last_entrys_on_the_marked_key() {
    // Sent: used as is.
    let sent = page(vec![version("k", "v2")], vec![], "k", Some("v2"), None);
    assert_eq!(sent.next_version_id_marker(), Ok(Some("v2".into())));
    // Missing, the page ends inside the marked key: its last entry in document order. The marker
    // follows the key's version in the document, though the SDK lists versions first…
    let marker_last = page(
        vec![version("a", "a1"), version("k", "k2")],
        vec![marker("k", "k1")],
        "k",
        None,
        Some((vec![0, 1], vec![2])),
    );
    assert_eq!(marker_last.next_version_id_marker(), Ok(Some("k1".into())));
    // …or a version follows the key's marker.
    let version_last = page(
        vec![version("a", "a1"), version("k", "k1")],
        vec![marker("k", "k2")],
        "k",
        None,
        Some((vec![0, 2], vec![1])),
    );
    assert_eq!(version_last.next_version_id_marker(), Ok(Some("k1".into())));
    // Only versions: the last one is the document's last, order or not.
    let versions = page(
        vec![version("a", "a1"), version("z", "z9")],
        vec![],
        "z",
        None,
        None,
    );
    assert_eq!(versions.next_version_id_marker(), Ok(Some("z9".into())));
    // No order, both kinds, only the versions on the key (keys ascend): their last one.
    let one_kind = page(
        vec![version("k", "k2")],
        vec![marker("a", "a1")],
        "k",
        None,
        None,
    );
    assert_eq!(one_kind.next_version_id_marker(), Ok(Some("k2".into())));
    // The page ended at a common prefix (the key marker names it) or after a whole key: the key
    // marker alone resumes after it.
    let at_prefix = page(vec![version("a", "a1")], vec![], "x/", None, None);
    assert_eq!(at_prefix.next_version_id_marker(), Ok(None));
    // An empty marker the server sent (StorageGRID) is none: never asked back as empty.
    let empty = page(vec![version("a", "a1")], vec![], "x/", Some(""), None);
    assert_eq!(empty.next_version_id_marker(), Ok(None));
    // A MinIO key marker is a token, never a key: the guard stays off.
    let minio = page(
        vec![version("k", "k1")],
        vec![],
        "k[minio_cache:v2,return:]",
        None,
        None,
    );
    assert_eq!(minio.next_version_id_marker(), Ok(None));
    // A complete page continues nowhere.
    let complete = VersionsInOrder {
        output: ListObjectVersionsOutput::builder()
            .versions(version("k", "k1"))
            .is_truncated(false)
            .next_key_marker("k")
            .build(),
        order: None,
    };
    assert_eq!(complete.next_version_id_marker(), Ok(None));
}

#[test]
fn a_resume_point_that_cannot_be_told_is_unknown() {
    // No document order and both kinds on the marked key.
    let unordered = page(
        vec![version("k", "k2")],
        vec![marker("k", "k1")],
        "k",
        None,
        None,
    );
    assert_eq!(unordered.next_version_id_marker(), Err(UnknownResumePoint));
    // RustFS's grouping: the key's marker first, another key's version between.
    let grouped = page(
        vec![version("a", "a1"), version("k", "k2")],
        vec![marker("k", "k1")],
        "k",
        None,
        Some((vec![1, 2], vec![0])),
    );
    assert_eq!(grouped.next_version_id_marker(), Err(UnknownResumePoint));
    // The key's entries are there but the page ends on another key.
    let elsewhere = page(
        vec![version("k", "k1"), version("m", "m1")],
        vec![],
        "k",
        None,
        None,
    );
    assert_eq!(elsewhere.next_version_id_marker(), Err(UnknownResumePoint));
    // No entry on the key at all: nothing to resume inside.
    let none = page(
        vec![version("a", "a2")],
        vec![marker("b", "b1")],
        "x/",
        None,
        None,
    );
    assert_eq!(none.next_version_id_marker(), Ok(None));
}
