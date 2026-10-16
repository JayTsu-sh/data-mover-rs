//! Tests of the in-memory S3 itself: it must answer as real S3 does, or the role tests prove
//! nothing.

#![allow(clippy::expect_used)]

use bytes::Bytes;

use super::{MemoryS3, content_md5, etag_of};
use crate::model::FailureClass;
use crate::storage::backends::s3::{S3NativeCopySource, S3Protocol as _, S3ProtocolFailure};

/// A single PUT stores the body and reports what real S3 does: the quoted hex MD5 as `ETag`, and
/// no version on an unversioned bucket.
#[tokio::test]
async fn memory_put_object_reports_md5_etag_and_no_version() {
    let s3 = MemoryS3::default();
    let body = Bytes::from_static(b"payload");
    let facts = s3
        .put_object("key", body.clone(), &content_md5(&body))
        .await
        .expect("put succeeds");
    assert_eq!(facts.etag, "\"321c3cf486ed509164edec1e1981fec8\"");
    assert_eq!(facts.version_id, None);
    let head = s3.head("key").await.expect("object exists");
    assert_eq!(head.etag, facts.etag);
}

/// On a versioned fake each PUT mints a new current version and reports it.
#[tokio::test]
async fn memory_put_object_on_a_versioned_fake_reports_a_new_version() {
    let s3 = MemoryS3::default();
    s3.put_version("key", "v1", Bytes::from_static(b"old"))
        .await;
    let body = Bytes::from_static(b"new");
    let facts = s3
        .put_object("key", body.clone(), &content_md5(&body))
        .await
        .expect("put succeeds");
    let version = facts
        .version_id
        .expect("a versioned write reports its version");
    assert_ne!(version, "v1");
    assert_eq!(s3.version.lock().await.as_deref(), Some(version.as_str()));
}

/// A `Content-MD5` that does not match the body, or the injected corruption, is `BadDigest`: a
/// transient `Corruption` of the entry, storing nothing. The injection covers one PUT only.
#[tokio::test]
async fn memory_put_object_refuses_a_bad_digest() {
    let s3 = MemoryS3::default();
    let body = Bytes::from_static(b"payload");
    let expected = S3ProtocolFailure::corrupted_upload("S3 PutObject request failed");
    let wrong = s3
        .put_object("key", body.clone(), &content_md5(b"other"))
        .await;
    assert_eq!(wrong, Err(expected.clone()));
    *s3.bad_digest_next_put.lock().await = true;
    let injected = s3
        .put_object("key", body.clone(), &content_md5(&body))
        .await;
    assert_eq!(injected, Err(expected));
    assert!(s3.objects.lock().await.is_empty());
    s3.put_object("key", body.clone(), &content_md5(&body))
        .await
        .expect("the injection covers one PUT");
}

/// Two uploads on one key get distinct ids.
#[tokio::test]
async fn memory_upload_ids_do_not_collide_on_one_key() {
    let s3 = MemoryS3::default();
    let first = s3.begin_multipart("key").await.expect("begin");
    let second = s3.begin_multipart("key").await.expect("begin");
    assert_ne!(first, second);
    assert_eq!(s3.uploads.lock().await.len(), 2);
}

/// A server-side copy is pinned to the source's `ETag` as `x-amz-copy-source-if-match` pins it: a
/// source that changed is a `Conflict` and nothing is written; `UploadPartCopy` stores the range
/// as the part, with its MD5 as `ETag`.
#[tokio::test]
async fn memory_copies_are_pinned_to_the_source_etag() {
    let s3 = MemoryS3::default();
    let body = Bytes::from((0..=250_u8).cycle().take(1024).collect::<Vec<_>>());
    s3.objects
        .lock()
        .await
        .insert("source".into(), body.clone());
    let mut source = S3NativeCopySource {
        bucket: "memory".into(),
        key: "source".into(),
        etag: etag_of(&body),
        version_id: None,
        size: 1024,
    };
    let copied = s3.copy_from(&source, "copy").await.expect("copy succeeds");
    assert_eq!(copied.etag, etag_of(&body));
    let upload = s3.begin_multipart("parts").await.expect("begin");
    let part = s3
        .upload_part_copy(&source, "parts", &upload, 1, 10..20)
        .await
        .expect("part copy succeeds");
    assert_eq!(part, etag_of(&body[10..20]));
    source.etag = "\"other\"".into();
    let refused = s3.copy_from(&source, "refused").await;
    assert!(matches!(
        refused,
        Err(S3ProtocolFailure::Entry {
            class: FailureClass::Conflict,
            ..
        })
    ));
    assert!(!s3.objects.lock().await.contains_key("refused"));
}
