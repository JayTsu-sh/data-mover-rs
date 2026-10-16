//! Multipart building blocks (ADR-0006 C15a): the composite `ETag`, and the in-memory S3's part
//! digests, completion checks and upload listing.

use std::slice;

use super::*;
use crate::model::{FailureClass, Transience};

const MIB: usize = 1024 * 1024;

fn quoted(hex: &str) -> String {
    format!("\"{hex}\"")
}

/// Frozen from Python: `hashlib.md5(b"".join(hashlib.md5(p).digest() for p in parts)).hexdigest()`
/// for the parts `b"part one"`, `b"part two"`, `b"three"`, and for one empty part.
#[test]
fn composite_etag_matches_an_independent_computation() {
    let parts = [
        "3303e12af474ca11d85ed2966a932992",
        "3ea4e15b91a17dc76052c56cfcdf67a2",
        "35d6d33467aae9a2e3dccb4b6b027878",
    ]
    .map(quoted);
    assert_eq!(
        parts.to_vec(),
        [&b"part one"[..], b"part two", b"three"].map(etag_of)
    );
    assert_eq!(
        composite_etag(&parts),
        Some(quoted("68daf77c9985b60757239608a9f5e69e-3"))
    );
    assert_eq!(
        composite_etag(&[etag_of(b"")]),
        Some(quoted("59adb24ef3cdbe0297f05b395827453f-1"))
    );
}

/// No composite without parts, or when any part `ETag` is not a quoted 32-hex MD5.
#[test]
fn composite_etag_needs_md5_part_etags() {
    let md5 = etag_of(b"part one");
    for odd in [
        "3303e12af474ca11d85ed2966a932992".to_string(),
        quoted("3303e12af474ca11d85ed2966a93299"),
        quoted("3303e12af474ca11d85ed2966a93299g"),
        quoted("+303e12af474ca11d85ed2966a932992"),
        quoted("68daf77c9985b60757239608a9f5e69e-3"),
        quoted("ÿ303e12af474ca11d85ed2966a93299"),
        String::new(),
    ] {
        assert_eq!(composite_etag(&[md5.clone(), odd.clone()]), None, "{odd}");
    }
    assert_eq!(composite_etag(&[]), None);
}

/// A part whose `Content-MD5` does not match is `BadDigest` and is not stored; a part number
/// uploaded again replaces the part.
#[tokio::test]
async fn memory_upload_part_checks_the_digest_and_replaces_a_part() -> S3Result<()> {
    let s3 = MemoryS3::default();
    let id = s3.begin_multipart("key").await?;
    let body = Bytes::from_static(b"part");
    let refused = s3
        .upload_part("key", &id, 1, body.clone(), &content_md5(b"other"))
        .await;
    assert_eq!(
        refused,
        Err(S3ProtocolFailure::corrupted_upload(
            "S3 UploadPart request failed"
        ))
    );
    assert!(s3.list_parts("key", &id).await?.is_empty());
    let first = s3
        .upload_part(
            "key",
            &id,
            1,
            Bytes::from_static(b"old"),
            &content_md5(b"old"),
        )
        .await?;
    let etag = s3
        .upload_part("key", &id, 1, body.clone(), &content_md5(&body))
        .await?;
    assert_ne!(first, etag);
    assert_eq!(etag, etag_of(&body));
    let parts = s3.list_parts("key", &id).await?;
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].etag, etag);
    Ok(())
}

async fn upload(
    s3: &MemoryS3,
    key: &str,
    parts: &[Bytes],
) -> S3Result<(String, Vec<(i32, String)>)> {
    let id = s3.begin_multipart(key).await?;
    let mut listed = Vec::new();
    for (number, part) in (1..).zip(parts) {
        let etag = s3
            .upload_part(key, &id, number, part.clone(), &content_md5(part))
            .await?;
        listed.push((number, etag));
    }
    Ok((id, listed))
}

/// A completion reports the composite `ETag` the helper computes, stores exactly the listed
/// parts, and on a versioned fake mints and reports a version.
#[tokio::test]
async fn memory_complete_reports_the_composite_etag_and_version() -> S3Result<()> {
    let s3 = MemoryS3::default();
    let parts = [Bytes::from(vec![1; 5 * MIB]), Bytes::from_static(b"tail")];
    let (id, listed) = upload(&s3, "key", &parts).await?;
    let facts = s3.complete_multipart("key", &id, &listed).await?;
    let etags: Vec<String> = listed.iter().map(|part| part.1.clone()).collect();
    assert_eq!(Some(facts.etag.clone()), composite_etag(&etags));
    assert_eq!(facts.version_id, None);
    assert_eq!(s3.head("key").await?.etag, facts.etag);
    assert_eq!(
        s3.objects.lock().await.get("key").map(Bytes::len),
        Some(5 * MIB + 4)
    );
    assert!(s3.list_uploads("key").await?.is_empty());

    s3.put_version("versioned", "v1", Bytes::from_static(b"old"))
        .await;
    let (id, listed) = upload(&s3, "versioned", &parts[1..]).await?;
    let facts = s3.complete_multipart("versioned", &id, &listed).await?;
    let version = facts
        .version_id
        .ok_or_else(|| S3ProtocolFailure::protocol("no version"))?;
    assert_ne!(version, "v1");
    assert_eq!(s3.version.lock().await.as_deref(), Some(version.as_str()));
    Ok(())
}

/// A part list the upload does not hold, out of order, or with a short part before the last is
/// refused as S3 refuses it, and the upload stays in progress.
#[tokio::test]
async fn memory_complete_checks_the_part_list() -> S3Result<()> {
    let s3 = MemoryS3::default();
    let parts = [Bytes::from(vec![2; 5 * MIB]), Bytes::from_static(b"end")];
    let (id, listed) = upload(&s3, "key", &parts).await?;
    let conflict = |diagnostic| {
        Err(S3ProtocolFailure::entry(
            FailureClass::Conflict,
            Transience::Permanent,
            diagnostic,
        ))
    };
    let wrong_etag = [listed[0].clone(), (2, etag_of(b"other"))];
    assert_eq!(
        s3.complete_multipart("key", &id, &wrong_etag).await,
        conflict("InvalidPart")
    );
    let missing = [listed[0].clone(), (3, listed[1].1.clone())];
    assert_eq!(
        s3.complete_multipart("key", &id, &missing).await,
        conflict("InvalidPart")
    );
    let reversed = [listed[1].clone(), listed[0].clone()];
    assert_eq!(
        s3.complete_multipart("key", &id, &reversed).await,
        conflict("InvalidPartOrder")
    );
    assert_eq!(
        s3.complete_multipart("key", &id, &[]).await,
        conflict("InvalidPartOrder")
    );

    let (short, short_listed) = upload(&s3, "short", &[parts[1].clone(), parts[1].clone()]).await?;
    assert_eq!(
        s3.complete_multipart("short", &short, &short_listed).await,
        Err(S3ProtocolFailure::entry(
            FailureClass::Corruption,
            Transience::Permanent,
            "EntityTooSmall",
        ))
    );
    assert_eq!(s3.list_uploads("key").await?, slice::from_ref(&id));
    assert!(s3.objects.lock().await.is_empty());
    s3.complete_multipart("key", &id, &listed).await?;
    Ok(())
}

/// With the switch set, a completion stores its object and then loses the reply, once.
#[tokio::test]
async fn memory_complete_can_commit_then_lose_the_reply() -> S3Result<()> {
    let s3 = MemoryS3::default();
    let body = Bytes::from_static(b"only part");
    let (id, listed) = upload(&s3, "key", slice::from_ref(&body)).await?;
    *s3.complete_commits_then_fails.lock().await = true;
    let lost = s3.complete_multipart("key", &id, &listed).await;
    assert!(matches!(
        lost,
        Err(S3ProtocolFailure::Session {
            class: FailureClass::Connectivity,
            ..
        })
    ));
    assert_eq!(s3.objects.lock().await.get("key"), Some(&body));
    assert!(s3.list_uploads("key").await?.is_empty());
    assert!(!*s3.complete_commits_then_fails.lock().await);
    Ok(())
}

/// Uploads are listed for exactly one key, never for keys that merely start with it.
#[tokio::test]
async fn memory_list_uploads_is_per_exact_key() -> S3Result<()> {
    let s3 = MemoryS3::default();
    let first = s3.begin_multipart("dir/a").await?;
    let second = s3.begin_multipart("dir/a").await?;
    s3.begin_multipart("dir/a.bak").await?;
    s3.begin_multipart("dir/a/b").await?;
    let mut expected = vec![first, second];
    expected.sort();
    assert_eq!(s3.list_uploads("dir/a").await?, expected);
    assert!(s3.list_uploads("dir").await?.is_empty());
    Ok(())
}
