//! Writing a directory object (P7 F5): the final key `dir/obj/` ends in `/`. A small or empty
//! one is one PUT with no pointer; a large one an upload on that key whose `.upload` pointer sits
//! in the parent `dir/`, named after `obj/` — never inside `obj/`, never the file `dir/obj`'s —
//! and resumes from it; a native copy writes it the same way.

use std::iter;
use std::sync::Arc;

use bytes::Bytes;
use futures::stream;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::model::{EntryKind, IdentityStrength, SourceIdentity, StoragePath};
use crate::storage::artifacts::{ArtifactKind, artifact_name};
use crate::storage::backends::s3::S3NativeCopySource;
use crate::storage::backends::s3::tests::{MemoryS3, etag_of, identity};
use crate::storage::{
    DeferredCheckpoint, FinalDestination, PrepareFact, ResumeMode, SourceDescriptor,
    StagedDestination,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const MIB: usize = 1024 * 1024;
const PART: usize = 8 * MIB;
const FINAL: &str = "dir/obj/";

/// Where the pointer of `dir/obj/` belongs: in `dir/`, named after `obj/`.
fn parent_pointer() -> String {
    format!("dir/{}", artifact_name("obj/", ArtifactKind::Upload))
}

fn payload(size: usize) -> Bytes {
    Bytes::from((0..=250_u8).cycle().take(size).collect::<Vec<_>>())
}

fn prepare(size: usize) -> TestResult<PrepareRequest> {
    Ok(PrepareRequest {
        final_destination: FinalDestination::new(StoragePath::new(FINAL)?),
        source: SourceDescriptor::new(
            StoragePath::new("src/obj/")?,
            EntryKind::File,
            Some(size as u64),
            SourceIdentity::new(identity(), IdentityStrength::PathScoped, b"source")?,
        ),
        recovery_binding: [7; 32],
    })
}

fn request(size: usize, resume: ResumeMode) -> TestResult<DestinationPrepareRequest> {
    Ok(DestinationPrepareRequest::new(prepare(size)?, [4; 32])
        .with_resume(resume)
        .with_recoverable(false))
}

fn destination(protocol: &Arc<MemoryS3>) -> S3StagedDestination<MemoryS3> {
    S3StagedDestination::new(protocol.clone(), identity())
        .with_checkpoint_interval(2 * PART as u64)
        .with_native_sizing(5 * MIB as u64, 5 * MIB as u64)
}

fn chunks(bytes: &[u8]) -> ByteStream {
    let pieces: Vec<_> = bytes
        .chunks(MIB)
        .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
        .collect();
    Box::pin(stream::iter(pieces))
}

fn publication(bytes: &Bytes) -> PublishRequest {
    PublishRequest {
        expected_size: bytes.len() as u64,
        expected_blake3: Some(*blake3::hash(bytes).as_bytes()),
        cancel: CancellationToken::new(),
    }
}

async fn publish(
    destination: &S3StagedDestination<MemoryS3>,
    stage: &PreparedStage,
    data: &Bytes,
) -> TestResult<PublicationEvidence> {
    Ok(destination
        .publish(stage, publication(data))
        .await
        .map_err(|failure| format!("{:?}", failure.error))?)
}

/// Every stored key other than the source, sorted.
async fn stored(protocol: &MemoryS3) -> Vec<String> {
    let mut keys: Vec<String> = protocol
        .objects
        .lock()
        .await
        .keys()
        .filter(|key| !key.starts_with("src/"))
        .cloned()
        .collect();
    keys.sort();
    keys
}

/// Up to the single-PUT threshold, content or none: one PUT of the key `dir/obj/`, no pointer.
#[tokio::test]
async fn a_directory_object_key_is_written_by_single_put() -> TestResult {
    for size in [0, 5] {
        let protocol = Arc::new(MemoryS3::default());
        let destination = destination(&protocol);
        let data = payload(size);
        let stage = destination
            .prepare_at_destination(request(size, ResumeMode::Discover)?)
            .await?;
        destination.write(&stage, chunks(&data)).await?;
        publish(&destination, &stage, &data).await?;
        assert_eq!(protocol.objects.lock().await.get(FINAL), Some(&data));
        assert_eq!(stored(&protocol).await, [FINAL], "size {size}");
        assert_eq!(*protocol.multipart_begins.lock().await, 0);
    }
    Ok(())
}

/// Above it: an upload on `dir/obj/` whose pointer is written at once in `dir/`, named after
/// `obj/`; nothing is written inside `obj/` and nothing under the file `dir/obj`'s artifact name.
/// Publication completes the upload and removes the pointer.
#[tokio::test]
async fn a_multipart_directory_object_keeps_its_pointer_in_the_parent() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let destination = destination(&protocol);
    let data = payload(5 * PART);
    let stage = destination
        .prepare_at_destination(request(data.len(), ResumeMode::Discover)?)
        .await?;
    // In `dir/`, named after `obj/`: nothing inside `obj/`, not the file `dir/obj`'s name.
    assert_eq!(stored(&protocol).await, [parent_pointer()]);
    assert_eq!(
        protocol
            .list_uploads(FINAL)
            .await
            .map_err(|e| format!("{e:?}"))?
            .len(),
        1
    );
    destination.write(&stage, chunks(&data)).await?;
    publish(&destination, &stage, &data).await?;
    assert_eq!(protocol.objects.lock().await.get(FINAL), Some(&data));
    assert_eq!(stored(&protocol).await, [FINAL]);
    Ok(())
}

/// A writer cut before its first checkpoint leaves the upload and the parent pointer; the next
/// prepare finds them from the final key alone and resumes.
#[tokio::test]
async fn a_directory_object_resumes_from_its_parent_pointer() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let destination = destination(&protocol);
    let data = payload(7 * PART);
    let mut stage = destination
        .prepare_at_destination(request(data.len(), ResumeMode::Discover)?)
        .await?;
    stage.deferred_checkpoint = Some(DeferredCheckpoint {
        interval_bytes: 6 * PART as u64,
        source_size: data.len() as u64,
    });
    let cut: Vec<_> = data[..3 * PART]
        .chunks(MIB)
        .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
        .chain(iter::once(Err(classified_entry(
            &StoragePath::new("src/obj/")?,
            Operation::Read,
            FailureClass::Connectivity,
            Transience::Transient,
            "cut",
        ))))
        .collect();
    assert!(
        destination
            .write(&stage, Box::pin(stream::iter(cut)))
            .await
            .is_err()
    );
    drop(stage);
    assert_eq!(stored(&protocol).await, [parent_pointer()]);

    let stage = destination
        .prepare_at_destination(request(data.len(), ResumeMode::Discover)?)
        .await?;
    assert_eq!(
        stage.prepare_fact(),
        PrepareFact::Resumed {
            bytes: 3 * PART as u64
        }
    );
    destination.write(&stage, chunks(&data[3 * PART..])).await?;
    publish(&destination, &stage, &data).await?;
    assert_eq!(protocol.objects.lock().await.get(FINAL), Some(&data));
    assert_eq!(stored(&protocol).await, [FINAL]);
    Ok(())
}

/// `Direct` writes `dir/obj/` inside `write`: one PUT when small, an upload completed there when
/// large; no pointer either way.
#[tokio::test]
async fn a_directory_object_is_written_directly() -> TestResult {
    for size in [5, 3 * PART] {
        let protocol = Arc::new(MemoryS3::default());
        let destination = destination(&protocol);
        let data = payload(size);
        let stage = destination
            .prepare_direct(prepare(size)?, CancellationToken::new())
            .await?;
        destination.write(&stage, chunks(&data)).await?;
        // Written inside `write`, by one PUT or one completed upload, and no pointer.
        assert_eq!(stored(&protocol).await, [FINAL], "size {size}");
        assert_eq!(
            *protocol.multipart_begins.lock().await,
            u32::from(size as u64 > DEFAULT_SINGLE_PUT_THRESHOLD),
            "size {size}"
        );
        publish(&destination, &stage, &data).await?;
        assert_eq!(protocol.objects.lock().await.get(FINAL), Some(&data));
        assert_eq!(stored(&protocol).await, [FINAL], "size {size}");
    }
    Ok(())
}

/// A native copy writes `dir/obj/` too: one `CopyObject` when small, an upload filled by
/// `UploadPartCopy` with its pointer in the parent when large.
#[tokio::test]
async fn a_directory_object_is_copied_natively() -> TestResult {
    for size in [3, 4 * 5 * MIB + 3] {
        let protocol = Arc::new(MemoryS3::default());
        let destination = destination(&protocol);
        let bytes = payload(size);
        protocol
            .objects
            .lock()
            .await
            .insert("src/obj/".into(), bytes.clone());
        let source = S3NativeCopySource {
            bucket: "memory".into(),
            key: "src/obj/".into(),
            etag: etag_of(&bytes),
            version_id: None,
            size: size as u64,
        };
        let stage = destination
            .prepare_native_stage(request(size, ResumeMode::Discover)?)
            .await?;
        let large = size > 5 * MIB;
        let pointers = if large {
            vec![parent_pointer()]
        } else {
            vec![]
        };
        assert_eq!(stored(&protocol).await, pointers, "size {size}");
        destination
            .fill_native(&stage, source, CancellationToken::new(), 4)
            .await
            .map_err(|failure| format!("{:?}", failure.error))?;
        publish(&destination, &stage, &bytes).await?;
        assert_eq!(protocol.objects.lock().await.get(FINAL), Some(&bytes));
        assert_eq!(stored(&protocol).await, [FINAL], "size {size}");
    }
    Ok(())
}
