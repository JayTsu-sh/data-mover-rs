//! Writing a directory object (P7 F5, D13): the final key `dir/obj/` ends in `/`, and it is
//! written by one request whatever its size — one PUT (streamed or `Direct`), or one `CopyObject`
//! for a native copy — never a multipart upload or `UploadPartCopy`, and never resumed. The fake
//! stores a completed multipart `dir/obj/` as `dir/obj`, as `RustFS` 1.0.0 does, so a regression
//! would also overwrite the real file `dir/obj` these tests keep beside it. More than 5 GiB, or an
//! unknown size, is `Unsupported` before any I/O.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::stream;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::model::{EntryKind, IdentityStrength, SourceIdentity, StoragePath};
use crate::storage::artifacts::{ArtifactKind, artifact_name};
use crate::storage::backends::s3::S3NativeCopySource;
use crate::storage::backends::s3::tests::{MemoryS3, content_md5, etag_of, identity};
use crate::storage::{
    DeferredCheckpoint, FinalDestination, PrepareFact, ResumeMode, SourceDescriptor,
    StagedDestination,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const MIB: usize = 1024 * 1024;
const PART: usize = 8 * MIB;
const FINAL: &str = "dir/obj/";
/// The real file beside the directory object: a multipart `dir/obj/` would land on it (`RustFS`).
const FILE: &str = "dir/obj";
const FILE_BODY: &[u8] = b"a real file";
const FIVE_GIB: u64 = 5 * 1024 * 1024 * 1024;

/// Where a leftover pointer of `dir/obj/` would be: in `dir/`, named after `obj/`.
fn parent_pointer() -> String {
    format!("dir/{}", artifact_name("obj/", ArtifactKind::Upload))
}

/// A pointer of `dir/obj/` left where a writer from before D13 put it.
async fn plant_pointer(protocol: &MemoryS3) -> TestResult {
    let stale = Bytes::from_static(b"stale pointer");
    protocol
        .put_object(&parent_pointer(), stale.clone(), &content_md5(&stale))
        .await
        .map_err(|e| format!("{e:?}"))?;
    Ok(())
}

fn payload(size: usize) -> Bytes {
    Bytes::from((0..=250_u8).cycle().take(size).collect::<Vec<_>>())
}

fn prepare(size: Option<u64>) -> TestResult<PrepareRequest> {
    Ok(PrepareRequest {
        final_destination: FinalDestination::new(StoragePath::new(FINAL)?),
        source: SourceDescriptor::new(
            StoragePath::new("src/obj/")?,
            EntryKind::File,
            size,
            SourceIdentity::new(identity(), IdentityStrength::PathScoped, b"source")?,
        ),
        recovery_binding: [7; 32],
    })
}

fn request(size: Option<u64>, resume: ResumeMode) -> TestResult<DestinationPrepareRequest> {
    Ok(DestinationPrepareRequest::new(prepare(size)?, [4; 32])
        .with_resume(resume)
        .with_recoverable(resume == ResumeMode::Discover))
}

/// A `RustFS`-like store holding the real file `dir/obj`, and a destination whose limits are
/// small (checkpoint interval 16 MiB, native single copy 5 MiB) so the sizes here exceed them.
async fn store() -> (Arc<MemoryS3>, S3StagedDestination<MemoryS3>) {
    let protocol = Arc::new(MemoryS3::default());
    *protocol.rustfs_multipart_directory_objects.lock().await = true;
    protocol
        .objects
        .lock()
        .await
        .insert(FILE.into(), Bytes::from_static(FILE_BODY));
    let destination = S3StagedDestination::new(protocol.clone(), identity())
        .with_checkpoint_interval(2 * PART as u64)
        .with_native_sizing(5 * MIB as u64, 5 * MIB as u64);
    (protocol, destination)
}

fn chunks(bytes: &[u8]) -> ByteStream {
    let pieces: Vec<_> = bytes
        .chunks(MIB)
        .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
        .collect();
    Box::pin(stream::iter(pieces))
}

async fn publish(
    destination: &S3StagedDestination<MemoryS3>,
    stage: &PreparedStage,
    data: &Bytes,
) -> TestResult<PublicationEvidence> {
    let request = PublishRequest {
        expected_size: data.len() as u64,
        expected_blake3: Some(*blake3::hash(data).as_bytes()),
        cancel: CancellationToken::new(),
    };
    Ok(destination
        .publish(stage, request)
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

/// `dir/obj/` holds `data`, the real file `dir/obj` is untouched, nothing else is stored, and no
/// multipart request — upload, part or part copy — was ever made.
async fn written_in_one_request(protocol: &MemoryS3, data: &Bytes, label: &str) {
    let objects = protocol.objects.lock().await.clone();
    assert_eq!(objects.get(FINAL), Some(data), "{label}");
    assert_eq!(
        objects.get(FILE).map(Bytes::as_ref),
        Some(FILE_BODY),
        "{label}"
    );
    assert_eq!(stored(protocol).await, [FILE, FINAL], "{label}");
    let multipart = (
        *protocol.multipart_begins.lock().await,
        *protocol.part_uploads.lock().await,
        *protocol.part_copies.lock().await,
    );
    assert_eq!(multipart, (0, 0, 0), "{label}");
}

/// Streamed at the destination, empty, small or over the single-PUT threshold (and the
/// checkpoint interval), resumable or not: one PUT at publication, no pointer, no recovery.
#[tokio::test]
async fn a_directory_object_is_one_put_at_any_size() -> TestResult {
    for size in [0, 5, 3 * PART] {
        for resume in [ResumeMode::Discover, ResumeMode::Restart] {
            let label = format!("{size} B {resume:?}");
            let (protocol, destination) = store().await;
            let data = payload(size);
            let mut stage = destination
                .prepare_at_destination(request(Some(size as u64), resume)?)
                .await?;
            assert!(!stage.recovery_enabled(), "{label}");
            // As the engine arms it for a checkpointed source over the interval: nothing changes.
            stage.deferred_checkpoint = Some(DeferredCheckpoint {
                interval_bytes: 2 * PART as u64,
                source_size: size as u64,
            });
            destination.write(&stage, chunks(&data)).await?;
            assert_eq!(
                stored(&protocol).await,
                [FILE],
                "{label}: nothing before publication"
            );
            publish(&destination, &stage, &data).await?;
            written_in_one_request(&protocol, &data, &label).await;
            assert_eq!(*protocol.puts.lock().await, 1, "{label}");
        }
    }
    Ok(())
}

/// `Direct` writes `dir/obj/` inside `write`, by one PUT whatever its size.
#[tokio::test]
async fn a_directory_object_is_written_directly_by_one_put() -> TestResult {
    for size in [5, 3 * PART] {
        let label = format!("{size} B");
        let (protocol, destination) = store().await;
        let data = payload(size);
        let stage = destination
            .prepare_direct(prepare(Some(size as u64))?, CancellationToken::new())
            .await?;
        destination.write(&stage, chunks(&data)).await?;
        written_in_one_request(&protocol, &data, &label).await;
        publish(&destination, &stage, &data).await?;
        written_in_one_request(&protocol, &data, &label).await;
        assert_eq!(*protocol.puts.lock().await, 1, "{label}");
    }
    Ok(())
}

/// A native copy of `dir/obj/` is one `CopyObject`, also above the single-copy limit.
#[tokio::test]
async fn a_directory_object_is_copied_natively_by_one_copy_object() -> TestResult {
    for size in [3, 4 * 5 * MIB + 3] {
        let label = format!("{size} B");
        let (protocol, destination) = store().await;
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
            .prepare_native_stage(request(Some(size as u64), ResumeMode::Discover)?)
            .await?;
        assert_eq!(stored(&protocol).await, [FILE], "{label}: no pointer");
        destination
            .fill_native(&stage, source, CancellationToken::new(), 4)
            .await
            .map_err(|failure| format!("{:?}", failure.error))?;
        publish(&destination, &stage, &bytes).await?;
        written_in_one_request(&protocol, &bytes, &label).await;
        assert_eq!(*protocol.native_copies.lock().await, 1, "{label}");
    }
    Ok(())
}

/// More than 5 GiB, or of unknown size: every prepare refuses it as `Unsupported` before any I/O
/// (a planted leftover pointer is still there). Exactly 5 GiB is one request.
#[tokio::test]
async fn a_directory_object_one_request_cannot_carry_is_refused_before_any_io() -> TestResult {
    let (protocol, destination) = store().await;
    assert_eq!(destination.directory_object_limit(), Some(FIVE_GIB));
    plant_pointer(&protocol).await?;
    let unsupported = |result: Result<PreparedStage, StorageRoleFailure>| {
        matches!(
            result,
            Err(StorageRoleFailure::Entry(failure))
                if failure.class() == FailureClass::Unsupported
                    && failure.transience() == Transience::Permanent
                    && failure.operation() == Operation::Prepare
                    && failure.path().as_str() == FINAL
        )
    };
    for size in [Some(FIVE_GIB + 1), None] {
        for resume in [ResumeMode::Discover, ResumeMode::Restart] {
            let prepared = destination
                .prepare_at_destination(request(size, resume)?)
                .await;
            assert!(unsupported(prepared), "{size:?} {resume:?}");
        }
        let direct = destination
            .prepare_direct(prepare(size)?, CancellationToken::new())
            .await;
        assert!(unsupported(direct), "direct {size:?}");
        let native = destination
            .prepare_native_stage(request(size, ResumeMode::Discover)?)
            .await;
        assert!(unsupported(native), "native {size:?}");
    }
    assert_eq!(
        stored(&protocol).await,
        [parent_pointer(), FILE.to_string()]
    );
    assert_eq!(*protocol.puts.lock().await, 1, "only the planted pointer");
    let multipart = (
        *protocol.multipart_begins.lock().await,
        *protocol.part_uploads.lock().await,
        *protocol.part_copies.lock().await,
        *protocol.aborts.lock().await,
    );
    assert_eq!(multipart, (0, 0, 0, 0));
    let direct = destination
        .prepare_direct(prepare(Some(FIVE_GIB))?, CancellationToken::new())
        .await?;
    assert!(streaming::of(&direct).is_some(), "5 GiB is streamed (F5h)");
    Ok(())
}

/// A pointer and an upload left on `dir/obj/` by a writer from before D13 are cleaned up at
/// prepare — the pointer where such a writer put it, in the parent — and the object is one PUT.
#[tokio::test]
async fn a_leftover_multipart_stage_of_a_directory_object_is_cleaned_up() -> TestResult {
    let (protocol, destination) = store().await;
    plant_pointer(&protocol).await?;
    protocol
        .begin_multipart(FINAL)
        .await
        .map_err(|e| format!("{e:?}"))?;
    *protocol.multipart_begins.lock().await = 0;
    let data = payload(3 * PART);
    let stage = destination
        .prepare_at_destination(request(Some(data.len() as u64), ResumeMode::Discover)?)
        .await?;
    assert!(matches!(
        stage.prepare_fact(),
        PrepareFact::Restarted { .. }
    ));
    let uploads = protocol
        .list_uploads(FINAL)
        .await
        .map_err(|e| format!("{e:?}"))?;
    assert!(uploads.is_empty(), "{uploads:?}");
    destination.write(&stage, chunks(&data)).await?;
    publish(&destination, &stage, &data).await?;
    written_in_one_request(&protocol, &data, "after a leftover").await;
    Ok(())
}

/// F5h: over the single-PUT threshold a directory object is one streamed `PutObject`, never held
/// whole in memory — after `write` the server already holds all but its last byte and nothing is
/// committed; publication commits it. Up to the threshold it is buffered as before.
#[tokio::test]
async fn a_directory_object_over_the_threshold_is_streamed() -> TestResult {
    let (protocol, destination) = store().await;
    let data = payload(3 * PART);
    let size = data.len() as u64;
    let stage = destination
        .prepare_at_destination(request(Some(size), ResumeMode::Discover)?)
        .await?;
    assert!(streaming::of(&stage).is_some());
    assert!(!stage.recovery_enabled());
    destination.write(&stage, chunks(&data)).await?;
    for _ in 0..500 {
        if protocol.streaming().received == size - 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(protocol.streaming().received, size - 1);
    assert!(!protocol.objects.lock().await.contains_key(FINAL));
    publish(&destination, &stage, &data).await?;
    written_in_one_request(&protocol, &data, "streamed").await;
    assert_eq!(protocol.streaming().puts, 1);
    let small = destination
        .prepare_at_destination(request(Some(5), ResumeMode::Discover)?)
        .await?;
    assert!(single::of(&small).is_some());
    Ok(())
}

/// The buffering limit of a directory object is the single-PUT threshold — exactly it is still
/// buffered, one byte more is streamed — or, where every other object goes through a multipart
/// upload (no threshold), its default 8 MiB. A leftover pointer is cleaned up on the buffered
/// route as on the streamed one.
#[tokio::test]
async fn a_directory_object_is_buffered_up_to_the_threshold() -> TestResult {
    let limit = DEFAULT_SINGLE_PUT_THRESHOLD;
    for (threshold, size, streamed) in [
        (Some(limit), limit, false),
        (Some(limit), limit + 1, true),
        (None, limit, false),
        (None, limit + 1, true),
    ] {
        let (protocol, destination) = store().await;
        let destination = destination.with_single_put_threshold(threshold);
        plant_pointer(&protocol).await?;
        let stage = destination
            .prepare_at_destination(request(Some(size), ResumeMode::Discover)?)
            .await?;
        let label = format!("{threshold:?} {size}");
        assert_eq!(streaming::of(&stage).is_some(), streamed, "{label}");
        assert_eq!(single::of(&stage).is_some(), !streamed, "{label}");
        assert!(
            matches!(stage.prepare_fact(), PrepareFact::Restarted { .. }),
            "{label}"
        );
        assert!(
            !stored(&protocol).await.contains(&parent_pointer()),
            "{label}"
        );
    }
    Ok(())
}

/// A store that drops the connection while the last byte of a streamed directory object is held
/// (its idle-body timeout shorter than the hold) ended the request with the body incomplete:
/// publication reports the final key unchanged — on a bucket where nothing else could prove it —
/// and nothing was stored.
#[tokio::test]
async fn a_connection_dropped_while_the_byte_is_held_changes_nothing() -> TestResult {
    let (protocol, destination) = store().await;
    protocol.streaming().drops_while_held = true;
    let data = payload(3 * PART);
    let stage = destination
        .prepare_at_destination(request(Some(data.len() as u64), ResumeMode::Restart)?)
        .await?;
    destination.write(&stage, chunks(&data)).await?;
    for _ in 0..500 {
        if protocol.streaming().received + 1 == data.len() as u64 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    let request = PublishRequest {
        expected_size: data.len() as u64,
        expected_blake3: None,
        cancel: CancellationToken::new(),
    };
    let Err(failure) = destination.publish(&stage, request).await else {
        return Err("a dropped upload was published".into());
    };
    assert!(!failure.final_destination_changed);
    assert!(!protocol.objects.lock().await.contains_key(FINAL));
    Ok(())
}
