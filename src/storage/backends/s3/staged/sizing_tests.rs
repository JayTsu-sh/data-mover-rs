use super::parts::upload;
use super::*;
use crate::model::{EntryKind, IdentityStrength, SourceIdentity, StoragePath};
use crate::storage::backends::s3::source::cancelled;
use crate::storage::backends::s3::tests::{MemoryS3, identity};
use crate::storage::{FinalDestination, PrepareFact, ResumeMode, SourceDescriptor};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[test]
fn part_sizes_cover_the_full_multipart_capacity() -> TestResult {
    let path = StoragePath::new("final")?;
    let old_capacity = PART_SIZE as u64 * 10_000;
    let capacity = 5 * 1024 * 1024 * 1024 * 10_000_u64;
    for size in [0, old_capacity, old_capacity + 1, 1024_u64.pow(4), capacity] {
        let part = planned_part_size(Some(size), &path)? as u64;
        assert!(size.div_ceil(part) <= 10_000);
        assert!(part >= PART_SIZE as u64);
        assert!(part <= 5 * 1024 * 1024 * 1024);
    }
    assert_eq!(planned_part_size(Some(old_capacity), &path)?, PART_SIZE);
    assert_eq!(
        planned_part_size(Some(old_capacity + 1), &path)?,
        PART_SIZE + 1
    );
    assert!(planned_part_size(Some(capacity + 1), &path).is_err());
    Ok(())
}

fn prepare_for_size(size: u64, resume: ResumeMode) -> TestResult<DestinationPrepareRequest> {
    let prepare = PrepareRequest {
        source: SourceDescriptor::new(
            StoragePath::new("source")?,
            EntryKind::File,
            Some(size),
            SourceIdentity::new(identity(), IdentityStrength::PathScoped, b"source")?,
        ),
        final_destination: FinalDestination::new(StoragePath::new("final")?),
        recovery_binding: [71; 32],
    };
    Ok(DestinationPrepareRequest::new(prepare, [72; 32]).with_resume(resume))
}

/// A source too large for 10 000 parts of 8 MiB gets larger parts, and a resume in a fresh
/// connection continues at that part size (that it is the pointer's, not recomputed, is shown by
/// the native / streamed cross-route resumes in `native_final_tests`).
#[tokio::test]
async fn larger_parts_survive_reconnecting_to_a_partial_upload() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let size = PART_SIZE as u64 * 10_000 + 1;
    let destination = S3StagedDestination::new(protocol.clone(), identity());
    let stage = destination
        .prepare_at_destination(prepare_for_size(size, ResumeMode::Discover)?)
        .await?;
    let source = StoragePath::new("source")?;
    let input = Box::pin(futures::stream::iter([
        Ok(Bytes::from(vec![7; PART_SIZE + 1])),
        Err(cancelled(&source, Operation::Read)),
    ]));
    assert!(destination.write(&stage, input).await.is_err());
    drop((stage, destination));
    let destination = S3StagedDestination::new(protocol.clone(), identity());
    let recovered = destination
        .prepare_at_destination(prepare_for_size(size, ResumeMode::Discover)?)
        .await?;
    let resumed = (PART_SIZE + 1) as u64;
    assert_eq!(
        recovered.prepare_fact(),
        PrepareFact::Resumed { bytes: resumed }
    );
    assert_eq!(recovered.write_offset, resumed);
    assert_eq!(
        at_destination::of(&recovered)
            .ok_or("an upload on the final key")?
            .part_size,
        PART_SIZE + 1
    );
    destination.discard(recovered).await?;
    assert!(
        upload(
            protocol,
            "unused".into(),
            "unused".into(),
            10_001,
            Bytes::new()
        )
        .await
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn aligned_uploads_do_not_append_empty_parts_but_empty_files_have_one() -> TestResult {
    for count in 0..=4 {
        let protocol = Arc::new(MemoryS3::default());
        // The multipart path itself: objects this small would otherwise be one PUT.
        let destination =
            S3StagedDestination::new(protocol, identity()).with_single_put_threshold(None);
        let size = count * PART_SIZE;
        let request = prepare_for_size(size as u64, ResumeMode::Restart)?.with_recoverable(false);
        let stage = destination.prepare_at_destination(request).await?;
        let bytes = Bytes::from(vec![7; size]);
        destination
            .write(
                &stage,
                Box::pin(futures::stream::once(async move { Ok(bytes) })),
            )
            .await?;
        {
            let upload = at_destination::of(&stage).ok_or("an upload on the final key")?;
            let sent = upload.lock();
            assert_eq!(sent.parts.len(), count.max(1));
            assert_eq!(sent.written, Some(size as u64));
        }
        destination.discard(stage).await?;
    }
    Ok(())
}
