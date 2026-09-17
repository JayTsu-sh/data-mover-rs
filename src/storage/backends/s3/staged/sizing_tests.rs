use super::*;
use crate::model::{EntryKind, IdentityStrength, SourceIdentity, StoragePath};
use crate::storage::backends::s3::tests::{MemoryS3, identity};
use crate::storage::{FinalDestination, SourceDescriptor};

#[test]
fn part_sizes_cover_the_full_multipart_capacity() -> Result<(), Box<dyn std::error::Error>> {
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

#[tokio::test]
async fn larger_parts_survive_reconnecting_to_a_partial_upload()
-> Result<(), Box<dyn std::error::Error>> {
    let protocol = Arc::new(MemoryS3::default());
    let destination = S3StagedDestination::new(protocol.clone(), identity());
    let source = SourceDescriptor::new(
        StoragePath::new("source")?,
        EntryKind::File,
        Some(PART_SIZE as u64 * 10_000 + 1),
        SourceIdentity::new(identity(), IdentityStrength::PathScoped, b"source")?,
    );
    let final_destination = FinalDestination::new(StoragePath::new("final")?);
    let stage = destination
        .prepare(PrepareRequest {
            source: source.clone(),
            final_destination: final_destination.clone(),
            recovery_binding: [27; 32],
        })
        .await?;
    let input = Box::pin(futures::stream::iter([
        Ok(Bytes::from(vec![7; PART_SIZE + 1])),
        Err(cancelled(&source.path, Operation::Read)),
    ]));
    assert!(destination.write(&stage, input).await.is_err());
    let recovery = destination.recovery_identity(&stage).await?;
    drop(destination);
    let destination = S3StagedDestination::new(protocol.clone(), identity());
    let recovered = destination
        .recover(RecoverRequest {
            identity: recovery,
            final_destination,
            source,
            recovery_binding: [27; 32],
            claim_token: [28; 32],
        })
        .await?;
    assert_eq!(recovered.write_offset, (PART_SIZE + 1) as u64);
    assert_eq!(
        destination
            .stage_state(&recovered, Operation::Prepare)
            .await?
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

fn prepare_for_size(size: u64) -> Result<PrepareRequest, Box<dyn std::error::Error>> {
    Ok(PrepareRequest {
        source: SourceDescriptor::new(
            StoragePath::new("source")?,
            EntryKind::File,
            Some(size),
            SourceIdentity::new(identity(), IdentityStrength::PathScoped, b"source")?,
        ),
        final_destination: FinalDestination::new(StoragePath::new("final")?),
        recovery_binding: [71; 32],
    })
}

#[tokio::test]
async fn aligned_uploads_do_not_append_empty_parts_but_empty_files_have_one()
-> Result<(), Box<dyn std::error::Error>> {
    for count in 0..=4 {
        let protocol = Arc::new(MemoryS3::default());
        let destination = S3StagedDestination::new(protocol, identity());
        let size = count * PART_SIZE;
        let stage = destination.prepare(prepare_for_size(size as u64)?).await?;
        let bytes = Bytes::from(vec![7; size]);
        destination
            .write(
                &stage,
                Box::pin(futures::stream::once(async move { Ok(bytes) })),
            )
            .await?;
        let upload_state = destination.stage_state(&stage, Operation::Write).await?;
        assert_eq!(upload_state.parts.len(), count.max(1));
        assert_eq!(upload_state.persisted, size as u64);
        destination.discard(stage).await?;
    }
    Ok(())
}

#[test]
fn zero_sized_parts_are_recoverable_only_as_complete_final_parts()
-> Result<(), Box<dyn std::error::Error>> {
    let path = StoragePath::new("stage")?;
    let part = |number, size| super::super::S3PartFacts {
        number,
        size,
        etag: format!("etag-{number}"),
    };
    assert_eq!(resumable_parts(&path, vec![part(1, 0)], Some(0))?.0, 0);
    assert_eq!(
        resumable_parts(
            &path,
            vec![part(1, PART_SIZE as u64), part(2, 0)],
            Some(PART_SIZE as u64)
        )?
        .0,
        PART_SIZE as u64
    );
    assert!(resumable_parts(&path, vec![part(1, 0)], None).is_err());
    assert!(resumable_parts(&path, vec![part(1, 0)], Some(1)).is_err());
    assert!(
        resumable_parts(
            &path,
            vec![part(1, 0), part(2, PART_SIZE as u64)],
            Some(PART_SIZE as u64)
        )
        .is_err()
    );
    assert!(resumable_parts(&path, vec![part(1, 0), part(2, 0)], Some(0)).is_err());
    Ok(())
}

#[tokio::test]
async fn retry_completion_reuses_zero_sized_final_parts() -> Result<(), Box<dyn std::error::Error>>
{
    for has_payload in [false, true] {
        let size = if has_payload { PART_SIZE } else { 0 };
        let protocol = Arc::new(MemoryS3::default());
        let destination = S3StagedDestination::new(protocol.clone(), identity());
        let request = prepare_for_size(size as u64)?;
        let stage = destination.prepare(request.clone()).await?;
        let key = S3StagedDestination::<MemoryS3>::key(&stage)?;
        let upload_state = destination.stage_state(&stage, Operation::Write).await?;
        let last = if has_payload {
            protocol
                .upload_part(&key, &upload_state.upload_id, 1, Bytes::from(vec![7; size]))
                .await
                .map_err(|error| format!("{error:?}"))?;
            2
        } else {
            1
        };
        protocol
            .upload_part(&key, &upload_state.upload_id, last, Bytes::new())
            .await
            .map_err(|error| format!("{error:?}"))?;
        let recovery = destination.recovery_identity(&stage).await?;
        drop(destination);
        let destination = S3StagedDestination::new(protocol.clone(), identity());
        let recovered = destination
            .recover(RecoverRequest {
                identity: recovery,
                final_destination: request.final_destination,
                source: request.source,
                recovery_binding: request.recovery_binding,
                claim_token: [72; 32],
            })
            .await?;
        assert_eq!(recovered.write_offset, size as u64);
        assert_eq!(
            destination
                .write(&recovered, Box::pin(futures::stream::empty()))
                .await?
                .persisted_bytes,
            size as u64
        );
        assert_eq!(
            protocol
                .objects
                .lock()
                .await
                .get(&key)
                .ok_or("completed object absent")?
                .len(),
            size
        );
        destination.discard(recovered).await?;
    }
    Ok(())
}
