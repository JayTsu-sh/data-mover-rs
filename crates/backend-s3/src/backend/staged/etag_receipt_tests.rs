//! DXN returns quoted `UploadPart` receipts but bare `ListParts` `ETags` for the same part.
use super::*;
use crate::storage::backends::s3::staged::checkpoint_record;

const ETAG: &str = "4f08eef4096726bff1125e1b1b734644";

fn legacy_receipt(etag: &str) -> [u8; 32] {
    legacy_parts(&[(1, etag)])
}

fn legacy_parts(parts: &[(i32, &str)]) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"data-mover S3 durable part receipts v1");
    for (number, etag) in parts {
        let start = u64::try_from(*number - 1).unwrap_or(0) * PART as u64;
        hash.update(&number.to_le_bytes());
        hash.update(&start.to_le_bytes());
        hash.update(&(start + PART as u64).to_le_bytes());
        hash.update(&(etag.len() as u64).to_le_bytes());
        hash.update(etag.as_bytes());
    }
    *hash.finalize().as_bytes()
}

async fn saved_stage(etag: &str) -> TestResult<(PreparedStage, Vec<u8>)> {
    let protocol = Arc::new(MemoryS3::default());
    let adapter = destination(&protocol);
    let stage = prepare(&adapter, PART + 17).await?;
    arm(&stage, &vec![7; PART + 17])?;
    let upload = final_upload::of(&stage).ok_or("upload")?;
    upload.lock().parts = vec![(1, etag.to_owned())];
    stage.confirmed_write(0, PART as u64);
    stage.checkpoint_barrier(PART as u64);
    let extension = checkpoint_record::extension(&stage, upload, true)?;
    stage.restore_checkpoint(stage.checkpoint_snapshot())?;
    Ok((stage, extension.to_vec()))
}

#[tokio::test]
async fn dxn_bare_list_parts_matches_the_saved_quoted_receipt() -> TestResult {
    let quoted = format!("\"{ETAG}\"");
    let (stage, extension) = saved_stage(&quoted).await?;
    let upload = final_upload::of(&stage).ok_or("upload")?;
    upload.lock().parts[0].1 = ETAG.to_owned();
    assert_eq!(
        CheckpointRecord::decode(&extension)
            .ok_or("record")?
            .receipts,
        legacy_receipt(&quoted),
        "already-quoted v1 checkpoints must remain byte-compatible"
    );
    S3StagedDestination::<MemoryS3>::validate_checkpoint_receipts(&stage, upload, &extension)?;
    Ok(())
}

#[tokio::test]
async fn bare_receipts_are_persisted_in_the_quoted_form() -> TestResult {
    let (stage, extension) = saved_stage(ETAG).await?;
    assert_eq!(
        CheckpointRecord::decode(&extension)
            .ok_or("record")?
            .receipts,
        legacy_receipt(&format!("\"{ETAG}\""))
    );
    let upload = final_upload::of(&stage).ok_or("upload")?;
    assert_eq!(
        upload.lock().parts[0].1,
        ETAG,
        "keep the wire receipt unchanged"
    );
    Ok(())
}

#[tokio::test]
async fn quoting_does_not_hide_opaque_case_content_or_malformed_tag_changes() -> TestResult {
    let (stage, extension) = saved_stage("\"opaque-A\"").await?;
    let upload = final_upload::of(&stage).ok_or("upload")?;
    for different in [
        "opaque-a",
        "opaque-B",
        "\"opaque-A",
        "opaque-A\"",
        "\"\"opaque-A\"\"",
        "W/\"opaque-A\"",
    ] {
        upload.lock().parts[0].1 = different.to_owned();
        assert!(
            S3StagedDestination::<MemoryS3>::validate_checkpoint_receipts(
                &stage, upload, &extension
            )
            .is_err(),
            "must reject {different:?}"
        );
    }
    upload.lock().parts[0].1 = "opaque-A".to_owned();
    S3StagedDestination::<MemoryS3>::validate_checkpoint_receipts(&stage, upload, &extension)?;
    Ok(())
}

#[tokio::test]
async fn an_old_bare_checkpoint_remains_readable_when_list_parts_is_quoted() -> TestResult {
    let (stage, extension) = saved_stage(ETAG).await?;
    let mut old = CheckpointRecord::decode(&extension).ok_or("record")?;
    old.receipts = legacy_receipt(ETAG);
    let upload = final_upload::of(&stage).ok_or("upload")?;
    upload.lock().parts[0].1 = format!("\"{ETAG}\"");
    S3StagedDestination::<MemoryS3>::validate_checkpoint_receipts(
        &stage,
        upload,
        &old.encode().ok_or("record")?,
    )?;
    Ok(())
}

#[tokio::test]
async fn mixed_native_and_streamed_styles_survive_repeated_checkpoint_recovery() -> TestResult {
    let protocol = Arc::new(MemoryS3::default());
    let stage = prepare(&destination(&protocol), 2 * PART + 17).await?;
    arm(&stage, &vec![7; 2 * PART + 17])?;
    let upload = final_upload::of(&stage).ok_or("upload")?;
    upload.lock().parts = vec![(1, "\"opaque-A\"".into()), (2, "opaque-B".into())];
    stage.confirmed_write(0, (2 * PART) as u64);
    stage.checkpoint_barrier((2 * PART) as u64);
    stage.restore_checkpoint(stage.checkpoint_snapshot())?;
    let first = checkpoint_record::extension(&stage, upload, true)?;
    upload.lock().parts = vec![(2, "\"opaque-B\"".into()), (1, "opaque-A".into())];
    S3StagedDestination::<MemoryS3>::validate_checkpoint_receipts(&stage, upload, &first)?;
    let next = checkpoint_record::extension(&stage, upload, true)?;
    assert_eq!(first, next);
    upload.lock().parts = vec![(1, "opaque-A".into()), (2, "opaque-B".into())];
    S3StagedDestination::<MemoryS3>::validate_checkpoint_receipts(&stage, upload, &next)?;
    let mut ambiguous_old = CheckpointRecord::decode(&first).ok_or("record")?;
    ambiguous_old.receipts = legacy_parts(&[(1, "\"opaque-A\""), (2, "opaque-B")]);
    upload.lock().parts = vec![(1, "\"opaque-A\"".into()), (2, "opaque-B".into())];
    let legacy = ambiguous_old.encode().ok_or("record")?;
    S3StagedDestination::<MemoryS3>::validate_checkpoint_receipts(&stage, upload, &legacy)?;
    upload.lock().parts = vec![(1, "opaque-A".into()), (2, "opaque-B".into())];
    assert!(
        S3StagedDestination::<MemoryS3>::validate_checkpoint_receipts(
            &stage,
            upload,
            &ambiguous_old.encode().ok_or("record")?,
        )
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn malformed_receipts_cannot_be_written_or_accepted_by_legacy_fallback() -> TestResult {
    let (stage, extension) = saved_stage("opaque-A").await?;
    let upload = final_upload::of(&stage).ok_or("upload")?;
    for invalid in [
        "",
        "\"\"",
        "\"opaque-A",
        "opaque-A\"",
        "\"\"opaque-A\"\"",
        "opaque A",
        "opaque\nA",
        "W/\"opaque-A\"",
    ] {
        upload.lock().parts[0].1 = invalid.to_owned();
        assert!(checkpoint_record::extension(&stage, upload, true).is_err());
        let mut old = CheckpointRecord::decode(&extension).ok_or("record")?;
        old.receipts = legacy_receipt(invalid);
        assert!(
            S3StagedDestination::<MemoryS3>::validate_checkpoint_receipts(
                &stage,
                upload,
                &old.encode().ok_or("record")?,
            )
            .is_err()
        );
    }
    Ok(())
}

#[tokio::test]
async fn published_recovery_keeps_the_saved_receipts_without_an_open_upload() -> TestResult {
    let (stage, _) = saved_stage(ETAG).await?;
    let upload = final_upload::of(&stage).ok_or("upload")?;
    upload.lock().parts.clear();
    upload.lock().recovered_receipts = Some([0xa5; 32]);
    let extension = checkpoint_record::extension(&stage, upload, true)?;
    assert_eq!(
        CheckpointRecord::decode(&extension)
            .ok_or("record")?
            .receipts,
        [0xa5; 32]
    );
    Ok(())
}
