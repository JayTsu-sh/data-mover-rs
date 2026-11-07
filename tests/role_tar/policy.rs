//! 使用实际文件身份和真实 S3 检查归档策略，不能把 Direct 静默替换成 staging。
use super::*;
use data_mover::storage::S3BackendConfig;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::time::{Duration, Instant};

async fn request(
    source: &Storage,
    destination: Storage,
    policy: TransferPolicy,
) -> Result<TarRequest> {
    Ok(TarRequest {
        source: source.clone(),
        destination,
        entries: vec![entry(source, "file").await?].into(),
        final_path: StoragePath::new("result.tar")?,
        cancel: CancellationToken::new(),
        source_qos: None,
        transfer_policy: policy,
    })
}

#[cfg(unix)]
#[tokio::test]
async fn direct_archive_updates_the_existing_inode_and_atomic_replaces_it() -> Result {
    for policy in [TransferPolicy::Direct, TransferPolicy::AtomicReplace] {
        let source_root = tempfile::tempdir()?;
        let target_root = tempfile::tempdir()?;
        std::fs::write(source_root.path().join("file"), b"archive payload")?;
        let path = target_root.path().join("result.tar");
        let basis = target_root.path().join("basis");
        std::fs::write(&path, b"old archive")?;
        std::fs::hard_link(&path, &basis)?;
        let inode = std::fs::metadata(&path)?.ino();
        let source = local(source_root.path()).await?;
        let request = request(&source, local(target_root.path()).await?, policy).await?;
        let expected = calculate_tar_size(&request.entries)?;
        let outcome = pack(request).await?;
        let archive = std::fs::read(&path)?;
        assert_eq!(archive.len() as u64, expected);
        assert_eq!(outcome.blake3, Some(*blake3::hash(&archive).as_bytes()));
        assert!(archive.windows(15).any(|bytes| bytes == b"archive payload"));
        assert_eq!(
            std::fs::metadata(&path)?.ino() == inode,
            policy == TransferPolicy::Direct
        );
        if policy == TransferPolicy::Direct {
            assert_eq!(std::fs::read(&basis)?, archive);
        } else {
            assert_eq!(std::fs::read(&basis)?, b"old archive");
        }
        assert_eq!(std::fs::read_dir(target_root.path())?.count(), 2);
    }
    Ok(())
}

#[tokio::test]
async fn checkpointed_archive_is_refused_before_touching_the_target() -> Result {
    let source_root = tempfile::tempdir()?;
    let target_root = tempfile::tempdir()?;
    std::fs::write(source_root.path().join("file"), b"payload")?;
    std::fs::write(target_root.path().join("result.tar"), b"retained")?;
    let source = local(source_root.path()).await?;
    let request = request(
        &source,
        local(target_root.path()).await?,
        TransferPolicy::Checkpointed,
    )
    .await?;
    std::fs::remove_file(source_root.path().join("file"))?;
    let result = pack(request).await;
    assert!(
        matches!(result, Err(TarFailure::Invalid(ref reason)) if reason.contains("Checkpointed"))
    );
    assert_eq!(
        std::fs::read(target_root.path().join("result.tar"))?,
        b"retained"
    );
    assert_eq!(std::fs::read_dir(target_root.path())?.count(), 1);
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn direct_archive_refuses_a_later_manifest_input_hardlinked_from_another_root() -> Result {
    let source_root = tempfile::tempdir()?;
    let target_root = tempfile::tempdir()?;
    std::fs::write(source_root.path().join("file"), b"first source")?;
    std::fs::write(source_root.path().join("later"), b"protected source")?;
    std::fs::hard_link(
        source_root.path().join("later"),
        target_root.path().join("result.tar"),
    )?;
    let source = local(source_root.path()).await?;
    let mut request = request(
        &source,
        local(target_root.path()).await?,
        TransferPolicy::Direct,
    )
    .await?;
    request.entries = vec![
        entry(&source, "file").await?,
        entry(&source, "later").await?,
    ]
    .into();
    let result = pack(request).await;
    assert!(
        matches!(result, Err(TarFailure::Transfer(ref failure)) if failure.to_string().contains("Conflict"))
    );
    assert_eq!(
        std::fs::read(source_root.path().join("file"))?,
        b"first source"
    );
    assert_eq!(
        std::fs::read(source_root.path().join("later"))?,
        b"protected source"
    );
    assert_eq!(
        std::fs::read(target_root.path().join("result.tar"))?,
        b"protected source"
    );
    assert_eq!(std::fs::read_dir(target_root.path())?.count(), 1);
    Ok(())
}

async fn s3(prefix: &str) -> Result<Storage> {
    Ok(connect_backend(BackendConfig::S3(S3BackendConfig {
        url: format!("{}/{prefix}", std::env::var("LAB_ARCHIVE_URL")?),
        block_size: None,
        single_put_threshold: Some(5 * 1024 * 1024),
    }))
    .await?)
}

async fn refused_write(source: &Storage, name: &str) -> Result {
    let request = request(source, s3(name).await?, TransferPolicy::Direct).await?;
    let started = Instant::now();
    let result = pack(request).await;
    let Err(TarFailure::Transfer(failure)) = result else {
        return Err(format!("{name} write refusal missing").into());
    };
    assert_eq!(failure.side(), TransferSide::Destination);
    assert!(failure.source().is_some(), "{failure}");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{name}: {failure}"
    );
    eprintln!(
        "archive fault={name} elapsed_ms={} reason={failure}",
        started.elapsed().as_millis()
    );
    Ok(())
}

async fn source_overlap_is_refused() -> Result {
    let source = s3("").await?;
    let request = TarRequest {
        source: source.clone(),
        destination: s3("single").await?,
        entries: vec![entry(&source, "single/result.tar").await?].into(),
        final_path: StoragePath::new("result.tar")?,
        cancel: CancellationToken::new(),
        source_qos: None,
        transfer_policy: TransferPolicy::Direct,
    };
    let result = pack(request).await;
    assert!(
        matches!(result, Err(TarFailure::Transfer(ref failure)) if failure.to_string().contains("Conflict"))
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires the owned S3 archive-policy runner"]
async fn direct_s3_archive_uses_final_key_and_keeps_source_on_write_refusal() -> Result {
    let root = tempfile::tempdir()?;
    let source = local(root.path()).await?;
    for (name, size) in [("single", 1024), ("multipart", 6 * 1024 * 1024)] {
        std::fs::write(root.path().join("file"), vec![0x5a; size])?;
        let request = request(&source, s3(name).await?, TransferPolicy::Direct).await?;
        let expected = calculate_tar_size(&request.entries)?;
        let outcome = pack(request).await?;
        assert_eq!(outcome.transferred_bytes, expected);
        assert!(outcome.blake3.is_some());
    }
    std::fs::write(std::env::var("LAB_ARCHIVE_CONTROL")?, "deny")?;
    std::fs::write(root.path().join("file"), b"retained source")?;
    let result = pack(request(&source, s3("denied").await?, TransferPolicy::Direct).await?).await;
    let Err(TarFailure::Transfer(failure)) = result else {
        return Err("write refusal missing".into());
    };
    assert!(
        failure.to_string().contains("PermissionDenied"),
        "{failure}"
    );
    assert_eq!(std::fs::read(root.path().join("file"))?, b"retained source");
    for mode in ["disconnect", "stall"] {
        std::fs::write(std::env::var("LAB_ARCHIVE_CONTROL")?, mode)?;
        refused_write(&source, mode).await?;
        std::fs::write(std::env::var("LAB_ARCHIVE_CONTROL")?, "normal")?;
        pack(
            request(
                &source,
                s3(&format!("recovered-{mode}")).await?,
                TransferPolicy::Direct,
            )
            .await?,
        )
        .await?;
        assert_eq!(std::fs::read(root.path().join("file"))?, b"retained source");
    }
    source_overlap_is_refused().await?;
    Ok(())
}
