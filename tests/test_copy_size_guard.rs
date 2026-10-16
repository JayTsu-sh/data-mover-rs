//! A stale enumeration observation cannot be streamed after the Local source is truncated.
//! The expert source session revalidates the observed size before transfer payload or destination
//! I/O, so the old optional-integrity switch is no longer a size-guard prerequisite.

use std::fs::OpenOptions;
use std::path::Path;

use data_mover::model::StoragePath;
use data_mover::transfer::{
    ExpertSourceRequest, ExpertSourceSession, InflightLimits, TransferPhase, TransferRequest,
    transfer,
};
use tokio_util::sync::CancellationToken;

mod common;

fn pattern(size: usize) -> Vec<u8> {
    (0..size)
        .map(|index| u8::try_from(index % 251).unwrap_or_default())
        .collect()
}

fn truncate(path: &Path, len: u64) -> Result<(), std::io::Error> {
    OpenOptions::new().write(true).open(path)?.set_len(len)
}

async fn stale_observation_is_refused(
    size: usize,
    truncated: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let source_root = tempfile::tempdir()?;
    let destination_root = tempfile::tempdir()?;
    let source_path = source_root.path().join("blob.bin");
    std::fs::write(&source_path, pattern(size))?;

    let source = common::connect_local(source_root.path(), 4).await?;
    let observation = common::observe_file(&source, "blob.bin").await?;
    truncate(&source_path, truncated)?;

    let Err(failure) = ExpertSourceSession::open(ExpertSourceRequest::new(
        source,
        observation,
        InflightLimits::new(2, 128 * 1024, 2)?,
        CancellationToken::new(),
    ))
    .await
    else {
        panic!("source size changed after observation");
    };

    assert_eq!(failure.phase(), TransferPhase::Describe);
    assert_eq!(
        std::fs::read_dir(destination_root.path())?.count(),
        0,
        "failed source revalidation must happen before destination preparation"
    );
    Ok(())
}

#[tokio::test]
async fn small_stale_source_observation_is_refused_before_destination_io()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    stale_observation_is_refused(8 * 1024, 5 * 1024).await
}

#[tokio::test]
async fn multi_chunk_stale_source_observation_is_refused_before_destination_io()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    stale_observation_is_refused(256 * 1024, 150 * 1024).await
}

#[tokio::test]
async fn intact_small_and_multi_chunk_transfers_still_match_the_source()
-> Result<(), Box<dyn std::error::Error>> {
    for size in [8 * 1024, 256 * 1024] {
        let source_root = tempfile::tempdir()?;
        let destination_root = tempfile::tempdir()?;
        let expected = pattern(size);
        std::fs::write(source_root.path().join("blob.bin"), &expected)?;
        let path = StoragePath::new("blob.bin")?;
        let outcome = transfer(TransferRequest::new(
            common::connect_local(source_root.path(), 4).await?,
            path.clone(),
            common::connect_local(destination_root.path(), 4).await?,
            InflightLimits::new(2, 128 * 1024, 2)?,
            CancellationToken::new(),
        ))
        .await?;
        assert_eq!(outcome.transferred_bytes, size as u64);
        assert_eq!(
            std::fs::read(destination_root.path().join(path.as_str()))?,
            expected
        );
    }
    Ok(())
}
