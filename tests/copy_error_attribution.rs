//! Transfer failures retain their phase and endpoint attribution through the role API.

use data_mover::model::StoragePath;
use data_mover::transfer::{
    InflightLimits, TransferPhase, TransferRequest, TransferSide, transfer,
};
use tokio_util::sync::CancellationToken;

mod common;

#[tokio::test]
async fn a_source_removed_after_observation_is_attributed_to_describe()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let source_root = tempfile::tempdir()?;
    let destination_root = tempfile::tempdir()?;
    std::fs::write(source_root.path().join("file"), b"payload")?;
    let source = common::connect_local(source_root.path(), 4).await?;
    let destination = common::connect_local(destination_root.path(), 4).await?;
    let path = StoragePath::new("file")?;
    let observation = common::observe_file(&source, "file").await?;
    std::fs::remove_file(source_root.path().join("file"))?;

    let Err(failure) = transfer(TransferRequest::new(
        source,
        path.clone(),
        destination,
        InflightLimits::new(2, 64 * 1024, 2)?,
        CancellationToken::new(),
    ))
    .await
    else {
        panic!("a removed source unexpectedly transferred");
    };

    assert_eq!(failure.side(), TransferSide::Source);
    assert_eq!(failure.phase(), TransferPhase::Describe);
    assert!(!failure.has_unpublished_stage());
    // The explicit observation above is deliberately kept alive until after removal: the role
    // transfer re-describes the path and attributes that failure to the source endpoint.
    drop(observation);
    Ok(())
}

#[tokio::test]
async fn an_unwritable_destination_is_attributed_to_destination_prepare()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = tempfile::tempdir()?;
    let destination_root = tempfile::tempdir()?;
    std::fs::write(source_root.path().join("file"), b"payload")?;
    std::fs::write(destination_root.path().join("blocker"), b"not a directory")?;
    let source = common::connect_local(source_root.path(), 4).await?;
    let destination = common::connect_local(destination_root.path(), 4).await?;

    let Err(failure) = transfer(
        TransferRequest::new(
            source,
            StoragePath::new("file")?,
            destination,
            InflightLimits::new(2, 64 * 1024, 2)?,
            CancellationToken::new(),
        )
        .with_final_path(StoragePath::new("blocker/child")?),
    )
    .await
    else {
        panic!("a file unexpectedly became a parent directory");
    };

    assert_eq!(failure.side(), TransferSide::Destination);
    assert_eq!(failure.phase(), TransferPhase::Prepare);
    assert!(!destination_root.path().join("blocker/child").exists());
    Ok(())
}
