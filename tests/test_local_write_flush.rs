//! Repeated role-based transfers preserve the small final block of a multi-chunk Local file.
//! Local-only — no external storage service is required.

use data_mover::model::StoragePath;
use data_mover::transfer::{InflightLimits, TransferRequest, transfer};
use tokio_util::sync::CancellationToken;

mod common;

const BLOCK: usize = 64 * 1024;
const SIZE: usize = 4 * BLOCK + 137;
const ITERS: usize = 256;

#[tokio::test(flavor = "multi_thread")]
async fn full_role_transfer_has_no_tail_loss_under_repetition()
-> Result<(), Box<dyn std::error::Error>> {
    let src = tempfile::tempdir()?;
    let dst = tempfile::tempdir()?;
    let source = common::connect_local(src.path(), 8).await?;
    let destination = common::connect_local(dst.path(), 8).await?;
    let expected: Vec<u8> = (0..SIZE)
        .map(|index| u8::try_from(index % 251).unwrap_or_default())
        .collect();
    let path = StoragePath::new("blob.bin")?;

    for iteration in 0..ITERS {
        tokio::fs::write(src.path().join(path.as_str()), &expected).await?;
        let request = TransferRequest::new(
            source.clone(),
            path.clone(),
            destination.clone(),
            InflightLimits::new(8, 8 * BLOCK, 8)?,
            CancellationToken::new(),
        );
        transfer(request).await?;

        let actual = tokio::fs::read(dst.path().join(path.as_str())).await?;
        assert_eq!(
            actual.len(),
            expected.len(),
            "iteration {iteration}: destination length changed"
        );
        assert_eq!(actual, expected, "iteration {iteration}: payload changed");
        tokio::fs::remove_file(dst.path().join(path.as_str())).await?;
    }
    Ok(())
}
