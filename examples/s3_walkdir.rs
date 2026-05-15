use std::time::{Duration, Instant};

use data_mover::storage_enum::create_storage;
use data_mover::{EntryEnum, Result, StorageEntryMessage};
use indicatif::{ProgressBar, ProgressStyle};

#[tokio::main]
async fn main() -> Result<()> {
    let storage = create_storage(
        "s3://H80NKRVS5DYOVE43U2HS:FBU8xNSKujskgO2bF6ctnd7dF2IeDodmoy3q6hNk@mbucket-src.10.128.137.245:8184",
        None,
    )
    .await?;

    let start = Instant::now();
    let mut total_entries = 0;

    // 创建进度条
    let pb = ProgressBar::new_spinner();
    pb.set_style(ProgressStyle::with_template("{spinner} [{elapsed:.0}] {msg}").unwrap());
    pb.set_message("Scanning files...");
    pb.enable_steady_tick(Duration::from_millis(100));

    // 记录上次更新时间
    let mut last_update = Instant::now();

    let iter = storage
        .walkdir(None, None, None, None, 1, true, false, 0)
        .await?;
    while let Some(msg) = iter.next().await {
        match msg {
            StorageEntryMessage::Scanned(entry) => match &*entry {
                EntryEnum::S3(_) => {
                    total_entries += 1;

                    // 每两秒更新一次进度
                    if last_update.elapsed() > Duration::from_secs(2) {
                        pb.set_message(format!("Scanning files... Total: {}", total_entries));
                        last_update = Instant::now();
                    }
                }
                _ => continue,
            },
            StorageEntryMessage::Error { path, reason, .. } => {
                println!("Error for {}: {}", path.display(), reason);
            }
            _ => {}
        }
    }

    // 完成扫描
    pb.finish_with_message("Scan completed");

    let duration = start.elapsed();
    println!("Total entries: {}", total_entries);
    println!("Scan time: {:?}", duration);

    Ok(())
}
