use std::time::{Duration, Instant};

use data_mover::storage_enum::create_storage;
use data_mover::{EntryEnum, Result, StorageEntryMessage};
use indicatif::{ProgressBar, ProgressStyle};

#[tokio::main]
async fn main() -> Result<()> {
    let storage = create_storage(
        "c:\\jay\\source",
        data_mover::CreateStorageOptions::default(),
    )
    .await?;

    let start = Instant::now();
    let mut total_entries = 0;
    let mut directories = 0;
    let mut files = 0;

    // 创建进度条
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::with_template("{spinner} [{elapsed:.0}] {msg}")
            .map_err(|error| data_mover::error::StorageError::OperationError(error.to_string()))?,
    );
    pb.set_message("Scanning files...");
    pb.enable_steady_tick(Duration::from_millis(100));

    // 记录上次更新时间
    let mut last_update = Instant::now();

    let iter = storage
        .walkdir(None, data_mover::WalkOptions::default())
        .await?;
    while let Some(msg) = iter.next().await {
        match msg {
            StorageEntryMessage::Scanned(entry) => match &*entry {
                EntryEnum::NAS(local_entry) => {
                    total_entries += 1;
                    if local_entry.is_dir {
                        directories += 1;
                    } else {
                        files += 1;
                    }

                    // 每两秒更新一次进度
                    if last_update.elapsed() > Duration::from_secs(2) {
                        pb.set_message(format!(
                            "Scanning files... Total: {total_entries}, Directories: {directories}, Files: {files}"
                        ));
                        last_update = Instant::now();
                    }
                }
                EntryEnum::S3(_) | EntryEnum::HDFS(_) => {}
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
    println!("Total entries: {total_entries}");
    println!("Directories: {directories}");
    println!("Files: {files}");
    println!("Scan time: {duration:?}");

    Ok(())
}
