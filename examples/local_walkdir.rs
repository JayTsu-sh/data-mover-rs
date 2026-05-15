use std::time::{Duration, Instant};

use data_mover::storage_enum::create_storage;
use data_mover::{EntryEnum, Result, StorageEntryMessage};
use indicatif::{ProgressBar, ProgressStyle};

#[tokio::main]
async fn main() -> Result<()> {
    let storage = create_storage("c:\\jay\\source", None).await?;

    let start = Instant::now();
    let mut total_entries = 0;
    let mut directories = 0;
    let mut files = 0;

    // 创建进度条
    let pb = ProgressBar::new_spinner();
    pb.set_style(ProgressStyle::with_template("{spinner} [{elapsed:.0}] {msg}").unwrap());
    pb.set_message("Scanning files...");
    pb.enable_steady_tick(Duration::from_millis(100));

    // 记录上次更新时间
    let mut last_update = Instant::now();

    let iter = storage
        .walkdir(None, None, None, None, 1, false, false, 0)
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
                            "Scanning files... Total: {}, Directories: {}, Files: {}",
                            total_entries, directories, files
                        ));
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
    println!("Directories: {}", directories);
    println!("Files: {}", files);
    println!("Scan time: {:?}", duration);

    Ok(())
}
