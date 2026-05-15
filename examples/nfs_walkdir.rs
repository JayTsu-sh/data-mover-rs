use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use data_mover::storage_enum::create_storage;
use data_mover::{EntryEnum, Result, StorageEntryMessage};
use indicatif::{ProgressBar, ProgressStyle};
use tokio::sync::Mutex;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// NFS URL
    #[arg(short, long)]
    url: String,

    /// Concurrency level
    #[arg(short, long, default_value = "8")]
    concurrency: usize,
}

// 统计信息结构体
struct Stats {
    total_entries: u64,
    directories: u64,
    files: u64,
    is_completed: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let storage = create_storage(&args.url, None).await?;

    let start = Instant::now();

    // 创建共享状态
    let stats = Arc::new(Mutex::new(Stats {
        total_entries: 0,
        directories: 0,
        files: 0,
        is_completed: false,
    }));

    // 创建进度条
    let pb = ProgressBar::new_spinner();
    pb.set_style(ProgressStyle::with_template("{spinner} [{elapsed_precise}] {msg}").unwrap());
    pb.set_message("Scanning files...");
    pb.enable_steady_tick(Duration::from_millis(100));

    // 启动异步进度更新任务
    let stats_clone = stats.clone();
    let pb_clone = pb.clone();
    tokio::spawn(async move {
        let mut last_update = Instant::now();
        loop {
            // 每两秒更新一次进度
            if last_update.elapsed() > Duration::from_secs(2) {
                let stats = stats_clone.lock().await;
                if stats.is_completed {
                    break;
                }
                pb_clone.set_message(format!(
                    "Scanning files... Total: {}, Directories: {}, Files: {}",
                    stats.total_entries, stats.directories, stats.files
                ));
                last_update = Instant::now();
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    let iter = storage
        .walkdir(None, None, None, None, args.concurrency, false, false, 0)
        .await?;
    while let Some(msg) = iter.next().await {
        match msg {
            StorageEntryMessage::Scanned(entry) => match &*entry {
                EntryEnum::NAS(local_entry) => {
                    let mut stats = stats.lock().await;
                    stats.total_entries += 1;
                    if local_entry.is_dir {
                        stats.directories += 1;
                    } else {
                        stats.files += 1;
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

    // 标记扫描完成
    let mut stats = stats.lock().await;
    stats.is_completed = true;
    // 完成扫描
    pb.finish_with_message("Scan completed");

    let duration = start.elapsed();
    let stats = stats;
    println!("Total entries: {}", stats.total_entries);
    println!("Directories: {}", stats.directories);
    println!("Files: {}", stats.files);
    println!("Scan time: {:?}", duration);

    Ok(())
}
