use std::env;
use std::time::{Duration, Instant};

use data_mover::error::StorageError;
use data_mover::storage_enum::create_storage;
use data_mover::{EntryEnum, Result, StorageEntryMessage};
use indicatif::{ProgressBar, ProgressStyle};

#[tokio::main]
async fn main() -> Result<()> {
    // URL 从命令行传入（e2e-s3 skill 即此调用方式），不在源码里硬编码凭证
    let url = env::args().nth(1).ok_or_else(|| {
        StorageError::ConfigError(
            "usage: s3_walkdir <s3://AK:SK@bucket.host:port/prefix>".to_string(),
        )
    })?;
    let storage = create_storage(&url, None, false).await?;

    let start = Instant::now();
    let mut total_entries = 0;
    let mut first_error: Option<String> = None;

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
                if first_error.is_none() {
                    first_error = Some(format!("{}: {}", path.display(), reason));
                }
            }
            _ => {}
        }
    }

    // 完成扫描
    pb.finish_with_message("Scan completed");

    let duration = start.elapsed();
    println!("Total entries: {}", total_entries);
    println!("Scan time: {:?}", duration);

    // 遍历期间出现过错误则以非零退出，便于 e2e skill 断言
    if let Some(reason) = first_error {
        return Err(StorageError::OperationError(format!(
            "walkdir reported errors, first: {reason}"
        )));
    }

    Ok(())
}
