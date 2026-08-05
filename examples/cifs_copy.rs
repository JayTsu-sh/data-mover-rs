use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clap::Parser;
use data_mover::storage_enum::{StorageEnum, create_storage};
use data_mover::{EntryEnum, Result, StorageEntryMessage};
use indicatif::{ProgressBar, ProgressStyle};
use num_traits::ToPrimitive;

/// CIFS/SMB 共享遍历 + 拷贝示例
///
/// 从源共享遍历所有文件/目录，复制到目标共享。
///
/// 用法：
///   cargo run -p data-mover --example `cifs_copy` -- \
///     --src "<smb://user:pass@server/Share1>" \
///     --dst "<smb://user:pass@server/Share2>"
#[derive(Parser, Debug)]
#[command(author, version, about = "CIFS/SMB copy example")]
struct Args {
    /// Source SMB URL, e.g. <smb://user:pass@server/share>
    #[arg(short, long)]
    src: String,

    /// Destination SMB URL
    #[arg(short, long)]
    dst: String,

    /// Concurrency for walkdir
    #[arg(short, long, default_value = "4")]
    concurrency: usize,
}

async fn copy_directories(
    src: &StorageEnum,
    dst: &StorageEnum,
    entries: &[Arc<EntryEnum>],
    progress: &ProgressBar,
) {
    let mut directories = entries
        .iter()
        .filter(|entry| entry.get_is_dir())
        .cloned()
        .collect::<Vec<_>>();
    directories.sort_by(|left, right| left.get_relative_path().cmp(right.get_relative_path()));
    for entry in directories {
        let path = entry.get_relative_path();
        progress.set_message(format!("mkdir {}", path.display()));
        if let Err(error) = dst.create_dir_all(&entry).await {
            eprintln!("  Failed to create dir {}: {error}", path.display());
        }
        if let Err(error) = StorageEnum::copy_acl(src, dst, path).await {
            eprintln!("  Failed to copy dir ACL {}: {error}", path.display());
        }
        progress.inc(1);
    }
}

async fn copy_files(
    src: &StorageEnum,
    dst: &StorageEnum,
    entries: &[Arc<EntryEnum>],
    progress: &ProgressBar,
    bytes_counter: &Arc<AtomicU64>,
) -> (usize, u64) {
    let files = entries
        .iter()
        .filter(|entry| entry.get_is_regular_file())
        .cloned()
        .collect::<Vec<_>>();
    let mut errors = 0;
    for entry in &files {
        let path = entry.get_relative_path();
        progress.set_message(format!("copy {}", path.display()));
        if let Err(error) = StorageEnum::copy_file(
            src,
            dst,
            entry,
            data_mover::CopyOptions {
                is_source_reserved: true,
                bytes_counter: Some(bytes_counter.clone()),
                ..Default::default()
            },
        )
        .await
        {
            eprintln!("  Failed to copy {}: {error}", path.display());
            errors += 1;
        }
        if let Err(error) = dst.set_entry_metadata(entry).await {
            eprintln!("  Failed to set metadata for {}: {error}", path.display());
        }
        if let Err(error) = StorageEnum::copy_acl(src, dst, path).await {
            eprintln!("  Failed to copy file ACL {}: {error}", path.display());
        }
        progress.inc(1);
    }
    (files.len(), errors)
}

async fn copy_symlinks(
    src: &StorageEnum,
    dst: &StorageEnum,
    entries: &[Arc<EntryEnum>],
    progress: &ProgressBar,
) -> u64 {
    let mut errors = 0;
    for entry in entries.iter().filter(|entry| entry.get_is_symlink()) {
        let path = entry.get_relative_path();
        progress.set_message(format!("symlink {}", path.display()));
        match src.read_symlink(entry).await {
            Ok(target) => {
                if let Err(error) = dst.create_symlink(entry, &target).await {
                    eprintln!("  Failed to create symlink {}: {error}", path.display());
                    errors += 1;
                }
            }
            Err(error) => {
                eprintln!("  Failed to read symlink {}: {error}", path.display());
                errors += 1;
            }
        }
        progress.inc(1);
    }
    errors
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    println!("Source: {}", args.src);
    println!("Target: {}", args.dst);
    println!();

    // 创建源和目标存储
    let src_storage = Arc::new(create_storage(&args.src, None, false).await?);
    let dst_storage = Arc::new(create_storage(&args.dst, None, true).await?);

    // ── Phase 1: 遍历源共享 ──────────────────────────────────────────────────
    println!("=== Phase 1: Walkdir ===");
    let start = Instant::now();

    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::with_template("{spinner} [{elapsed_precise}] {msg}")
            .map_err(|error| data_mover::error::StorageError::OperationError(error.to_string()))?,
    );
    pb.set_message("Scanning source share...");
    pb.enable_steady_tick(Duration::from_millis(100));

    let iter = src_storage
        .walkdir(
            None,
            data_mover::WalkOptions {
                concurrency: args.concurrency,
                ..Default::default()
            },
        )
        .await?;

    let mut entries: Vec<Arc<EntryEnum>> = Vec::new();
    let mut total_size: u64 = 0;
    let mut dir_count: u64 = 0;
    let mut file_count: u64 = 0;
    let mut symlink_count: u64 = 0;
    let mut error_count: u64 = 0;

    while let Some(msg) = iter.next().await {
        match msg {
            StorageEntryMessage::Scanned(entry) => {
                if entry.get_is_dir() {
                    dir_count += 1;
                } else if entry.get_is_symlink() {
                    symlink_count += 1;
                } else {
                    file_count += 1;
                    total_size += entry.get_size();
                }
                entries.push(entry);
            }
            StorageEntryMessage::Error { path, reason, .. } => {
                eprintln!("  Scan error: {} - {}", path.display(), reason);
                error_count += 1;
            }
            _ => {}
        }
    }

    pb.finish_with_message("Scan completed");
    let scan_duration = start.elapsed();

    println!("  Directories:  {dir_count}");
    println!("  Files:        {file_count}");
    println!("  Symlinks:     {symlink_count}");
    println!("  Errors:       {error_count}");
    println!(
        "  Total size:   {:.2} MB",
        total_size.to_f64().unwrap_or(f64::MAX) / (1024.0 * 1024.0)
    );
    println!("  Scan time:    {scan_duration:?}");
    println!();

    if entries.is_empty() {
        println!("No entries to copy.");
        return Ok(());
    }

    // ── Phase 2: 拷贝 ──────────────────────────────────────────────────────
    println!("=== Phase 2: Copy ===");
    let copy_start = Instant::now();
    let bytes_counter = Arc::new(AtomicU64::new(0));

    let pb = ProgressBar::new(entries.len() as u64);
    pb.set_style(
        ProgressStyle::with_template("{spinner} [{elapsed_precise}] [{bar:40}] {pos}/{len} {msg}")
            .map_err(|error| data_mover::error::StorageError::OperationError(error.to_string()))?,
    );

    copy_directories(&src_storage, &dst_storage, &entries, &pb).await;
    let (copied_files, mut copy_errors) =
        copy_files(&src_storage, &dst_storage, &entries, &pb, &bytes_counter).await;
    copy_errors += copy_symlinks(&src_storage, &dst_storage, &entries, &pb).await;

    pb.finish_with_message("Copy completed");
    let copy_duration = copy_start.elapsed();
    let total_bytes = bytes_counter.load(Ordering::Relaxed);

    println!("  Copied files: {copied_files}");
    println!("  Copy errors:  {copy_errors}");
    println!(
        "  Bytes copied: {:.2} MB",
        total_bytes.to_f64().unwrap_or(f64::MAX) / (1024.0 * 1024.0)
    );
    println!("  Copy time:    {copy_duration:?}");
    if copy_duration.as_secs() > 0 {
        println!(
            "  Throughput:   {:.2} MB/s",
            total_bytes.to_f64().unwrap_or(f64::MAX)
                / (1024.0 * 1024.0)
                / copy_duration.as_secs_f64()
        );
    }
    println!();
    println!("=== Done ===");
    println!("Total time: {:?}", start.elapsed());

    Ok(())
}
