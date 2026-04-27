use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use data_mover::storage_enum::{StorageEnum, create_storage, create_storage_for_dest};
use data_mover::{EntryEnum, Result, StorageEntryMessage};

/// CIFS/SMB 共享遍历 + 拷贝示例
///
/// 从源共享遍历所有文件/目录，复制到目标共享。
///
/// 用法：
///   cargo run -p data-mover --example cifs_copy -- \
///     --src "smb://user:pass@server/Share1" \
///     --dst "smb://user:pass@server/Share2"
#[derive(Parser, Debug)]
#[command(author, version, about = "CIFS/SMB copy example")]
struct Args {
    /// Source SMB URL, e.g. smb://user:pass@server/share
    #[arg(short, long)]
    src: String,

    /// Destination SMB URL
    #[arg(short, long)]
    dst: String,

    /// Concurrency for walkdir
    #[arg(short, long, default_value = "4")]
    concurrency: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    println!("Source: {}", args.src);
    println!("Target: {}", args.dst);
    println!();

    // 创建源和目标存储
    let src_storage = Arc::new(create_storage(&args.src, None).await?);
    let dst_storage = Arc::new(create_storage_for_dest(&args.dst, None).await?);

    // ── Phase 1: 遍历源共享 ──────────────────────────────────────────────────
    println!("=== Phase 1: Walkdir ===");
    let start = Instant::now();

    let pb = ProgressBar::new_spinner();
    pb.set_style(ProgressStyle::with_template("{spinner} [{elapsed_precise}] {msg}").unwrap());
    pb.set_message("Scanning source share...");
    pb.enable_steady_tick(Duration::from_millis(100));

    let iter = src_storage
        .walkdir(None, None, None, None, args.concurrency, false, false, 0)
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

    println!("  Directories:  {}", dir_count);
    println!("  Files:        {}", file_count);
    println!("  Symlinks:     {}", symlink_count);
    println!("  Errors:       {}", error_count);
    println!("  Total size:   {:.2} MB", total_size as f64 / (1024.0 * 1024.0));
    println!("  Scan time:    {:?}", scan_duration);
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
    pb.set_style(ProgressStyle::with_template("{spinner} [{elapsed_precise}] [{bar:40}] {pos}/{len} {msg}").unwrap());

    // 先创建目录结构（按层级排序，确保父目录先创建）
    let mut dirs: Vec<_> = entries.iter().filter(|e| e.get_is_dir()).cloned().collect();
    dirs.sort_by(|a, b| a.get_relative_path().cmp(b.get_relative_path()));

    for entry in &dirs {
        let rel_path = entry.get_relative_path();
        pb.set_message(format!("mkdir {}", rel_path.display()));
        if let Err(e) = dst_storage.create_dir_all(entry).await {
            eprintln!("  Failed to create dir {:?}: {}", rel_path, e);
        }
        // 拷贝目录 ACL
        pb.set_message(format!("acl {}", rel_path.display()));
        if let Err(e) = StorageEnum::copy_acl(&src_storage, &dst_storage, rel_path).await {
            eprintln!("  Failed to copy dir ACL {:?}: {}", rel_path, e);
        }
        pb.inc(1);
    }

    // 拷贝文件
    let files: Vec<_> = entries.iter().filter(|e| e.get_is_regular_file()).cloned().collect();
    let mut copy_errors: u64 = 0;

    for entry in &files {
        let rel_path = entry.get_relative_path();
        pb.set_message(format!("copy {}", rel_path.display()));

        match StorageEnum::copy_file(
            &src_storage,
            &dst_storage,
            entry,
            None,  // 不限速
            false, // 不校验完整性
            true,  // 保留源文件
            Some(bytes_counter.clone()),
        )
        .await
        {
            Ok(()) => {}
            Err(e) => {
                eprintln!("  Failed to copy {:?}: {}", rel_path, e);
                copy_errors += 1;
            }
        }

        // 设置元数据（时间戳、权限）
        if let Err(e) = dst_storage.set_entry_metadata(entry).await {
            eprintln!("  Failed to set metadata for {:?}: {}", rel_path, e);
        }

        // 拷贝文件 ACL
        if let Err(e) = StorageEnum::copy_acl(&src_storage, &dst_storage, rel_path).await {
            eprintln!("  Failed to copy file ACL {:?}: {}", rel_path, e);
        }

        pb.inc(1);
    }

    // 拷贝 symlink
    let symlinks: Vec<_> = entries.iter().filter(|e| e.get_is_symlink()).cloned().collect();
    for entry in &symlinks {
        let rel_path = entry.get_relative_path();
        pb.set_message(format!("symlink {}", rel_path.display()));

        match src_storage.read_symlink(entry).await {
            Ok(target) => {
                if let Err(e) = dst_storage.create_symlink(entry, &target).await {
                    eprintln!("  Failed to create symlink {:?}: {}", rel_path, e);
                    copy_errors += 1;
                }
            }
            Err(e) => {
                eprintln!("  Failed to read symlink {:?}: {}", rel_path, e);
                copy_errors += 1;
            }
        }
        pb.inc(1);
    }

    pb.finish_with_message("Copy completed");
    let copy_duration = copy_start.elapsed();
    let total_bytes = bytes_counter.load(Ordering::Relaxed);

    println!("  Copied files: {}", files.len());
    println!("  Copy errors:  {}", copy_errors);
    println!("  Bytes copied: {:.2} MB", total_bytes as f64 / (1024.0 * 1024.0));
    println!("  Copy time:    {:?}", copy_duration);
    if copy_duration.as_secs() > 0 {
        println!(
            "  Throughput:   {:.2} MB/s",
            total_bytes as f64 / (1024.0 * 1024.0) / copy_duration.as_secs_f64()
        );
    }
    println!();
    println!("=== Done ===");
    println!("Total time: {:?}", start.elapsed());

    Ok(())
}
