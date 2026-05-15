use std::time::{Duration, Instant};

use data_mover::Result;
use data_mover::dir_tree::NdxEvent;
use data_mover::storage_enum::create_storage;
use indicatif::{ProgressBar, ProgressStyle};

#[tokio::main]
async fn main() -> Result<()> {
    // 参数：路径（默认 c:\jay\source）、并发数（默认 4）
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "c:\\jay\\source".to_string());
    let concurrency: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);

    println!("walkdir_2: path={}, concurrency={}", path, concurrency);

    let storage = create_storage(&path, None).await?;
    let start = Instant::now();

    let mut total_pages = 0u64;
    let mut total_files = 0u64;
    let mut total_dirs = 0u64;
    let mut total_errors = 0u64;
    let mut max_ndx: i32 = -1;

    let pb = ProgressBar::new_spinner();
    if let Ok(style) = ProgressStyle::with_template("{spinner} [{elapsed:.0}] {msg}") {
        pb.set_style(style);
    }
    pb.set_message("Scanning with walkdir_2...");
    pb.enable_steady_tick(Duration::from_millis(100));
    let mut last_update = Instant::now();

    let iter = storage
        .walkdir_2(None, None, None, None, concurrency, false)
        .await?;

    while let Some(event) = iter.next().await {
        match event {
            NdxEvent::Page(page) => {
                total_pages += 1;
                total_files += page.files.len() as u64;
                total_dirs += page.subdirs.len() as u64;

                // 追踪最大 NDX
                if let Some(last_file) = page.files.last() {
                    max_ndx = max_ndx.max(last_file.ndx);
                }
                if let Some(last_dir) = page.subdirs.last() {
                    max_ndx = max_ndx.max(last_dir.ndx);
                }
                if page.gap_ndx > 0 {
                    max_ndx = max_ndx.max(page.gap_ndx);
                }

                // 打印所有页详细信息
                {
                    println!("\n--- Page #{} [dir: {:?}] ---", total_pages, page.dir_path);
                    println!("  ndx_start={}, gap_ndx={}", page.ndx_start, page.gap_ndx);
                    for f in &page.files {
                        println!("  FILE  ndx={:>4}  {}", f.ndx, f.entry.get_name());
                    }
                    for d in &page.subdirs {
                        println!("  DIR   ndx={:>4}  {}", d.ndx, d.entry.get_name());
                    }
                }

                if last_update.elapsed() > Duration::from_secs(2) {
                    pb.set_message(format!(
                        "Pages: {}, Files: {}, Dirs: {}, Errors: {}",
                        total_pages, total_files, total_dirs, total_errors
                    ));
                    last_update = Instant::now();
                }
            }
            NdxEvent::Error { path, reason } => {
                total_errors += 1;
                eprintln!("ERROR [{}]: {}", path, reason);
            }
            NdxEvent::Done => {
                break;
            }
        }
    }

    pb.finish_with_message("walkdir_2 completed");

    let duration = start.elapsed();
    println!("\n=== walkdir_2 Summary ===");
    println!("Path: {}", path);
    println!("Concurrency: {}", concurrency);
    println!("Total pages (directories): {}", total_pages);
    println!("Total files: {}", total_files);
    println!("Total subdirs: {}", total_dirs);
    println!("Total errors: {}", total_errors);
    println!("Max NDX: {}", max_ndx);
    println!("Scan time: {:?}", duration);
    if duration.as_secs_f64() > 0.0 {
        println!(
            "Pages/sec: {:.0}",
            total_pages as f64 / duration.as_secs_f64()
        );
        println!(
            "Entries/sec: {:.0}",
            (total_files + total_dirs) as f64 / duration.as_secs_f64()
        );
    }

    Ok(())
}
