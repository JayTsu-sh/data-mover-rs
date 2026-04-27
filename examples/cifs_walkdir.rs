use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use data_mover::storage_enum::create_storage;
use data_mover::{EntryEnum, Result, StorageEntryMessage};
use tokio::sync::Mutex;

#[derive(Parser, Debug)]
#[command(author, version, about = "CIFS/SMB walkdir example", long_about = None)]
struct Args {
    /// SMB URL, e.g. smb://user:pass@server/share/path
    #[arg(short, long)]
    url: String,

    /// Concurrency level
    #[arg(short, long, default_value = "8")]
    concurrency: usize,

    /// Max depth (0 = unlimited)
    #[arg(short, long, default_value = "0")]
    depth: usize,
}

struct Stats {
    total_entries: u64,
    directories: u64,
    files: u64,
    total_size: u64,
    is_completed: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let storage = create_storage(&args.url, None).await?;

    let start = Instant::now();
    let depth = if args.depth == 0 { None } else { Some(args.depth) };

    let stats = Arc::new(Mutex::new(Stats {
        total_entries: 0,
        directories: 0,
        files: 0,
        total_size: 0,
        is_completed: false,
    }));

    let pb = ProgressBar::new_spinner();
    pb.set_style(ProgressStyle::with_template("{spinner} [{elapsed_precise}] {msg}").unwrap());
    pb.set_message("Scanning CIFS share...");
    pb.enable_steady_tick(Duration::from_millis(100));

    let stats_clone = stats.clone();
    let pb_clone = pb.clone();
    tokio::spawn(async move {
        let mut last_update = Instant::now();
        loop {
            if last_update.elapsed() > Duration::from_secs(2) {
                let stats = stats_clone.lock().await;
                if stats.is_completed {
                    break;
                }
                pb_clone.set_message(format!(
                    "Scanning... Total: {}, Dirs: {}, Files: {}, Size: {:.2} GB",
                    stats.total_entries,
                    stats.directories,
                    stats.files,
                    stats.total_size as f64 / (1024.0 * 1024.0 * 1024.0)
                ));
                last_update = Instant::now();
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    let iter = storage
        .walkdir(None, depth, None, None, args.concurrency, false, false, 0)
        .await?;

    while let Some(msg) = iter.next().await {
        match msg {
            StorageEntryMessage::Scanned(entry) => match &*entry {
                EntryEnum::NAS(nas_entry) => {
                    let mut stats = stats.lock().await;
                    stats.total_entries += 1;
                    stats.total_size += nas_entry.size;
                    if nas_entry.is_dir {
                        stats.directories += 1;
                    } else {
                        stats.files += 1;
                    }
                }
                _ => continue,
            },
            StorageEntryMessage::Error { path, reason, .. } => {
                eprintln!("Error for {}: {}", path.display(), reason);
            }
            _ => {}
        }
    }

    let mut stats = stats.lock().await;
    stats.is_completed = true;
    pb.finish_with_message("Scan completed");

    let duration = start.elapsed();
    println!("\n--- CIFS Walkdir Results ---");
    println!("Total entries: {}", stats.total_entries);
    println!("Directories:   {}", stats.directories);
    println!("Files:         {}", stats.files);
    println!(
        "Total size:    {:.2} GB",
        stats.total_size as f64 / (1024.0 * 1024.0 * 1024.0)
    );
    println!("Scan time:     {:?}", duration);
    if duration.as_secs() > 0 {
        println!(
            "Throughput:    {:.0} entries/sec",
            stats.total_entries as f64 / duration.as_secs_f64()
        );
    }

    Ok(())
}
