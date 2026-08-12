//! Terrasync-shaped local I/O validation: eight concurrent files through `StorageEnum::copy_file`.

#![allow(clippy::cast_precision_loss)]

use std::path::{Path, PathBuf};
use std::time::Instant;

use clap::Parser;
use data_mover::{CopyOptions, LocalIoConfig, LocalIoEngine, LocalStorage, Result, StorageEnum};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "auto")]
    engine: String,
    #[arg(long, default_value_t = 8)]
    files: usize,
    #[arg(long, default_value_t = 128)]
    size_mib: usize,
    #[arg(long, default_value_t = 2)]
    rings: usize,
    #[arg(long, default_value_t = 64)]
    ring_entries: u32,
    #[arg(long, default_value_t = 3)]
    repeats: usize,
    #[arg(long, default_value_t = true)]
    checksum: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let engine = match args.engine.as_str() {
        "auto" => LocalIoEngine::Auto,
        "uring" => LocalIoEngine::Uring,
        "blocking" => LocalIoEngine::Blocking,
        value => {
            return Err(data_mover::error::StorageError::ConfigError(format!(
                "invalid engine {value:?}"
            )));
        }
    };
    let config = LocalIoConfig::builder()
        .engine(engine)
        .read_rings(args.rings)
        .write_rings(args.rings)
        .ring_entries(args.ring_entries)
        .build()?;
    let root = std::env::temp_dir().join(format!("data-mover-validation-{}", std::process::id()));
    let source_root = root.join("source");
    let destination_root = root.join("destination");
    std::fs::create_dir_all(&source_root)?;
    std::fs::create_dir_all(&destination_root)?;
    create_fixtures(&source_root, args.files, args.size_mib)?;
    println!(
        "engine,repeat,files,size_mib,elapsed_s,throughput_mib_s,user_ticks,system_ticks,voluntary_ctxt,involuntary_ctxt,rss_kib,max_file_s,verified"
    );
    for repeat in 1..=args.repeats {
        clear_destination(&destination_root)?;
        let before = ProcStats::read()?;
        let started = Instant::now();
        let source =
            StorageEnum::Local(LocalStorage::new(&source_root, None).with_local_io_config(&config));
        let destination = StorageEnum::Local(
            LocalStorage::new(&destination_root, None).with_local_io_config(&config),
        );
        let mut tasks = Vec::new();
        for index in 0..args.files {
            let source = source.clone();
            let destination = destination.clone();
            let path = PathBuf::from(format!("file-{index}.bin"));
            let checksum = args.checksum;
            tasks.push(tokio::spawn(async move {
                let entry = source.get_metadata(&path).await?;
                let start = Instant::now();
                StorageEnum::copy_file(
                    &source,
                    &destination,
                    &entry,
                    CopyOptions {
                        enable_integrity_check: checksum,
                        is_source_reserved: true,
                        ..Default::default()
                    },
                )
                .await?;
                Ok::<_, data_mover::error::StorageError>(start.elapsed())
            }));
        }
        let mut max_file = 0.0_f64;
        for task in tasks {
            max_file = max_file.max(task.await??.as_secs_f64());
        }
        let elapsed = started.elapsed();
        let after = ProcStats::read()?;
        let verified = verify(&source_root, &destination_root, args.files)?;
        let total_mib = args.files.saturating_mul(args.size_mib);
        println!(
            "{},{},{},{},{:.6},{:.3},{},{},{},{},{},{:.6},{}",
            args.engine,
            repeat,
            args.files,
            args.size_mib,
            elapsed.as_secs_f64(),
            total_mib as f64 / elapsed.as_secs_f64(),
            after.user_ticks.saturating_sub(before.user_ticks),
            after.system_ticks.saturating_sub(before.system_ticks),
            after.voluntary.saturating_sub(before.voluntary),
            after.involuntary.saturating_sub(before.involuntary),
            after.rss_kib,
            max_file,
            verified
        );
    }
    std::fs::remove_dir_all(root)?;
    Ok(())
}

fn create_fixtures(root: &Path, files: usize, size_mib: usize) -> std::io::Result<()> {
    let block = vec![0x5a_u8; 1024 * 1024];
    for index in 0..files {
        let mut file = std::fs::File::create(root.join(format!("file-{index}.bin")))?;
        for _ in 0..size_mib {
            std::io::Write::write_all(&mut file, &block)?;
        }
    }
    Ok(())
}

fn clear_destination(root: &Path) -> std::io::Result<()> {
    if root.exists() {
        std::fs::remove_dir_all(root)?;
    }
    std::fs::create_dir_all(root)
}

fn verify(source: &Path, destination: &Path, files: usize) -> std::io::Result<bool> {
    for index in 0..files {
        let name = format!("file-{index}.bin");
        if blake3::hash(&std::fs::read(source.join(&name))?)
            != blake3::hash(&std::fs::read(destination.join(name))?)
        {
            return Ok(false);
        }
    }
    Ok(true)
}

#[derive(Default)]
struct ProcStats {
    user_ticks: u64,
    system_ticks: u64,
    voluntary: u64,
    involuntary: u64,
    rss_kib: u64,
}

impl ProcStats {
    fn read() -> std::io::Result<Self> {
        let stat = std::fs::read_to_string("/proc/self/stat")?;
        let fields = stat
            .rsplit_once(") ")
            .map_or("", |(_, fields)| fields)
            .split_whitespace()
            .collect::<Vec<_>>();
        let status = std::fs::read_to_string("/proc/self/status")?;
        Ok(Self {
            user_ticks: parse_field(&fields, 11),
            system_ticks: parse_field(&fields, 12),
            voluntary: parse_status(&status, "voluntary_ctxt_switches:"),
            involuntary: parse_status(&status, "nonvoluntary_ctxt_switches:"),
            rss_kib: parse_status(&status, "VmHWM:"),
        })
    }
}

fn parse_field(fields: &[&str], index: usize) -> u64 {
    fields.get(index).and_then(|v| v.parse().ok()).unwrap_or(0)
}
fn parse_status(status: &str, name: &str) -> u64 {
    status
        .lines()
        .find_map(|line| line.strip_prefix(name))
        .and_then(|value| value.split_whitespace().next())
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}
