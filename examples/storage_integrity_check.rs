use std::path::PathBuf;

use clap::{Parser, ValueEnum};
use std::time::Duration;

#[allow(dead_code)]
mod endpoint_support;
use data_mover::integrity::{
    IntegrityMode, IntegrityOptions, IntegrityRequest, MtimePrecision, compare,
};
use data_mover::model::StoragePath;
use endpoint_support::Result;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Mode {
    Quick,
    Full,
}

impl From<Mode> for IntegrityMode {
    fn from(value: Mode) -> Self {
        match value {
            Mode::Quick => Self::Metadata,
            Mode::Full => Self::Content,
        }
    }
}

#[derive(Debug, Parser)]
#[command(about = "Compare one path across two data-mover storage URLs")]
struct Args {
    /// Source storage URL.
    #[arg(long)]
    source: String,

    /// Destination storage URL.
    #[arg(long)]
    destination: String,

    /// Path relative to both storage roots.
    #[arg(long)]
    path: PathBuf,

    /// Quick compares type, size, and metadata; Full also compares content.
    #[arg(long, value_enum, default_value_t = Mode::Full)]
    mode: Mode,

    /// Accept modification-time differences up to this many milliseconds.
    #[arg(long, default_value_t = 0)]
    mtime_tolerance_ms: u64,

    /// Compare mtimes at the coarser apparent timestamp precision.
    #[arg(long)]
    mtime_auto_precision: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let source = endpoint_support::connect(&args.source).await?;
    let destination = endpoint_support::connect(&args.destination).await?;
    let path = StoragePath::new(args.path.to_str().ok_or("path must be UTF-8")?)?;
    let report = compare(IntegrityRequest {
        source,
        source_path: path.clone(),
        destination,
        destination_path: path,
        options: IntegrityOptions {
            mode: args.mode.into(),
            mtime_precision: if args.mtime_auto_precision {
                MtimePrecision::Coarsest
            } else {
                MtimePrecision::Exact
            },
            mtime_tolerance: Duration::from_millis(args.mtime_tolerance_ms),
            permissions: true,
            ..IntegrityOptions::default()
        },
        cancel: tokio_util::sync::CancellationToken::new(),
    })
    .await?;
    if !report.matches() {
        return Err(format!("integrity differences: {:?}", report.differences()).into());
    }
    println!(
        "integrity {:?} verified {} bytes: {}",
        args.mode,
        report.compared_bytes(),
        args.path.display()
    );
    Ok(())
}
