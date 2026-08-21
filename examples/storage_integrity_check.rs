use std::path::PathBuf;

use clap::{Parser, ValueEnum};
use std::time::Duration;

use data_mover::{
    IntegrityCheck, IntegrityCheckMode, IntegrityCheckOptions, MtimePrecision, Result,
    create_storage,
};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Mode {
    Quick,
    Full,
}

impl From<Mode> for IntegrityCheckMode {
    fn from(value: Mode) -> Self {
        match value {
            Mode::Quick => Self::Quick,
            Mode::Full => Self::Full,
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
    let source = create_storage(&args.source, data_mover::CreateStorageOptions::default()).await?;
    let destination = create_storage(
        &args.destination,
        data_mover::CreateStorageOptions::default(),
    )
    .await?;
    let precision = if args.mtime_auto_precision {
        MtimePrecision::Auto
    } else {
        MtimePrecision::Exact
    };
    let options = IntegrityCheckOptions::new(args.mode.into())
        .with_mtime_precision(precision)
        .with_mtime_tolerance(Duration::from_millis(args.mtime_tolerance_ms));
    let entry =
        IntegrityCheck::check_path_with_options(&source, &destination, &args.path, options, None)
            .await?;

    println!(
        "integrity {:?} verified {} bytes: {}",
        args.mode,
        entry.get_size(),
        args.path.display()
    );
    Ok(())
}
