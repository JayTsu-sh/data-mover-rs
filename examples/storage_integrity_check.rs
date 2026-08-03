use std::path::PathBuf;

use clap::{Parser, ValueEnum};
use data_mover::{IntegrityCheck, IntegrityCheckMode, Result, create_storage};

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
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let source = create_storage(&args.source, None, false).await?;
    let destination = create_storage(&args.destination, None, false).await?;
    let entry =
        IntegrityCheck::check_path(&source, &destination, &args.path, args.mode.into(), None)
            .await?;

    println!(
        "integrity {:?} verified {} bytes: {}",
        args.mode,
        entry.get_size(),
        args.path.display()
    );
    Ok(())
}
