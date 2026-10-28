use std::path::PathBuf;

use clap::{Parser, ValueEnum};
use std::time::Duration;

#[allow(dead_code)]
#[path = "support/endpoint.rs"]
mod endpoint_support;
use data_mover::integrity::{
    IntegrityMode, IntegrityOptions, IntegrityPlan, IntegrityRequest, MtimePrecision, compare,
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
#[allow(clippy::struct_excessive_bools)] // Independent CLI switches.
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

    /// Accept modification-time differences up to this many nanoseconds.
    #[arg(long, default_value_t = 0)]
    mtime_tolerance_ns: u64,

    /// Compare mtimes at the coarser apparent timestamp precision.
    #[arg(long)]
    mtime_auto_precision: bool,

    /// Also compare native ACLs when both backends can report the same encoding.
    #[arg(long)]
    acl: bool,
    /// Require complete native ACL observations.
    #[arg(long)]
    acl_required: bool,
    /// Assert a shared numeric identity/group namespace and compare regular-file access.
    #[arg(long)]
    acl_equivalent: bool,

    /// Also compare extended attributes when both backends can report them.
    #[arg(long)]
    xattrs: bool,
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
        plan: IntegrityPlan {
            acl: args.acl || args.acl_required || args.acl_equivalent,
            xattrs: args.xattrs,
            acl_required: args.acl_required || args.acl_equivalent,
            acl_equivalent: args.acl_equivalent,
        },
        options: IntegrityOptions {
            mode: args.mode.into(),
            mtime_precision: if args.mtime_auto_precision {
                MtimePrecision::Coarsest
            } else {
                MtimePrecision::Exact
            },
            mtime_tolerance: Duration::from_nanos(args.mtime_tolerance_ns),
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
