//! One-file streaming measurements: connections and optional read-back are outside elapsed time.
use std::time::Instant;

use clap::{Parser, ValueEnum};
use data_mover::model::StoragePath;
use data_mover::transfer::{ReadBackVerification, TransferPolicy, TransferRequest, transfer};
use tokio_util::sync::CancellationToken;

#[allow(dead_code)]
#[path = "../../../examples/support/copy.rs"]
mod copy_support;
#[allow(dead_code)]
#[path = "../../../examples/support/endpoint.rs"]
mod endpoint_support;
use endpoint_support::{Result, connect_with_options};

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Policy {
    AtomicReplace,
    Checkpointed,
}

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    source: String,
    #[arg(long)]
    destination: String,
    #[arg(long)]
    path: String,
    /// None preserves the backend's own chunk default.
    #[arg(long)]
    chunk_bytes: Option<u64>,
    #[arg(long, value_enum, default_value_t = Policy::AtomicReplace)]
    policy: Policy,
    /// Only enable for smoke checks: whole-process CPU/RSS then also includes verification.
    #[arg(long)]
    verify: bool,
}

#[tokio::main]
async fn main() -> Result {
    let args = Args::parse();
    if args.chunk_bytes == Some(0) {
        return Err("chunk size must be non-zero".into());
    }
    let source = connect_with_options(&args.source, false, args.chunk_bytes).await?;
    let destination = connect_with_options(&args.destination, true, args.chunk_bytes).await?;
    let path = StoragePath::new(&args.path)?;
    copy_support::create_parent(&destination, &path).await?;
    let reader = source.read_source()?;
    let request = TransferRequest::new(
        source.clone(),
        path.clone(),
        destination.clone(),
        CancellationToken::new(),
    )
    .with_read_back_verification(ReadBackVerification::Disabled)
    .with_transfer_policy(match args.policy {
        Policy::AtomicReplace => TransferPolicy::AtomicReplace,
        Policy::Checkpointed => TransferPolicy::Checkpointed,
    });
    let started = Instant::now();
    let outcome = transfer(request).await?;
    let elapsed_ns = started.elapsed().as_nanos();
    if args.verify {
        copy_support::verify(&source, &destination, &path).await?;
    }
    println!(
        "bytes={} elapsed_ns={elapsed_ns} source_read={} source_chunk={} verified={}",
        outcome.transferred_bytes,
        reader.read_concurrency(),
        reader.maximum_read_chunk_bytes(),
        args.verify,
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_preserve_backend_sizing_and_measure_streaming_without_readback() -> Result {
        let args = Args::try_parse_from([
            "benchmark",
            "--source",
            "s3:source",
            "--destination",
            "s3:target",
            "--path",
            "fixture.bin",
        ])?;
        assert_eq!(args.chunk_bytes, None);
        assert!(matches!(args.policy, Policy::AtomicReplace));
        assert!(!args.verify);
        Ok(())
    }

    #[test]
    fn checkpointed_smoke_checks_are_explicit() -> Result {
        let args = Args::try_parse_from([
            "benchmark",
            "--source",
            "source",
            "--destination",
            "target",
            "--path",
            "fixture.bin",
            "--policy",
            "checkpointed",
            "--verify",
            "--chunk-bytes",
            "1048576",
        ])?;
        assert_eq!(args.chunk_bytes, Some(1024 * 1024));
        assert!(matches!(args.policy, Policy::Checkpointed));
        assert!(args.verify);
        Ok(())
    }
}
