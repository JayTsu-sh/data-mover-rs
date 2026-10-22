use std::time::{Duration, Instant};

use clap::Parser;
use data_mover::model::StoragePath;
use data_mover::qos::parse_bandwidth_string;
use data_mover::transfer::{
    SourceQosGroup, SourceQosPolicy, SourceQosStats, TransferRequest, move_file, transfer,
};
use num_traits::ToPrimitive as _;
use tokio_util::sync::CancellationToken;

#[allow(dead_code)]
#[path = "support/copy.rs"]
mod copy_support;
#[allow(dead_code)]
#[path = "support/endpoint.rs"]
mod endpoint_support;
use endpoint_support::{Result, connect_with_options};

#[derive(Debug, Parser)]
#[command(about = "Copy one file between two data-mover storage URLs")]
struct Args {
    /// Source storage URL.
    #[arg(long)]
    source: String,

    /// Destination storage URL.
    #[arg(long)]
    destination: String,

    /// Path relative to both storage roots.
    #[arg(long)]
    path: String,

    /// Emit flushed phase markers for the external lab memory sampler.
    #[arg(long)]
    report_phases: bool,

    /// Requested transfer chunk size. Backends may negotiate a lower safe value.
    #[arg(long, default_value_t = 2 * 1024 * 1024)]
    chunk_bytes: u64,

    /// Delete the source only after destination publish, metadata and integrity succeed.
    #[arg(long)]
    delete_source: bool,

    /// Source-side sustained bandwidth limit, for example `8MiB/s`.
    #[arg(long)]
    qos_bandwidth: Option<String>,

    /// Hard source-side peak bandwidth; requires `--qos-bandwidth`.
    #[arg(long, requires = "qos_bandwidth")]
    qos_hard_bandwidth: Option<String>,

    /// Duration that a full soft-credit bucket may sustain the hard peak.
    #[arg(long, requires = "qos_hard_bandwidth")]
    qos_peak_duration_ms: Option<u64>,

    /// Hard source-read burst/request cap in bytes; requires `--qos-bandwidth`.
    #[arg(long, requires = "qos_bandwidth")]
    qos_burst_bytes: Option<u64>,

    /// Hard source-side protocol operation limit in operations per second.
    #[arg(long)]
    qos_iops: Option<u32>,

    /// Hard source-side peak IOPS; requires the sustained `--qos-iops` rate.
    #[arg(long, requires = "qos_iops")]
    qos_hard_iops: Option<u32>,

    /// Duration that a full soft-IOPS credit bucket may sustain the hard peak.
    #[arg(long, requires = "qos_hard_iops")]
    qos_iops_peak_duration_ms: Option<u64>,
}

fn qos_from_args(args: &Args) -> Result<Option<SourceQosGroup>> {
    if args.qos_bandwidth.is_none() && args.qos_iops.is_none() {
        return Ok(None);
    }
    let bandwidth = args
        .qos_bandwidth
        .as_deref()
        .map(|soft| {
            let soft_rate = parse_bandwidth_string(soft)?;
            let hard_rate =
                parse_bandwidth_string(args.qos_hard_bandwidth.as_deref().unwrap_or(soft))?;
            let peak = if args.qos_hard_bandwidth.is_some() {
                Duration::from_millis(args.qos_peak_duration_ms.unwrap_or(500))
            } else {
                Duration::ZERO
            };
            Ok::<_, data_mover::error::StorageError>((soft_rate, hard_rate, peak))
        })
        .transpose()?;
    let iops = args.qos_iops.map(|soft| {
        (
            soft,
            args.qos_hard_iops.unwrap_or(soft),
            if args.qos_hard_iops.is_some() {
                Duration::from_millis(args.qos_iops_peak_duration_ms.unwrap_or(500))
            } else {
                Duration::ZERO
            },
        )
    });
    Ok(Some(SourceQosGroup::new(SourceQosPolicy::new(
        bandwidth,
        args.qos_burst_bytes.unwrap_or(args.chunk_bytes),
        iops,
    )?)))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let source = connect_with_options(&args.source, false, Some(args.chunk_bytes)).await?;
    let destination = connect_with_options(&args.destination, true, Some(args.chunk_bytes)).await?;
    let path = StoragePath::new(&args.path)?;
    copy_support::create_parent(&destination, &path).await?;
    let qos = qos_from_args(&args)?;
    let report_qos = qos.is_some();
    let mut request = TransferRequest::new(
        source.clone(),
        path.clone(),
        destination.clone(),
        CancellationToken::new(),
    );
    if let Some(qos) = qos {
        request = request.with_source_qos(qos);
    }
    let started = Instant::now();
    report_phase(args.report_phases, "transfer")?;
    let outcome = if args.delete_source {
        move_file(request).await?
    } else {
        transfer(request).await?
    };
    let elapsed = started.elapsed().as_secs_f64();
    report_phase(args.report_phases, "verification")?;
    let bytes = if args.delete_source {
        // move_file read back the copy and applied metadata before guarded source removal.
        outcome.transferred_bytes
    } else {
        copy_support::verify(&source, &destination, &path).await?
    };
    println!(
        "copied and verified {bytes} bytes and applicable metadata: {}",
        args.path
    );
    report_phase(args.report_phases, "complete")?;
    if report_qos {
        report_stats(outcome.source_qos, elapsed);
    }
    Ok(())
}

fn report_phase(enabled: bool, phase: &str) -> Result<()> {
    if enabled {
        use std::io::Write as _;
        println!("memory_phase\t{phase}");
        std::io::stdout().flush()?;
    }
    Ok(())
}

fn report_stats(stats: SourceQosStats, elapsed: f64) {
    let bytes = stats.client_streamed_shaped_bytes;
    let operations = stats.source_read_operations;
    println!(
        "qos_stats\ttotal_bytes={bytes}\ttotal_iops={operations}\telapsed_seconds={elapsed:.6}\tactual_mibps={:.6}\tactual_iops={:.6}",
        bytes.to_f64().unwrap_or(0.0) / elapsed / 1_048_576.0,
        operations.to_f64().unwrap_or(0.0) / elapsed
    );
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::Args;
    use clap::Parser;

    #[test]
    fn memory_phase_reporting_is_opt_in() {
        let plain = [
            "storage_copy",
            "--source",
            "/source",
            "--destination",
            "/dest",
            "--path",
            "file",
        ];
        assert!(
            !Args::try_parse_from(plain)
                .expect("ordinary copy args")
                .report_phases
        );
        assert!(
            Args::try_parse_from(plain.into_iter().chain(["--report-phases"]))
                .expect("memory sampler args")
                .report_phases
        );
    }

    #[test]
    fn qos_cli_accepts_explicit_bandwidth_burst_and_iops() {
        let args = Args::try_parse_from([
            "storage_copy",
            "--source",
            "/source",
            "--destination",
            "/destination",
            "--path",
            "payload.bin",
            "--qos-bandwidth",
            "8MiB/s",
            "--qos-burst-bytes",
            "65536",
            "--qos-iops",
            "32",
        ])
        .expect("QoS arguments should parse");

        assert_eq!(args.qos_bandwidth.as_deref(), Some("8MiB/s"));
        assert_eq!(args.qos_burst_bytes, Some(65_536));
        assert_eq!(args.qos_iops, Some(32));
    }

    #[test]
    fn qos_cli_accepts_soft_hard_limits_and_peak_duration() {
        let args = Args::try_parse_from([
            "storage_copy",
            "--source",
            "/source",
            "--destination",
            "/destination",
            "--path",
            "payload.bin",
            "--qos-bandwidth",
            "100MiB/s",
            "--qos-hard-bandwidth",
            "150MiB/s",
            "--qos-peak-duration-ms",
            "500",
            "--qos-burst-bytes",
            "2097152",
            "--qos-iops",
            "100",
            "--qos-hard-iops",
            "150",
            "--qos-iops-peak-duration-ms",
            "500",
        ])
        .expect("dual-rate QoS arguments should parse");

        assert_eq!(args.qos_hard_bandwidth.as_deref(), Some("150MiB/s"));
        assert_eq!(args.qos_peak_duration_ms, Some(500));
        assert_eq!(args.qos_hard_iops, Some(150));
        assert_eq!(args.qos_iops_peak_duration_ms, Some(500));
    }

    #[test]
    fn qos_burst_requires_bandwidth() {
        let error = Args::try_parse_from([
            "storage_copy",
            "--source",
            "/source",
            "--destination",
            "/destination",
            "--path",
            "payload.bin",
            "--qos-burst-bytes",
            "65536",
        ])
        .expect_err("burst without bandwidth must be rejected");

        assert!(error.to_string().contains("--qos-bandwidth"));
    }
}
