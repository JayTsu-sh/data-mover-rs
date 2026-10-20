//! Fresh-process recovery through the ordinary transfer engine. The state file is an
//! observation guard, never an exported destination handle or an estimated durable prefix.
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::{Parser, ValueEnum};
use data_mover::model::StoragePath;
use data_mover::storage::Storage;
use data_mover::transfer::{
    PayloadShapingPolicy, SourceQosGroup, SourceQosPolicy, TransferRequest, move_file, transfer,
};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

#[allow(dead_code)]
#[path = "../../../examples/support/copy.rs"]
mod copy_support;
#[allow(dead_code)]
#[path = "../../../examples/support/endpoint.rs"]
mod endpoint_support;
use endpoint_support::{Result, connect_with_options};

const CHUNK: u64 = 2 << 20;
const BANDWIDTH: u64 = 8 << 20;
#[derive(Debug, Clone, Copy, ValueEnum)]
enum Phase {
    Interrupt,
    Resume,
}

#[derive(Debug, Parser)]
#[command(about = "Exercise a resumable copy in two independent processes")]
struct Args {
    #[arg(long)]
    source: String,
    #[arg(long)]
    destination: String,
    #[arg(long)]
    path: PathBuf,
    #[arg(long)]
    phase: Phase,
    /// Write transfer identity and interruption evidence; recovery state stays at the destination.
    #[arg(long)]
    state_file: Option<PathBuf>,
    /// Prior identity/observation guard, used to reject a changed source before resuming.
    #[arg(long)]
    prior_state_file: Option<PathBuf>,
    /// After publishing interrupt state, remain alive so the lab can SIGKILL us.
    #[arg(long)]
    hold_after_interrupt: bool,
    /// Delete the source only after resume commit and full hash verification.
    #[arg(long)]
    delete_source: bool,
}

#[derive(Debug, Deserialize, Serialize)]
struct ResumeState {
    schema: u32,
    size: u64,
    identity: String,
    source_observation: String,
    recovery_at_destination: bool,
    source_streamed_bytes: u64,
    reused_bytes: Option<u64>,
}

fn write_state(path: Option<&Path>, state: &ResumeState) -> Result {
    if let Some(path) = path {
        fs::write(path, serde_json::to_vec(state)?)?;
    }
    Ok(())
}

fn validate_prior_state(path: Option<&Path>, current: &ResumeState) -> Result {
    let Some(path) = path else {
        return Ok(());
    };
    let prior: ResumeState = serde_json::from_slice(&fs::read(path)?)?;
    if prior.schema != 2 || !prior.recovery_at_destination {
        return Err("source or transfer identity changed since interruption".into());
    }
    validate_observation(
        &prior,
        Some(current.size),
        &current.identity,
        &current.source_observation,
    )
}

fn validate_observation(
    expected: &ResumeState,
    size: Option<u64>,
    identity: &str,
    observation: &str,
) -> Result {
    if size != Some(expected.size)
        || identity != expected.identity
        || observation != expected.source_observation
    {
        return Err("source or transfer identity changed between interruption attempts".into());
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result {
    let args = Args::parse();
    let source = connect_with_options(&args.source, false, Some(CHUNK)).await?;
    let destination = connect_with_options(&args.destination, true, Some(CHUNK)).await?;
    let path = StoragePath::new(args.path.to_str().ok_or("path must be UTF-8")?)?;
    let observation = copy_support::observe(&source, &path).await?;
    let size = observation.size().ok_or("source size is unknown")?;
    copy_support::create_parent(&destination, &path).await?;
    let cancel = CancellationToken::new();
    let request = TransferRequest::new(
        source.clone(),
        path.clone(),
        destination.clone(),
        cancel.clone(),
    )
    .with_payload_shaping(PayloadShapingPolicy::RequireClientShaped);
    let mut state = ResumeState {
        schema: 2,
        size,
        identity: request.identity().to_string(),
        source_observation: blake3::hash(observation.encode_snapshot().as_bytes())
            .to_hex()
            .to_string(),
        recovery_at_destination: false,
        source_streamed_bytes: 0,
        reused_bytes: None,
    };
    match args.phase {
        Phase::Interrupt => interrupt(&args, &path, &mut state).await?,
        Phase::Resume => resume(&args, &source, &destination, &path, request, &mut state).await?,
    }
    Ok(())
}

async fn resume(
    args: &Args,
    source: &Storage,
    destination: &Storage,
    path: &StoragePath,
    request: TransferRequest,
    state: &mut ResumeState,
) -> Result {
    validate_prior_state(args.prior_state_file.as_deref(), state)?;
    let outcome = if args.delete_source {
        move_file(request).await?
    } else {
        transfer(request).await?
    };
    if outcome.reused_bytes == 0 || outcome.reused_bytes >= state.size {
        return Err(format!(
            "resume did not reuse a partial transfer from the destination: reused={} size={} prepare={:?}",
            outcome.reused_bytes, state.size, outcome.prepare,
        ).into());
    }
    let verified = if args.delete_source {
        // The move verified content and metadata before deleting its bound source.
        outcome.transferred_bytes
    } else {
        copy_support::verify(source, destination, path).await?
    };
    if verified != state.size {
        return Err("resumed verification size mismatch".into());
    }
    state.reused_bytes = Some(outcome.reused_bytes);
    state.source_streamed_bytes = outcome.source_qos.client_streamed_shaped_bytes;
    write_state(args.state_file.as_deref(), state)?;
    println!(
        "resumed {} bytes, reused {} durable bytes and verified {} bytes",
        state.size - outcome.reused_bytes,
        outcome.reused_bytes,
        state.size
    );
    Ok(())
}

async fn interrupt(args: &Args, path: &StoragePath, state: &mut ResumeState) -> Result {
    for (attempt, delay) in interruption_delays(state.size)?.into_iter().enumerate() {
        // This is a new job attempt, not a protocol retry. A cancelled NFS read can leave
        // in-flight RPCs on the old session; do not carry that transport into the next try.
        let source = connect_with_options(&args.source, false, Some(CHUNK)).await?;
        let destination = connect_with_options(&args.destination, true, Some(CHUNK)).await?;
        let observation = copy_support::observe(&source, path).await?;
        let cancel = CancellationToken::new();
        let request = TransferRequest::new(
            source.clone(),
            path.clone(),
            destination.clone(),
            cancel.clone(),
        )
        .with_payload_shaping(PayloadShapingPolicy::RequireClientShaped);
        let observation_guard = blake3::hash(observation.encode_snapshot().as_bytes())
            .to_hex()
            .to_string();
        validate_observation(
            state,
            observation.size(),
            &request.identity().to_string(),
            &observation_guard,
        )?;
        let timer_cancel = cancel.clone();
        let timer = tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            timer_cancel.cancel();
        });
        let qos = SourceQosGroup::new(SourceQosPolicy::new(
            Some((BANDWIDTH, BANDWIDTH, Duration::ZERO)),
            CHUNK,
            None,
        )?);
        let result = transfer(request.with_source_qos(qos)).await;
        timer.abort();
        let _ = timer.await;
        let failure = result
            .err()
            .ok_or("transfer completed before interruption")?;
        if !cancel.is_cancelled() || failure.final_destination_changed() {
            return Err(failure.into());
        }
        if !failure.has_recoverable_stage() {
            eprintln!(
                "interruption attempt {} stopped before a durable checkpoint; retrying",
                attempt + 1
            );
            drop(failure); // The next prepare cleans or resumes the below-threshold stage.
            continue;
        }
        state.recovery_at_destination = true;
        state.source_streamed_bytes = failure.source_qos().client_streamed_shaped_bytes;
        drop(failure); // Release the destination lease; the retained recovery state stays there.
        write_state(args.state_file.as_deref(), state)?;
        println!(
            "interrupted after {} streamed bytes; recoverable state retained at destination",
            state.source_streamed_bytes
        );
        if args.hold_after_interrupt {
            std::future::pending::<()>().await;
        }
        return Ok(());
    }
    Err("unable to establish a durable recovery checkpoint after repeated interruptions".into())
}

fn interruption_delays(size: u64) -> Result<[Duration; 6]> {
    if size < 96 << 20 {
        return Err("resume fixture must be at least 96 MiB for automatic checkpoints".into());
    }
    // At 8 MiB/s the 96 MiB fixture leaves a partial-upload window at 10 seconds
    // (80 MiB plus a 2 MiB admission burst). Repeating a slow attempt is valid;
    // extending it to 14 seconds is not: S3 may then retain every part, so the
    // next process correctly reuses the entire file instead of a partial prefix.
    // Keep the bounded window on EVERY retry; source bytes are not durable bytes.
    let stop_after_bytes = (size / 2).saturating_add(32 << 20);
    let delay = Duration::from_millis(stop_after_bytes.saturating_mul(1000) / BANDWIDTH);
    Ok([delay; 6])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retries_preserve_a_partial_upload_window() -> Result {
        for (size, expected_seconds) in [(96 << 20, 10), (128 << 20, 12), (192 << 20, 16)] {
            let delays = interruption_delays(size)?;
            assert_eq!(delays, [Duration::from_secs(expected_seconds); 6]);
            for delay in delays {
                // Include the admitted source burst, not just the sustained byte rate.
                assert!(delay.as_secs() * BANDWIDTH + CHUNK < size);
            }
        }
        assert!(interruption_delays((96 << 20) - 1).is_err());
        Ok(())
    }

    #[test]
    fn prior_state_rejects_changed_identity_size_and_source() -> Result {
        let file = tempfile::NamedTempFile::new()?;
        let mut state = ResumeState {
            schema: 2,
            size: 100,
            identity: "identity".into(),
            source_observation: "snapshot".into(),
            recovery_at_destination: true,
            source_streamed_bytes: 80,
            reused_bytes: None,
        };
        write_state(Some(file.path()), &state)?;
        validate_prior_state(Some(file.path()), &state)?;
        state.size += 1;
        assert!(validate_prior_state(Some(file.path()), &state).is_err());
        state.size -= 1;
        state.source_observation.push('x');
        assert!(validate_prior_state(Some(file.path()), &state).is_err());
        state.source_observation.pop();
        state.identity.push('x');
        assert!(validate_prior_state(Some(file.path()), &state).is_err());
        Ok(())
    }
}
