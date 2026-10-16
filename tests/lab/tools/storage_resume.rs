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
    if prior.schema != 2
        || !prior.recovery_at_destination
        || prior.size != current.size
        || prior.identity != current.identity
        || prior.source_observation != current.source_observation
    {
        return Err("source or transfer identity changed since interruption".into());
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
        Phase::Interrupt => interrupt(&args, &source, &destination, &path, &mut state).await?,
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
        return Err("resume did not reuse a partial transfer from the destination".into());
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

async fn interrupt(
    args: &Args,
    source: &Storage,
    destination: &Storage,
    path: &StoragePath,
    state: &mut ResumeState,
) -> Result {
    // Automatic checkpoints start after 64 MiB. Leave room for a durable checkpoint and
    // bounded in-flight reads before cancellation; never call streamed bytes durable bytes.
    if state.size < 96 << 20 {
        return Err("resume fixture must be at least 96 MiB for automatic checkpoints".into());
    }
    // Throughput varies substantially between real backends. A cancellation before the first
    // durable checkpoint is valid, but it cannot produce the recovery state this tool is meant
    // to hand to the next process. Retry those attempts with a longer window instead of turning
    // a below-threshold interruption into a lab failure.
    for attempt in 0_u64..6 {
        let cancel = CancellationToken::new();
        let request = TransferRequest::new(
            source.clone(),
            path.clone(),
            destination.clone(),
            cancel.clone(),
        )
        .with_payload_shaping(PayloadShapingPolicy::RequireClientShaped);
        let stop_after_bytes = (state.size / 2)
            .saturating_add(32 << 20)
            .saturating_add(attempt * (32 << 20));
        let delay = Duration::from_millis(stop_after_bytes.saturating_mul(1000) / BANDWIDTH);
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

#[cfg(test)]
mod tests {
    use super::*;

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
