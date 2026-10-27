//! Real S3 -> filesystem creation and recovery permissions. The external lab driver supplies
//! isolated endpoints and acknowledges each pause after inspecting the server with its CLI.
use std::env::var;
use std::error::Error;
use std::fs;
#[cfg(unix)]
use std::fs::Permissions;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use data_mover::model::StoragePath;
use data_mover::storage::{
    DestinationPrepareRequest, FinalDestination, PrepareRequest, ResumeMode, Storage,
    create_directory_all,
};
use data_mover::transfer::{
    SourceQosGroup, SourceQosPolicy, TransferFailure, TransferPolicy, TransferRequest, transfer,
};
use tokio::fs as async_fs;
use tokio::spawn;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

#[allow(dead_code)]
#[path = "../examples/support/copy.rs"]
mod copy;
#[allow(dead_code)]
#[path = "../examples/support/endpoint.rs"]
mod endpoint;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

async fn assert_mode(storage: &Storage, relative: &str, expected: u32) -> TestResult {
    let path = StoragePath::new(relative)?;
    let entry = storage.namespace()?.stat(&path).await?;
    let actual = storage.metadata()?.permissions(&entry).await?.mode;
    assert_eq!(
        actual.map(|mode| mode & 0o7777),
        Some(expected),
        "{relative}"
    );
    Ok(())
}

async fn pause(control: &Path, phase: &str) -> TestResult {
    async_fs::write(control.join(format!("{phase}.ready")), b"ready\n").await?;
    for _ in 0..1200 {
        if async_fs::try_exists(control.join(format!("{phase}.continue"))).await? {
            return Ok(());
        }
        sleep(Duration::from_millis(100)).await;
    }
    Err(format!("external server inspection did not acknowledge {phase}").into())
}

async fn run() -> TestResult {
    let control = PathBuf::from(var("CREATION_MODE_CONTROL")?);
    let source =
        endpoint::connect_with_options(&var("CREATION_MODE_SOURCE")?, false, Some(2 << 20)).await?;
    let destination_url = var("CREATION_MODE_DESTINATION")?;
    let destination = endpoint::connect_with_options(&destination_url, true, Some(2 << 20)).await?;
    let sample = source
        .read_source()?
        .describe(&StoragePath::new("copy-checkpointed/nested/small.bin")?)
        .await?;
    assert!(
        source
            .metadata()?
            .permissions(&sample)
            .await?
            .mode
            .is_none()
    );

    // Observe actual CREATE permissions before any bytes or source metadata are applied.
    create_directory_all(&destination, &StoragePath::new("prepared/sub")?).await?;
    let staged = destination.staged_destination()?;
    let stage = staged
        .prepare_at_destination(
            DestinationPrepareRequest::new(
                PrepareRequest {
                    final_destination: FinalDestination::new(StoragePath::new(
                        "prepared/sub/file.bin",
                    )?),
                    source: sample,
                    recovery_binding: [0x71; 32],
                },
                [0x72; 32],
            )
            .with_resume(ResumeMode::Restart)
            .with_recoverable(true),
        )
        .await?;
    pause(&control, "created").await?;
    staged.discard(stage).await?;

    verify_copies(&source, &destination).await?;
    verify_source_modes(&destination).await?;
    verify_recovery(&source, &destination, &destination_url, &control).await
}

async fn verify_copies(source: &Storage, destination: &Storage) -> TestResult {
    let staged = destination.staged_destination()?;
    let mut policies = vec![
        ("checkpointed", TransferPolicy::Checkpointed),
        ("atomic", TransferPolicy::AtomicReplace),
    ];
    if staged.supports_direct() {
        policies.push(("direct", TransferPolicy::Direct));
    }
    for (name, policy) in policies {
        for file in ["empty.bin", "small.bin"] {
            let relative = format!("copy-{name}/nested/{file}");
            let path = StoragePath::new(&relative)?;
            create_directory_all(
                destination,
                &StoragePath::new(format!("copy-{name}/nested"))?,
            )
            .await?;
            let outcome = transfer(
                TransferRequest::new(
                    source.clone(),
                    path.clone(),
                    destination.clone(),
                    CancellationToken::new(),
                )
                .with_transfer_policy(policy),
            )
            .await?;
            assert_mode(destination, &relative, 0o644).await?;
            assert_mode(destination, &format!("copy-{name}/nested"), 0o755).await?;
            copy::verify(source, destination, &path).await?;
            println!(
                "PASS {name} {file} mode=0644 bytes={}",
                outcome.transferred_bytes
            );
        }
    }

    Ok(())
}

async fn verify_source_modes(destination: &Storage) -> TestResult {
    // Numeric owner/group may be absent on a backend while a real source mode still exists.
    let local_root = tempfile::tempdir()?;
    let local = endpoint::connect(local_root.path().to_str().ok_or("non-UTF8 fixture")?).await?;
    fs::create_dir(local_root.path().join("preserve"))?;
    for mode in [0o600, 0o755] {
        let relative = format!("preserve/source-{mode:o}.bin");
        let path = local_root.path().join(&relative);
        fs::write(&path, b"preserved source permissions")?;
        #[cfg(unix)]
        {
            fs::set_permissions(path, Permissions::from_mode(mode))?;
        }
        create_directory_all(destination, &StoragePath::new("preserve")?).await?;
        let storage_path = StoragePath::new(&relative)?;
        transfer(TransferRequest::new(
            local.clone(),
            storage_path.clone(),
            destination.clone(),
            CancellationToken::new(),
        ))
        .await?;
        assert_mode(destination, &relative, mode).await?;
        copy::verify(&local, destination, &storage_path).await?;
        println!("PASS source mode={mode:o} preserved");
    }

    Ok(())
}

async fn verify_recovery(
    source: &Storage,
    destination: &Storage,
    destination_url: &str,
    control: &Path,
) -> TestResult {
    let large = StoragePath::new("checkpointed/large.bin")?;
    create_directory_all(destination, &StoragePath::new("checkpointed")?).await?;
    let failure = interrupt_transfer(source, destination, &large).await?;
    println!("PASS interrupted transfer retained recovery evidence");
    pause(control, "interrupted").await?;
    drop(failure);
    let reconnected = endpoint::connect_with_options(destination_url, true, Some(2 << 20)).await?;
    let outcome = transfer(TransferRequest::new(
        source.clone(),
        large.clone(),
        reconnected.clone(),
        CancellationToken::new(),
    ))
    .await?;
    assert!(
        outcome.reused_bytes > 0,
        "checkpointed data was not reused: {:?}",
        outcome.prepare
    );
    assert_mode(&reconnected, "checkpointed/large.bin", 0o644).await?;
    copy::verify(source, &reconnected, &large).await?;
    println!(
        "PASS recovered transfer mode=0644 bytes={} reused={}",
        outcome.transferred_bytes, outcome.reused_bytes
    );
    pause(control, "completed").await?;
    Ok(())
}

async fn interrupt_transfer(
    source: &Storage,
    destination: &Storage,
    large: &StoragePath,
) -> TestResult<TransferFailure> {
    // A real multi-chunk transfer leaves v2 checkpoint evidence on interruption.
    let cancel = CancellationToken::new();
    let timer_cancel = cancel.clone();
    let timer = spawn(async move {
        // Production stages establish their first automatic checkpoint after 64 MiB.
        // At 8 MiB/s, ten seconds leaves the 96 MiB fixture partly transferred past it.
        sleep(Duration::from_secs(10)).await;
        timer_cancel.cancel();
    });
    let qos = SourceQosGroup::new(SourceQosPolicy::new(
        Some((8 << 20, 8 << 20, Duration::ZERO)),
        2 << 20,
        None,
    )?);
    let result = transfer(
        TransferRequest::new(
            source.clone(),
            large.clone(),
            destination.clone(),
            cancel.clone(),
        )
        .with_source_qos(qos),
    )
    .await;
    timer.abort();
    let _ = timer.await;
    let failure = result
        .err()
        .ok_or("large transfer completed before interruption")?;
    assert!(cancel.is_cancelled(), "{failure}");
    assert!(failure.has_recoverable_stage(), "{failure}");
    Ok(failure)
}

#[tokio::test]
#[ignore = "requires isolated real S3 and filesystem endpoints plus external CLI inspection"]
async fn creation_permissions_on_real_backend() -> TestResult {
    if let Err(error) = timeout(Duration::from_mins(5), Box::pin(run())).await? {
        eprintln!("{error}");
        return Err(error);
    }
    Ok(())
}
