//! Helpers shared by the integration tests.
//!
//! Each test binary compiles this module separately and uses only part of it, so the unused
//! half is dead code in that binary by construction.
#![allow(dead_code)]

use std::num::NonZeroUsize;
use std::path::Path;

use data_mover::model::{EntryKind, ObservedEntry, StoragePath};
use data_mover::storage::{
    BackendConfig, BackendConnectError, LocalBackendConfig, Storage, connect_backend,
};

/// Connects a LOCAL endpoint through the role factory with symmetric read/write concurrency.
pub(crate) async fn connect_local(
    root: &Path,
    concurrency: usize,
) -> Result<Storage, BackendConnectError> {
    let concurrency = NonZeroUsize::new(concurrency).unwrap_or(NonZeroUsize::MIN);
    connect_backend(BackendConfig::Local(LocalBackendConfig {
        read_chunk_bytes: None,
        write_chunk_bytes: None,
        root: root.to_path_buf(),
        read_concurrency: concurrency,
        write_concurrency: concurrency,
    }))
    .await
}

/// Describes one file and builds the neutral observation required by expert sessions.
pub(crate) async fn observe_file(
    storage: &Storage,
    path: &str,
) -> Result<ObservedEntry, Box<dyn std::error::Error + Send + Sync>> {
    let path = StoragePath::new(path)?;
    let source = storage.read_source()?;
    let descriptor = source.describe(&path).await?;
    if descriptor.kind != EntryKind::File {
        return Err(format!("{} is not a file", descriptor.path.as_str()).into());
    }
    let modified = descriptor
        .inline_timestamps()
        .and_then(|timestamps| timestamps.modified);
    Ok(ObservedEntry::new(
        descriptor.path,
        descriptor.kind,
        descriptor.size,
        modified,
        descriptor.source_identity,
    )?)
}

/// Turns on protocol tracing for a real-server test when `RUST_LOG` asks for it.
///
/// These tests talk to a live server, so the only record of what the wire actually carried is
/// whatever the client logged. Without a subscriber installed there is none, and diagnosing a
/// failure means editing the test, rebuilding and hoping the failure repeats — twice over, for
/// an intermittent one. Installing it here costs nothing when `RUST_LOG` is unset: the filter
/// is then empty and every event is discarded before it is formatted.
///
/// ```text
/// RUST_LOG=smb=debug cargo test --release --test cifs_namespace_contract -- --ignored --nocapture
/// RUST_LOG=smb=trace ...   # adds the raw frames, which is what identifies an NTSTATUS
/// ```
///
/// Safe to call from every test in a binary: a second install is ignored.
pub(crate) fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_thread_ids(true)
        .with_ansi(false)
        .with_test_writer()
        .try_init();
}

pub(crate) trait AssertTestValue {
    type Value;

    fn assert_value(self, context: &str) -> Self::Value;
}

impl<T, E: std::fmt::Debug> AssertTestValue for Result<T, E> {
    type Value = T;

    fn assert_value(self, context: &str) -> T {
        match self {
            Ok(value) => value,
            Err(error) => panic!("{context}: {error:?}"),
        }
    }
}

impl<T> AssertTestValue for Option<T> {
    type Value = T;

    fn assert_value(self, context: &str) -> T {
        match self {
            Some(value) => value,
            None => panic!("{context}"),
        }
    }
}
