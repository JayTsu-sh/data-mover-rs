//! HDFS contracts exercised through the storage roles. Raw hdfs-native calls seed and verify fixtures.
use data_mover::DslTraversalFilter;
use data_mover::model::{EntryKind, FailureClass, StoragePath};
use data_mover::storage::{
    BackendConfig, HdfsBackendConfig, Storage, StorageRoleFailure, connect_backend,
};
use data_mover::storage::{HdfsConfig, build_hdfs_client};
use data_mover::traversal::{
    StorageTraversalSource, TraversalItem, TraversalOutcome, TraversalRequest, TraversalSource,
};
use hdfs_native::file::{FileReader, FileWriter};
use hdfs_native::{Client, ClientBuilder, WriteOptions};
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
#[path = "hdfs_native_contract/cleanup.rs"]
mod cleanup;
#[path = "hdfs_native_contract/policy_runtime.rs"]
mod policy_runtime;
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
fn valid_hdfs_lab_run_id(run_id: &str) -> bool {
    let Some((kind, suffix)) = run_id.split_once('-') else {
        return false;
    };
    matches!(kind, "nightly" | "release")
        && !suffix.is_empty()
        && suffix.len() <= 80
        && !suffix.contains("..")
        && suffix
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn validated_hdfs_lab_root(location: &str) -> Result<data_mover::storage::HdfsLocation, String> {
    validated_hdfs_lab_root_with_config(location, &HdfsConfig::default())
}

fn validated_hdfs_lab_root_with_config(
    location: &str,
    config: &HdfsConfig,
) -> Result<data_mover::storage::HdfsLocation, String> {
    let authority_end = location
        .find("://")
        .and_then(|offset| {
            location[offset + 3..]
                .find('/')
                .map(|path| offset + 3 + path)
        })
        .ok_or_else(|| "HDFS lab root must contain an explicit path".to_string())?;
    if location[authority_end..].contains("//") {
        return Err("HDFS lab root must not contain empty path components".to_string());
    }
    let parsed = data_mover::storage::HdfsLocation::parse_configured(location, config)
        .map_err(|error| error.to_string())?;
    let components = parsed
        .root()
        .split('/')
        .filter(|component| !component.is_empty())
        .collect::<Vec<_>>();
    let ["tmp", parent, run_id, "hdfs"] = components.as_slice() else {
        return Err("HDFS lab root must have the fixed confined shape".to_string());
    };
    let expected_parent = if run_id.starts_with("nightly-") {
        "data-mover-nightly"
    } else if run_id.starts_with("release-") {
        "data-mover-release"
    } else {
        return Err("HDFS lab root has an invalid run kind".to_string());
    };
    if *parent != expected_parent || !valid_hdfs_lab_run_id(run_id) {
        return Err("HDFS lab root does not match its validated run id".to_string());
    }
    Ok(parsed)
}

fn hdfs_lab_root() -> Result<data_mover::storage::HdfsLocation, Box<dyn std::error::Error>> {
    let location = std::env::var("LAB_HDFS_RUN_ROOT")?;
    validated_hdfs_lab_root_with_config(&location, &hdfs_lab_config()).map_err(Into::into)
}

fn hdfs_lab_config() -> HdfsConfig {
    let config_dir = std::env::var_os("LAB_HDFS_CONFIG_DIR").map(Into::into);
    let kerberos_credentials = std::env::var_os("LAB_HDFS_KEYTAB").map(|keytab| {
        data_mover::storage::HdfsKerberosCredentials {
            keytab: Some(keytab.into()),
            ..Default::default()
        }
    });
    HdfsConfig {
        config_dir,
        kerberos_credentials,
        ..Default::default()
    }
}

fn hdfs_short_user(user: &str) -> &str {
    user.split_once('/')
        .or_else(|| user.split_once('@'))
        .map_or(user, |(short, _)| short)
}

fn hdfs_lab_location(case: &str) -> Result<String, Box<dyn std::error::Error>> {
    let root = hdfs_lab_root()?;
    hdfs_lab_location_as(case, root.user())
}

fn hdfs_lab_location_as(case: &str, user: &str) -> Result<String, Box<dyn std::error::Error>> {
    if case.is_empty()
        || !case
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err("unsafe HDFS lab case name".into());
    }
    if user.is_empty() {
        return Err("empty HDFS lab user".into());
    }
    let root = hdfs_lab_root()?;
    let endpoint = root
        .endpoint()
        .strip_prefix("hdfs://")
        .ok_or("invalid HDFS endpoint")?;
    let user = percent_encoding::utf8_percent_encode(user, percent_encoding::NON_ALPHANUMERIC);
    Ok(format!("hdfs://{user}@{endpoint}{}/{case}", root.root()))
}

fn hdfs_lab_local_path(case: &str) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    if case.is_empty()
        || !case
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err("unsafe local HDFS lab case name".into());
    }
    let root = hdfs_lab_root()?;
    let run_id = root
        .root()
        .split('/')
        .nth(3)
        .ok_or("validated HDFS run root lost its run id")?;
    Ok(std::env::temp_dir()
        .join("data-mover-lab")
        .join(run_id)
        .join("hdfs")
        .join(case))
}

struct HdfsFixture {
    client: Client,
    location: data_mover::storage::HdfsLocation,
}
impl HdfsFixture {
    fn path(&self, relative: &str) -> String {
        assert!(!relative.starts_with('/') && !relative.split('/').any(|part| part == ".."));
        format!(
            "{}/{}",
            self.location.root().trim_end_matches('/'),
            relative
        )
    }
    async fn cleanup(&self) -> TestResult {
        self.client.delete(self.location.root(), true).await?;
        Ok(())
    }
}
async fn fixture(case: &str) -> TestResult<(HdfsFixture, Storage)> {
    let url = hdfs_lab_location(case)?;
    let storage = connect_backend(BackendConfig::Hdfs(HdfsBackendConfig {
        location: url.clone(),
        client: hdfs_lab_config(),
        block_size: None,
        ensure_dir: true,
    }))
    .await?;
    let (client, location) = build_hdfs_client(&url, &hdfs_lab_config())?;
    Ok((HdfsFixture { client, location }, storage))
}
async fn create_hdfs_file(fixture: &HdfsFixture, name: &str, data: bytes::Bytes) -> TestResult {
    let mut writer = fixture
        .client
        .create(
            &fixture.path(name),
            WriteOptions::default().overwrite(true).create_parent(true),
        )
        .await?;
    Box::pin(writer.write_bytes(data)).await?;
    Box::pin(writer.close()).await?;
    Ok(())
}
async fn create_empty_hdfs_files(fixture: &HdfsFixture, paths: &[&str]) -> TestResult {
    for path in paths {
        create_hdfs_file(fixture, path, bytes::Bytes::new()).await?;
    }
    Ok(())
}
async fn raw_read(fixture: &HdfsFixture, name: &str) -> TestResult<bytes::Bytes> {
    let path = fixture.path(name);
    let size = fixture.client.get_file_info(&path).await?.length;
    Ok(fixture
        .client
        .read(&path)
        .await?
        .read_range(0, size)
        .await?)
}
fn assert_clone_send_sync<T: Clone + Send + Sync>() {}
fn assert_send<T: Send>() {}

fn assert_send_value<T: Send>(_: T) {}

fn assert_upstream_status_shape(status: hdfs_native::client::FileStatus) {
    let hdfs_native::client::FileStatus {
        path,
        length,
        isdir,
        permission,
        owner,
        group,
        modification_time,
        access_time,
        replication,
        blocksize,
    } = status;
    drop((
        path,
        length,
        isdir,
        permission,
        owner,
        group,
        modification_time,
        access_time,
        replication,
        blocksize,
    ));
}

async fn assert_upstream_file_method_contract(
    reader: &FileReader,
    writer: &mut FileWriter,
) -> hdfs_native::Result<()> {
    let _: bytes::Bytes = reader.read_range(0, 1).await?;
    let _: usize = Box::pin(writer.write_bytes(bytes::Bytes::from_static(b"contract"))).await?;
    let _: () = Box::pin(writer.close()).await?;
    Ok(())
}

#[test]
fn async_types_meet_streaming_adapter_ownership_contract() {
    assert_clone_send_sync::<Client>();
    assert_send::<FileReader>();
    assert_send::<FileWriter>();
    assert_send::<data_mover::error::StorageError>();
    assert_clone_send_sync::<tokio_util::sync::CancellationToken>();

    let client = ClientBuilder::new()
        .with_url("hdfs://127.0.0.1:9000")
        .with_user("data-mover-contract")
        .with_config([("hadoop.security.authentication", "simple")])
        .build()
        .unwrap_or_else(|error| panic!("explicit Simple client must build: {error}"));
    assert_send_value(client.list_status_iter("/", false));
    let _ = assert_upstream_status_shape;
    let _ = assert_upstream_file_method_contract;

    let options = WriteOptions::default()
        .block_size(1_048_576)
        .replication(1)
        .permission(0o640)
        .overwrite(true)
        .create_parent(true);
    assert_send_value(client.get_file_info("/contract"));
    assert_send_value(client.list_status("/contract", false));
    assert_send_value(client.read("/contract"));
    assert_send_value(client.create("/contract", options));
    assert_send_value(client.append("/contract"));
    assert_send_value(client.recover_lease("/contract"));
    assert_send_value(client.mkdirs("/contract", 0o750, true));
    assert_send_value(client.rename("/contract", "/renamed", true));
    assert_send_value(client.delete("/contract", true));
    assert_send_value(client.set_times("/contract", 1, 2));
    assert_send_value(client.set_owner("/contract", Some("owner"), Some("group")));
    assert_send_value(client.set_permission("/contract", 0o640));
}

#[tokio::test]
async fn client_can_be_built_from_explicit_inputs_only() -> Result<(), Box<dyn std::error::Error>> {
    let client = ClientBuilder::new()
        .with_url("hdfs://127.0.0.1:9000")
        .with_user("data-mover-contract")
        .with_config_dir("/path/that/does/not/contain/hadoop/config")
        .with_config([("hadoop.security.authentication", "simple")])
        .with_io_runtime(tokio::runtime::Handle::current())
        .build()?;

    assert_clone_send_sync::<Client>();
    drop(client);
    Ok(())
}

#[test]
fn simple_location_contract_is_platform_independent() -> Result<(), Box<dyn std::error::Error>> {
    let parsed = data_mover::storage::HdfsLocation::parse(
        "hdfs://migration%20user@127.0.0.1:9000/warehouse/%E7%9B%AE%E5%BD%95/",
    )?;
    assert_eq!(parsed.user(), "migration user");
    assert_eq!(parsed.endpoint(), "hdfs://127.0.0.1:9000");
    assert_eq!(parsed.root(), "/warehouse/目录");

    let kerberos = HdfsConfig {
        config_dir: None,
        overrides: std::collections::HashMap::from([(
            "hadoop.security.authentication".to_string(),
            "kerberos".to_string(),
        )]),
        ..Default::default()
    };
    assert!(
        data_mover::storage::HdfsLocation::parse_configured(
            "hdfs://migration@127.0.0.1:9000/warehouse",
            &kerberos,
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn hdfs_lab_root_validation_rejects_broad_or_escaped_cleanup_targets() {
    for accepted in [
        "hdfs://hdfs@127.0.0.1:9000/tmp/data-mover-nightly/nightly-42-1/hdfs",
        "hdfs://hdfs@127.0.0.1:9000/tmp/data-mover-release/release-v1.2_3/hdfs",
    ] {
        assert!(
            validated_hdfs_lab_root(accepted).is_ok(),
            "rejected {accepted}"
        );
    }
    for rejected in [
        "hdfs://hdfs@127.0.0.1:9000/",
        "hdfs://hdfs@127.0.0.1:9000/tmp/data-mover-nightly",
        "hdfs://hdfs@127.0.0.1:9000/tmp/data-mover-nightly/nightly-x",
        "hdfs://hdfs@127.0.0.1:9000/tmp//data-mover-nightly/nightly-x/hdfs",
        "hdfs://hdfs@127.0.0.1:9000/tmp/data-mover-nightly/release-x/hdfs",
        "hdfs://hdfs@127.0.0.1:9000/tmp/data-mover-nightly/manual-x/hdfs",
        "hdfs://hdfs@127.0.0.1:9000/tmp/data-mover-nightly/nightly-%2E%2E/hdfs",
        "hdfs://hdfs:secret@127.0.0.1:9000/tmp/data-mover-nightly/nightly-x/hdfs",
    ] {
        let Err(error) = validated_hdfs_lab_root(rejected) else {
            panic!("unsafe HDFS lab root was accepted: {rejected}");
        };
        assert!(!error.contains("secret"));
    }
    assert!(!valid_hdfs_lab_run_id("nightly-"));
    assert!(!valid_hdfs_lab_run_id("manual-1"));
    assert!(!valid_hdfs_lab_run_id("nightly-a/b"));
    assert!(!valid_hdfs_lab_run_id(&format!(
        "nightly-{}",
        "x".repeat(81)
    )));
}

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn nightly_lab_reads_root_status_without_mutation() -> Result<(), Box<dyn std::error::Error>>
{
    let location = std::env::var("LAB_HDFS_LOCATION")?;
    let (client, parsed) = build_hdfs_client(&location, &hdfs_lab_config())?;

    let root = client.get_file_info(parsed.root()).await?;
    assert!(root.isdir, "HDFS root must be a directory");
    Ok(())
}

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn nightly_lab_cleanup_confined_run_root() -> Result<(), Box<dyn std::error::Error>> {
    let root = hdfs_lab_root()?;
    let (client, _) = build_hdfs_client(&std::env::var("LAB_HDFS_RUN_ROOT")?, &hdfs_lab_config())?;
    cleanup::delete_with_recovery_wait(|| client.delete(root.root(), true)).await
}

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn nightly_lab_streams_recursive_scan_with_depth_limit()
-> Result<(), Box<dyn std::error::Error>> {
    let (hdfs, roles) = fixture("scan").await?;
    for directory in ["甲", "甲/乙", "甲/乙/丙", "并列"] {
        let path = hdfs.path(directory);
        hdfs.client.mkdirs(&path, 0o755, true).await?;
    }
    create_empty_hdfs_files(
        &hdfs,
        &["root.txt", "甲/a.txt", "甲/乙/b.txt", "甲/乙/丙/c.txt"],
    )
    .await?;

    let request = TraversalRequest::new(StoragePath::root())
        .max_depth(NonZeroUsize::new(2))
        .max_inflight_operations(NonZeroUsize::new(3).ok_or("nonzero")?);
    let mut session = StorageTraversalSource::new(&roles)?.traverse(request.clone());
    let mut paths = Vec::new();
    while let Some(item) = session.next_item().await {
        match item {
            TraversalItem::Entry(entry) => {
                paths.push(std::path::PathBuf::from(entry.path().as_str()));
            }
            other => return Err(format!("unexpected traversal item: {other:?}").into()),
        }
    }
    assert!(matches!(
        session.finish().await?,
        TraversalOutcome::Completed(_)
    ));
    paths.sort();
    assert_eq!(
        paths,
        vec![
            std::path::PathBuf::from("root.txt"),
            std::path::PathBuf::from("并列"),
            std::path::PathBuf::from("甲"),
            std::path::PathBuf::from("甲/a.txt"),
            std::path::PathBuf::from("甲/乙"),
        ]
    );

    let mut session = StorageTraversalSource::new(&roles)?.traverse(request.filter(Arc::new(
        DslTraversalFilter::parse(Some("extension == \"txt\""), None)?,
    )));
    let mut filtered = Vec::new();
    while let Some(item) = session.next_item().await {
        match item {
            TraversalItem::Entry(entry) => {
                filtered.push(std::path::PathBuf::from(entry.path().as_str()));
            }
            other => return Err(format!("unexpected traversal item: {other:?}").into()),
        }
    }
    assert!(matches!(
        session.finish().await?,
        TraversalOutcome::Completed(_)
    ));
    filtered.sort();
    assert_eq!(
        filtered,
        vec![
            std::path::PathBuf::from("root.txt"),
            std::path::PathBuf::from("甲/a.txt"),
        ]
    );

    assert!(hdfs.client.delete(hdfs.location.root(), true).await?);
    Ok(())
}

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn nightly_lab_validates_factory_root_lifecycle() -> TestResult {
    let location = hdfs_lab_location("factory")?;
    let (raw, parsed) = build_hdfs_client(&location, &hdfs_lab_config())?;
    let _ = raw.delete(parsed.root(), true).await;
    let config = |ensure_dir| {
        BackendConfig::Hdfs(HdfsBackendConfig {
            location: location.clone(),
            client: hdfs_lab_config(),
            block_size: None,
            ensure_dir,
        })
    };
    assert!(connect_backend(config(false)).await.is_err());
    let storage = connect_backend(config(true)).await?;
    assert_eq!(
        storage.namespace()?.stat(&StoragePath::root()).await?.kind,
        EntryKind::Directory
    );
    raw.delete(parsed.root(), true).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn nightly_lab_lists_exactly_one_directory() -> TestResult {
    use data_mover::storage::{ListRequest, create_directory_all};
    use futures::StreamExt as _;
    let (raw, storage) = fixture("listing").await?;
    for directory in ["空目录", "混合/nested", "新建/嵌套"] {
        create_directory_all(&storage, &StoragePath::new(directory)?).await?;
        create_directory_all(&storage, &StoragePath::new(directory)?).await?;
    }
    create_hdfs_file(&raw, "混合/file.txt", bytes::Bytes::from_static(b"content")).await?;
    create_hdfs_file(&raw, "混合/nested/deep.txt", bytes::Bytes::new()).await?;
    let namespace = storage.namespace()?;
    assert!(
        create_directory_all(&storage, &StoragePath::new("混合/file.txt")?)
            .await
            .is_err()
    );
    for (directory, expected) in [("空目录", vec![]), ("混合", vec!["file.txt", "nested"])] {
        let mut stream = namespace.list(
            ListRequest::new(StoragePath::new(directory)?, CancellationToken::new())
                .with_page_hint(NonZeroUsize::MIN),
        );
        let mut names = Vec::new();
        while let Some(page) = stream.next().await {
            let page = page?;
            assert!(page.failures.is_empty());
            assert!(page.children.len() <= 1);
            names.extend(page.children.iter().map(|child| child.name().to_owned()));
        }
        names.sort();
        assert_eq!(names, expected);
    }
    let file = namespace.stat(&StoragePath::new("混合/file.txt")?).await?;
    assert_eq!(file.kind, EntryKind::File);
    assert_eq!(file.size, Some(7));
    let mut invalid = namespace.list(ListRequest::new(file.path, CancellationToken::new()));
    assert!(
        invalid
            .next()
            .await
            .ok_or("missing listing failure")?
            .is_err()
    );
    raw.cleanup().await
}

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn nightly_lab_namespace_rename_delete_and_artifacts() -> TestResult {
    use data_mover::storage::{
        DeleteTreeItem, DeleteTreeOutcome, DeleteTreeRequest, create_directory_all, delete_tree,
    };
    let (raw, storage) = fixture("namespace").await?;
    create_directory_all(&storage, &StoragePath::new("tree/nested")?).await?;
    create_hdfs_file(&raw, "tree/nested/file", bytes::Bytes::from_static(b"new")).await?;
    create_hdfs_file(&raw, "tree/target", bytes::Bytes::from_static(b"old")).await?;
    create_hdfs_file(
        &raw,
        "tree/nested/.data-mover-orphan",
        bytes::Bytes::from_static(b"artifact"),
    )
    .await?;
    let namespace = storage.namespace()?;
    assert!(namespace.delete(&StoragePath::new("tree")?).await.is_err());
    namespace
        .rename(
            &StoragePath::new("tree/nested/file")?,
            &StoragePath::new("tree/target")?,
        )
        .await?;
    assert_eq!(raw_read(&raw, "tree/target").await?, b"new"[..]);
    assert!(
        namespace
            .stat(&StoragePath::new("tree/nested/file")?)
            .await
            .is_err()
    );
    namespace
        .rename(
            &StoragePath::new("tree/nested")?,
            &StoragePath::new("tree/moved")?,
        )
        .await?;
    let mut deletion = delete_tree(
        &storage,
        DeleteTreeRequest {
            root: StoragePath::new("tree")?,
            delete_root: true,
            max_inflight_operations: NonZeroUsize::new(2).ok_or("nonzero")?,
            max_buffered_items: NonZeroUsize::MIN,
            cancel: CancellationToken::new(),
        },
    )?;
    let mut deleted = Vec::new();
    while let Some(item) = deletion.next_item().await {
        match item {
            DeleteTreeItem::Deleted { path, kind } => deleted.push((path, kind)),
            DeleteTreeItem::EntryFailure(failure) => panic!("delete failed: {failure:?}"),
        }
    }
    deleted.sort_unstable_by(|left, right| left.0.as_str().cmp(right.0.as_str()));
    assert_eq!(
        deleted,
        vec![
            (StoragePath::new("tree")?, EntryKind::Directory),
            (StoragePath::new("tree/moved")?, EntryKind::Directory),
            (StoragePath::new("tree/target")?, EntryKind::File),
        ],
        "progress must include every visible entry and hide transfer artifacts"
    );
    let outcome = deletion.finish().await?;
    assert!(
        matches!(outcome, DeleteTreeOutcome::Completed(done)
        if done.entry_failures == 0 && done.deleted_entries == 1 && done.deleted_directories == 2),
        "unexpected deletion accounting: {outcome:?}"
    );
    assert!(raw.client.get_file_info(&raw.path("tree")).await.is_err());
    assert!(raw.client.get_file_info(raw.location.root()).await?.isdir);
    raw.cleanup().await
}

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn nightly_lab_reads_positional_ranges_and_common_stream() -> TestResult {
    use data_mover::storage::ReadRequest;
    use futures::TryStreamExt as _;
    let (raw, storage) = fixture("read-contract").await?;
    let data = bytes::Bytes::from(
        (0..3 * 1024 * 1024)
            .map(|i| u8::try_from(i % 251).unwrap_or_default())
            .collect::<Vec<_>>(),
    );
    create_hdfs_file(&raw, "file", data.clone()).await?;
    let source = storage.read_source()?;
    let descriptor = source.describe(&StoragePath::new("file")?).await?;
    for (offset, length) in [(0, 7), (1024 * 1024 - 3, 19), (0, data.len() as u64)] {
        let stream = source
            .read(ReadRequest {
                path: descriptor.path.clone(),
                range: Some(offset..offset + length),
                expected_source: Some(descriptor.source_identity.clone()),
                maximum_chunk_bytes: 64 * 1024,
                read_inflight: 2,
                read_budget: None,
                source_qos: None,
                version: data_mover::model::SourceVersion::Current,
                cancel: CancellationToken::new(),
            })
            .await?;
        let chunks = stream.try_collect::<Vec<_>>().await?;
        assert!(chunks.iter().all(|chunk| chunk.len() <= 64 * 1024));
        assert_eq!(
            chunks.concat(),
            data[usize::try_from(offset)?..usize::try_from(offset + length)?]
        );
    }
    raw.cleanup().await
}

async fn verified_copy(
    source: &Storage,
    destination: &Storage,
    from: &str,
    to: &str,
    policy: data_mover::transfer::TransferPolicy,
) -> TestResult {
    use data_mover::integrity::{IntegrityMode, IntegrityOptions, IntegrityRequest, compare};
    use data_mover::transfer::{ReadBackVerification, TransferRequest, transfer};
    assert_eq!(from, to, "migration preserves relative paths");
    let outcome = transfer(
        TransferRequest::new(
            source.clone(),
            StoragePath::new(from)?,
            destination.clone(),
            CancellationToken::new(),
        )
        .with_transfer_policy(policy)
        .with_read_back_verification(ReadBackVerification::Enabled),
    )
    .await?;
    let report = compare(IntegrityRequest {
        source: source.clone(),
        source_path: StoragePath::new(from)?,
        destination: destination.clone(),
        destination_path: StoragePath::new(to)?,
        plan: data_mover::integrity::IntegrityPlan::default(),
        options: IntegrityOptions {
            mode: IntegrityMode::Content,
            ..Default::default()
        },
        cancel: CancellationToken::new(),
    })
    .await?;
    assert!(report.differences().is_empty(), "{report:?}");
    assert!(outcome.blake3.is_some());
    Ok(())
}

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn nightly_lab_cross_protocol_copy_and_integrity() -> TestResult {
    use data_mover::storage::LocalBackendConfig;
    use data_mover::transfer::TransferPolicy;
    let (raw, hdfs) = fixture("cross-protocol").await?;
    let (second_raw, second_hdfs) = fixture("cross-protocol-second").await?;
    let root = tempfile::tempdir()?;
    let slots = NonZeroUsize::new(2).ok_or("nonzero")?;
    let local = connect_backend(BackendConfig::Local(LocalBackendConfig {
        rename_context: None,
        read_chunk_bytes: None,
        write_chunk_bytes: None,
        root: root.path().into(),
        read_concurrency: slots,
        write_concurrency: slots,
    }))
    .await?;
    for size in [0, 4096, 2 * 1024 * 1024 + 13] {
        let data = vec![77; size];
        std::fs::write(root.path().join("source"), &data)?;
        for policy in [
            TransferPolicy::Checkpointed,
            TransferPolicy::AtomicReplace,
            TransferPolicy::Direct,
        ] {
            verified_copy(&local, &hdfs, "source", "source", policy).await?;
            assert_eq!(raw_read(&raw, "source").await?, data);
            verified_copy(&hdfs, &local, "source", "source", policy).await?;
            assert_eq!(std::fs::read(root.path().join("source"))?, data);
            verified_copy(&hdfs, &second_hdfs, "source", "source", policy).await?;
        }
    }
    create_hdfs_file(
        &second_raw,
        "source",
        bytes::Bytes::from_static(b"corrupted"),
    )
    .await?;
    let report = data_mover::integrity::compare(data_mover::integrity::IntegrityRequest {
        source: hdfs.clone(),
        source_path: StoragePath::new("source")?,
        destination: second_hdfs,
        destination_path: StoragePath::new("source")?,
        plan: data_mover::integrity::IntegrityPlan::default(),
        options: data_mover::integrity::IntegrityOptions {
            mode: data_mover::integrity::IntegrityMode::Content,
            ..Default::default()
        },
        cancel: CancellationToken::new(),
    })
    .await?;
    assert!(!report.differences().is_empty());
    second_raw.cleanup().await?;
    raw.cleanup().await
}

#[tokio::test]
#[ignore = "requires the nightly lab S3 and HDFS clusters"]
async fn nightly_lab_copies_between_s3_and_hdfs() -> TestResult {
    use data_mover::storage::S3BackendConfig;
    use data_mover::transfer::TransferPolicy;
    let location = std::env::var("LAB_S3_HDFS_LOCATION")?;
    let s3 = connect_backend(BackendConfig::S3(S3BackendConfig {
        url: location,
        block_size: None,
        single_put_threshold: None,
    }))
    .await?;
    let (raw, hdfs) = fixture("s3-cross-protocol").await?;
    for size in [4096, 6 * 1024 * 1024 + 13] {
        let data = bytes::Bytes::from(vec![69; size]);
        create_hdfs_file(&raw, "source", data.clone()).await?;
        for policy in [
            TransferPolicy::Checkpointed,
            TransferPolicy::AtomicReplace,
            TransferPolicy::Direct,
        ] {
            let name = format!("hdfs-{size}-{policy:?}").to_ascii_lowercase();
            create_hdfs_file(&raw, &name, data.clone()).await?;
            verified_copy(&hdfs, &s3, &name, &name, policy).await?;
            verified_copy(&s3, &hdfs, &name, &name, policy).await?;
            assert_eq!(raw_read(&raw, &name).await?, data);
        }
    }
    raw.cleanup().await
}

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn nightly_lab_role_tar_write() -> TestResult {
    let (raw, storage) = fixture("tar").await?;
    let outcome = data_mover::tar_pack::pack(data_mover::tar_pack::TarRequest {
        source: storage.clone(),
        destination: storage,
        entries: Vec::new(),
        final_path: StoragePath::new("empty.tar")?,
        cancel: CancellationToken::new(),
        source_qos: None,
    })
    .await?;
    assert_eq!(outcome.transferred_bytes, 1024);
    assert_eq!(raw_read(&raw, "empty.tar").await?, vec![0; 1024]);
    raw.cleanup().await
}

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn nightly_lab_mutates_hdfs_native_metadata_safely() -> TestResult {
    use data_mover::model::{OwnershipMode, StorageTimestamp, TimePrecision, TimestampMetadata};
    use data_mover::storage::{MetadataMutation, PrincipalIdentity};
    let (raw, storage) = fixture("metadata").await?;
    create_hdfs_file(&raw, "file", bytes::Bytes::from_static(b"content")).await?;
    let path = StoragePath::new("file")?;
    let metadata = storage.metadata()?;
    metadata
        .apply(
            &path,
            MetadataMutation::Mode(0o640),
            CancellationToken::new(),
        )
        .await?;
    metadata
        .apply(
            &path,
            MetadataMutation::Timestamps(TimestampMetadata {
                accessed: Some(StorageTimestamp::new(
                    1_600_000_000_456_000_000,
                    TimePrecision::Milliseconds,
                )?),
                modified: Some(StorageTimestamp::new(
                    1_700_000_000_123_000_000,
                    TimePrecision::Milliseconds,
                )?),
                created: None,
            }),
            CancellationToken::new(),
        )
        .await?;
    let status = raw.client.get_file_info(&raw.path("file")).await?;
    assert_eq!(status.permission, 0o640);
    assert_eq!(status.modification_time, 1_700_000_000_123);
    assert_eq!(status.access_time, 1_600_000_000_456);
    let described = storage.read_source()?.describe(&path).await?;
    let permissions = metadata.permissions(&described).await?;
    assert_eq!(
        permissions.owner,
        Some(PrincipalIdentity::Named(status.owner.clone()))
    );
    assert_eq!(
        permissions.group,
        Some(PrincipalIdentity::Named(status.group.clone()))
    );
    assert_eq!(permissions.mode, Some(0o640));
    assert!(
        metadata
            .apply(
                &path,
                MetadataMutation::NumericOwnership(OwnershipMode {
                    uid: 1,
                    gid: 1,
                    mode: 0o777
                }),
                CancellationToken::new()
            )
            .await
            .is_err()
    );
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(
        metadata
            .apply(&path, MetadataMutation::Mode(0o777), cancelled)
            .await
            .is_err()
    );
    let after = raw.client.get_file_info(&raw.path("file")).await?;
    assert_eq!(after.permission, status.permission);
    assert_eq!(after.owner, status.owner);
    assert_eq!(after.group, status.group);
    raw.cleanup().await
}

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn nightly_lab_confines_independent_roots_and_cleanup() -> TestResult {
    use data_mover::storage::{DeleteTreeItem, DeleteTreeRequest, delete_tree};
    let (left_raw, left) = fixture("confine-left").await?;
    let (right_raw, right) = fixture("confine-right").await?;
    create_hdfs_file(&left_raw, "sentinel", bytes::Bytes::from_static(b"left")).await?;
    create_hdfs_file(&right_raw, "sentinel", bytes::Bytes::from_static(b"right")).await?;
    assert_ne!(left.identity(), right.identity());
    // StoragePath preserves backend path spelling. The connected namespace must reject
    // traversal before reading or mutating the sentinel below the independent sibling root.
    let namespace = left.namespace()?;
    let escape = StoragePath::new("../confine-right/sentinel")?;
    let sentinel = StoragePath::new("sentinel")?;
    let attempts = [
        namespace.stat(&escape).await.map(|_| ()),
        namespace.delete(&escape).await,
        namespace.rename(&escape, &sentinel).await,
        namespace.rename(&sentinel, &escape).await,
    ];
    for result in attempts {
        assert!(
            matches!(result, Err(StorageRoleFailure::Entry(ref failure))
            if failure.class() == FailureClass::InvalidInput),
            "the connected HDFS root must refuse traversal: {result:?}"
        );
    }
    assert_eq!(raw_read(&left_raw, "sentinel").await?, b"left"[..]);
    assert_eq!(raw_read(&right_raw, "sentinel").await?, b"right"[..]);
    let mut delete = delete_tree(
        &left,
        DeleteTreeRequest {
            root: StoragePath::root(),
            delete_root: true,
            max_inflight_operations: NonZeroUsize::MIN,
            max_buffered_items: NonZeroUsize::MIN,
            cancel: CancellationToken::new(),
        },
    )?;
    while let Some(item) = delete.next_item().await {
        assert!(matches!(item, DeleteTreeItem::Deleted { .. }), "{item:?}");
    }
    delete.finish().await?;
    assert!(
        left_raw
            .client
            .get_file_info(left_raw.location.root())
            .await?
            .isdir
    );
    assert_eq!(raw_read(&right_raw, "sentinel").await?, b"right"[..]);
    left_raw.cleanup().await?;
    right_raw.cleanup().await
}

#[tokio::test]
#[ignore = "requires the nightly lab HDFS cluster"]
async fn nightly_lab_namespace_native_subtree_delete_requires_root_opt_in() -> TestResult {
    let run = format!("nightly-{}", uuid::Uuid::new_v4().simple());
    let (raw, storage) = fixture(&format!("subtree-{run}")).await?;
    create_hdfs_file(
        &raw,
        "nested/child/file.bin",
        bytes::Bytes::from_static(b"remove me"),
    )
    .await?;
    let namespace = storage.namespace()?;
    let root = StoragePath::root();

    let ordinary_delete = namespace.delete(&root).await;
    assert!(
        matches!(ordinary_delete, Err(StorageRoleFailure::Entry(ref failure))
        if failure.class() == FailureClass::InvalidInput)
    );
    let no_opt_in = namespace.delete_subtree(&root, false).await;
    assert!(
        matches!(no_opt_in, Err(StorageRoleFailure::Entry(ref failure))
        if failure.class() == FailureClass::InvalidInput)
    );
    assert!(
        raw.client.get_file_info(raw.location.root()).await.is_ok(),
        "refused root deletion must leave the configured root intact"
    );

    namespace.delete_subtree(&root, true).await?;
    assert!(
        raw.client.get_file_info(raw.location.root()).await.is_err(),
        "opted-in recursive deletion must remove the nonempty configured root"
    );
    Ok(())
}
