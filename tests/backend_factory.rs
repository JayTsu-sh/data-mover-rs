use std::num::NonZeroUsize;

use data_mover::model::StoragePath;
use data_mover::storage::{
    BackendConfig, CifsBackendConfig, CifsGuestPolicy, CifsSigningPolicy, HdfsBackendConfig,
    LocalBackendConfig, NfsBackendConfig, S3BackendConfig, connect_backend, endpoint_identity,
};
use data_mover::transfer::{InflightLimits, TransferIdentity, TransferRequest, transfer};
use tokio_util::sync::CancellationToken;

#[test]
fn cifs_config_debug_never_exposes_credentials() {
    let config = CifsBackendConfig {
        signing_policy: CifsSigningPolicy::default(),
        guest_policy: CifsGuestPolicy::default(),
        server: "server".to_string(),
        share: "share".to_string(),
        root: None,
        ensure_dir: false,
        username: "sensitive-user".to_string(),
        password: "sensitive-password".to_string(),
    };

    let debug = format!("{config:?}");

    assert!(!debug.contains("sensitive-user"));
    assert!(!debug.contains("sensitive-password"));
}

#[test]
fn s3_config_debug_never_exposes_the_key_pair() {
    // A secret with `/`, `+` and `=` is exactly what a URL parser cannot find.
    let config = BackendConfig::S3(S3BackendConfig {
        url: "s3://SENSITIVEAK:wJalr/XUtn+FEMI=@bucket.host:9000/prefix".to_string(),
        block_size: None,
    });

    let debug = format!("{config:?}");

    assert!(!debug.contains("SENSITIVEAK"), "{debug}");
    assert!(
        !debug.contains("wJalr") && !debug.contains("FEMI"),
        "{debug}"
    );
    assert!(debug.contains("bucket.host:9000/prefix"), "{debug}");
}

#[tokio::test]
async fn explicit_factory_handles_source_and_destination_without_pair_dispatch()
-> Result<(), Box<dyn std::error::Error>> {
    let source_root = tempfile::tempdir()?;
    let destination_root = tempfile::tempdir()?;
    tokio::fs::write(source_root.path().join("payload.bin"), b"protocol-neutral").await?;
    let source = connect_backend(BackendConfig::Local(LocalBackendConfig {
        root: source_root.path().to_path_buf(),
        read_concurrency: NonZeroUsize::new(2).ok_or("non-zero")?,
        write_concurrency: NonZeroUsize::new(2).ok_or("non-zero")?,
    }))
    .await?;
    let destination = connect_backend(BackendConfig::Local(LocalBackendConfig {
        root: destination_root.path().to_path_buf(),
        read_concurrency: NonZeroUsize::new(2).ok_or("non-zero")?,
        write_concurrency: NonZeroUsize::new(2).ok_or("non-zero")?,
    }))
    .await?;
    let path = StoragePath::new("payload.bin")?;
    let request = TransferRequest::new(
        TransferIdentity::new("factory-transfer")?,
        source,
        path.clone(),
        destination,
        path,
        InflightLimits::new(2, 128 * 1024, 2)?,
        CancellationToken::new(),
    );

    let outcome = transfer(request).await?;

    assert_eq!(outcome.transferred_bytes, 16);
    assert_eq!(
        tokio::fs::read(destination_root.path().join("payload.bin")).await?,
        b"protocol-neutral"
    );
    Ok(())
}

fn local_config(root: &std::path::Path) -> Result<BackendConfig, Box<dyn std::error::Error>> {
    Ok(BackendConfig::Local(LocalBackendConfig {
        root: root.to_path_buf(),
        read_concurrency: NonZeroUsize::new(2).ok_or("non-zero")?,
        write_concurrency: NonZeroUsize::new(2).ok_or("non-zero")?,
    }))
}

/// ADR-0006: the identity is derived from what the backend reaches, so any spelling of the same
/// directory — a symlink, `x/..` — reconnects as the same endpoint, and another directory does not.
#[tokio::test]
async fn derived_local_identity_is_canonical_and_reconnect_stable()
-> Result<(), Box<dyn std::error::Error>> {
    let base = tempfile::tempdir()?;
    let root = base.path().join("root");
    std::fs::create_dir_all(root.join("x"))?;
    let other = base.path().join("other");
    std::fs::create_dir(&other)?;

    let direct = connect_backend(local_config(&root)?).await?;
    let dotted = connect_backend(local_config(&root.join("x").join(".."))?).await?;
    assert_eq!(direct.identity(), dotted.identity());
    assert!(direct.identity().stable_id().starts_with("file:///"));
    assert_eq!(
        &endpoint_identity(&local_config(&root)?)?,
        direct.identity(),
        "the offline derivation equals the connected identity"
    );
    #[cfg(unix)]
    {
        let link = base.path().join("link");
        std::os::unix::fs::symlink(&root, &link)?;
        let linked = connect_backend(local_config(&link)?).await?;
        assert_eq!(direct.identity(), linked.identity());
    }
    let elsewhere = connect_backend(local_config(&other)?).await?;
    assert_ne!(direct.identity(), elsewhere.identity());
    Ok(())
}

/// Network endpoints are derived offline, without credentials, the way their backends parse them.
#[test]
fn network_endpoint_identities_are_derived_offline_without_credentials()
-> Result<(), Box<dyn std::error::Error>> {
    let s3 = endpoint_identity(&BackendConfig::S3(S3BackendConfig {
        url: "s3+https://SENSITIVEAK:wJal/r+X=@Data-Mover-Test.10.131.9.11:9000/resume-base".into(),
        block_size: None,
    }))?;
    assert_eq!(
        s3.stable_id(),
        "s3://10.131.9.11:9000/data-mover-test/resume-base"
    );
    let nfs = endpoint_identity(&BackendConfig::Nfs(NfsBackendConfig {
        url: "nfs://10.128.61.200/ontap_lisaauto_nfs:/data?version=4.1&uid=0&gid=0".into(),
        block_size: None,
        ensure_dir: false,
    }))?;
    assert_eq!(
        nfs.stable_id(),
        "nfs://10.128.61.200/ontap_lisaauto_nfs/data"
    );
    let cifs = endpoint_identity(&BackendConfig::Cifs(CifsBackendConfig {
        server: "10.128.61.200".into(),
        share: "ontap_lisaauto_cifs".into(),
        root: Some("resume-1".into()),
        ensure_dir: false,
        username: "sensitive-user".into(),
        password: "sensitive-password".into(),
        signing_policy: CifsSigningPolicy::default(),
        guest_policy: CifsGuestPolicy::default(),
    }))?;
    assert_eq!(
        cifs.stable_id(),
        "smb://10.128.61.200/ontap_lisaauto_cifs/resume-1"
    );
    for identity in [&s3, &nfs, &cifs] {
        assert!(!identity.stable_id().contains("SENSITIVE"));
        assert!(!identity.stable_id().contains("sensitive"));
    }
    Ok(())
}

/// HDFS is parsed offline exactly as `create_hdfs_storage` parses it: the user is dropped, a direct
/// `NameNode` keeps its port, and a `NameService` keeps the case of its configuration key.
#[test]
fn hdfs_endpoint_identity_is_derived_offline() -> Result<(), Box<dyn std::error::Error>> {
    let direct = endpoint_identity(&BackendConfig::Hdfs(HdfsBackendConfig {
        location: "hdfs://dm@NameNode:8020/data/root".into(),
        client: data_mover::HdfsConfig::default(),
        block_size: None,
        ensure_dir: false,
    }))?;
    assert_eq!(direct.stable_id(), "hdfs://namenode:8020/data/root");
    let mut client = data_mover::HdfsConfig::default();
    client
        .overrides
        .insert("dfs.ha.namenodes.NS1".into(), "nn1,nn2".into());
    client.overrides.insert(
        "dfs.namenode.rpc-address.NS1.nn1".into(),
        "nn-a.example:8020".into(),
    );
    client.overrides.insert(
        "dfs.namenode.rpc-address.NS1.nn2".into(),
        "nn-b.example:8020".into(),
    );
    let service = endpoint_identity(&BackendConfig::Hdfs(HdfsBackendConfig {
        location: "hdfs://dm@NS1/".into(),
        client,
        block_size: None,
        ensure_dir: false,
    }))?;
    assert_eq!(service.stable_id(), "hdfs://NS1");
    Ok(())
}
