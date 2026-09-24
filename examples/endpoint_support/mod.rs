//! Endpoints for examples that take any backend on the command line.
//!
//! - `nfs://…` — an NFS URL, used as given;
//! - `smb:<sub-path>` — that sub-path of the share named by `CIFS_REAL_SERVER` / `CIFS_REAL_SHARE` /
//!   `CIFS_REAL_USER` / `CIFS_REAL_PASS` (the e2e-cifs `.env`);
//! - `s3:<prefix>` — that prefix of the bucket named by `S3_HOST` / `S3_BUCKET` / `S3_AK` / `S3_SK` /
//!   `S3_USE_HTTPS` / `S3_COMPAT` (the e2e-s3 `.env`, or its `dxn/` / `storagegrid/` siblings);
//! - `hdfs://…` — an HDFS location, with `LAB_HDFS_CONFIG_DIR` / `LAB_HDFS_KEYTAB` for the client;
//! - anything else — a local directory, created if missing.

use std::env;
use std::error::Error;
use std::fs;
use std::io;
use std::num::NonZeroUsize;
use std::path::Path;

use data_mover::model::StoragePath;
use data_mover::storage::{
    BackendConfig, CifsBackendConfig, CifsGuestPolicy, CifsSigningPolicy, DeleteTreeRequest,
    HdfsBackendConfig, LocalBackendConfig, NamespaceRequest, NfsBackendConfig, PreflightPolicy,
    S3BackendConfig, Storage, connect_backend, delete_tree,
};
use futures::TryStreamExt as _;
use tokio_util::sync::CancellationToken;

pub type Result<T = (), E = Box<dyn Error>> = std::result::Result<T, E>;

fn env_var(name: &str) -> Result<String> {
    env::var(name).map_err(|_| format!("{name} is not set (source the skill's .env)").into())
}

/// `s3[+sg|+dxn][+https]://AK:SK@bucket.host/prefix` — the crate takes the key and secret as
/// written, undecoded. `S3_COMPAT=sg|dxn` selects the `StorageGRID` / DXN profile.
fn s3_url(prefix: &str) -> Result<String> {
    let compat = match env::var("S3_COMPAT").unwrap_or_default().as_str() {
        "" | "standard" => "",
        "sg" => "+sg",
        "dxn" => "+dxn",
        _ => return Err("S3_COMPAT must be standard, sg or dxn".into()),
    };
    let tls = if env::var("S3_USE_HTTPS").is_ok_and(|value| value == "true") {
        "+https"
    } else {
        ""
    };
    let scheme = format!("s3{compat}{tls}");
    Ok(format!(
        "{scheme}://{}:{}@{}.{}/{}",
        env_var("S3_AK")?,
        env_var("S3_SK")?,
        env_var("S3_BUCKET")?,
        env_var("S3_HOST")?,
        prefix.trim_start_matches('/')
    ))
}

fn hdfs_client() -> data_mover::HdfsConfig {
    data_mover::HdfsConfig {
        config_dir: env::var_os("LAB_HDFS_CONFIG_DIR").map(Into::into),
        kerberos_credentials: env::var_os("LAB_HDFS_KEYTAB").map(|keytab| {
            data_mover::HdfsKerberosCredentials {
                keytab: Some(keytab.into()),
                ..Default::default()
            }
        }),
        ..Default::default()
    }
}

/// Connects one endpoint; its identity is the endpoint data-mover derives (ADR-0006).
pub async fn connect(endpoint: &str) -> Result<Storage> {
    let config = if endpoint.starts_with("nfs://") {
        BackendConfig::Nfs(NfsBackendConfig {
            url: endpoint.to_owned(),
            block_size: None,
            ensure_dir: true,
        })
    } else if let Some(root) = endpoint.strip_prefix("smb:") {
        BackendConfig::Cifs(CifsBackendConfig {
            server: env_var("CIFS_REAL_SERVER")?,
            share: env_var("CIFS_REAL_SHARE")?,
            root: (!root.is_empty()).then(|| root.to_owned()),
            ensure_dir: true,
            username: env_var("CIFS_REAL_USER")?,
            password: env_var("CIFS_REAL_PASS")?,
            signing_policy: CifsSigningPolicy::default(),
            guest_policy: CifsGuestPolicy::default(),
        })
    } else if endpoint.starts_with("s3://") {
        return Err("give `s3:<prefix>` with the e2e-s3 .env, not an s3:// URL".into());
    } else if let Some(prefix) = endpoint.strip_prefix("s3:") {
        BackendConfig::S3(S3BackendConfig {
            url: s3_url(prefix)?,
            block_size: None,
        })
    } else if endpoint.starts_with("hdfs://") {
        BackendConfig::Hdfs(HdfsBackendConfig {
            location: endpoint.to_owned(),
            client: hdfs_client(),
            block_size: None,
            ensure_dir: true,
        })
    } else {
        fs::create_dir_all(endpoint)?;
        let slots = NonZeroUsize::new(4).ok_or("non-zero")?;
        BackendConfig::Local(LocalBackendConfig {
            root: endpoint.into(),
            read_concurrency: slots,
            write_concurrency: slots,
        })
    };
    Ok(connect_backend(config).await?)
}

/// Names starting with `.data-mover-` in `dir` of `endpoint`: what a transfer left next to its final
/// file. Read below the storage roles where those hide them (Local traversal, NFS namespace), so a
/// check sees exactly what is on the server. S3 is listed by its bucket listing instead.
pub async fn artifacts(endpoint: &str, dir: &str) -> Result<Vec<String>> {
    Ok(names(endpoint, dir)
        .await?
        .into_iter()
        .filter(|name| name.starts_with(".data-mover-"))
        .collect())
}

/// Removes what a test run left in `dir` of `endpoint`: every `.data-mover-*` artifact, then the
/// directory with its final files. Returns the artifact names removed. S3 is cleaned by prefix instead.
pub async fn remove_run(endpoint: &str, dir: &str) -> Result<Vec<String>> {
    // A mistyped argument must not empty a share: only a test run's own top-level directory.
    if !dir.starts_with("resume-") || dir.contains(['/', '\\']) || dir.contains("..") {
        return Err(
            format!("refusing to remove {dir:?}: not a single resume-<run> directory").into(),
        );
    }
    let removed = artifacts(endpoint, dir).await?;
    if endpoint.starts_with("nfs://") {
        // The NFS namespace refuses `.data-mover-*` paths, so they go through a raw mount first.
        let (mount, directory) = nfs_mount(endpoint, dir).await?;
        for name in &removed {
            mount.remove_path(&join(&directory, name)).await?;
        }
    } else if !endpoint.starts_with("smb:") && !endpoint.starts_with("hdfs://") {
        fs::remove_dir_all(Path::new(endpoint).join(dir))?;
        return Ok(removed);
    }
    let storage = connect(endpoint).await?;
    let mut session = delete_tree(
        &storage,
        DeleteTreeRequest {
            root: StoragePath::new(dir)?,
            delete_root: true,
            max_inflight_operations: NonZeroUsize::new(4).ok_or("non-zero")?,
            max_buffered_items: NonZeroUsize::new(64).ok_or("non-zero")?,
            cancel: CancellationToken::new(),
        },
    )?;
    while session.next_item().await.is_some() {}
    session
        .finish()
        .await
        .map_err(|failure| format!("{failure:?}"))?;
    Ok(removed)
}

fn join(dir: &str, name: &str) -> String {
    [dir.trim_matches('/'), name]
        .iter()
        .filter(|part| !part.is_empty())
        .copied()
        .collect::<Vec<_>>()
        .join("/")
}

async fn names(endpoint: &str, dir: &str) -> Result<Vec<String>> {
    if endpoint.starts_with("nfs://") {
        let (mount, directory) = nfs_mount(endpoint, dir).await?;
        let names = mount
            .readdir_path(&directory)
            .await?
            .map_ok(|entry| entry.file_name)
            .try_collect::<Vec<_>>()
            .await?;
        return Ok(names);
    }
    if endpoint.starts_with("smb:") || endpoint.starts_with("hdfs://") {
        let listed = connect(endpoint)
            .await?
            .namespace(&PreflightPolicy::production())?
            .execute(NamespaceRequest::List(StoragePath::new(dir)?))
            .await?
            .into_listing()
            .ok_or("the namespace returned no listing")?;
        return Ok(listed
            .0
            .iter()
            .filter_map(|entry| entry.path.as_str().rsplit('/').next().map(str::to_owned))
            .collect());
    }
    if endpoint.starts_with("s3:") {
        return Err("list S3 artifacts with the bucket listing".into());
    }
    Ok(fs::read_dir(Path::new(endpoint).join(dir))?
        .map(|entry| entry.map(|entry| entry.file_name().to_string_lossy().into_owned()))
        .collect::<io::Result<Vec<_>>>()?)
}

/// A raw mount of the export behind an `nfs://host/export[:root]?…` endpoint, and `dir` as a path
/// inside it (below the endpoint's root). Uses uid/gid 0 unless the URL says otherwise, as the
/// NFS backend does, so it sees what the transfer wrote.
async fn nfs_mount(endpoint: &str, dir: &str) -> Result<(Box<dyn nfs_rs::Mount>, String)> {
    let rest = endpoint
        .strip_prefix("nfs://")
        .ok_or("not an nfs:// endpoint")?;
    let (rest, query) = rest.split_once('?').unwrap_or((rest, ""));
    let (host, path) = rest.split_once('/').ok_or("the NFS URL has no export")?;
    let (export, root) = path.split_once(':').unwrap_or((path, ""));
    let directory = join(root, dir.trim_matches('/'));
    let mut params = query
        .split('&')
        .filter(|param| !param.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    for default in ["uid=0", "gid=0"] {
        let key = &default[..4];
        if !params.iter().any(|param| param.starts_with(key)) {
            params.push(default.to_owned());
        }
    }
    let url = format!("nfs://{host}/{export}?{}", params.join("&"));
    let mount = nfs_rs::parse_url_and_mount(&url).await?;
    Ok((mount, directory))
}
