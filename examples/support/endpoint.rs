//! Endpoints for examples that take any backend on the command line.
//!
//! - `nfs://…` — an NFS URL, used as given;
//! - `smb:<sub-path>` — that sub-path of the share named by `CIFS_REAL_SERVER` / `CIFS_REAL_SHARE` /
//!   `CIFS_REAL_USER` / `CIFS_REAL_PASS` (the e2e-cifs `.env`);
//!   `CIFS_GUEST_POLICY=allow_unsigned` explicitly permits unsigned guest for lab fixtures only;
//!   the default remains `deny`;
//! - `s3:<prefix>` — that prefix of the bucket named by `S3_HOST` / `S3_BUCKET` / `S3_AK` / `S3_SK` /
//!   `S3_USE_HTTPS` / `S3_COMPAT` / `S3_TLS_VERIFY` / `S3_BUCKET_TYPE` (the e2e-s3 `.env`, or its
//!   `dxn/` / `storagegrid/` / `ontap/` / `rustfs/` siblings);
//! - `s3@<bucket>:<prefix>` — the same, in another bucket of that server (`S3_BUCKET_TYPE` applies to
//!   `S3_BUCKET` only: another bucket is an object bucket);
//! - `hdfs://…` — an HDFS location, with `LAB_HDFS_CONFIG_DIR` / `LAB_HDFS_KEYTAB` for the client;
//! - anything else — a local directory, created if missing.

use std::env;
use std::error::Error;
use std::fs;
use std::io;
use std::num::NonZeroUsize;
use std::path::Path;

use data_mover::model::{BackendKind, StoragePath};
use data_mover::storage::{
    BackendConfig, CifsBackendConfig, CifsGuestPolicy, CifsSigningPolicy, DeleteTreeRequest,
    HdfsBackendConfig, LocalBackendConfig, NfsBackendConfig, S3BackendConfig, Storage,
    connect_backend, delete_tree,
};
use futures::TryStreamExt as _;
use tokio_util::sync::CancellationToken;

pub type Result<T = (), E = Box<dyn Error>> = std::result::Result<T, E>;

fn env_var(name: &str) -> Result<String> {
    env::var(name).map_err(|_| format!("{name} is not set (source the skill's .env)").into())
}

fn cifs_guest_policy(value: Option<&str>) -> Result<CifsGuestPolicy> {
    match value {
        None | Some("deny") => Ok(CifsGuestPolicy::Deny),
        Some("allow_unsigned") => Ok(CifsGuestPolicy::AllowUnsigned),
        _ => Err("CIFS_GUEST_POLICY must be deny or allow_unsigned".into()),
    }
}

/// `s3[+https]://AK:SK@bucket.host/prefix[?options]` — the crate takes the key and secret as
/// written, undecoded. `S3_USE_HTTPS=true` selects HTTPS; `S3_COMPAT=sg|dxn|ontap`,
/// `S3_TLS_VERIFY=false` and `S3_BUCKET_TYPE=nas` become the URL's options (empty, `true` /
/// `object` = the default), as `.claude/skills/_shared/s3_url.sh` builds them. The bucket is
/// `S3_BUCKET` unless one is named. `S3_BUCKET_TYPE` describes `S3_BUCKET` alone: another bucket
/// named with `s3@<bucket>:` is an object bucket (an ONTAP NAS bucket and an object bucket of the
/// same server in one copy).
fn s3_url(bucket: Option<&str>, prefix: &str) -> Result<String> {
    let https = env::var("S3_USE_HTTPS").is_ok_and(|value| value == "true");
    let other_bucket =
        bucket.is_some_and(|bucket| env::var("S3_BUCKET").ok().as_deref() != Some(bucket));
    let query = s3_options(!other_bucket)?;
    Ok(format!(
        "{}://{}:{}@{}.{}/{}{query}",
        if https { "s3+https" } else { "s3" },
        env_var("S3_AK")?,
        env_var("S3_SK")?,
        bucket.map_or_else(|| env_var("S3_BUCKET"), |bucket| Ok(bucket.to_owned()))?,
        env_var("S3_HOST")?,
        prefix.trim_start_matches('/')
    ))
}

/// The query of the options the environment names (`""` for none); `bucket_type` only for
/// `S3_BUCKET` (`own_bucket`).
fn s3_options(own_bucket: bool) -> Result<String> {
    let var = |name: &str| env::var(name).unwrap_or_default();
    let mut options = Vec::new();
    match var("S3_COMPAT").as_str() {
        "" => {}
        compat @ ("sg" | "dxn" | "ontap") => options.push(format!("compat={compat}")),
        _ => return Err("S3_COMPAT must be empty, sg, dxn or ontap".into()),
    }
    match var("S3_TLS_VERIFY").as_str() {
        "" | "true" => {}
        "false" => options.push("tls_verify=false".to_owned()),
        _ => return Err("S3_TLS_VERIFY must be empty, true or false".into()),
    }
    match var("S3_BUCKET_TYPE").as_str() {
        "" | "object" => {}
        "nas" if !own_bucket => {}
        "nas" => options.push("bucket_type=nas".to_owned()),
        _ => return Err("S3_BUCKET_TYPE must be empty, object or nas".into()),
    }
    Ok(if options.is_empty() {
        String::new()
    } else {
        format!("?{}", options.join("&"))
    })
}

fn hdfs_client() -> data_mover::storage::HdfsConfig {
    data_mover::storage::HdfsConfig {
        config_dir: env::var_os("LAB_HDFS_CONFIG_DIR").map(Into::into),
        kerberos_credentials: env::var_os("LAB_HDFS_KEYTAB").map(|keytab| {
            data_mover::storage::HdfsKerberosCredentials {
                keytab: Some(keytab.into()),
                ..Default::default()
            }
        }),
        ..Default::default()
    }
}

/// Connects one endpoint; its identity is the endpoint data-mover derives (ADR-0006).
pub async fn connect(endpoint: &str) -> Result<Storage> {
    connect_with_options(endpoint, true, None).await
}

/// Connects a source without creating it, or a destination with an optional I/O block ceiling.
pub async fn connect_with_options(
    endpoint: &str,
    ensure_dir: bool,
    block_size: Option<u64>,
) -> Result<Storage> {
    let config = if endpoint.starts_with("nfs://") {
        BackendConfig::Nfs(NfsBackendConfig {
            transfer_concurrency: None,
            url: endpoint.to_owned(),
            block_size,
            ensure_dir,
        })
    } else if let Some(root) = endpoint.strip_prefix("smb:") {
        BackendConfig::Cifs(CifsBackendConfig {
            server: env_var("CIFS_REAL_SERVER")?,
            share: env_var("CIFS_REAL_SHARE")?,
            root: (!root.is_empty()).then(|| root.to_owned()),
            ensure_dir,
            username: env_var("CIFS_REAL_USER")?,
            password: env_var("CIFS_REAL_PASS")?,
            signing_policy: CifsSigningPolicy::default(),
            guest_policy: cifs_guest_policy(env::var("CIFS_GUEST_POLICY").ok().as_deref())?,
        })
    } else if endpoint.split_once("://").is_some_and(|(scheme, _)| {
        scheme.eq_ignore_ascii_case("s3") || scheme.to_ascii_lowercase().starts_with("s3+")
    }) {
        BackendConfig::S3(S3BackendConfig {
            url: endpoint.to_owned(),
            block_size,
            single_put_threshold: None,
        })
    } else if let Some(prefix) = endpoint.strip_prefix("s3:") {
        BackendConfig::S3(S3BackendConfig {
            url: s3_url(None, prefix)?,
            block_size,
            single_put_threshold: None,
        })
    } else if let Some(rest) = endpoint.strip_prefix("s3@") {
        let (bucket, prefix) = rest.split_once(':').ok_or("give `s3@<bucket>:<prefix>`")?;
        BackendConfig::S3(S3BackendConfig {
            url: s3_url(Some(bucket), prefix)?,
            block_size,
            single_put_threshold: None,
        })
    } else if endpoint.starts_with("hdfs://") {
        BackendConfig::Hdfs(HdfsBackendConfig {
            location: endpoint.to_owned(),
            client: hdfs_client(),
            block_size: None,
            ensure_dir,
        })
    } else {
        if ensure_dir {
            fs::create_dir_all(endpoint)?;
        }
        let concurrency = data_mover::TransferConcurrency::from_env(
            BackendKind::Local,
            data_mover::TransferConcurrency::new(4, 2)?,
        )?;
        BackendConfig::Local(LocalBackendConfig {
            read_chunk_bytes: block_size
                .and_then(|bytes| usize::try_from(bytes).ok())
                .and_then(NonZeroUsize::new),
            write_chunk_bytes: None,
            root: endpoint.into(),
            read_concurrency: NonZeroUsize::new(concurrency.read()).ok_or("non-zero")?,
            write_concurrency: NonZeroUsize::new(concurrency.write()).ok_or("non-zero")?,
        })
    };
    Ok(connect_backend(config).await?)
}

/// Names starting with `.data-mover-` in `dir` of `endpoint`: what a transfer left next to its final
/// file. Every listing data-mover offers hides them (ADR-0006 C3b), so this reads below the storage
/// layer — a raw NFS mount, SMB directory query, HDFS `list_status`, or `std::fs` — and sees exactly
/// what is on the server. S3 is listed by its bucket listing instead.
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
    // SMB and HDFS need no separate pass: deleting a directory removes the artifacts in it (the
    // CIFS namespace sweeps them, HDFS deletes recursively).
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
    if let Some(root) = endpoint.strip_prefix("smb:") {
        return smb_names(root, dir).await;
    }
    if endpoint.starts_with("hdfs://") {
        let (client, location) = data_mover::storage::build_hdfs_client(endpoint, &hdfs_client())?;
        let path = format!(
            "{}/{}",
            location.root().trim_end_matches('/'),
            dir.trim_matches('/')
        );
        let statuses = client.list_status(&path, false).await?;
        return Ok(statuses
            .iter()
            .filter_map(|status| status.path.rsplit('/').next().map(str::to_owned))
            .collect());
    }
    if endpoint.starts_with("s3:") || endpoint.starts_with("s3@") {
        return Err("list S3 artifacts with the bucket listing".into());
    }
    Ok(fs::read_dir(Path::new(endpoint).join(dir))?
        .map(|entry| entry.map(|entry| entry.file_name().to_string_lossy().into_owned()))
        .collect::<io::Result<Vec<_>>>()?)
}

/// The names in `dir` below the share sub-path `root`, read with a plain SMB directory query.
async fn smb_names(root: &str, dir: &str) -> Result<Vec<String>> {
    let client = smb_domain::Client::with_policies(
        smb_domain::SigningPolicy::WhenRequired,
        smb_domain::GuestPolicy::Deny,
    );
    let target =
        smb_domain::ShareTarget::new(&env_var("CIFS_REAL_SERVER")?, &env_var("CIFS_REAL_SHARE")?)?;
    let share = client
        .connect_share(
            &target,
            smb_domain::Credentials::ntlm(env_var("CIFS_REAL_USER")?, env_var("CIFS_REAL_PASS")?),
        )
        .await?;
    let path = join(root, dir).replace('/', "\\");
    let path = smb_domain::SharePath::new(if path.is_empty() {
        ".".to_owned()
    } else {
        path
    })?;
    let directory = share
        .open_directory(&path, smb_domain::DirectoryOpenOptions::open_existing())
        .await?;
    let entries = directory.entries("*").try_collect::<Vec<_>>().await;
    directory.close().await?;
    Ok(entries?
        .iter()
        .map(|entry| entry.name().to_owned())
        .filter(|name| !matches!(name.as_str(), "." | ".."))
        .collect())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guest_access_is_denied_unless_explicitly_selected() -> Result {
        assert_eq!(cifs_guest_policy(None)?, CifsGuestPolicy::Deny);
        assert_eq!(cifs_guest_policy(Some("deny"))?, CifsGuestPolicy::Deny);
        assert_eq!(
            cifs_guest_policy(Some("allow_unsigned"))?,
            CifsGuestPolicy::AllowUnsigned,
        );
        assert!(cifs_guest_policy(Some("true")).is_err());
        assert!(cifs_guest_policy(Some("")).is_err());
        Ok(())
    }
}
