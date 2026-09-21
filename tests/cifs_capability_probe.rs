//! Real-environment capability probe: evidence for keeping or dropping legacy CIFS features.
//!
//! Every probe prints one `[probe]` line and never fails on an unsupported outcome; only
//! setup failures (no server, bad primary credentials) fail the test. Run with `--nocapture`.
mod common;

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use data_mover::model::{BackendIdentity, BackendKind, FailureClass, StoragePath};
use data_mover::storage::{
    BackendConfig, CifsBackendConfig, CifsGuestPolicy, CifsSigningPolicy, NamespaceRequest,
    PreflightPolicy, StorageRoleFailure, connect_backend,
};
use futures::TryStreamExt as _;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

struct Endpoint {
    server: String,
    share: String,
    user: String,
    pass: String,
    location: smb_domain::ShareTarget,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an explicitly configured CIFS share"]
async fn probe_real_share_capabilities() -> Result {
    common::init_tracing();
    let server = std::env::var("CIFS_REAL_SERVER")?;
    let share = std::env::var("CIFS_REAL_SHARE")?;
    let target = Endpoint {
        location: smb_domain::ShareTarget::new(&server, &share)?,
        server,
        share,
        user: std::env::var("CIFS_REAL_USER")?,
        pass: std::env::var("CIFS_REAL_PASS")?,
    };
    probe_credentials(&target).await;
    probe_signing(&target).await;
    let client = smb_client();
    let share = client
        .connect_share(
            &target.location,
            smb_domain::Credentials::ntlm(raw_username_of(&target.user), &target.pass),
        )
        .await?;
    let result = async {
        probe_root_listing(&share).await?;
        probe_clock_and_precision(&share).await?;
        probe_acl(&share).await?;
        probe_symlink(&share, &target).await?;
        probe_namespace_role(&target).await
    }
    .await;
    let _ = client.close().await;
    result
}

/// Credential variants the legacy `anon` URL option used to cover.
async fn probe_credentials(target: &Endpoint) {
    for (label, u, p) in [
        ("anonymous (empty user + empty password)", "", ""),
        ("guest account, empty password", "guest", ""),
        ("named user, empty password", target.user.as_str(), ""),
        (
            "named user, wrong password",
            target.user.as_str(),
            "definitely-wrong",
        ),
    ] {
        // Guest sessions are unsigned; allow them here so the server's own verdict shows.
        let client = smb_domain::Client::with_policies(
            smb_domain::SigningPolicy::WhenRequired,
            smb_domain::GuestPolicy::AllowUnsigned,
        );
        let outcome = client
            .connect_share(&target.location, smb_domain::Credentials::ntlm(u, p))
            .await;
        println!(
            "[probe] auth {label}: {}",
            match &outcome {
                Ok(_) => "ACCEPTED".to_owned(),
                Err(error) => format!("rejected ({error})"),
            }
        );
        drop(outcome);
        let _ = client.close().await;
    }
}

/// Server-required signing shows up as a failure under `WhenRequired`-only expectations.
async fn probe_signing(target: &Endpoint) {
    for (label, policy) in [
        ("WhenRequired", smb_domain::SigningPolicy::WhenRequired),
        ("Required", smb_domain::SigningPolicy::Required),
    ] {
        let client = smb_domain::Client::with_policies(
            policy,
            match guest_policy() {
                CifsGuestPolicy::AllowUnsigned => smb_domain::GuestPolicy::AllowUnsigned,
                CifsGuestPolicy::Deny => smb_domain::GuestPolicy::Deny,
            },
        );
        let outcome = client
            .connect_share(
                &target.location,
                smb_domain::Credentials::ntlm(raw_username_of(&target.user), &target.pass),
            )
            .await;
        println!(
            "[probe] signing {label}: {}",
            match &outcome {
                Ok(_) => "connected".to_owned(),
                Err(error) => format!("failed ({error})"),
            }
        );
        drop(outcome);
        let _ = client.close().await;
    }
}

async fn probe_root_listing(share: &smb_domain::Share) -> Result {
    let started = Instant::now();
    let root = share
        .open_directory(
            &smb_domain::SharePath::new(".")?,
            smb_domain::DirectoryOpenOptions::open_existing(),
        )
        .await?;
    let entries = root.entries("*").try_collect::<Vec<_>>().await;
    let _ = root.close().await;
    let entries = entries?;
    println!(
        "[probe] root listing: {} entries ({} directories) in {} ms",
        entries.len(),
        entries.iter().filter(|entry| entry.is_directory()).count(),
        started.elapsed().as_millis()
    );
    Ok(())
}

/// Server clock skew and timestamp precision as seen through the facade.
async fn probe_clock_and_precision(share: &smb_domain::Share) -> Result {
    let name = format!("dm-cifs-probe-{}", uuid::Uuid::new_v4().simple());
    let file = share
        .open_file(
            &smb_domain::SharePath::new(&name)?,
            smb_domain::FileOpenOptions::create_new(),
        )
        .await?;
    let local_now = SystemTime::now();
    let metadata = file.metadata().await;
    let deleted = file.delete().await;
    let _ = file.close().await;
    deleted?;
    let written = metadata?.written().duration_since(UNIX_EPOCH)?;
    let local = local_now.duration_since(UNIX_EPOCH)?;
    let skew_ms = i128::try_from(written.as_millis())? - i128::try_from(local.as_millis())?;
    println!(
        "[probe] server clock skew vs local: {skew_ms} ms; written timestamp sub-100ns remainder: {} \
         (facade exposes timestamps, len, readonly/reparse attributes and the QFid file id; no owner or POSIX mode)",
        written.subsec_nanos() % 100
    );
    Ok(())
}

async fn probe_acl(share: &smb_domain::Share) -> Result {
    let name = format!("dm-cifs-probe-acl-{}", uuid::Uuid::new_v4().simple());
    let file = share
        .open_file(
            &smb_domain::SharePath::new(&name)?,
            smb_domain::FileOpenOptions::create_new(),
        )
        .await?;
    let _ = file.close().await;
    let resource = share
        .open_security(
            &smb_domain::SharePath::new(&name)?,
            smb_domain::SecurityOpenOptions::default(),
        )
        .await?;
    let descriptor = resource
        .query_security(smb_domain::SecuritySelection::default().dacl(true))
        .await;
    let _ = match resource {
        smb_domain::Resource::File(file) => file.close().await.map(|_| ()),
        smb_domain::Resource::Directory(directory) => directory.close().await.map(|_| ()),
        smb_domain::Resource::Pipe(pipe) => pipe.close().await.map(|_| ()),
    };
    println!(
        "[probe] ACL query: {}",
        match &descriptor {
            Ok(descriptor) => {
                let text = format!("{descriptor:?}");
                let shown: String = text.chars().take(160).collect();
                format!("ok, {} chars of descriptor: {shown}", text.len())
            }
            Err(error) => format!("failed ({error})"),
        }
    );
    let file = share
        .open_file(
            &smb_domain::SharePath::new(&name)?,
            smb_domain::FileOpenOptions::open_existing(),
        )
        .await?;
    let _ = file.delete().await;
    let _ = file.close().await;
    Ok(())
}

/// How a UNIX symlink created out-of-band (`CIFS_PROBE_SYMLINK`, share-relative) looks
/// through the facade: listing kind/len, `open` outcome, and the role's `Stat`.
async fn probe_symlink(share: &smb_domain::Share, target: &Endpoint) -> Result {
    let fixture = NfsSymlinkFixture::create().await?;
    let Some(name) = fixture
        .as_ref()
        .map(|fixture| fixture.name.clone())
        .or_else(|| std::env::var("CIFS_PROBE_SYMLINK").ok())
    else {
        println!("[probe] symlink: neither CIFS_PROBE_NFS_URL nor CIFS_PROBE_SYMLINK set, skipped");
        return Ok(());
    };
    let result = probe_symlink_named(share, target, &name).await;
    if let Some(fixture) = fixture {
        fixture.remove().await;
    }
    result
}

/// Creates a UNIX symlink on the same volume through the crate's user-space NFS client
/// when `CIFS_PROBE_NFS_URL` points at the CIFS share's export.
struct NfsSymlinkFixture {
    storage: data_mover::NFSStorage,
    name: String,
}

impl NfsSymlinkFixture {
    async fn create() -> Result<Option<Self>> {
        let Ok(url) = std::env::var("CIFS_PROBE_NFS_URL") else {
            return Ok(None);
        };
        let storage = data_mover::NFSStorage::new(&url, None).await?;
        let id = uuid::Uuid::new_v4().simple();
        let name = format!("dm-probe-symlink-{id}");
        let now = data_mover::time_util::now_nanos();
        storage
            .create_symlink(
                std::path::Path::new(&name),
                std::path::Path::new("dm-probe-symlink-target"),
                now,
                now,
                None,
                None,
            )
            .await?;
        println!("[probe] symlink {name}: created over NFS -> dm-probe-symlink-target");
        Ok(Some(Self { storage, name }))
    }

    async fn remove(self) {
        let _ = self
            .storage
            .delete_file(std::path::Path::new(&self.name))
            .await;
    }
}

async fn probe_symlink_named(share: &smb_domain::Share, target: &Endpoint, name: &str) -> Result {
    let name = name.to_owned();
    let root = share
        .open_directory(
            &smb_domain::SharePath::new(".")?,
            smb_domain::DirectoryOpenOptions::open_existing(),
        )
        .await?;
    let entries = root.entries("*").try_collect::<Vec<_>>().await;
    let _ = root.close().await;
    let listed = entries?
        .into_iter()
        .find(|entry| entry.name() == name)
        .map(|entry| {
            format!(
                "listed as directory={} len={}",
                entry.is_directory(),
                entry.len()
            )
        });
    println!(
        "[probe] symlink {name}: {}",
        listed.unwrap_or_else(|| "absent from root listing".to_owned())
    );
    match share.open(&smb_domain::SharePath::new(&name)?).await {
        Ok(smb_domain::Resource::File(file)) => {
            let metadata = file.metadata().await;
            let _ = file.close().await;
            println!(
                "[probe] symlink {name}: open -> File, {}",
                describe_len(metadata)
            );
        }
        Ok(smb_domain::Resource::Directory(directory)) => {
            let metadata = directory.metadata().await;
            let _ = directory.close().await;
            println!(
                "[probe] symlink {name}: open -> Directory, {}",
                describe_len(metadata)
            );
        }
        Ok(smb_domain::Resource::Pipe(pipe)) => {
            let _ = pipe.close().await;
            println!("[probe] symlink {name}: open -> Pipe");
        }
        Err(error) => println!("[probe] symlink {name}: open failed ({error})"),
    }
    let storage = connect_backend(BackendConfig::Cifs(CifsBackendConfig {
        signing_policy: CifsSigningPolicy::default(),
        guest_policy: guest_policy(),
        server: target.server.clone(),
        share: target.share.clone(),
        root: None,
        ensure_dir: false,
        username: target.user.clone(),
        password: target.pass.clone(),
        identity: BackendIdentity::new(BackendKind::Cifs, "symlink-probe")?,
    }))
    .await?;
    let namespace = storage.namespace(&PreflightPolicy::production())?;
    let stat = namespace
        .execute(NamespaceRequest::Stat(StoragePath::new(&name)?))
        .await;
    println!("[probe] symlink {name}: role Stat -> {}", describe(&stat));
    Ok(())
}

fn describe_len(metadata: smb_domain::Result<smb_domain::ResourceMetadata>) -> String {
    match metadata {
        Ok(value) => format!("metadata len={}", value.len()),
        Err(error) => format!("metadata failed ({error})"),
    }
}

/// Directory rename and `ReadLink` through the namespace role.
async fn probe_namespace_role(target: &Endpoint) -> Result {
    let storage = connect_backend(BackendConfig::Cifs(CifsBackendConfig {
        signing_policy: CifsSigningPolicy::default(),
        guest_policy: guest_policy(),
        server: target.server.clone(),
        share: target.share.clone(),
        root: None,
        ensure_dir: false,
        username: target.user.clone(),
        password: target.pass.clone(),
        identity: BackendIdentity::new(
            BackendKind::Cifs,
            format!("{}/{}", target.server, target.share),
        )?,
    }))
    .await?;
    let namespace = storage.namespace(&PreflightPolicy::production())?;
    let dir = StoragePath::new(format!(
        "dm-cifs-probe-dir-{}",
        uuid::Uuid::new_v4().simple()
    ))?;
    namespace
        .execute(NamespaceRequest::CreateDirectory(dir.clone()))
        .await?;
    let moved = StoragePath::new(format!("{}-moved", dir.as_str()))?;
    let rename = namespace
        .execute(NamespaceRequest::Rename {
            from: dir.clone(),
            to: moved.clone(),
        })
        .await;
    println!("[probe] directory rename via role: {}", describe(&rename));
    let _ = namespace.execute(NamespaceRequest::Delete(dir)).await;
    let _ = namespace.execute(NamespaceRequest::Delete(moved)).await;
    let link = namespace
        .execute(NamespaceRequest::ReadLink(StoragePath::new(".")?))
        .await;
    println!("[probe] ReadLink via role: {}", describe(&link));
    Ok(())
}

fn describe<T: std::fmt::Debug>(outcome: &std::result::Result<T, StorageRoleFailure>) -> String {
    match outcome {
        Ok(value) => format!("ok ({value:?})"),
        Err(StorageRoleFailure::Entry(failure)) => {
            let class = failure.class();
            if class == FailureClass::Unsupported {
                "Unsupported (typed refusal)".to_owned()
            } else {
                format!("{class:?}")
            }
        }
        Err(StorageRoleFailure::Session(failure)) => format!("session {:?}", failure.class()),
    }
}

/// `CIFS_REAL_GUEST_POLICY=allow-unsigned` opts the real-share tests into guest sessions.
fn guest_policy() -> CifsGuestPolicy {
    if std::env::var("CIFS_REAL_GUEST_POLICY").is_ok_and(|value| value == "allow-unsigned") {
        CifsGuestPolicy::AllowUnsigned
    } else {
        CifsGuestPolicy::Deny
    }
}

fn smb_client() -> smb_domain::Client {
    smb_domain::Client::with_policies(
        smb_domain::SigningPolicy::WhenRequired,
        match guest_policy() {
            CifsGuestPolicy::AllowUnsigned => smb_domain::GuestPolicy::AllowUnsigned,
            CifsGuestPolicy::Deny => smb_domain::GuestPolicy::Deny,
        },
    )
}

fn raw_username_of(user: &str) -> String {
    if user.is_empty() && guest_policy() == CifsGuestPolicy::AllowUnsigned {
        "anonymous".to_owned()
    } else {
        user.to_owned()
    }
}
