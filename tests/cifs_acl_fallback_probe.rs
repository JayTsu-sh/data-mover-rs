//! Read-only verification of one isolated file after an ACL fallback transfer.
//!
//! Set `CIFS_REAL_{SERVER,SHARE,USER,PASS}` and `CIFS_ACL_PROBE_PATH` (share-relative,
//! under data-mover-test/ or dm-meta-<run>/). This test never creates or changes a file.
//! The SDK filters some trustee types when querying security: the ACE count is the
//! facade's observation, not an independent raw-wire count of every native ACE.
//! A denied open requests `GENERIC_READ`; it is not a per-right authorization proof.

use smb_domain::{Client, Error, Resource, Share, SharePath};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires one explicitly configured isolated CIFS fallback file"]
async fn probe_exact_path_protected_empty_dacl() -> Result {
    // Do not install tracing: credentials and raw SMB frames must not enter this report.
    let server = required("CIFS_REAL_SERVER")?;
    let share_name = required("CIFS_REAL_SHARE")?;
    let user = required("CIFS_REAL_USER")?;
    let pass = required("CIFS_REAL_PASS")?;
    let path = required("CIFS_ACL_PROBE_PATH")?;
    let path = data_mover::model::StoragePath::new(path).map_err(|_| "invalid probe path")?;
    let root = path.as_str().split('/').next().unwrap_or("");
    if !(path.as_str().starts_with("data-mover-test/")
        || root.starts_with("dm-meta-") && path.as_str().contains('/'))
    {
        return Err("probe path must be inside an isolated test directory".into());
    }
    let target =
        smb_domain::ShareTarget::new(&server, &share_name).map_err(|_| "invalid CIFS target")?;
    let client = Client::with_policies(
        smb_domain::SigningPolicy::WhenRequired,
        smb_domain::GuestPolicy::Deny,
    );
    let share = client
        .connect_share(&target, smb_domain::Credentials::ntlm(user, pass))
        .await
        .map_err(|_| "CIFS connection failed; credentials and protocol diagnostics omitted")?;
    let result = inspect(&share, path.as_str()).await;
    let closed = client.close().await;
    result?;
    closed.map_err(|_| "CIFS client close failed")?;
    Ok(())
}

async fn inspect(share: &Share, path: &str) -> Result {
    let path = SharePath::new(path).map_err(|_| "invalid share-relative probe path")?;
    let resource = share
        .open_security(&path, smb_domain::SecurityOpenOptions::default())
        .await
        .map_err(|_| "security open failed; DACL remains unverified")?;
    let descriptor = resource
        .query_security(smb_domain::SecuritySelection::default().dacl(true))
        .await;
    let closed = close_resource(resource).await;
    let descriptor = descriptor.map_err(|_| "DACL query failed; DACL remains unverified")?;
    closed.map_err(|_| "security handle close failed")?;
    let protected = descriptor.control.dacl_protected();
    let present = descriptor.control.dacl_present();
    let ace_count = descriptor.dacl.as_ref().map(|acl| acl.ace.len());
    let denied = read_denied(share, &path).await?;
    println!(
        "[acl-probe] protected={protected} present={present} \
         observed_ace_count={ace_count:?} generic_read_denied={denied}"
    );
    assert!(protected, "observed DACL must be protected");
    assert!(present, "observed DACL must be present");
    assert_eq!(
        ace_count,
        Some(0),
        "observed DACL must be empty, never NULL"
    );
    assert!(
        denied,
        "the configured principal must not read through an ordinary open"
    );
    Ok(())
}

async fn read_denied(share: &Share, path: &SharePath) -> Result<bool> {
    let file = match share
        .open_file(path, smb_domain::FileOpenOptions::open_existing())
        .await
    {
        Ok(file) => file,
        Err(error) if is_access_denied(&error) => return Ok(true),
        Err(_) => return Err("data open failed for a reason other than ACCESS_DENIED".into()),
    };
    let read = file.read_at(0, 1).await;
    let closed = file.close().await;
    closed.map_err(|_| "data handle close failed")?;
    match read {
        Ok(_) => Ok(false),
        Err(error) if is_access_denied(&error) => Ok(true),
        Err(_) => Err("data read failed for a reason other than ACCESS_DENIED".into()),
    }
}

fn is_access_denied(error: &Error) -> bool {
    matches!(
        error,
        Error::UnexpectedMessageStatus(0xc000_0022) | Error::ReceivedErrorMessage(0xc000_0022, _)
    )
}

async fn close_resource(resource: Resource) -> smb_domain::Result<()> {
    match resource {
        Resource::File(file) => file.close().await.map(|_| ()),
        Resource::Directory(directory) => directory.close().await.map(|_| ()),
        Resource::Pipe(pipe) => pipe.close().await.map(|_| ()),
    }
}

fn required(name: &'static str) -> Result<String> {
    std::env::var(name).map_err(|_| format!("missing environment variable {name}").into())
}
