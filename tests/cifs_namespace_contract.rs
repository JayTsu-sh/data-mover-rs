//! Optional real SMB namespace-role contract, confined to unique entry names.
mod common;

use data_mover::model::{
    BackendIdentity, BackendKind, EntryKind, FailureClass, IdentityStrength, StoragePath,
};
use data_mover::storage::{
    BackendConfig, CifsBackendConfig, CifsGuestPolicy, CifsSigningPolicy, Namespace,
    NamespaceRequest, NamespaceResult, PreflightPolicy, Storage, StorageRoleFailure,
    connect_backend,
};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an explicitly configured writable CIFS share"]
async fn real_share_namespace_verbs_roundtrip() -> Result {
    common::init_tracing();
    let server = std::env::var("CIFS_REAL_SERVER")?;
    let share_name = std::env::var("CIFS_REAL_SHARE")?;
    let client = smb_client();
    let share = client
        .connect_share(
            &smb_domain::ShareTarget::new(&server, &share_name)?,
            smb_domain::Credentials::ntlm(raw_username()?, std::env::var("CIFS_REAL_PASS")?),
        )
        .await?;
    let storage = connect_remote(&server, &share_name).await?;
    let namespace = storage.namespace(&PreflightPolicy::production())?;
    let dir = format!("dm-cifs-ns-{}", uuid::Uuid::new_v4().simple());
    let result = exercise(namespace.as_ref(), &share, &dir).await;
    let cleanup = cleanup(namespace.as_ref(), &dir).await;
    let _ = client.close().await;
    result?;
    cleanup
}

async fn exercise(namespace: &dyn Namespace, share: &smb_domain::Share, dir: &str) -> Result {
    let a = format!("{dir}/a.bin");
    let b = format!("{dir}/b.bin");
    let c = format!("{dir}/c.bin");
    exercise_directory(namespace, dir).await?;
    create_empty_file(share, &a).await?;
    exercise_listing(namespace, dir, &a).await?;
    exercise_rename(namespace, &a, &b).await?;
    create_empty_file(share, &c).await?;
    exercise_rename(namespace, &b, &c).await?;
    exercise_refusals(namespace, dir).await?;
    exercise_delete(namespace, dir, &c).await
}

async fn exercise_directory(namespace: &dyn Namespace, dir: &str) -> Result {
    completed(
        namespace
            .execute(NamespaceRequest::CreateDirectory(path(dir)?))
            .await?,
    )?;
    let stat = single(
        namespace
            .execute(NamespaceRequest::Stat(path(dir)?))
            .await?,
    )?;
    assert_eq!(
        stat.kind,
        EntryKind::Directory,
        "created directory stats as a directory"
    );
    assert_eq!(stat.path.as_str(), dir);
    expect_class(
        namespace
            .execute(NamespaceRequest::CreateDirectory(path(dir)?))
            .await,
        FailureClass::Conflict,
        "second CreateDirectory on an existing directory",
    )
}

async fn exercise_listing(namespace: &dyn Namespace, dir: &str, file: &str) -> Result {
    let listed = entries(
        namespace
            .execute(NamespaceRequest::List(path(dir)?))
            .await?,
    )?;
    assert_eq!(listed.len(), 1, "List returns exactly the one fixture file");
    assert_eq!(listed[0].path.as_str(), file);
    assert_eq!(listed[0].kind, EntryKind::File);
    assert_eq!(listed[0].size, Some(0));
    assert!(
        listed[0].inline_timestamps().is_some(),
        "the listing carries the timestamps its QUERY_DIRECTORY records already returned"
    );
    // A transfer validates a traversal observation against a fresh describe, so the identity a
    // listing produces has to equal the identity Stat produces for the same unchanged entry.
    let stat = single(
        namespace
            .execute(NamespaceRequest::Stat(path(file)?))
            .await?,
    )?;
    assert_eq!(
        listed[0].source_identity.identity_key(),
        stat.source_identity.identity_key(),
        "List and Stat must agree on identity for an unchanged entry"
    );
    // The server must have answered with a file id on both paths — the wide directory class
    // for List and the QFid create context for Stat — or rename detection silently degrades to
    // path joins. Print which one we got so a real-server run leaves evidence either way.
    println!(
        "[identity] strength={:?} (List) / {:?} (Stat)",
        listed[0].source_identity.strength(),
        stat.source_identity.strength()
    );
    // The connect-time probe turns file ids off on *both* paths when either half is missing, so a
    // PathScoped result here does not say which half failed: look for the connect-time warn
    // ("file id available on only one path" / "nothing to judge" / "probe failed") to tell
    // them apart.
    assert_eq!(
        listed[0].source_identity.strength(),
        IdentityStrength::StableWithinBackend,
        "session is not using file ids (see the connect-time identity-probe warning): the wide \
         directory class or the QFid create context was unavailable"
    );
    assert_eq!(
        stat.source_identity.strength(),
        IdentityStrength::StableWithinBackend,
        "session is not using file ids (see the connect-time identity-probe warning)"
    );
    let directory = single(
        namespace
            .execute(NamespaceRequest::Stat(path(dir)?))
            .await?,
    )?;
    assert_eq!(directory.kind, EntryKind::Directory);
    Ok(())
}

/// Rename moves `from` onto `to`, replacing an existing destination like the NFS and HDFS roles.
async fn exercise_rename(namespace: &dyn Namespace, from: &str, to: &str) -> Result {
    let before = single(
        namespace
            .execute(NamespaceRequest::Stat(path(from)?))
            .await?,
    )?;
    completed(
        namespace
            .execute(NamespaceRequest::Rename {
                from: path(from)?,
                to: path(to)?,
            })
            .await?,
    )?;
    let after = single(namespace.execute(NamespaceRequest::Stat(path(to)?)).await?)?;
    assert_eq!(
        before.source_identity.identity_key(),
        after.source_identity.identity_key(),
        "a rename must not change the identity — this is the property rename detection relies on"
    );
    expect_class(
        namespace.execute(NamespaceRequest::Stat(path(from)?)).await,
        FailureClass::NotFound,
        "Stat of the renamed-away source",
    )?;
    assert_eq!(
        single(namespace.execute(NamespaceRequest::Stat(path(to)?)).await?)?.kind,
        EntryKind::File
    );
    Ok(())
}

async fn exercise_refusals(namespace: &dyn Namespace, dir: &str) -> Result {
    let moved = format!("{dir}-moved");
    completed(
        namespace
            .execute(NamespaceRequest::Rename {
                from: path(dir)?,
                to: path(&moved)?,
            })
            .await?,
    )?;
    expect_class(
        namespace.execute(NamespaceRequest::Stat(path(dir)?)).await,
        FailureClass::NotFound,
        "Stat of the renamed-away directory",
    )?;
    completed(
        namespace
            .execute(NamespaceRequest::Rename {
                from: path(&moved)?,
                to: path(dir)?,
            })
            .await?,
    )?;
    assert_eq!(
        single(
            namespace
                .execute(NamespaceRequest::Stat(path(dir)?))
                .await?
        )?
        .kind,
        EntryKind::Directory,
        "directory renamed back keeps its kind"
    );
    expect_class(
        namespace
            .execute(NamespaceRequest::ReadLink(path(dir)?))
            .await,
        FailureClass::Unsupported,
        "ReadLink",
    )
}

async fn exercise_delete(namespace: &dyn Namespace, dir: &str, file: &str) -> Result {
    completed(
        namespace
            .execute(NamespaceRequest::Delete(path(file)?))
            .await?,
    )?;
    expect_class(
        namespace
            .execute(NamespaceRequest::Delete(path(file)?))
            .await,
        FailureClass::NotFound,
        "Delete of an already-deleted file",
    )?;
    completed(
        namespace
            .execute(NamespaceRequest::Delete(path(dir)?))
            .await?,
    )?;
    expect_class(
        namespace.execute(NamespaceRequest::Stat(path(dir)?)).await,
        FailureClass::NotFound,
        "Stat of the deleted directory",
    )
}

async fn cleanup(namespace: &dyn Namespace, dir: &str) -> Result {
    let Ok(NamespaceResult::Entries(entries)) =
        namespace.execute(NamespaceRequest::List(path(dir)?)).await
    else {
        return Ok(());
    };
    for entry in entries {
        let _ = namespace
            .execute(NamespaceRequest::Delete(entry.path))
            .await;
    }
    let _ = namespace
        .execute(NamespaceRequest::Delete(path(dir)?))
        .await;
    let _ = namespace
        .execute(NamespaceRequest::Delete(path(&format!("{dir}-moved"))?))
        .await;
    Ok(())
}

async fn connect_remote(server: &str, share: &str) -> Result<Storage> {
    Ok(connect_backend(BackendConfig::Cifs(CifsBackendConfig {
        signing_policy: CifsSigningPolicy::default(),
        guest_policy: guest_policy(),
        server: server.to_owned(),
        share: share.to_owned(),
        root: None,
        ensure_dir: false,
        username: std::env::var("CIFS_REAL_USER")?,
        password: std::env::var("CIFS_REAL_PASS")?,
        identity: BackendIdentity::new(BackendKind::Cifs, format!("{server}/{share}"))?,
    }))
    .await?)
}

async fn create_empty_file(share: &smb_domain::Share, path: &str) -> Result {
    let file = share
        .open_file(
            &smb_domain::SharePath::new(path.replace('/', "\\"))?,
            smb_domain::FileOpenOptions::create_new(),
        )
        .await?;
    match file.close().await? {
        smb_domain::CloseOutcome::Confirmed | smb_domain::CloseOutcome::AlreadyClosed => Ok(()),
        smb_domain::CloseOutcome::OutcomeUnknown => Err("SMB close outcome unknown".into()),
    }
}

fn path(value: &str) -> Result<StoragePath> {
    Ok(StoragePath::new(value)?)
}

fn completed(result: NamespaceResult) -> Result {
    match result {
        NamespaceResult::Completed => Ok(()),
        other => Err(format!("expected Completed, got {other:?}").into()),
    }
}

fn entries(result: NamespaceResult) -> Result<Vec<data_mover::storage::SourceDescriptor>> {
    match result {
        NamespaceResult::Entries(entries) => Ok(entries),
        other => Err(format!("expected Entries, got {other:?}").into()),
    }
}

fn single(result: NamespaceResult) -> Result<data_mover::storage::SourceDescriptor> {
    let mut entries = entries(result)?;
    if entries.len() == 1 {
        Ok(entries.remove(0))
    } else {
        Err(format!("expected one entry, got {}", entries.len()).into())
    }
}

fn class(failure: &StorageRoleFailure) -> FailureClass {
    match failure {
        StorageRoleFailure::Entry(failure) => failure.class(),
        StorageRoleFailure::Session(failure) => failure.class(),
    }
}

fn expect_class(
    outcome: std::result::Result<NamespaceResult, StorageRoleFailure>,
    expected: FailureClass,
    what: &str,
) -> Result {
    match outcome {
        Err(failure) if class(&failure) == expected => Ok(()),
        Err(failure) => Err(format!(
            "{what}: expected {expected:?}, got {:?} ({failure:?})",
            class(&failure)
        )
        .into()),
        Ok(result) => Err(format!("{what}: expected {expected:?}, got {result:?}").into()),
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

/// Raw smb-domain connections in these tests mirror the factory: an empty username under
/// `allow-unsigned` becomes the placeholder identity that reaches the server's guest mapping.
fn raw_username() -> std::result::Result<String, std::env::VarError> {
    std::env::var("CIFS_REAL_USER").map(|user| raw_username_of(&user))
}

fn raw_username_of(user: &str) -> String {
    if user.is_empty() && guest_policy() == CifsGuestPolicy::AllowUnsigned {
        "anonymous".to_owned()
    } else {
        user.to_owned()
    }
}
