//! Optional real SMB namespace-role contract, confined to unique entry names.
mod common;

use std::num::NonZeroUsize;

use data_mover::model::{EntryKind, FailureClass, IdentityStrength, StoragePath};
use data_mover::storage::{
    BackendConfig, CifsBackendConfig, CifsGuestPolicy, CifsSigningPolicy, CollectLimits,
    ListRequest, ListedChild, ListingPage, Namespace, PreflightPolicy, SourceDescriptor, Storage,
    StorageRoleFailure, collect_listing, connect_backend,
};
use futures::StreamExt as _;
use tokio_util::sync::CancellationToken;

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
    exercise_paging(namespace, share, dir).await?;
    exercise_delete(namespace, dir, &c).await
}

async fn exercise_directory(namespace: &dyn Namespace, dir: &str) -> Result {
    namespace.create_directory(&path(dir)?).await?;
    let stat = namespace.stat(&path(dir)?).await?;
    assert_eq!(
        stat.kind,
        EntryKind::Directory,
        "created directory stats as a directory"
    );
    assert_eq!(stat.path.as_str(), dir);
    expect_class(
        namespace.create_directory(&path(dir)?).await,
        FailureClass::Conflict,
        "second create_directory on an existing directory",
    )
}

async fn exercise_listing(namespace: &dyn Namespace, dir: &str, file: &str) -> Result {
    let listed = entries(namespace, dir).await?;
    assert_eq!(listed.len(), 1, "list returns exactly the one fixture file");
    assert_eq!(listed[0].path.as_str(), file);
    assert_eq!(listed[0].kind, EntryKind::File);
    assert_eq!(listed[0].size, Some(0));
    assert!(
        listed[0].inline_timestamps().is_some(),
        "the listing carries the timestamps its QUERY_DIRECTORY records already returned"
    );
    // A transfer validates a traversal observation against a fresh describe, so the identity a
    // listing produces has to equal the identity stat produces for the same unchanged entry.
    let stat = namespace.stat(&path(file)?).await?;
    assert_eq!(
        listed[0].source_identity.identity_key(),
        stat.source_identity.identity_key(),
        "list and stat must agree on identity for an unchanged entry"
    );
    // The server must have answered with a file id on both paths — the wide directory class
    // for list and the QFid create context for stat — or rename detection silently degrades to
    // path joins. Print which one we got so a real-server run leaves evidence either way.
    println!(
        "[identity] strength={:?} (list) / {:?} (stat)",
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
    let directory = namespace.stat(&path(dir)?).await?;
    assert_eq!(directory.kind, EntryKind::Directory);
    Ok(())
}

/// Rename moves `from` onto `to`, replacing an existing destination like the NFS and HDFS roles.
async fn exercise_rename(namespace: &dyn Namespace, from: &str, to: &str) -> Result {
    let before = namespace.stat(&path(from)?).await?;
    namespace.rename(&path(from)?, &path(to)?).await?;
    let after = namespace.stat(&path(to)?).await?;
    assert_eq!(
        before.source_identity.identity_key(),
        after.source_identity.identity_key(),
        "a rename must not change the identity — this is the property rename detection relies on"
    );
    expect_class(
        namespace.stat(&path(from)?).await,
        FailureClass::NotFound,
        "stat of the renamed-away source",
    )?;
    assert_eq!(namespace.stat(&path(to)?).await?.kind, EntryKind::File);
    Ok(())
}

async fn exercise_refusals(namespace: &dyn Namespace, dir: &str) -> Result {
    let moved = format!("{dir}-moved");
    namespace.rename(&path(dir)?, &path(&moved)?).await?;
    expect_class(
        namespace.stat(&path(dir)?).await,
        FailureClass::NotFound,
        "stat of the renamed-away directory",
    )?;
    namespace.rename(&path(&moved)?, &path(dir)?).await?;
    assert_eq!(
        namespace.stat(&path(dir)?).await?.kind,
        EntryKind::Directory,
        "directory renamed back keeps its kind"
    );
    expect_class(
        namespace.read_link(&path(dir)?).await,
        FailureClass::Unsupported,
        "read_link",
    )
}

/// A listing arrives in pages of at most the requested size, naming each child once; a listing
/// dropped after its first page leaves nothing open that would keep the directory from being
/// deleted.
async fn exercise_paging(
    namespace: &dyn Namespace,
    share: &smb_domain::Share,
    dir: &str,
) -> Result {
    let paged = format!("{dir}/paged");
    namespace.create_directory(&path(&paged)?).await?;
    let names: Vec<String> = (0..5).map(|index| format!("p{index}.bin")).collect();
    for name in &names {
        create_empty_file(share, &format!("{paged}/{name}")).await?;
    }
    let two = NonZeroUsize::new(2).ok_or("zero page size")?;
    let request = ListRequest::new(path(&paged)?, CancellationToken::new()).with_page_hint(two);
    let delivered: Vec<ListingPage> = namespace
        .list(request)
        .map(|page| page.map_err(|error| error.to_string()))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<std::result::Result<_, _>>()?;
    let sizes: Vec<usize> = delivered.iter().map(|page| page.children.len()).collect();
    assert_eq!(sizes, [2, 2, 1], "pages of at most the hint");
    let mut listed: Vec<&str> = delivered
        .iter()
        .flat_map(|page| page.children.iter().map(ListedChild::name))
        .collect();
    listed.sort_unstable();
    assert_eq!(listed, names);

    let request = ListRequest::new(path(&paged)?, CancellationToken::new()).with_page_hint(two);
    let mut stream = namespace.list(request);
    let first = stream.next().await.ok_or("the listing ended at once")??;
    assert_eq!(first.children.len(), 2);
    drop(stream);
    for name in &names {
        namespace.delete(&path(&format!("{paged}/{name}"))?).await?;
    }
    namespace.delete(&path(&paged)?).await?;
    expect_class(
        namespace.stat(&path(&paged)?).await,
        FailureClass::NotFound,
        "stat of the deleted paged directory",
    )
}

async fn exercise_delete(namespace: &dyn Namespace, dir: &str, file: &str) -> Result {
    namespace.delete(&path(file)?).await?;
    expect_class(
        namespace.delete(&path(file)?).await,
        FailureClass::NotFound,
        "delete of an already-deleted file",
    )?;
    namespace.delete(&path(dir)?).await?;
    expect_class(
        namespace.stat(&path(dir)?).await,
        FailureClass::NotFound,
        "stat of the deleted directory",
    )
}

async fn cleanup(namespace: &dyn Namespace, dir: &str) -> Result {
    // A partial listing still names every child it could describe: delete those, one level of
    // subdirectories included.
    for sub in [format!("{dir}/paged"), dir.to_owned()] {
        let Ok(children) = entries(namespace, &sub).await else {
            continue;
        };
        for entry in children {
            let _ = namespace.delete(&entry.path).await;
        }
        let _ = namespace.delete(&path(&sub)?).await;
    }
    let _ = namespace.delete(&path(&format!("{dir}-moved"))?).await;
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

/// Every child of `dir` a whole listing describes, with full paths; a child it could not
/// describe fails the call.
async fn entries(namespace: &dyn Namespace, dir: &str) -> Result<Vec<SourceDescriptor>> {
    let directory = path(dir)?;
    let request = ListRequest::new(directory.clone(), CancellationToken::new());
    let page = collect_listing(namespace.list(request), CollectLimits::UNBOUNDED).await?;
    let (entries, failures) = page.into_descriptors(&directory);
    if failures.is_empty() {
        Ok(entries)
    } else {
        Err(format!("children could not be described: {failures:?}").into())
    }
}

fn class(failure: &StorageRoleFailure) -> FailureClass {
    match failure {
        StorageRoleFailure::Entry(failure) => failure.class(),
        StorageRoleFailure::Session(failure) => failure.class(),
    }
}

fn expect_class<T: std::fmt::Debug>(
    outcome: std::result::Result<T, StorageRoleFailure>,
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
