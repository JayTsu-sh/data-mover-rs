use std::error::Error;
use std::fs;
use std::sync::Arc;

use cap_std::ambient_authority;
use cap_std::fs::Dir;

#[cfg(unix)]
use super::*;
use crate::model::{ObservationPlan, RenameContext, RenamePolicy, RenameScope, StoragePath};
use crate::storage::backends::local::{
    namespace::LocalNamespace, observation::LocalObservationAdapter, source::LocalReadSource,
    test_identity,
};
use crate::storage::listing::testing::listed;
use crate::storage::{Namespace, ReadSource};
#[cfg(unix)]
use crate::time_util::checked_system_time_to_nanos;

type Result<T = ()> = std::result::Result<T, Box<dyn Error>>;

fn path(value: &str) -> StoragePath {
    StoragePath::new(value).unwrap_or_else(|error| panic!("{error}"))
}

fn open(root: &std::path::Path) -> Result<Arc<Dir>> {
    Ok(Arc::new(Dir::open_ambient_dir(root, ambient_authority())?))
}

#[cfg(unix)]
#[test]
fn rename_evidence_excludes_other_devices_and_unknown_root() -> Result {
    let temp = tempfile::tempdir()?;
    fs::write(temp.path().join("file"), b"abc")?;
    let root = open(temp.path())?;
    let metadata = root.symlink_metadata("file")?;
    let device = root_device(&root).ok_or("expected Unix root device")?;
    assert_eq!(evidence(&metadata, None), None);
    assert_eq!(evidence(&metadata, Some(device.wrapping_add(1))), None);
    let expected = metadata
        .created()
        .ok()
        .and_then(|created| checked_system_time_to_nanos(created.into_std()));
    assert_eq!(
        evidence(&metadata, Some(device)).is_some(),
        expected.is_some()
    );
    Ok(())
}

#[tokio::test]
async fn rename_evidence_agrees_across_list_stat_observe_and_read_describe() -> Result {
    let temp = tempfile::tempdir()?;
    fs::write(temp.path().join("file"), b"abc")?;
    let root = open(temp.path())?;
    let identity = test_identity("rename-role-consistency");
    let namespace = LocalNamespace::from_root(Arc::clone(&root), identity.clone());
    let adapter = LocalObservationAdapter::from_root(root, identity.clone());
    let reader =
        LocalReadSource::new(temp.path(), identity, 1).map_err(|error| format!("{error:?}"))?;
    let (entries, failures) = listed(&namespace, &StoragePath::root())
        .await
        .map_err(|error| format!("{error:?}"))?;
    assert!(failures.is_empty());
    assert_eq!(entries.len(), 1);
    let stat = namespace
        .stat(&path("file"))
        .await
        .map_err(|error| format!("{error:?}"))?;
    let observed = adapter
        .observe(path("file"))
        .await
        .map_err(|error| format!("{error:?}"))?;
    let described = reader
        .describe(&path("file"))
        .await
        .map_err(|error| format!("{error:?}"))?;
    assert_eq!(entries[0].rename_evidence(), stat.rename_evidence());
    assert_eq!(stat.rename_evidence(), observed.rename_evidence());
    assert_eq!(described.rename_evidence(), observed.rename_evidence());
    assert_eq!(entries[0].source_identity, *observed.source_identity());
    assert_eq!(adapter.optional_call_count(), 0);
    assert!(namespace.rename_context().is_none());
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn rename_evidence_survives_rename_content_change_and_hard_links() -> Result {
    let temp = tempfile::tempdir()?;
    fs::write(temp.path().join("before"), b"abc")?;
    fs::create_dir(temp.path().join("nested"))?;
    let adapter = LocalObservationAdapter::new(temp.path(), test_identity("rename-invariance"))
        .map_err(|error| format!("{error:?}"))?;
    let (before, version) = adapter
        .observe_versioned(path("before"), ObservationPlan::default())
        .await
        .map_err(|error| format!("{error:?}"))?;
    fs::rename(temp.path().join("before"), temp.path().join("nested/after"))?;
    fs::write(temp.path().join("nested/after"), b"changed content")?;
    fs::hard_link(temp.path().join("nested/after"), temp.path().join("alias"))?;
    let (after, after_version) = adapter
        .observe_versioned(path("nested/after"), ObservationPlan::default())
        .await
        .map_err(|error| format!("{error:?}"))?;
    let alias = adapter
        .observe(path("alias"))
        .await
        .map_err(|error| format!("{error:?}"))?;
    assert_eq!(before.rename_evidence(), after.rename_evidence());
    assert_eq!(after.rename_evidence(), alias.rename_evidence());
    assert_eq!(before.source_identity(), after.source_identity());
    assert_ne!(version, after_version);
    assert_eq!(after.size(), Some(15));
    assert_eq!(adapter.optional_call_count(), 0);
    Ok(())
}

#[test]
fn rename_context_binds_scope_policy_and_presence_to_listing_checkpoints() -> Result {
    let temp = tempfile::tempdir()?;
    let root = open(temp.path())?;
    let identity = test_identity("rename-checkpoint");
    let namespace = LocalNamespace::from_root(root, identity);
    let candidates = RenameContext::new(
        RenameScope::new("dataset:epoch1")?,
        RenamePolicy::CandidatesOnly,
    );
    let enabled = RenameContext::new(
        RenameScope::new("dataset:epoch1")?,
        RenamePolicy::AllowHeuristic,
    );
    let replaced = RenameContext::new(
        RenameScope::new("dataset:epoch2")?,
        RenamePolicy::AllowHeuristic,
    );
    let configured = namespace.clone().with_rename_context(Some(candidates));
    let equivalent = namespace.clone().with_rename_context(Some(candidates));
    let enabled = namespace.clone().with_rename_context(Some(enabled));
    let replaced = namespace.clone().with_rename_context(Some(replaced));
    assert_eq!(
        configured.listing_resume_key(),
        equivalent.listing_resume_key()
    );
    assert_ne!(
        namespace.listing_resume_key(),
        configured.listing_resume_key()
    );
    assert_ne!(
        configured.listing_resume_key(),
        enabled.listing_resume_key()
    );
    assert_ne!(enabled.listing_resume_key(), replaced.listing_resume_key());
    assert!(configured.rename_context().is_some());
    Ok(())
}
