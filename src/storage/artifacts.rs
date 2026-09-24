//! Shared names for sibling transfer artifacts; ownership remains backend-specific.

use std::ffi::OsStr;
use std::path::{Component, Path};

/// Every transfer artifact name, on every backend, starts with this.
pub(crate) const ARTIFACT_PREFIX: &str = ".data-mover-";

/// Whether one file name is a transfer artifact (a name that is not UTF-8 never is).
pub(crate) fn is_artifact_name(name: &OsStr) -> bool {
    name.to_str()
        .is_some_and(|name| name.starts_with(ARTIFACT_PREFIX))
}

/// Whether a native path names a transfer artifact or lies inside one: any normal component is an
/// artifact name. The native counterpart of [`is_artifact_path`], for paths with platform
/// separators.
pub(crate) fn is_artifact_native(path: &Path) -> bool {
    path.components()
        .any(|component| matches!(component, Component::Normal(name) if is_artifact_name(name)))
}

/// Whether a `/`-separated path names a transfer artifact or lies inside one: any segment starts
/// with [`ARTIFACT_PREFIX`]. Listings hide these; pass the path relative to the storage root so a
/// root that itself sits inside an artifact still lists. A walk started *below* the root inside an
/// artifact (`sub_path = ".data-mover-stage"`) therefore lists nothing.
pub(crate) fn is_artifact_path(path: &str) -> bool {
    path.split('/')
        .any(|segment| segment.starts_with(ARTIFACT_PREFIX))
}

pub(crate) fn stage_name(destination: &str) -> String {
    format!(
        "{ARTIFACT_PREFIX}{}-{}.stage",
        &blake3::hash(destination.as_bytes()).to_hex()[..16],
        uuid::Uuid::new_v4().simple()
    )
}

pub(crate) fn temporary_name(checkpoint: &str) -> String {
    format!("{checkpoint}.tmp-{}", uuid::Uuid::new_v4().simple())
}

/// Returns the immutable base name, accepting an optional NFS claim suffix.
pub(crate) fn stage_base<'a>(name: &'a str, destination: &str) -> Option<&'a str> {
    let base = if let Some((base, claim)) = name.split_once(".claim-") {
        if !hex(claim, 32) {
            return None;
        }
        base
    } else {
        name
    };
    let body = base.strip_prefix(ARTIFACT_PREFIX)?.strip_suffix(".stage")?;
    let (target, id) = body.split_once('-')?;
    if !hex(target, 16)
        || !hex(id, 32)
        || target != &blake3::hash(destination.as_bytes()).to_hex()[..16]
    {
        return None;
    }
    Some(base)
}

fn hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn artifact_names_and_native_paths_match_like_the_segment_rule() {
        assert!(is_artifact_name(OsStr::new(".data-mover-0123.stage")));
        assert!(!is_artifact_name(OsStr::new("x.data-mover-y")));
        assert!(!is_artifact_name(OsStr::new(".data-mover")));
        assert!(is_artifact_native(Path::new("dir/.data-mover-stage/x")));
        assert!(is_artifact_native(Path::new(".data-mover-a.part")));
        assert!(!is_artifact_native(Path::new("dir/x.data-mover-y/z")));
        assert!(!is_artifact_native(Path::new("")));
    }

    #[test]
    fn artifact_paths_match_any_segment_but_not_lookalikes() {
        assert!(is_artifact_path(".data-mover-0123-abcd.stage"));
        assert!(is_artifact_path("dir/.data-mover-stage/"));
        assert!(is_artifact_path("dir/.data-mover-stage/binding/hash"));
        assert!(!is_artifact_path("dir/data-mover-file"));
        assert!(!is_artifact_path("dir/x.data-mover-y"));
        assert!(!is_artifact_path(".data-mover"));
        assert!(!is_artifact_path(""));
    }

    #[test]
    fn claimed_names_preserve_the_base_and_reject_wrong_destinations() {
        let base = stage_name("nested/file.bin");
        assert_ne!(base, stage_name("nested/file.bin"));
        let claimed = format!("{base}.claim-{}", "a".repeat(32));
        assert_eq!(stage_base(&claimed, "nested/file.bin"), Some(base.as_str()));
        assert_eq!(stage_base(&base, "nested/file.bin"), Some(base.as_str()));
        assert!(stage_base(&claimed, "other/file.bin").is_none());
        assert!(stage_base(&format!("{base}.claim-invalid"), "nested/file.bin").is_none());
        assert!(
            stage_base(
                &format!("{claimed}.claim-{}", "b".repeat(32)),
                "nested/file.bin"
            )
            .is_none()
        );
    }
}
