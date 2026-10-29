//! Shared names for sibling transfer artifacts; ownership remains backend-specific.

use std::ffi::OsStr;
use std::path::{Component, Path};

use crate::model::StoragePath;

/// Every transfer artifact name, on every backend, starts with this.
pub const ARTIFACT_PREFIX: &str = ".data-mover-";

/// Whether one file name is a transfer artifact (a name that is not UTF-8 never is).
#[must_use]
pub fn is_artifact_name(name: &OsStr) -> bool {
    name.to_str()
        .is_some_and(|name| name.starts_with(ARTIFACT_PREFIX))
}

/// Whether a native path names a transfer artifact or lies inside one: any normal component is an
/// artifact name. The native counterpart of [`is_artifact_path`], for paths with platform
/// separators.
#[must_use]
pub fn is_artifact_native(path: &Path) -> bool {
    path.components()
        .any(|component| matches!(component, Component::Normal(name) if is_artifact_name(name)))
}

/// Whether a `/`-separated path names a transfer artifact or lies inside one: any segment starts
/// with [`ARTIFACT_PREFIX`]. Listings hide these; pass the path relative to the storage root so a
/// root that itself sits inside an artifact still lists. A walk started *below* the root inside an
/// artifact (`sub_path = ".data-mover-stage"`) therefore lists nothing.
#[must_use]
pub fn is_artifact_path(path: &str) -> bool {
    path.split('/')
        .any(|segment| segment.starts_with(ARTIFACT_PREFIX))
}

/// What one deterministic destination artifact holds (ADR-0006 "Destination artifacts").
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArtifactKind {
    /// The staged content.
    Stage,
    /// An exclusive claim on the final name.
    Claim,
    /// The pointer that holds the recovery binding.
    Pointer,
    /// An S3 multipart upload's record: the pointer object beside the final key.
    Upload,
}

impl ArtifactKind {
    pub const ALL: [Self; 4] = [Self::Stage, Self::Claim, Self::Pointer, Self::Upload];

    #[must_use]
    pub const fn suffix(self) -> &'static str {
        match self {
            Self::Stage => "stage",
            Self::Claim => "claim",
            Self::Pointer => "pointer",
            Self::Upload => "upload",
        }
    }
}

/// Hex length of the final-name digest in an artifact name: 16 bytes.
const FINAL_NAME_DIGEST_HEX: usize = 32;
const TEMPORARY_SUFFIX: &str = ".tmp";

/// The digest naming every artifact of one final file: the first 16 bytes of
/// `blake3("data-mover/artifact-name/v1\0" ‖ u64le(len) ‖ final name)`, as lowercase hex. Only the
/// file's own name goes in — artifacts sit beside it in the same parent (a directory object's,
/// named after `d/`, in its parent: [`directory_object_artifact`]) — so no process state, and no
/// random part, is needed to find them again.
#[must_use]
pub fn final_name_digest(final_name: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"data-mover/artifact-name/v1\0");
    hasher.update(&(final_name.len() as u64).to_le_bytes());
    hasher.update(final_name.as_bytes());
    hasher.finalize().to_hex()[..FINAL_NAME_DIGEST_HEX].to_string()
}

/// `.data-mover-<digest(final name)>.<kind>`: at most 55 bytes, 59 as a temporary, whatever the
/// final name's length.
#[must_use]
pub fn artifact_name(final_name: &str, kind: ArtifactKind) -> String {
    format!(
        "{ARTIFACT_PREFIX}{}.{}",
        final_name_digest(final_name),
        kind.suffix()
    )
}

/// The one temporary name an artifact is written under before it replaces the artifact. Fixed, so
/// a crash leaves nothing a later prepare cannot find; one writer per final name is guaranteed
/// by the in-process guard and the caller contract.
#[must_use]
pub fn artifact_temporary_name(final_name: &str, kind: ArtifactKind) -> String {
    format!("{}{TEMPORARY_SUFFIX}", artifact_name(final_name, kind))
}

/// The `kind` artifact beside `final_path`, in the same parent; `None` when the path names no file
/// (the root, or a key ending in `/`: an object store's directory object, whose artifacts
/// [`directory_object_artifact`] names; a filesystem has none).
///
/// `/` is the only separator here. A backend whose native paths also treat `\` as one (CIFS,
/// Windows Local) must refuse or split such final paths itself, or the artifacts would land outside
/// the final file's parent.
#[must_use]
pub fn sibling_artifact(final_path: &StoragePath, kind: ArtifactKind) -> Option<StoragePath> {
    let path = final_path.as_str();
    let sibling = match path.rsplit_once('/') {
        Some((_, "")) => return None,
        Some((parent, name)) => format!("{parent}/{}", artifact_name(name, kind)),
        None if path.is_empty() => return None,
        None => artifact_name(path, kind),
    };
    StoragePath::new(sibling).ok()
}

/// The `kind` artifact of a directory object `…/name/` (an object store's key ending in `/`, P7
/// F5): in the object's parent, `…/.data-mover-<digest("name/")>.<kind>`, or at the root for a
/// root-level `name/`. Not inside `name/`, where it would make that prefix exist and sit in the
/// directory's own listing. The digest input keeps the `/`, so it is never an artifact of the file
/// `…/name` (`digest("name")`), and still derives from the final key alone (stateless runtime).
/// `None` for a path that does not end in `/`, or whose last segment before it is empty (`""`,
/// `/`, `a//`).
#[must_use]
pub fn directory_object_artifact(
    final_path: &StoragePath,
    kind: ArtifactKind,
) -> Option<StoragePath> {
    let key = final_path.as_str().strip_suffix('/')?;
    let (parent, name) = match key.rsplit_once('/') {
        Some((parent, name)) => (Some(parent), name),
        None => (None, key),
    };
    if name.is_empty() {
        return None;
    }
    let artifact = artifact_name(&format!("{name}/"), kind);
    let path = match parent {
        Some(parent) => format!("{parent}/{artifact}"),
        None => artifact,
    };
    StoragePath::new(path).ok()
}

/// A deterministic artifact name, taken apart. A listing-based sweep must match a digest of `d/`
/// found in `p/` against the prefix `p/d/` (a directory object's artifacts, P7 F5), not against a
/// sibling object, or it would take a live pointer for an orphan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParsedArtifact<'a> {
    pub digest: &'a str,
    pub kind: ArtifactKind,
    pub temporary: bool,
}

/// Splits a deterministic artifact name into its final-name digest, kind and whether it is the
/// temporary; `None` for any other name.
#[must_use]
pub fn parse_artifact_name(name: &str) -> Option<ParsedArtifact<'_>> {
    let (name, temporary) = name
        .strip_suffix(TEMPORARY_SUFFIX)
        .map_or((name, false), |name| (name, true));
    let (digest, suffix) = name.strip_prefix(ARTIFACT_PREFIX)?.split_once('.')?;
    if !hex(digest, FINAL_NAME_DIGEST_HEX) {
        return None;
    }
    let kind = ArtifactKind::ALL
        .into_iter()
        .find(|kind| kind.suffix() == suffix)?;
    Some(ParsedArtifact {
        digest,
        kind,
        temporary,
    })
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

    /// P7 F5: the artifacts of a directory object `p/d/` sit in the parent `p`, named after `d/`,
    /// and never collide with those of the file `p/d`, at any depth.
    #[test]
    fn artifacts_of_d_and_d_slash_never_collide() {
        let path = |value: &str| StoragePath::new(value).unwrap_or_else(|error| panic!("{error}"));
        for kind in ArtifactKind::ALL {
            for (file, object, parent) in [("p/d", "p/d/", "p/"), ("d", "d/", "")] {
                let of_file = sibling_artifact(&path(file), kind);
                let of_object = directory_object_artifact(&path(object), kind);
                let expected = format!("{parent}{}", artifact_name("d/", kind));
                assert_eq!(
                    of_object.as_ref().map(StoragePath::as_str),
                    Some(&*expected)
                );
                assert!(of_file.is_some());
                assert_ne!(of_file, of_object, "{kind:?} {file}");
                assert!(is_artifact_path(&expected));
            }
            assert_eq!(sibling_artifact(&path("p/d/"), kind), None);
            for none in ["", "/", "a//", "p/d"] {
                assert_eq!(
                    directory_object_artifact(&path(none), kind),
                    None,
                    "{none:?}"
                );
            }
        }
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

    /// Pins the naming: computed independently (Python `blake3`) from the documented input.
    #[test]
    fn deterministic_names_match_the_frozen_vector() {
        assert_eq!(
            artifact_name("file.bin", ArtifactKind::Pointer),
            ".data-mover-37b14e7406c1d03de316e3fdf5c615c7.pointer"
        );
        assert_eq!(
            artifact_name("数据.bin", ArtifactKind::Stage),
            ".data-mover-8caab937b05a37ff8b619c016bd32ef7.stage"
        );
    }

    #[test]
    fn deterministic_names_are_short_hidden_and_parse_back() {
        for kind in ArtifactKind::ALL {
            let name = artifact_name("file.bin", kind);
            let temporary = artifact_temporary_name("file.bin", kind);
            assert!(name.len() <= 55, "{name}");
            assert!(temporary.len() <= 59, "{temporary}");
            assert!(is_artifact_name(OsStr::new(&name)));
            assert!(is_artifact_path(&temporary));
            let digest = final_name_digest("file.bin");
            for (text, temporary) in [(&name, false), (&temporary, true)] {
                assert_eq!(
                    parse_artifact_name(text),
                    Some(ParsedArtifact {
                        digest: digest.as_str(),
                        kind,
                        temporary
                    })
                );
            }
        }
        assert_ne!(
            artifact_name("a.bin", ArtifactKind::Stage),
            artifact_name("b.bin", ArtifactKind::Stage)
        );
        for other in [
            ".data-mover-0123.stage",
            ".data-mover-37b14e7406c1d03de316e3fdf5c615c7.unknown",
            "file.bin",
            // The random stage name used before ADR-0006 C8.
            ".data-mover-0123456789abcdef-0123456789abcdef0123456789abcdef.stage",
            // A kind no longer written (the checkpoint record before ADR-0006 C10d / C11d).
            ".data-mover-37b14e7406c1d03de316e3fdf5c615c7.checkpoint",
        ] {
            assert_eq!(parse_artifact_name(other), None, "{other}");
        }
    }

    /// Only the file name counts: the same name in two parents names its artifacts alike, each in
    /// its own parent.
    #[test]
    fn siblings_sit_in_the_final_files_parent() -> Result<(), crate::model::ModelValueError> {
        let name = artifact_name("x.bin", ArtifactKind::Pointer);
        for (final_path, expected) in [
            ("x.bin", name.clone()),
            ("a/x.bin", format!("a/{name}")),
            ("a/b/x.bin", format!("a/b/{name}")),
        ] {
            assert_eq!(
                sibling_artifact(&StoragePath::new(final_path)?, ArtifactKind::Pointer),
                Some(StoragePath::new(expected)?)
            );
        }
        assert_eq!(
            sibling_artifact(&StoragePath::root(), ArtifactKind::Stage),
            None
        );
        assert_eq!(
            sibling_artifact(&StoragePath::new("dir/")?, ArtifactKind::Stage),
            None
        );
        Ok(())
    }
}
