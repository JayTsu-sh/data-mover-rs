//! Shared names for sibling transfer artifacts; ownership remains backend-specific.
pub(crate) fn stage_name(destination: &str) -> String {
    format!(
        ".data-mover-{}-{}.stage",
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
    let body = base.strip_prefix(".data-mover-")?.strip_suffix(".stage")?;
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
