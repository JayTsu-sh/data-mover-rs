//! Which multipart manifests a resume may build on.

use super::*;

fn part(number: i32, size: u64) -> super::super::S3PartFacts {
    super::super::S3PartFacts {
        number,
        size,
        etag: format!("etag-{number}"),
    }
}

#[test]
fn resumable_manifest_requires_contiguous_full_parts() -> Result<(), Box<dyn std::error::Error>> {
    let path = crate::model::StoragePath::new("stage")?;
    let valid = resumable_parts(
        &path,
        vec![part(2, PART_SIZE as u64), part(1, PART_SIZE as u64)],
        Some((PART_SIZE * 2) as u64),
    );
    assert_eq!(valid?.0, (PART_SIZE * 2) as u64);
    assert!(resumable_parts(&path, vec![part(2, PART_SIZE as u64)], None).is_err());
    assert!(resumable_parts(&path, vec![part(1, 17)], None).is_err());
    Ok(())
}

#[test]
fn resumable_manifest_accepts_native_copy_part_sizes() -> Result<(), Box<dyn std::error::Error>> {
    let path = crate::model::StoragePath::new("native-stage")?;
    let native_part_size = 1024 * 1024 * 1024;
    let observed = resumable_parts(
        &path,
        vec![part(2, native_part_size), part(1, native_part_size)],
        Some(2 * native_part_size),
    )?;
    assert_eq!(observed.0, 2 * native_part_size);
    assert_eq!(observed.1.len(), 2);
    Ok(())
}

#[test]
fn resumable_manifest_accepts_only_a_complete_short_final_part()
-> Result<(), Box<dyn std::error::Error>> {
    let path = crate::model::StoragePath::new("short-final-stage")?;
    let final_size = PART_SIZE as u64 + 17;
    let observed = resumable_parts(
        &path,
        vec![part(1, PART_SIZE as u64), part(2, 17)],
        Some(final_size),
    )?;
    assert_eq!(observed.0, final_size);
    assert!(
        resumable_parts(
            &path,
            vec![part(1, PART_SIZE as u64), part(2, 17)],
            Some(final_size + 1),
        )
        .is_err()
    );
    assert!(
        resumable_parts(
            &path,
            vec![part(1, 17), part(2, PART_SIZE as u64)],
            Some(final_size),
        )
        .is_err()
    );
    Ok(())
}
