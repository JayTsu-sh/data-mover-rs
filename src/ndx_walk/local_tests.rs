//! The NDX walk over the real Local filesystem.

use std::io;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use super::{NdxWalkRequest, ndx_walk};
use crate::dir_tree::NdxEvent;
use crate::model::StoragePath;
use crate::storage::backends::local::test_traversal_storage;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

/// The NDX walk collects each directory whole: a directory read failing midway is that
/// directory's soft listing error, and none of its entries is paged out.
#[tokio::test]
async fn ndx_walk_reports_a_directory_whose_read_fails_midway_as_unlisted() -> Result {
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("bad"))?;
    std::fs::create_dir(root.path().join("good"))?;
    for index in 0..5 {
        std::fs::write(root.path().join(format!("bad/f{index}")), b"x")?;
        std::fs::write(root.path().join(format!("good/f{index}")), b"x")?;
    }
    let (storage, namespace, _) = test_traversal_storage(root.path(), "ndx-walk-local")?;
    namespace.failing_directory_reads(Arc::new(|native: &Path, read: usize| {
        (native == Path::new("bad") && read == 3).then(|| io::Error::other("injected EIO"))
    }));
    let walk = ndx_walk(
        &storage,
        NdxWalkRequest {
            root: StoragePath::root(),
            max_depth: None,
            match_expressions: None,
            exclude_expressions: None,
            concurrency: NonZeroUsize::MIN,
            cancel: CancellationToken::new(),
        },
    )?;
    let (mut listed, mut errors) = (Vec::new(), Vec::new());
    while let Some(event) = walk.next().await {
        match event {
            NdxEvent::Page(page) => listed.extend(
                page.files
                    .iter()
                    .chain(&page.subdirs)
                    .map(|entry| entry.entry.path().as_str().to_owned()),
            ),
            NdxEvent::Error { path, .. } => errors.push(path),
            NdxEvent::Done => break,
        }
    }
    assert!(
        errors.iter().any(|path| path.ends_with("bad")),
        "{errors:?}"
    );
    assert!(
        !listed.iter().any(|path| path.starts_with("bad/")),
        "{listed:?}"
    );
    assert_eq!(
        listed
            .iter()
            .filter(|path| path.starts_with("good/"))
            .count(),
        5
    );
    Ok(())
}
