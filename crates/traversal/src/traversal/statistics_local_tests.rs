//! 真文件系统验证类型统计不依赖测试 descriptor。
#![allow(clippy::unwrap_used)]
#[cfg(unix)]
use std::os::unix::{fs::symlink, net::UnixListener};
use std::{
    fs::{create_dir, write},
    io,
};

use tokio_util::sync::CancellationToken;

use super::*;
use crate::traversal::DirectoryStatistics;

#[tokio::test]
async fn real_local_directory_statistics_match_entries_in_both_orders() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    write(dir.path().join("file"), b"1234567")?;
    create_dir(dir.path().join("nested"))?;
    write(dir.path().join("nested/child"), b"12")?;
    #[cfg(unix)]
    let _socket = {
        symlink("file", dir.path().join("link"))?;
        UnixListener::bind(dir.path().join("socket"))?
    };
    for order in [TraversalOrder::Blocks, TraversalOrder::Unordered] {
        let mut session = fixture(dir.path())?.source.traverse(
            request(CancellationToken::new(), 4)
                .order(order)
                .directory_statistics(true),
        );
        let items = drain(&mut session).await;
        completed(session.finish().await.map_err(io::Error::other)?)?;
        let stats: Vec<&DirectoryStatistics> = items
            .iter()
            .filter_map(|item| match item {
                TraversalItem::DirectoryStatistics(stats)
                    if stats.outcome == Some(ListingOutcome::Complete) =>
                {
                    Some(stats.as_ref())
                }
                _ => None,
            })
            .collect();
        assert_eq!(stats.len(), 2);
        let files: u64 = stats.iter().map(|s| s.retained.regular_files).sum();
        let bytes: u64 = stats.iter().map(|s| s.regular_file_bytes).sum();
        assert_eq!((files, bytes), (2, 9));
        assert_eq!(stats.iter().map(|s| s.retained.directories).sum::<u64>(), 1);
        #[cfg(unix)]
        {
            assert_eq!(stats.iter().map(|s| s.retained.symlinks).sum::<u64>(), 1);
            assert_eq!(stats.iter().map(|s| s.retained.sockets).sum::<u64>(), 1);
        }
    }
    Ok(())
}
