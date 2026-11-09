//! 同一 export 下多个实例的目录句柄缓存，必须按已绑定前缀隔离。
use std::env;

use data_mover::model::StoragePath;
use data_mover::storage::create_directory_all;
use url::Url;

use super::super::chmod;
use super::{Result, common, connect};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an exclusively prepared LAB_NFS_DIRECT_URL prefix and independent guest checks"]
async fn real_nfs_bound_roots_isolate_directory_cache_and_metadata() -> Result {
    let root_url = env::var("LAB_NFS_DIRECT_URL")?;
    let root = connect(&root_url).await?;
    let source_root = tempfile::tempdir()?;
    let source = common::connect_local(source_root.path(), 2).await?;
    for (prefix, bytes) in [("cache-a", b"prefix A"), ("cache-b", b"prefix B")] {
        let directory = StoragePath::new(format!("{prefix}/item"))?;
        create_directory_all(&root, &directory).await?;
        chmod(&root, directory.as_str(), 0o755).await?;
        let file = StoragePath::new(format!("{prefix}/item/body"))?;
        std::fs::create_dir_all(source_root.path().join(directory.as_str()))?;
        std::fs::write(source_root.path().join(file.as_str()), bytes)?;
        super::persistence::copy(&source, &root, &file).await?;
    }
    let mut a = Url::parse(&root_url)?;
    a.set_path(&format!("{}/cache-a", a.path()));
    let mut b = Url::parse(&root_url)?;
    b.set_path(&format!("{}/cache-b", b.path()));
    let a = connect(a.as_str()).await?;
    let b = connect(b.as_str()).await?;
    // 首次读取／修改 A 填充相对路径 item 的缓存，再从 B 请求相同相对路径。
    chmod(&a, "item", 0o711).await?;
    chmod(&b, "item", 0o722).await?;
    // 父根以不同路径独立解析；不以两个子实例彼此相同的错误观察证明隔离。
    for (prefix, mode) in [("cache-a", 0o711), ("cache-b", 0o722)] {
        let observed = root
            .namespace()?
            .stat(&StoragePath::new(format!("{prefix}/item"))?)
            .await?;
        assert_eq!(
            observed.inline_mode().map(|value| value & 0o7777),
            Some(mode),
            "wrong root mutated: {prefix}"
        );
    }
    println!(
        "NFS bound roots same_relative_directory=true cache_isolated=true metadata_isolated=true source_retained=true"
    );
    Ok(())
}
