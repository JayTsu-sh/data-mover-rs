use std::path::Path;

use data_mover::{NFSStorage, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let mut storage = NFSStorage::new("nfs://10.131.9.20/mnt/zfs/jay/dataset/source", None).await?;

    let path = Path::new("dir1/dir2/dir3");

    storage.create_dir_all(path).await?;

    let entry = storage.get_metadata(path).await?;
    println!("{:?}", entry);
    storage = NFSStorage::new("nfs://10.131.9.20/mnt/zfs/jay/dataset/source:dir1/dir2/", None).await?;
    storage.delete_dir_all(None).await?;

    Ok(())
}
