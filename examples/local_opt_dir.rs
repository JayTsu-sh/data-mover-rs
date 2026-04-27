use std::path::Path;

use data_mover::{LocalStorage, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let storage = LocalStorage::new("c:\\jay\\source", None);

    let path = Path::new("dir1\\dir2\\dir3");

    storage.create_dir_all(path).await?;

    let entry = storage.get_metadata(path).await?;
    println!("{:?}", entry);

    storage.delete_dir_all(Some(Path::new("dir1"))).await?;

    Ok(())
}
