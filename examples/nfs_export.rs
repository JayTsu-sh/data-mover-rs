use data_mover::{NFSStorage, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let entries = NFSStorage::list_exports("10.131.9.20").await?;

    for entry in entries {
        println!("{:?}", entry);
    }

    Ok(())
}
