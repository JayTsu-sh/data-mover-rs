//! Reading a CIFS directory through the [`CifsDirectory`](super::namespace::CifsDirectory) seam.

use futures::TryStreamExt as _;

use super::namespace::{CifsListedChild, CifsNamespaceProtocol};
use crate::model::StoragePath;

/// Reads a whole directory: opens it, reads every record and closes it, whatever the read did.
/// The read error wins over the close's.
pub(super) async fn read_whole(
    protocol: &dyn CifsNamespaceProtocol,
    path: &StoragePath,
) -> smb_domain::Result<Vec<CifsListedChild>> {
    let directory = protocol.open_directory(path).await?;
    let entries = directory.entries().try_collect::<Vec<_>>().await;
    let close = directory.close().await;
    let entries = entries?;
    close?;
    Ok(entries)
}
