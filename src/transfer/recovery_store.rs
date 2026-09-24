use std::fs::{self, File, OpenOptions};
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use super::model::{RecoveryContext, RecoveryRegistrar, RecoveryRegistrationFailure};
use crate::storage::RecoveryIdentity;

const MAGIC_V1: &[u8; 8] = b"DMRECOV1";
const MAGIC: &[u8; 8] = b"DMRECOV2";
const V1_HEADER_BYTES: usize = 8 + 32 + 32 + 4;
const HEADER_BYTES: usize = 8 + 32 + 32 + 1 + 4;
const CHECKSUM_BYTES: usize = 32;
const LEASE_NAMESPACE_LOCK: &str = ".lease-namespace.lock";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RecoveryState {
    Staged,
    Publishing,
}

struct RecoveryRecord {
    identity: RecoveryIdentity,
    claim: [u8; 32],
    state: RecoveryState,
}

pub(super) async fn open(
    binding: [u8; 32],
) -> Result<RecoveryContext, RecoveryRegistrationFailure> {
    tokio::task::spawn_blocking(move || open_sync(binding))
        .await
        .map_err(|_| RecoveryRegistrationFailure::unavailable())?
}

pub(super) async fn complete(binding: [u8; 32]) -> Result<(), RecoveryRegistrationFailure> {
    tokio::task::spawn_blocking(move || remove_record(binding))
        .await
        .map_err(|_| RecoveryRegistrationFailure::unavailable())?
        .map_err(|_| RecoveryRegistrationFailure::unavailable())
}

pub(super) async fn open_existing(
    binding: [u8; 32],
) -> Result<Option<RecoveryContext>, RecoveryRegistrationFailure> {
    tokio::task::spawn_blocking(move || open_existing_sync(binding))
        .await
        .map_err(|_| RecoveryRegistrationFailure::unavailable())?
}

pub(super) async fn mark_publishing(binding: [u8; 32]) -> Result<(), RecoveryRegistrationFailure> {
    tokio::task::spawn_blocking(move || mark_publishing_sync(binding))
        .await
        .map_err(|_| RecoveryRegistrationFailure::unavailable())?
}

fn open_sync(binding: [u8; 32]) -> Result<RecoveryContext, RecoveryRegistrationFailure> {
    let root = recovery_root();
    create_private_directory(&root).map_err(|_| RecoveryRegistrationFailure::unavailable())?;
    let lease = open_lease(&root, binding)?;
    let path = record_path(&root, binding);
    let existing = match fs::read(&path) {
        Ok(bytes) => Some(decode_record(&bytes, binding)?),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(_) => return Err(RecoveryRegistrationFailure::unavailable()),
    };
    let (identity, claim, publication_pending) = existing.map_or_else(
        || (None, new_claim(), false),
        |record| {
            (
                Some(record.identity),
                record.claim,
                record.state == RecoveryState::Publishing,
            )
        },
    );
    let registrar: Arc<dyn RecoveryRegistrar> = Arc::new(FileRecoveryRegistrar {
        root,
        binding,
        claim,
    });
    Ok(RecoveryContext::new(
        identity,
        claim,
        publication_pending,
        registrar,
        lease,
    ))
}

fn open_existing_sync(
    binding: [u8; 32],
) -> Result<Option<RecoveryContext>, RecoveryRegistrationFailure> {
    let root = recovery_root();
    match fs::metadata(record_path(&root, binding)) {
        Ok(_) => open_sync(binding).map(|context| {
            if context.identity.is_some() {
                Some(context)
            } else {
                None
            }
        }),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err(RecoveryRegistrationFailure::unavailable()),
    }
}

fn mark_publishing_sync(binding: [u8; 32]) -> Result<(), RecoveryRegistrationFailure> {
    let root = recovery_root();
    let bytes = fs::read(record_path(&root, binding))
        .map_err(|_| RecoveryRegistrationFailure::unavailable())?;
    let record = decode_record(&bytes, binding)?;
    write_record(
        &root,
        binding,
        record.claim,
        &record.identity,
        RecoveryState::Publishing,
    )
    .map_err(|_| RecoveryRegistrationFailure::unavailable())
}

/// Whether the store holds anything for `binding`: a record or its lease file.
#[cfg(test)]
pub(super) fn has_entry(binding: [u8; 32]) -> bool {
    let root = recovery_root();
    record_path(&root, binding).exists() || lock_path(&root, binding).exists()
}

fn recovery_root() -> PathBuf {
    if let Some(path) = std::env::var_os("DATA_MOVER_RECOVERY_DIR").filter(|path| !path.is_empty())
    {
        return PathBuf::from(path);
    }
    if let Some(path) = std::env::var_os("XDG_STATE_HOME").filter(|path| !path.is_empty()) {
        return PathBuf::from(path).join("data-mover/recovery");
    }
    if let Some(path) = std::env::var_os("HOME").filter(|path| !path.is_empty()) {
        return PathBuf::from(path).join(".local/state/data-mover/recovery");
    }
    std::env::temp_dir().join("data-mover-recovery")
}

fn record_path(root: &Path, binding: [u8; 32]) -> PathBuf {
    root.join(format!(
        "{}.state",
        blake3::Hash::from_bytes(binding).to_hex()
    ))
}

fn lock_path(root: &Path, binding: [u8; 32]) -> PathBuf {
    root.join(format!(
        "{}.lock",
        blake3::Hash::from_bytes(binding).to_hex()
    ))
}

fn open_lease(root: &Path, binding: [u8; 32]) -> Result<Arc<File>, RecoveryRegistrationFailure> {
    let namespace =
        open_namespace_lock(root).map_err(|_| RecoveryRegistrationFailure::unavailable())?;
    namespace
        .lock()
        .map_err(|_| RecoveryRegistrationFailure::unavailable())?;
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true);
    set_private_file_mode(&mut options);
    let file = options
        .open(lock_path(root, binding))
        .map_err(|_| RecoveryRegistrationFailure::unavailable())?;
    file.try_lock()
        .map_err(|_| RecoveryRegistrationFailure::unavailable())?;
    Ok(Arc::new(file))
}

fn new_claim() -> [u8; 32] {
    let mut claim = [0; 32];
    claim[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    claim[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    claim
}

fn decode_record(
    bytes: &[u8],
    expected_binding: [u8; 32],
) -> Result<RecoveryRecord, RecoveryRegistrationFailure> {
    let (header_bytes, state, identity_len_at) = if bytes.starts_with(MAGIC) {
        if bytes.len() < HEADER_BYTES + CHECKSUM_BYTES {
            return Err(RecoveryRegistrationFailure::rejected());
        }
        let state = match bytes[72] {
            0 => RecoveryState::Staged,
            1 => RecoveryState::Publishing,
            _ => return Err(RecoveryRegistrationFailure::rejected()),
        };
        (HEADER_BYTES, state, 73)
    } else if bytes.starts_with(MAGIC_V1) {
        if bytes.len() < V1_HEADER_BYTES + CHECKSUM_BYTES {
            return Err(RecoveryRegistrationFailure::rejected());
        }
        (V1_HEADER_BYTES, RecoveryState::Staged, 72)
    } else {
        return Err(RecoveryRegistrationFailure::rejected());
    };
    let checksum_at = bytes.len() - CHECKSUM_BYTES;
    let expected_checksum = blake3::hash(&bytes[..checksum_at]);
    if expected_checksum.as_bytes() != &bytes[checksum_at..] {
        return Err(RecoveryRegistrationFailure::rejected());
    }
    if bytes[8..40] != expected_binding {
        return Err(RecoveryRegistrationFailure::rejected());
    }
    let mut claim = [0; 32];
    claim.copy_from_slice(&bytes[40..72]);
    let identity_len = u32::from_le_bytes(
        bytes[identity_len_at..identity_len_at + 4]
            .try_into()
            .map_err(|_| RecoveryRegistrationFailure::rejected())?,
    ) as usize;
    if header_bytes + identity_len != checksum_at {
        return Err(RecoveryRegistrationFailure::rejected());
    }
    let identity =
        RecoveryIdentity::from_bytes(Bytes::copy_from_slice(&bytes[header_bytes..checksum_at]))
            .map_err(|_| RecoveryRegistrationFailure::rejected())?;
    Ok(RecoveryRecord {
        identity,
        claim,
        state,
    })
}

fn encode_record(
    binding: [u8; 32],
    claim: [u8; 32],
    identity: &RecoveryIdentity,
    state: RecoveryState,
) -> Vec<u8> {
    let identity = identity.as_bytes();
    let mut record = Vec::with_capacity(HEADER_BYTES + identity.len() + CHECKSUM_BYTES);
    record.extend_from_slice(MAGIC);
    record.extend_from_slice(&binding);
    record.extend_from_slice(&claim);
    record.push(match state {
        RecoveryState::Staged => 0,
        RecoveryState::Publishing => 1,
    });
    let Ok(identity_len) = u32::try_from(identity.len()) else {
        unreachable!("RecoveryIdentity enforces a 4096-byte bound")
    };
    record.extend_from_slice(&identity_len.to_le_bytes());
    record.extend_from_slice(identity);
    let checksum = blake3::hash(&record);
    record.extend_from_slice(checksum.as_bytes());
    record
}

struct FileRecoveryRegistrar {
    root: PathBuf,
    binding: [u8; 32],
    claim: [u8; 32],
}

#[async_trait]
impl RecoveryRegistrar for FileRecoveryRegistrar {
    async fn register(
        &self,
        identity: RecoveryIdentity,
    ) -> Result<(), RecoveryRegistrationFailure> {
        let root = self.root.clone();
        let binding = self.binding;
        let claim = self.claim;
        tokio::task::spawn_blocking(move || {
            write_record(&root, binding, claim, &identity, RecoveryState::Staged)
        })
        .await
        .map_err(|_| RecoveryRegistrationFailure::unavailable())?
        .map_err(|_| RecoveryRegistrationFailure::unavailable())
    }
}

fn write_record(
    root: &Path,
    binding: [u8; 32],
    claim: [u8; 32],
    identity: &RecoveryIdentity,
    state: RecoveryState,
) -> io::Result<()> {
    create_private_directory(root)?;
    let path = record_path(root, binding);
    let temporary = root.join(format!(".{}.tmp", uuid::Uuid::new_v4()));
    let bytes = encode_record(binding, claim, identity, state);
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    set_private_file_mode(&mut options);
    let mut file = options.open(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&temporary, &path)?;
    sync_directory(root)?;
    Ok(())
}

fn remove_record(binding: [u8; 32]) -> io::Result<()> {
    let root = recovery_root();
    remove_record_at(&root, binding)
}

fn remove_record_at(root: &Path, binding: [u8; 32]) -> io::Result<()> {
    if !root.exists() {
        return Ok(());
    }
    let namespace = open_namespace_lock(root)?;
    namespace.lock()?;
    let mut removed = false;
    for path in [record_path(root, binding), lock_path(root, binding)] {
        match fs::remove_file(path) {
            Ok(()) => removed = true,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    if removed {
        sync_directory(root)?;
    }
    Ok(())
}

fn open_namespace_lock(root: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true);
    set_private_file_mode(&mut options);
    options.open(root.join(LEASE_NAMESPACE_LOCK))
}

fn create_private_directory(path: &Path) -> io::Result<()> {
    fs::create_dir_all(path)?;
    set_private_directory_mode(path)
}

#[cfg(unix)]
fn set_private_directory_mode(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
fn set_private_directory_mode(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_private_file_mode(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt as _;
    options.mode(0o600);
}

#[cfg(not(unix))]
fn set_private_file_mode(_options: &mut OpenOptions) {}

#[cfg(unix)]
fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

#[cfg(windows)]
fn sync_directory(path: &Path) -> io::Result<()> {
    let directory = cap_std::fs::Dir::open_ambient_dir(path, cap_std::ambient_authority())?;
    crate::storage::durability::sync_directory(&directory)
}

#[cfg(not(any(unix, windows)))]
fn sync_directory(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovery_record_rejects_tampering() {
        let binding = [7; 32];
        let identity = RecoveryIdentity::from_bytes(Bytes::from_static(b"identity"));
        assert!(identity.is_ok());
        let mut record = encode_record(
            binding,
            [9; 32],
            &identity.unwrap_or_else(|_| unreachable!()),
            RecoveryState::Staged,
        );
        record[HEADER_BYTES] ^= 1;
        assert_eq!(
            decode_record(&record, binding).map(|_| ()),
            Err(RecoveryRegistrationFailure::Rejected)
        );
    }

    #[test]
    fn publishing_state_roundtrips() {
        let binding = [3; 32];
        let identity = RecoveryIdentity::from_bytes(Bytes::from_static(b"identity"))
            .unwrap_or_else(|_| unreachable!());
        let record = decode_record(
            &encode_record(binding, [4; 32], &identity, RecoveryState::Publishing),
            binding,
        )
        .unwrap_or_else(|_| unreachable!());
        assert_eq!(record.identity, identity);
        assert_eq!(record.claim, [4; 32]);
        assert_eq!(record.state, RecoveryState::Publishing);
    }

    #[cfg(unix)]
    #[test]
    fn completed_recovery_reclaims_its_per_transfer_lock() -> Result<(), Box<dyn std::error::Error>>
    {
        let root = tempfile::tempdir()?;
        let binding = [5; 32];
        create_private_directory(root.path())?;
        let lease = open_lease(root.path(), binding)?;
        let identity = RecoveryIdentity::from_bytes(Bytes::from_static(b"stage"))?;
        write_record(
            root.path(),
            binding,
            [6; 32],
            &identity,
            RecoveryState::Staged,
        )?;

        remove_record_at(root.path(), binding)?;

        assert!(!record_path(root.path(), binding).exists());
        assert!(!lock_path(root.path(), binding).exists());
        assert!(root.path().join(LEASE_NAMESPACE_LOCK).exists());
        drop(lease);
        let recreated = open_lease(root.path(), binding)?;
        assert!(lock_path(root.path(), binding).exists());
        drop(recreated);
        Ok(())
    }
}
