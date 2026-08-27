const MAX_TRANSFER_ID_BYTES: usize = 256;

/// Preparation policy for one stable HDFS staged transfer.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum HdfsResumeMode {
    /// Resume a valid partial and create/rebuild missing or overlong state.
    #[default]
    Auto,
    /// Rebuild the matching partial and copy from byte zero.
    Restart,
    /// Require an existing valid partial without modifying invalid state.
    Require,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HdfsPrepareAction {
    Rebuild,
    Resume(u64),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HdfsPartialObservation {
    Missing,
    File(u64),
    Directory,
}

pub(crate) fn plan_staged_prepare(
    mode: HdfsResumeMode,
    partial: HdfsPartialObservation,
    expected_size: u64,
) -> Result<HdfsPrepareAction, StorageError> {
    match (mode, partial) {
        (_, HdfsPartialObservation::Directory) => Err(StorageError::InvalidPath(
            "HDFS transfer partial path is a directory".to_string(),
        )),
        (
            HdfsResumeMode::Auto | HdfsResumeMode::Require,
            HdfsPartialObservation::File(size),
        )
            if size <= expected_size =>
        {
            Ok(HdfsPrepareAction::Resume(size))
        }
        (HdfsResumeMode::Restart, _)
        | (
            HdfsResumeMode::Auto,
            HdfsPartialObservation::Missing | HdfsPartialObservation::File(_),
        ) => {
            Ok(HdfsPrepareAction::Rebuild)
        }
        (HdfsResumeMode::Require, HdfsPartialObservation::Missing) => {
            Err(StorageError::FileNotFound(
            "required HDFS transfer partial is missing".to_string(),
            ))
        }
        (HdfsResumeMode::Require, HdfsPartialObservation::File(size)) => {
            Err(StorageError::OperationError(format!(
                "required HDFS transfer partial is overlong: size={size}, expected={expected_size}"
            )))
        }
    }
}

/// A backend-neutral stable source fact available in addition to size and mtime.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HdfsStableSourceFact<'a> {
    /// A filesystem identity such as a NAS file handle or inode.
    FileIdentity(&'a [u8]),
    /// An object-store version identifier.
    ObjectVersion(&'a str),
}

/// A deterministic digest of the source facts that bind resumable HDFS state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HdfsSourceFingerprint {
    size: u64,
    mtime: i64,
    digest: [u8; 16],
}

impl HdfsSourceFingerprint {
    /// Build a fingerprint from size, mtime, and an optional typed stable fact.
    #[must_use]
    pub fn new(size: u64, mtime: i64, stable_fact: Option<HdfsStableSourceFact<'_>>) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"data-mover:hdfs-source-fingerprint:v1\0");
        hasher.update(&size.to_le_bytes());
        hasher.update(&mtime.to_le_bytes());
        if let Some(fact) = stable_fact {
            let (domain, fact) = match fact {
                HdfsStableSourceFact::FileIdentity(value) => {
                    (b"file-identity".as_slice(), value)
                }
                HdfsStableSourceFact::ObjectVersion(value) => {
                    (b"object-version".as_slice(), value.as_bytes())
                }
            };
            hasher.update(domain);
            let fact_len = u64::try_from(fact.len()).unwrap_or(u64::MAX);
            hasher.update(&fact_len.to_le_bytes());
            hasher.update(fact);
        } else {
            hasher.update(b"absent");
            hasher.update(&0_u64.to_le_bytes());
        }
        let mut digest = [0_u8; 16];
        digest.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
        Self {
            size,
            mtime,
            digest,
        }
    }

    /// Return the source size bound into this fingerprint.
    #[must_use]
    pub const fn size(&self) -> u64 {
        self.size
    }

    /// Return the source modification time bound into this fingerprint.
    #[must_use]
    pub const fn mtime(&self) -> i64 {
        self.mtime
    }
}

/// A validated stable HDFS transfer request with a deterministic partial path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HdfsTransferRequest {
    identity_digest: [u8; 16],
    source_fingerprint: HdfsSourceFingerprint,
    final_path: PathBuf,
    partial_path: PathBuf,
    expected_size: u64,
    mode: u32,
    replication: Option<u32>,
}

impl HdfsTransferRequest {
    /// Build one stable HDFS transfer request from an opaque caller identity.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty/oversized identity, an unsafe final path,
    /// a non-UTF-8 path, or a fingerprint whose size differs from the request.
    pub fn new(
        transfer_identity: &str,
        source_fingerprint: HdfsSourceFingerprint,
        final_path: PathBuf,
        expected_size: u64,
        mode: u32,
        replication: Option<u32>,
    ) -> Result<Self, StorageError> {
        let identity = transfer_identity.as_bytes();
        if identity.is_empty() || identity.len() > MAX_TRANSFER_ID_BYTES {
            return Err(StorageError::OperationError(
                "HDFS transfer identity must contain 1 to 256 bytes".to_string(),
            ));
        }
        validate_relative_file_path(&final_path, "HDFS transfer final path")?;
        if source_fingerprint.size() != expected_size {
            return Err(StorageError::OperationError(format!(
                "HDFS source fingerprint size {} does not match expected size {expected_size}",
                source_fingerprint.size()
            )));
        }
        let final_path_text = final_path.to_str().ok_or_else(|| {
            StorageError::InvalidPath("HDFS transfer final path must be UTF-8".to_string())
        })?;
        let identity_digest = digest_16(b"data-mover:hdfs-transfer-identity:v1\0", identity);
        let mut binding = blake3::Hasher::new();
        binding.update(b"data-mover:hdfs-partial-path:v1\0");
        binding.update(&identity_digest);
        binding.update(&source_fingerprint.digest);
        binding.update(final_path_text.as_bytes());
        let binding = binding.finalize().to_hex();
        let partial_name = format!(".data-mover-{}.part", &binding[..32]);
        let partial_path = final_path.with_file_name(partial_name);
        Ok(Self {
            identity_digest,
            source_fingerprint,
            final_path,
            partial_path,
            expected_size,
            mode: mode & 0o7777,
            replication,
        })
    }

    /// Return the deterministic same-directory partial path.
    #[must_use]
    pub fn partial_path(&self) -> &Path {
        &self.partial_path
    }

    /// Return the final path bound into this request.
    #[must_use]
    pub fn final_path(&self) -> &Path {
        &self.final_path
    }

    /// Return the expected final size.
    #[must_use]
    pub const fn expected_size(&self) -> u64 {
        self.expected_size
    }

    /// Return the requested HDFS permission bits.
    #[must_use]
    pub const fn mode(&self) -> u32 {
        self.mode
    }

    /// Return the requested HDFS replication, when explicitly available.
    #[must_use]
    pub const fn replication(&self) -> Option<u32> {
        self.replication
    }
}

fn digest_16(domain: &[u8], value: &[u8]) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    hasher.update(value);
    let mut digest = [0_u8; 16];
    digest.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    digest
}

fn validate_relative_file_path(path: &Path, label: &str) -> Result<(), StorageError> {
    if path.file_name().is_none()
        || path.components().any(|component| match component {
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => true,
            Component::Normal(value) => value.as_encoded_bytes().len() > 255,
            Component::CurDir => false,
        })
    {
        return Err(StorageError::InvalidPath(format!(
            "{label} must be a relative file path"
        )));
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum HdfsTransferBinding {
    Legacy,
    Stable {
        identity_digest: [u8; 16],
        source_digest: [u8; 16],
        final_path: PathBuf,
    },
}

/// A validated HDFS tail-transfer state returned by prepare and consumed by append/commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HdfsPreparedTransfer {
    part_path: PathBuf,
    prefix_len: u64,
    expected_size: u64,
    mode: u32,
    replication: Option<u32>,
    binding: HdfsTransferBinding,
}

impl HdfsPreparedTransfer {
    pub(crate) fn new(
        part_path: PathBuf,
        prefix_len: u64,
        expected_size: u64,
        entry_size: u64,
        mode: u32,
        replication: Option<u32>,
    ) -> Result<Self, StorageError> {
        validate_relative_file_path(&part_path, "HDFS resume temporary path")?;
        if expected_size != entry_size || prefix_len > expected_size {
            return Err(StorageError::OperationError(format!(
                "invalid HDFS resume state: prefix={prefix_len}, expected={expected_size}, entry={entry_size}"
            )));
        }
        Ok(Self {
            part_path,
            prefix_len,
            expected_size,
            mode: mode & 0o7777,
            replication,
            binding: HdfsTransferBinding::Legacy,
        })
    }

    fn from_stable_request(request: &HdfsTransferRequest, prefix_len: u64) -> Result<Self, StorageError> {
        if prefix_len > request.expected_size {
            return Err(StorageError::OperationError(format!(
                "invalid HDFS stable transfer prefix {prefix_len}, expected {}",
                request.expected_size
            )));
        }
        Ok(Self {
            part_path: request.partial_path.clone(),
            prefix_len,
            expected_size: request.expected_size,
            mode: request.mode,
            replication: request.replication,
            binding: HdfsTransferBinding::Stable {
                identity_digest: request.identity_digest,
                source_digest: request.source_fingerprint.digest,
                final_path: request.final_path.clone(),
            },
        })
    }

    fn with_prefix(&self, prefix_len: u64) -> Result<Self, StorageError> {
        if prefix_len > self.expected_size {
            return Err(StorageError::OperationError(format!(
                "invalid HDFS transfer prefix {prefix_len}, expected {}",
                self.expected_size
            )));
        }
        let mut state = self.clone();
        state.prefix_len = prefix_len;
        Ok(state)
    }

    /// Return the partial path owned by this prepared state.
    #[must_use]
    pub fn part_path(&self) -> &Path {
        &self.part_path
    }

    /// Return the validated contiguous prefix.
    #[must_use]
    pub const fn prefix_len(&self) -> u64 {
        self.prefix_len
    }

    /// Return the expected final size.
    #[must_use]
    pub const fn expected_size(&self) -> u64 {
        self.expected_size
    }

    /// Return the single missing tail range, or `None` when data is complete.
    #[must_use]
    pub const fn missing_tail(&self) -> Option<(u64, u64)> {
        if self.prefix_len < self.expected_size {
            Some((self.prefix_len, self.expected_size))
        } else {
            None
        }
    }

    pub(crate) const fn mode(&self) -> u32 {
        self.mode
    }

    pub(crate) const fn replication(&self) -> Option<u32> {
        self.replication
    }

    pub(crate) fn validate_current_prefix(&self, current_size: u64) -> Result<(), StorageError> {
        if current_size != self.prefix_len {
            return Err(StorageError::OperationError(format!(
                "stale HDFS append offset {}, current length is {current_size}: {}",
                self.prefix_len,
                self.part_path.display()
            )));
        }
        Ok(())
    }

    fn validate_final_path(&self, final_path: &Path) -> Result<(), StorageError> {
        if let HdfsTransferBinding::Stable {
            final_path: bound, ..
        } = &self.binding
            && bound != final_path
        {
            return Err(StorageError::OperationError(format!(
                "HDFS prepared transfer is bound to a different final path: {}",
                final_path.display()
            )));
        }
        Ok(())
    }
}

pub(crate) enum HdfsAppendTarget<'a> {
    Raw {
        path: &'a Path,
        prefix_len: u64,
    },
    Prepared(&'a HdfsPreparedTransfer),
}

impl<'a> HdfsAppendTarget<'a> {
    pub(crate) fn path(&self) -> &'a Path {
        match self {
            Self::Raw { path, .. } => path,
            Self::Prepared(state) => state.part_path(),
        }
    }

    pub(crate) const fn prefix_len(&self) -> u64 {
        match self {
            Self::Raw { prefix_len, .. } => *prefix_len,
            Self::Prepared(state) => state.prefix_len(),
        }
    }

    pub(crate) fn validate_current_prefix(&self, current_size: u64) -> Result<(), StorageError> {
        match self {
            Self::Prepared(state) => state.validate_current_prefix(current_size),
            Self::Raw { path, prefix_len } if current_size != *prefix_len => {
                Err(StorageError::OperationError(format!(
                    "stale HDFS append offset {prefix_len}, current length is {current_size}: {}",
                    path.display()
                )))
            }
            Self::Raw { .. } => Ok(()),
        }
    }
}
