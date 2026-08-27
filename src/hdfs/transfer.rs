#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HdfsPreparedTransfer {
    part_path: PathBuf,
    prefix_len: u64,
    expected_size: u64,
    mode: u32,
    replication: Option<u32>,
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
        if part_path.file_name().is_none()
            || part_path.components().any(|component| {
                matches!(
                    component,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            })
        {
            return Err(StorageError::InvalidPath(
                "HDFS resume temporary path must be a relative file path".to_string(),
            ));
        }
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
        })
    }

    pub(crate) fn part_path(&self) -> &Path {
        &self.part_path
    }

    pub(crate) const fn prefix_len(&self) -> u64 {
        self.prefix_len
    }

    pub(crate) const fn expected_size(&self) -> u64 {
        self.expected_size
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
