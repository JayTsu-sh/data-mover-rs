//! Local file data-plane seam.
//!
//! Callers depend on positional read/write and durable sync semantics, while
//! the adapter owns how those operations are executed. The initial adapter
//! preserves the existing Tokio blocking-pool implementation.

use std::sync::Arc;

#[cfg(unix)]
use std::os::unix::fs::FileExt as _;
#[cfg(windows)]
use std::os::windows::fs::FileExt as _;

use bytes::Bytes;

use crate::Result;
use crate::error::StorageError;

#[derive(Clone, Debug, Default)]
pub(crate) struct LocalDataIo {
    adapter: BlockingLocalDataIo,
}

impl LocalDataIo {
    pub(crate) async fn attach(&self, file: tokio::fs::File) -> LocalIoFile {
        self.adapter.attach(file).await
    }

    pub(crate) async fn read_at(
        &self,
        file: &LocalIoFile,
        offset: u64,
        count: u64,
    ) -> Result<Bytes> {
        self.adapter.read_at(file, offset, count).await
    }

    pub(crate) async fn write_at(
        &self,
        file: &LocalIoFile,
        offset: u64,
        data: Bytes,
    ) -> Result<usize> {
        self.adapter.write_at(file, offset, data).await
    }

    pub(crate) async fn sync_all(&self, file: &LocalIoFile) -> Result<()> {
        self.adapter.sync_all(file).await
    }
}

#[derive(Debug)]
pub(crate) struct LocalIoFile {
    inner: Arc<std::fs::File>,
}

#[derive(Clone, Copy, Debug, Default)]
struct BlockingLocalDataIo;

impl BlockingLocalDataIo {
    async fn attach(self, file: tokio::fs::File) -> LocalIoFile {
        LocalIoFile {
            inner: Arc::new(file.into_std().await),
        }
    }

    async fn read_at(self, file: &LocalIoFile, offset: u64, count: u64) -> Result<Bytes> {
        let capacity = usize::try_from(count).map_err(|_| {
            StorageError::OperationError(format!("read size {count} exceeds platform capacity"))
        })?;
        let file = Arc::clone(&file.inner);
        let buffer = tokio::task::spawn_blocking(move || {
            let mut buffer = vec![0_u8; capacity];
            let filled = read_fully_at(&mut buffer, offset, |remaining, position| {
                #[cfg(unix)]
                let read = file.read_at(remaining, position)?;
                #[cfg(windows)]
                let read = file.seek_read(remaining, position)?;
                Ok(read)
            })?;
            buffer.truncate(filled);
            Ok::<_, std::io::Error>(buffer)
        })
        .await??;
        Ok(Bytes::from(buffer))
    }

    async fn write_at(self, file: &LocalIoFile, offset: u64, data: Bytes) -> Result<usize> {
        let length = data.len();
        let file = Arc::clone(&file.inner);
        tokio::task::spawn_blocking(move || {
            write_fully_at(&data, offset, |remaining, position| {
                #[cfg(unix)]
                let count = file.write_at(remaining, position)?;
                #[cfg(windows)]
                let count = file.seek_write(remaining, position)?;
                Ok(count)
            })
        })
        .await??;
        Ok(length)
    }

    async fn sync_all(self, file: &LocalIoFile) -> Result<()> {
        let file = Arc::clone(&file.inner);
        tokio::task::spawn_blocking(move || file.sync_all())
            .await?
            .map_err(StorageError::IoError)
    }
}

fn read_fully_at(
    buffer: &mut [u8],
    offset: u64,
    mut read: impl FnMut(&mut [u8], u64) -> std::io::Result<usize>,
) -> std::io::Result<usize> {
    let mut filled = 0usize;
    while filled < buffer.len() {
        let position = offset.checked_add(filled as u64).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "read offset overflow")
        })?;
        let count = read(&mut buffer[filled..], position)?;
        if count == 0 {
            break;
        }
        filled += count;
    }
    Ok(filled)
}

fn write_fully_at(
    data: &[u8],
    offset: u64,
    mut write: impl FnMut(&[u8], u64) -> std::io::Result<usize>,
) -> std::io::Result<usize> {
    let mut written = 0usize;
    while written < data.len() {
        let position = offset.checked_add(written as u64).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "write offset overflow")
        })?;
        let count = write(&data[written..], position)?;
        if count == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "failed to write the complete local file chunk",
            ));
        }
        written += count;
    }
    Ok(written)
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use bytes::Bytes;

    use super::{LocalDataIo, read_fully_at, write_fully_at};

    static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(label: &str) -> std::io::Result<Self> {
            let serial = NEXT_DIR.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "data-mover-local-io-{label}-{}-{serial}",
                std::process::id()
            ));
            std::fs::create_dir(&path)?;
            Ok(Self(path))
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_positional_reads_preserve_offsets_and_eof_length()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = TempDir::new("reads")?;
        let path = temp.path().join("source.bin");
        std::fs::write(&path, b"0123456789abcdef")?;
        let io = LocalDataIo::default();
        let file = io.attach(tokio::fs::File::open(path).await?).await;

        let (first, middle, tail) = tokio::join!(
            io.read_at(&file, 0, 4),
            io.read_at(&file, 6, 5),
            io.read_at(&file, 14, 8),
        );
        assert_eq!(first?.as_ref(), b"0123");
        assert_eq!(middle?.as_ref(), b"6789a");
        assert_eq!(tail?.as_ref(), b"ef");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn out_of_order_writes_sync_and_reopen_with_expected_digest()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = TempDir::new("writes")?;
        let path = temp.path().join("destination.bin");
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)
            .await?;
        let io = LocalDataIo::default();
        let file = io.attach(file).await;

        let (tail, head, middle) = tokio::join!(
            io.write_at(&file, 8, Bytes::from_static(b"ijkl")),
            io.write_at(&file, 0, Bytes::from_static(b"abcd")),
            io.write_at(&file, 4, Bytes::from_static(b"efgh")),
        );
        assert_eq!(tail?, 4);
        assert_eq!(head?, 4);
        assert_eq!(middle?, 4);
        io.sync_all(&file).await?;
        drop(file);

        let actual = std::fs::read(path)?;
        let expected = b"abcdefghijkl";
        assert_eq!(actual, expected);
        assert_eq!(blake3::hash(&actual), blake3::hash(expected));
        Ok(())
    }

    #[test]
    fn blocking_loops_continue_after_short_io() -> Result<(), Box<dyn std::error::Error>> {
        let source = b"abcdefgh";
        let mut destination = [0_u8; 8];
        let mut read_calls = 0usize;
        let filled = read_fully_at(&mut destination, 9, |remaining, position| {
            let source_offset = usize::try_from(position - 9)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
            let count = remaining.len().min(3);
            remaining[..count].copy_from_slice(&source[source_offset..source_offset + count]);
            read_calls += 1;
            Ok(count)
        })?;
        assert_eq!(filled, source.len());
        assert_eq!(destination, *source);
        assert_eq!(read_calls, 3);

        let mut output = Vec::new();
        let mut write_calls = 0usize;
        let written = write_fully_at(source, 20, |remaining, position| {
            assert_eq!(position, 20 + output.len() as u64);
            let count = remaining.len().min(3);
            output.extend_from_slice(&remaining[..count]);
            write_calls += 1;
            Ok(count)
        })?;
        assert_eq!(written, source.len());
        assert_eq!(output, source);
        assert_eq!(write_calls, 3);
        Ok(())
    }

    #[test]
    fn blocking_write_zero_and_offset_overflow_keep_io_error_kinds() {
        let Err(write_zero) = write_fully_at(b"x", 0, |_, _| Ok(0)) else {
            panic!("zero write must fail");
        };
        assert_eq!(write_zero.kind(), std::io::ErrorKind::WriteZero);

        let mut read_buffer = [0_u8; 2];
        let mut first_read = true;
        let Err(read_overflow) = read_fully_at(&mut read_buffer, u64::MAX, |remaining, _| {
            if first_read {
                remaining[0] = b'x';
                first_read = false;
                Ok(1)
            } else {
                Ok(0)
            }
        }) else {
            panic!("second read position must overflow");
        };
        assert_eq!(read_overflow.kind(), std::io::ErrorKind::InvalidInput);

        let mut first_write = true;
        let Err(write_overflow) = write_fully_at(b"xy", u64::MAX, |_, _| {
            if first_write {
                first_write = false;
                Ok(1)
            } else {
                Ok(0)
            }
        }) else {
            panic!("second write position must overflow");
        };
        assert_eq!(write_overflow.kind(), std::io::ErrorKind::InvalidInput);
    }
}
