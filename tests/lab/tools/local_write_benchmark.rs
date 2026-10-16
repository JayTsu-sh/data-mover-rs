#[cfg(unix)]
mod unix {
    use std::fs::OpenOptions;
    use std::os::unix::fs::FileExt as _;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Instant;

    use bytes::Bytes;
    use clap::Parser;
    use tokio::task::JoinSet;

    #[derive(Debug, Parser)]
    #[command(about = "Measure isolated positional Local destination writes")]
    struct Args {
        /// Isolated scratch file. It is deleted after a successful sample.
        #[arg(long)]
        file: PathBuf,
        #[arg(long, default_value_t = 256 * 1024 * 1024)]
        total_bytes: u64,
        #[arg(long)]
        chunk_bytes: usize,
        #[arg(long)]
        inflight: usize,
    }

    #[derive(Default)]
    struct WriteCounters {
        calls: AtomicU64,
        short_writes: AtomicU64,
    }

    fn write_piece(
        file: &std::fs::File,
        counters: &WriteCounters,
        offset: u64,
        data: &[u8],
    ) -> std::io::Result<u64> {
        let mut written = 0_usize;
        while written < data.len() {
            counters.calls.fetch_add(1, Ordering::Relaxed);
            let position = offset.checked_add(written as u64).ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "write offset overflow")
            })?;
            let count = file.write_at(&data[written..], position)?;
            if count == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "local positional write made no progress",
                ));
            }
            if count < data.len() - written {
                counters.short_writes.fetch_add(1, Ordering::Relaxed);
            }
            written += count;
        }
        Ok(data.len() as u64)
    }

    async fn settle_one(
        writes: &mut JoinSet<std::io::Result<u64>>,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let result = writes
            .join_next()
            .await
            .ok_or("write set ended before all pieces completed")??;
        Ok(result?)
    }

    fn peak_rss_kib() -> Option<u64> {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        let line = status.lines().find(|line| line.starts_with("VmHWM:"))?;
        line.split_ascii_whitespace().nth(1)?.parse().ok()
    }

    #[tokio::main]
    pub(crate) async fn run() -> Result<(), Box<dyn std::error::Error>> {
        let args = Args::parse();
        if args.total_bytes == 0 || args.chunk_bytes == 0 || args.inflight == 0 {
            return Err("total bytes, chunk bytes, and inflight must be greater than zero".into());
        }
        if args.file.parent().is_none_or(|parent| !parent.is_dir()) {
            return Err("benchmark file parent must be an existing directory".into());
        }

        let file = Arc::new(
            OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(&args.file)?,
        );
        let counters = Arc::new(WriteCounters::default());
        // One immutable payload excludes source allocation/copying from the destination benchmark.
        let payload = Bytes::from(vec![0xa5; args.chunk_bytes]);
        let mut writes = JoinSet::new();
        let mut issued = 0_u64;
        let mut completed = 0_u64;
        let submit_started = Instant::now();

        while issued < args.total_bytes {
            let length =
                usize::try_from((args.total_bytes - issued).min(u64::try_from(args.chunk_bytes)?))?;
            let offset = issued;
            let data = payload.slice(..length);
            let task_file = Arc::clone(&file);
            let task_counters = Arc::clone(&counters);
            writes.spawn_blocking(move || {
                write_piece(&task_file, &task_counters, offset, data.as_ref())
            });
            issued += length as u64;
            if writes.len() >= args.inflight {
                completed += settle_one(&mut writes).await?;
            }
        }
        while !writes.is_empty() {
            completed += settle_one(&mut writes).await?;
        }
        let submit_ns = submit_started.elapsed().as_nanos();

        let sync_started = Instant::now();
        let sync_file = Arc::clone(&file);
        tokio::task::spawn_blocking(move || sync_file.sync_all()).await??;
        let sync_ns = sync_started.elapsed().as_nanos();
        let total_ns = submit_ns + sync_ns;
        if completed != args.total_bytes || file.metadata()?.len() != args.total_bytes {
            return Err("benchmark did not persist the requested file length".into());
        }

        let calls = counters.calls.load(Ordering::Relaxed);
        let short_writes = counters.short_writes.load(Ordering::Relaxed);
        let peak_rss_kib = peak_rss_kib().unwrap_or(0);
        drop(file);
        std::fs::remove_file(&args.file)?;

        println!(
            "total_bytes={}\tchunk_bytes={}\tinflight={}\tsubmit_ns={}\tsync_ns={}\ttotal_ns={}\twrite_calls={}\tshort_writes={}\tpeak_rss_kib={}",
            args.total_bytes,
            args.chunk_bytes,
            args.inflight,
            submit_ns,
            sync_ns,
            total_ns,
            calls,
            short_writes,
            peak_rss_kib
        );
        Ok(())
    }
}

#[cfg(unix)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    unix::run()
}

#[cfg(not(unix))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    Err("the Local positional-write benchmark currently requires Unix FileExt".into())
}
