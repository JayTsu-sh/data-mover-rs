//! PROTOTYPE ONLY: compare blocking positional I/O with whole-file mmap copying.

#[cfg(not(target_os = "linux"))]
compile_error!("this prototype requires Linux");

use std::env;
use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

const BLOCK_SIZE: usize = 2 * 1024 * 1024;

#[derive(Clone, Debug)]
struct Config {
    files: usize,
    sizes_mib: Vec<usize>,
    concurrency: usize,
    repeats: usize,
}

fn main() -> ExitCode {
    match real_main() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

fn real_main() -> io::Result<()> {
    let args = env::args_os().collect::<Vec<_>>();
    if args.get(1).is_some_and(|arg| arg == "--child") {
        return child(&args[2..]);
    }
    parent(parse_config(&args[1..])?)
}

fn parse_config(args: &[OsString]) -> io::Result<Config> {
    let mut config = Config {
        files: 2048,
        sizes_mib: vec![4, 16, 64],
        concurrency: 8,
        repeats: 1,
    };
    let mut index = 0;
    while index < args.len() {
        let name = args[index].to_string_lossy();
        let value = args
            .get(index + 1)
            .ok_or_else(|| invalid(format!("missing value for {name}")))?;
        match name.as_ref() {
            "--files" => config.files = parse(value, "files")?,
            "--sizes-mib" => {
                config.sizes_mib = value
                    .to_string_lossy()
                    .split(',')
                    .map(|item| item.parse().map_err(|_| invalid("invalid sizes-mib")))
                    .collect::<io::Result<Vec<_>>>()?;
            }
            "--concurrency" => config.concurrency = parse(value, "concurrency")?,
            "--repeats" => config.repeats = parse(value, "repeats")?,
            _ => return Err(invalid(format!("unknown option {name}"))),
        }
        index += 2;
    }
    if config.files == 0
        || config.concurrency == 0
        || config.repeats == 0
        || config.sizes_mib.contains(&0)
    {
        return Err(invalid("all numeric values must be positive"));
    }
    Ok(config)
}

fn parse<T: std::str::FromStr>(value: &OsString, name: &str) -> io::Result<T> {
    value
        .to_string_lossy()
        .parse()
        .map_err(|_| invalid(format!("invalid {name}")))
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn parent(config: Config) -> io::Result<()> {
    let root = env::temp_dir().join(format!("data-mover-mmap-prototype-{}", std::process::id()));
    fs::create_dir_all(&root)?;
    println!(
        "prototype=blocking_vs_mmap files={} sizes_mib={:?} concurrency={} block_mib=2 repeats={} temp_root={}",
        config.files,
        config.sizes_mib,
        config.concurrency,
        config.repeats,
        root.display()
    );
    println!(
        "engine,repeat,files,size_mib,concurrency,elapsed_s,files_per_s,throughput_mib_s,user_s,system_s,cpu_percent,rss_kib,verified"
    );
    let executable = env::current_exe()?;
    let result = (|| {
        for &size_mib in &config.sizes_mib {
            let source = root.join(format!("source-{size_mib}"));
            create_fixtures(&source, config.files, size_mib)?;
            for repeat in 1..=config.repeats {
                let engines = if repeat % 2 == 0 {
                    ["mmap", "blocking"]
                } else {
                    ["blocking", "mmap"]
                };
                for engine in engines {
                    let destination = root.join(format!("destination-{size_mib}-{engine}"));
                    let status = Command::new(&executable)
                        .args(["--child", engine])
                        .arg(&source)
                        .arg(&destination)
                        .args([
                            config.files.to_string(),
                            size_mib.to_string(),
                            config.concurrency.to_string(),
                            repeat.to_string(),
                        ])
                        .status()?;
                    if !status.success() {
                        return Err(io::Error::other(format!("{engine} child failed")));
                    }
                    fs::remove_dir_all(destination)?;
                }
            }
            fs::remove_dir_all(source)?;
        }
        Ok(())
    })();
    let cleanup = fs::remove_dir_all(&root);
    result.and(cleanup)
}

fn child(args: &[OsString]) -> io::Result<()> {
    if args.len() != 7 {
        return Err(invalid("invalid child arguments"));
    }
    let engine = args[0].to_string_lossy();
    let source = PathBuf::from(&args[1]);
    let destination = PathBuf::from(&args[2]);
    let files: usize = parse(&args[3], "files")?;
    let size_mib: usize = parse(&args[4], "size_mib")?;
    let concurrency: usize = parse(&args[5], "concurrency")?;
    let repeat: usize = parse(&args[6], "repeat")?;
    fs::create_dir_all(&destination)?;
    let usage_before = usage()?;
    let started = Instant::now();
    copy_many(&engine, &source, &destination, files, concurrency)?;
    let elapsed = started.elapsed().as_secs_f64();
    let usage_after = usage()?;
    let user = usage_after.0 - usage_before.0;
    let system = usage_after.1 - usage_before.1;
    let verified = verify(&source, &destination, files)?;
    let mib = files as f64 * size_mib as f64;
    println!(
        "{engine},{repeat},{files},{size_mib},{concurrency},{elapsed:.6},{:.3},{:.3},{user:.3},{system:.3},{:.1},{},{}",
        files as f64 / elapsed,
        mib / elapsed,
        (user + system) * 100.0 / elapsed,
        peak_rss_kib()?,
        verified
    );
    Ok(())
}

fn copy_many(
    engine: &str,
    source: &Path,
    destination: &Path,
    files: usize,
    concurrency: usize,
) -> io::Result<()> {
    let next = Arc::new(AtomicUsize::new(0));
    let mut workers = Vec::new();
    for _ in 0..concurrency {
        let next = Arc::clone(&next);
        let source = source.to_owned();
        let destination = destination.to_owned();
        let engine = engine.to_owned();
        workers.push(std::thread::spawn(move || -> io::Result<()> {
            loop {
                let index = next.fetch_add(1, Ordering::Relaxed);
                if index >= files {
                    return Ok(());
                }
                let name = format!("file-{index}.bin");
                match engine.as_str() {
                    "blocking" => copy_blocking(&source.join(&name), &destination.join(&name))?,
                    "mmap" => copy_mmap(&source.join(&name), &destination.join(&name))?,
                    _ => return Err(invalid("invalid engine")),
                }
            }
        }));
    }
    for worker in workers {
        worker
            .join()
            .map_err(|_| io::Error::other("worker panicked"))??;
    }
    Ok(())
}

fn copy_blocking(source: &Path, destination: &Path) -> io::Result<()> {
    let source = File::open(source)?;
    let destination = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(destination)?;
    let size = source.metadata()?.len();
    destination.set_len(size)?;
    let mut buffer = vec![0_u8; BLOCK_SIZE];
    let mut offset = 0_u64;
    while offset < size {
        let length = usize::try_from((size - offset).min(BLOCK_SIZE as u64)).unwrap();
        source.read_exact_at(&mut buffer[..length], offset)?;
        destination.write_all_at(&buffer[..length], offset)?;
        offset += length as u64;
    }
    destination.sync_all()
}

fn copy_mmap(source: &Path, destination: &Path) -> io::Result<()> {
    let source = File::open(source)?;
    let destination = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(destination)?;
    let size = source.metadata()?.len();
    destination.set_len(size)?;
    let length = usize::try_from(size).map_err(|_| invalid("file too large for address space"))?;
    unsafe {
        let src = libc::mmap(
            std::ptr::null_mut(),
            length,
            libc::PROT_READ,
            libc::MAP_PRIVATE,
            source.as_raw_fd(),
            0,
        );
        if src == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        let dst = libc::mmap(
            std::ptr::null_mut(),
            length,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            destination.as_raw_fd(),
            0,
        );
        if dst == libc::MAP_FAILED {
            libc::munmap(src, length);
            return Err(io::Error::last_os_error());
        }
        libc::madvise(src, length, libc::MADV_SEQUENTIAL);
        std::ptr::copy_nonoverlapping(src.cast::<u8>(), dst.cast::<u8>(), length);
        let unmap_dst = libc::munmap(dst, length);
        let unmap_src = libc::munmap(src, length);
        if unmap_dst != 0 || unmap_src != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    destination.sync_all()
}

fn create_fixtures(root: &Path, files: usize, size_mib: usize) -> io::Result<()> {
    fs::create_dir_all(root)?;
    let block = vec![0x5a; 1024 * 1024];
    for index in 0..files {
        let mut file = File::create(root.join(format!("file-{index}.bin")))?;
        for _ in 0..size_mib {
            file.write_all(&block)?;
        }
    }
    Ok(())
}

fn verify(source: &Path, destination: &Path, files: usize) -> io::Result<bool> {
    for index in 0..files {
        let name = format!("file-{index}.bin");
        if hash(&source.join(&name))? != hash(&destination.join(&name))? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn hash(path: &Path) -> io::Result<blake3::Hash> {
    let mut file = File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0_u8; BLOCK_SIZE];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(hasher.finalize())
}

fn usage() -> io::Result<(f64, f64)> {
    let mut usage = unsafe { std::mem::zeroed::<libc::rusage>() };
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let seconds = |value: libc::timeval| value.tv_sec as f64 + value.tv_usec as f64 / 1_000_000.0;
    Ok((seconds(usage.ru_utime), seconds(usage.ru_stime)))
}

fn peak_rss_kib() -> io::Result<u64> {
    let status = fs::read_to_string("/proc/self/status")?;
    Ok(status
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))
        .and_then(|value| value.split_whitespace().next())
        .and_then(|value| value.parse().ok())
        .unwrap_or(0))
}
