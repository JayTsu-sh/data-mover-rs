//! PROTOTYPE ONLY: compare blocking positional I/O with io_uring on Linux.

#[cfg(not(target_os = "linux"))]
compile_error!("this prototype requires Linux");

use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use io_uring::{IoUring, opcode, types};

const DEFAULT_SIZE_MIB: u64 = 512;
const DEFAULT_BLOCK_MIB: u64 = 2;
const FSYNC_TAG: u64 = u64::MAX;

#[derive(Clone, Copy, Debug)]
struct PipelineConfig {
    read_inflight: usize,
    write_inflight: usize,
    channel_capacity: usize,
}

#[derive(Clone, Debug)]
struct Config {
    size: u64,
    block_size: usize,
    repeats: usize,
    pipelines: Vec<PipelineConfig>,
    ring_entries: u32,
}

#[derive(Clone, Copy, Debug)]
struct Usage {
    user: Duration,
    system: Duration,
}

#[derive(Debug)]
struct Measurement {
    elapsed: Duration,
    usage: Usage,
}

#[derive(Debug)]
struct Chunk {
    buffer: Vec<u8>,
    offset: u64,
    len: usize,
}

fn main() -> io::Result<()> {
    let config = parse_config()?;
    let root = env::temp_dir().join(format!(
        "data-mover-io-uring-prototype-{}",
        std::process::id()
    ));
    let result = run(&root, &config);
    let cleanup = cleanup_temp_root(&root);
    match (result, cleanup) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    }
}

fn run(root: &Path, config: &Config) -> io::Result<()> {
    fs::create_dir_all(root)?;
    let source_path = root.join("source.bin");
    println!(
        "prototype=io_uring_pipeline size_mib={} block_mib={:.3} repeats={} pipelines={:?} ring_entries={} temp_root={}",
        config.size / 1_048_576,
        config.block_size as f64 / 1_048_576.0,
        config.repeats,
        config.pipelines,
        config.ring_entries,
        root.display()
    );
    create_fixture(&source_path, config.size, config.block_size)?;
    let expected_hash = hash_file(&source_path)?;
    probe_required_opcodes()?;

    println!(
        "engine,read_inflight,write_inflight,channel_capacity,ring_entries,repeat,elapsed_s,user_s,system_s,cpu_percent,throughput_mib_s,verified"
    );
    for repeat in 1..=config.repeats {
        let pipelines: Box<dyn Iterator<Item = &PipelineConfig>> = if repeat % 2 == 0 {
            Box::new(config.pipelines.iter().rev())
        } else {
            Box::new(config.pipelines.iter())
        };
        for &pipeline in pipelines {
            let blocking_path = root.join("blocking.bin");
            let measurement = measure(|| {
                copy_blocking(
                    &source_path,
                    &blocking_path,
                    config.size,
                    config.block_size,
                    pipeline,
                )
            })?;
            print_measurement(
                "blocking",
                pipeline,
                config.ring_entries,
                repeat,
                config.size,
                &measurement,
                hash_file(&blocking_path)? == expected_hash,
            );

            let uring_path = root.join("uring.bin");
            let measurement = measure(|| {
                copy_uring(
                    &source_path,
                    &uring_path,
                    config.size,
                    config.block_size,
                    pipeline,
                    config.ring_entries,
                )
            })?;
            print_measurement(
                "io_uring",
                pipeline,
                config.ring_entries,
                repeat,
                config.size,
                &measurement,
                hash_file(&uring_path)? == expected_hash,
            );
        }
    }
    Ok(())
}

fn parse_config() -> io::Result<Config> {
    let mut size_mib = DEFAULT_SIZE_MIB;
    let mut block_mib = DEFAULT_BLOCK_MIB;
    let mut repeats = 2usize;
    let mut pipelines = vec![
        PipelineConfig {
            read_inflight: 4,
            write_inflight: 8,
            channel_capacity: 8,
        },
        PipelineConfig {
            read_inflight: 8,
            write_inflight: 8,
            channel_capacity: 8,
        },
        PipelineConfig {
            read_inflight: 8,
            write_inflight: 16,
            channel_capacity: 16,
        },
        PipelineConfig {
            read_inflight: 16,
            write_inflight: 16,
            channel_capacity: 16,
        },
        PipelineConfig {
            read_inflight: 16,
            write_inflight: 32,
            channel_capacity: 32,
        },
        PipelineConfig {
            read_inflight: 32,
            write_inflight: 32,
            channel_capacity: 32,
        },
    ];
    let mut ring_entries = 64_u32;
    let mut args = env::args().skip(1);
    while let Some(flag) = args.next() {
        let value = args
            .next()
            .ok_or_else(|| invalid(format!("missing value for {flag}")))?;
        match flag.as_str() {
            "--size-mib" => size_mib = parse(&flag, &value)?,
            "--block-mib" => block_mib = parse(&flag, &value)?,
            "--repeats" => repeats = parse(&flag, &value)?,
            "--pipelines" => {
                pipelines = value
                    .split(',')
                    .map(|part| parse_pipeline(&flag, part))
                    .collect::<io::Result<Vec<_>>>()?;
            }
            "--ring-entries" => ring_entries = parse(&flag, &value)?,
            _ => return Err(invalid(format!("unknown option {flag}"))),
        }
    }
    if size_mib == 0
        || block_mib == 0
        || repeats == 0
        || pipelines.is_empty()
        || pipelines.iter().any(|pipeline| {
            pipeline.read_inflight == 0
                || pipeline.write_inflight == 0
                || pipeline.channel_capacity == 0
                || pipeline.read_inflight > 1024
                || pipeline.write_inflight > 1024
                || pipeline.channel_capacity > 1024
        })
        || ring_entries < 8
        || !ring_entries.is_power_of_two()
    {
        return Err(invalid(
            "pipeline values must be in 1..=1024; ring entries must be a power of two >= 8",
        ));
    }
    let size = size_mib
        .checked_mul(1_048_576)
        .ok_or_else(|| invalid("size overflow"))?;
    let block_bytes = block_mib
        .checked_mul(1_048_576)
        .ok_or_else(|| invalid("block size overflow"))?;
    let block_size =
        usize::try_from(block_bytes).map_err(|_| invalid("block size exceeds usize"))?;
    Ok(Config {
        size,
        block_size,
        repeats,
        pipelines,
        ring_entries,
    })
}

fn parse_pipeline(flag: &str, value: &str) -> io::Result<PipelineConfig> {
    let parts = value.split(':').collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err(invalid(format!(
            "invalid value for {flag}: {value}; expected read:write:channel"
        )));
    }
    Ok(PipelineConfig {
        read_inflight: parse(flag, parts[0])?,
        write_inflight: parse(flag, parts[1])?,
        channel_capacity: parse(flag, parts[2])?,
    })
}

fn parse<T: std::str::FromStr>(flag: &str, value: &str) -> io::Result<T> {
    value
        .parse()
        .map_err(|_| invalid(format!("invalid value for {flag}: {value}")))
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn create_fixture(path: &Path, size: u64, block_size: usize) -> io::Result<()> {
    let mut file = File::create(path)?;
    let mut block = vec![0_u8; block_size];
    let mut offset = 0_u64;
    while offset < size {
        for (index, byte) in block.iter_mut().enumerate() {
            let absolute = offset + index as u64;
            *byte = absolute.wrapping_mul(0x9e37_79b9_7f4a_7c15).rotate_left(17) as u8;
        }
        let len = usize::try_from((size - offset).min(block_size as u64))
            .map_err(|_| invalid("fixture length overflow"))?;
        file.write_all(&block[..len])?;
        offset += len as u64;
    }
    file.sync_all()
}

fn copy_blocking(
    source_path: &Path,
    destination_path: &Path,
    size: u64,
    block_size: usize,
    pipeline: PipelineConfig,
) -> io::Result<()> {
    let source = Arc::new(File::open(source_path)?);
    let destination = Arc::new(
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(destination_path)?,
    );
    let next_offset = Arc::new(AtomicU64::new(0));
    let (sender, receiver) = mpsc::sync_channel::<Chunk>(pipeline.channel_capacity);
    let mut readers = Vec::with_capacity(pipeline.read_inflight);
    for _ in 0..pipeline.read_inflight {
        let source = Arc::clone(&source);
        let next_offset = Arc::clone(&next_offset);
        let sender = sender.clone();
        readers.push(std::thread::spawn(move || -> io::Result<()> {
            let mut buffer = vec![0_u8; block_size];
            loop {
                let offset = next_offset.fetch_add(block_size as u64, Ordering::Relaxed);
                if offset >= size {
                    break;
                }
                let len = usize::try_from((size - offset).min(block_size as u64))
                    .map_err(|_| invalid("copy length overflow"))?;
                read_exact_at(&source, &mut buffer[..len], offset)?;
                sender
                    .send(Chunk {
                        buffer,
                        offset,
                        len,
                    })
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::BrokenPipe, "write pipeline stopped")
                    })?;
                buffer = vec![0_u8; block_size];
            }
            Ok(())
        }));
    }
    drop(sender);
    let receiver = Arc::new(Mutex::new(receiver));
    let mut writers = Vec::with_capacity(pipeline.write_inflight);
    for _ in 0..pipeline.write_inflight {
        let destination = Arc::clone(&destination);
        let receiver = Arc::clone(&receiver);
        writers.push(std::thread::spawn(move || -> io::Result<()> {
            loop {
                let chunk = receiver
                    .lock()
                    .map_err(|_| io::Error::other("blocking channel lock poisoned"))?
                    .recv();
                let Ok(chunk) = chunk else { break };
                write_all_at(&destination, &chunk.buffer[..chunk.len], chunk.offset)?;
            }
            Ok(())
        }));
    }
    for reader in readers {
        reader
            .join()
            .map_err(|_| io::Error::other("blocking read worker panicked"))??;
    }
    for writer in writers {
        writer
            .join()
            .map_err(|_| io::Error::other("blocking write worker panicked"))??;
    }
    destination.sync_all()
}

fn read_exact_at(file: &File, mut buffer: &mut [u8], mut offset: u64) -> io::Result<()> {
    while !buffer.is_empty() {
        let count = file.read_at(buffer, offset)?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "short positional read",
            ));
        }
        offset += count as u64;
        buffer = &mut buffer[count..];
    }
    Ok(())
}

fn write_all_at(file: &File, mut buffer: &[u8], mut offset: u64) -> io::Result<()> {
    while !buffer.is_empty() {
        let count = file.write_at(buffer, offset)?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "short positional write",
            ));
        }
        offset += count as u64;
        buffer = &buffer[count..];
    }
    Ok(())
}

fn copy_uring(
    source_path: &Path,
    destination_path: &Path,
    size: u64,
    block_size: usize,
    pipeline: PipelineConfig,
    ring_entries: u32,
) -> io::Result<()> {
    if ring_entries < pipeline.read_inflight.max(pipeline.write_inflight) as u32 {
        return Err(invalid("ring entries must cover each inflight window"));
    }
    let source = File::open(source_path)?;
    let destination = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(destination_path)?;
    let (sender, receiver) = mpsc::sync_channel::<Chunk>(pipeline.channel_capacity);
    let read_handle = std::thread::spawn(move || {
        uring_read_pipeline(
            source,
            size,
            block_size,
            pipeline.read_inflight,
            ring_entries,
            sender,
        )
    });
    uring_write_pipeline(destination, pipeline.write_inflight, ring_entries, receiver)?;
    read_handle
        .join()
        .map_err(|_| io::Error::other("io_uring read worker panicked"))??;
    Ok(())
}

fn uring_read_pipeline(
    source: File,
    size: u64,
    block_size: usize,
    read_inflight: usize,
    ring_entries: u32,
    sender: mpsc::SyncSender<Chunk>,
) -> io::Result<()> {
    let mut ring = IoUring::new(ring_entries)?;
    let mut inflight = std::collections::HashMap::<u64, Chunk>::new();
    let mut next_offset = 0_u64;
    let mut next_id = 0_u64;
    while next_offset < size || !inflight.is_empty() {
        while inflight.len() < read_inflight && next_offset < size {
            let len = usize::try_from((size - next_offset).min(block_size as u64))
                .map_err(|_| invalid("read length overflow"))?;
            let chunk = Chunk {
                buffer: vec![0_u8; block_size],
                offset: next_offset,
                len,
            };
            let entry = opcode::Read::new(
                types::Fd(source.as_raw_fd()),
                chunk.buffer.as_ptr().cast_mut(),
                u32::try_from(len).map_err(|_| invalid("read length exceeds u32"))?,
            )
            .offset(next_offset)
            .build()
            .user_data(next_id);
            unsafe {
                ring.submission()
                    .push(&entry)
                    .map_err(|_| io::Error::other("read SQ full"))?;
            }
            inflight.insert(next_id, chunk);
            next_id += 1;
            next_offset += len as u64;
        }
        ring.submit_and_wait(1)?;
        let completions = ring
            .completion()
            .map(|cqe| (cqe.user_data(), cqe.result()))
            .collect::<Vec<_>>();
        for (id, result) in completions {
            let chunk = inflight
                .remove(&id)
                .ok_or_else(|| io::Error::other("unknown read completion"))?;
            check_completion(result, chunk.len, io::ErrorKind::UnexpectedEof)?;
            sender
                .send(chunk)
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "write pipeline stopped"))?;
        }
    }
    Ok(())
}

fn uring_write_pipeline(
    destination: File,
    write_inflight: usize,
    ring_entries: u32,
    receiver: mpsc::Receiver<Chunk>,
) -> io::Result<()> {
    let mut ring = IoUring::new(ring_entries)?;
    let mut inflight = std::collections::HashMap::<u64, Chunk>::new();
    let mut next_id = 0_u64;
    let mut input_open = true;
    while input_open || !inflight.is_empty() {
        while input_open && inflight.len() < write_inflight {
            let chunk = if inflight.is_empty() {
                match receiver.recv() {
                    Ok(chunk) => chunk,
                    Err(_) => {
                        input_open = false;
                        break;
                    }
                }
            } else {
                match receiver.try_recv() {
                    Ok(chunk) => chunk,
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        input_open = false;
                        break;
                    }
                }
            };
            let entry = opcode::Write::new(
                types::Fd(destination.as_raw_fd()),
                chunk.buffer.as_ptr(),
                u32::try_from(chunk.len).map_err(|_| invalid("write length exceeds u32"))?,
            )
            .offset(chunk.offset)
            .build()
            .user_data(next_id);
            unsafe {
                ring.submission()
                    .push(&entry)
                    .map_err(|_| io::Error::other("write SQ full"))?;
            }
            inflight.insert(next_id, chunk);
            next_id += 1;
        }
        if !inflight.is_empty() {
            ring.submit_and_wait(1)?;
            let completions = ring
                .completion()
                .map(|cqe| (cqe.user_data(), cqe.result()))
                .collect::<Vec<_>>();
            for (id, result) in completions {
                let chunk = inflight
                    .remove(&id)
                    .ok_or_else(|| io::Error::other("unknown write completion"))?;
                check_completion(result, chunk.len, io::ErrorKind::WriteZero)?;
            }
        }
    }
    submit_fsync(&mut ring, &destination)?;
    ring.submit_and_wait(1)?;
    let completion = ring
        .completion()
        .next()
        .ok_or_else(|| io::Error::other("missing io_uring fsync completion"))?;
    if completion.result() < 0 {
        return Err(io::Error::from_raw_os_error(-completion.result()));
    }
    Ok(())
}

fn check_completion(result: i32, expected: usize, short_kind: io::ErrorKind) -> io::Result<()> {
    if result < 0 {
        return Err(io::Error::from_raw_os_error(-result));
    }
    if usize::try_from(result).ok() != Some(expected) {
        return Err(io::Error::new(
            short_kind,
            format!("short io_uring operation: expected {expected}, got {result}"),
        ));
    }
    Ok(())
}

fn submit_fsync(ring: &mut IoUring, destination: &File) -> io::Result<()> {
    let entry = opcode::Fsync::new(types::Fd(destination.as_raw_fd()))
        .build()
        .user_data(FSYNC_TAG);
    unsafe {
        ring.submission()
            .push(&entry)
            .map_err(|_| io::Error::other("io_uring submission queue full"))?;
    }
    Ok(())
}

fn probe_required_opcodes() -> io::Result<()> {
    let ring = IoUring::new(8)?;
    let mut probe = io_uring::Probe::new();
    ring.submitter().register_probe(&mut probe)?;
    for (name, supported) in [
        ("READ", probe.is_supported(opcode::Read::CODE)),
        ("WRITE", probe.is_supported(opcode::Write::CODE)),
        ("FSYNC", probe.is_supported(opcode::Fsync::CODE)),
    ] {
        if !supported {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("required io_uring opcode is unavailable: {name}"),
            ));
        }
    }
    println!("io_uring_probe=ok required_opcodes=READ,WRITE,FSYNC");
    Ok(())
}

fn hash_file(path: &Path) -> io::Result<blake3::Hash> {
    let mut file = File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(hasher.finalize())
}

fn measure(operation: impl FnOnce() -> io::Result<()>) -> io::Result<Measurement> {
    let before = usage()?;
    let started = Instant::now();
    operation()?;
    let elapsed = started.elapsed();
    let after = usage()?;
    Ok(Measurement {
        elapsed,
        usage: Usage {
            user: after.user.saturating_sub(before.user),
            system: after.system.saturating_sub(before.system),
        },
    })
}

fn usage() -> io::Result<Usage> {
    let mut value = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, value.as_mut_ptr()) };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    let value = unsafe { value.assume_init() };
    Ok(Usage {
        user: timeval(value.ru_utime),
        system: timeval(value.ru_stime),
    })
}

fn timeval(value: libc::timeval) -> Duration {
    Duration::from_secs(value.tv_sec as u64) + Duration::from_micros(value.tv_usec as u64)
}

fn print_measurement(
    engine: &str,
    pipeline: PipelineConfig,
    ring_entries: u32,
    repeat: usize,
    size: u64,
    measurement: &Measurement,
    verified: bool,
) {
    let elapsed = measurement.elapsed.as_secs_f64();
    let cpu = (measurement.usage.user + measurement.usage.system).as_secs_f64() / elapsed * 100.0;
    let throughput = size as f64 / 1_048_576.0 / elapsed;
    println!(
        "{engine},{},{},{},{ring_entries},{repeat},{elapsed:.3},{:.3},{:.3},{cpu:.1},{throughput:.1},{verified}",
        pipeline.read_inflight,
        pipeline.write_inflight,
        pipeline.channel_capacity,
        measurement.usage.user.as_secs_f64(),
        measurement.usage.system.as_secs_f64()
    );
}

fn cleanup_temp_root(root: &Path) -> io::Result<()> {
    let expected_prefix = "data-mover-io-uring-prototype-";
    if root.parent() != Some(env::temp_dir().as_path())
        || !root
            .file_name()
            .is_some_and(|name| name.to_string_lossy().starts_with(expected_prefix))
    {
        return Err(invalid(format!(
            "refusing to remove unexpected path: {}",
            root.display()
        )));
    }
    if root.exists() {
        fs::remove_dir_all(root)?;
        println!("cleaned_temp_root={}", root.display());
    }
    Ok(())
}
