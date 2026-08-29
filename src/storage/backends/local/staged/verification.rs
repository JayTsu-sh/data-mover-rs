use std::io::{self, Read as _};
#[cfg(all(test, unix))]
use std::os::unix::fs::FileExt as _;
#[cfg(all(test, windows))]
use std::os::windows::fs::FileExt as _;
#[cfg(test)]
use std::sync::atomic::Ordering;

use cap_std::fs::{Dir, OpenOptions};

use super::WriteProbe;
use crate::storage::{VerificationEvidence, VerifyRequest};

pub(super) fn verify_local(
    staging: &Dir,
    name: &std::ffi::OsStr,
    request: &VerifyRequest,
    probe: &WriteProbe,
) -> io::Result<VerificationEvidence> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(test)]
    options.write(true);
    let mut file = staging.open_with(name, &options)?.into_std();
    #[cfg(test)]
    if probe.corrupt_before_verify.swap(false, Ordering::SeqCst) {
        #[cfg(unix)]
        file.write_at(&[0xff], 0)?;
        #[cfg(windows)]
        file.seek_write(&[0xff], 0)?;
    }
    #[cfg(not(test))]
    let _ = probe;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    let mut verified_bytes = 0_u64;
    loop {
        if request.cancel.is_cancelled() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "verification cancelled",
            ));
        }
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        verified_bytes = verified_bytes
            .checked_add(read as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "verified size overflow"))?;
        if verified_bytes > request.expected_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "staged content is too large",
            ));
        }
        hasher.update(&buffer[..read]);
    }
    let digest = *hasher.finalize().as_bytes();
    if verified_bytes != request.expected_size || digest != request.expected_blake3 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "staged content verification failed",
        ));
    }
    Ok(VerificationEvidence {
        verified_bytes,
        blake3: digest,
    })
}
