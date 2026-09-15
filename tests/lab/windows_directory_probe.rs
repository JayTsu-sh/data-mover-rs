use std::fs::OpenOptions;
use std::os::windows::fs::OpenOptionsExt;
fn main() -> std::io::Result<()> {
    let directory = std::env::temp_dir().join(format!("data-mover-dirsync-probe-{}", std::process::id()));
    std::fs::create_dir(&directory)?;
    for (label, write, flags) in [("read-default", false, 0), ("read-directory", false, 0x02000000), ("write-directory", true, 0x02000000)] {
        match OpenOptions::new().read(true).write(write).custom_flags(flags).open(&directory) {
            Ok(file) => println!("[DIRSYNC-PROBE] {label}: open=OK, sync={:?}", file.sync_all()),
            Err(error) => println!("[DIRSYNC-PROBE] {label}: open={error:?}"),
        }
    }
    std::fs::remove_dir(directory)
}
