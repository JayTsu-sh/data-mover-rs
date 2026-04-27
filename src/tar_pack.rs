// 标准库
use std::sync::Arc;

// 外部crate
use bytes::{Bytes, BytesMut};

// 内部模块
use crate::EntryEnum;

/// 构建 512 字节的 ustar header
///
/// ustar 格式布局（512 bytes total）：
/// - 0..100:   name (null-terminated)
/// - 100..108: mode (octal, null-terminated)
/// - 108..116: uid (octal, null-terminated)
/// - 116..124: gid (octal, null-terminated)
/// - 124..136: size (octal, null-terminated)
/// - 136..148: mtime (octal, null-terminated)
/// - 148..156: checksum (octal + space + null)
/// - 156:      `type_flag` ('0'=file, '5'=dir, '2'=symlink)
/// - 157..257: `link_name`
/// - 257..263: "ustar\0"
/// - 263..265: "00"
/// - 265..512: 填零
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_ustar_header(
    path: &str, size: u64, mtime: i64, mode: u32, uid: u32, gid: u32, type_flag: u8, link_name: &str,
) -> [u8; 512] {
    let mut header = [0u8; 512];

    // name (0..100)
    write_bytes(&mut header[0..100], path.as_bytes());

    // mode (100..108) — octal, 7 chars + NUL
    write_octal(&mut header[100..108], u64::from(mode), 7);

    // uid (108..116)
    write_octal(&mut header[108..116], u64::from(uid), 7);

    // gid (116..124)
    write_octal(&mut header[116..124], u64::from(gid), 7);

    // size (124..136) — 11 chars + NUL
    write_octal(&mut header[124..136], size, 11);

    // mtime (136..148) — epoch seconds, 11 chars + NUL
    let mtime_secs = if mtime > 0 {
        crate::time_util::nanos_to_secs(mtime) as u64
    } else {
        0
    };
    write_octal(&mut header[136..148], mtime_secs, 11);

    // type_flag (156)
    header[156] = type_flag;

    // link_name (157..257)
    write_bytes(&mut header[157..257], link_name.as_bytes());

    // magic "ustar\0" (257..263)
    header[257..263].copy_from_slice(b"ustar\0");

    // version "00" (263..265)
    header[263..265].copy_from_slice(b"00");

    // checksum (148..156) — 先用空格填充，然后计算
    header[148..156].copy_from_slice(b"        "); // 8 spaces
    let checksum: u64 = header.iter().map(|&b| u64::from(b)).sum();
    write_octal(&mut header[148..156], checksum, 6);
    header[154] = 0; // NUL
    header[155] = b' '; // trailing space

    header
}

/// 将 octal 值写入指定字段（右对齐，前置零）
fn write_octal(field: &mut [u8], value: u64, width: usize) {
    let s = format!("{value:0>width$o}");
    let bytes = s.as_bytes();
    let len = bytes.len().min(field.len() - 1);
    field[..len].copy_from_slice(&bytes[bytes.len() - len..]);
    // 最后一个字节为 NUL
    if len < field.len() {
        field[len] = 0;
    }
}

/// 将字节拷贝到字段中（不超出字段长度）
fn write_bytes(field: &mut [u8], data: &[u8]) {
    let len = data.len().min(field.len());
    field[..len].copy_from_slice(&data[..len]);
}

/// 计算 tar 文件的总大小
///
/// 每个条目 = 512 字节 header + 文件数据（对齐到 512 字节）
/// 结尾 = 两个 512 字节全零块（EOF marker）
pub fn calculate_tar_size(entries: &[Arc<EntryEnum>]) -> u64 {
    let mut size = 0u64;
    for entry in entries {
        size += 512; // ustar header
        if entry.get_is_regular_file() {
            let file_size = entry.get_size();
            size += file_size;
            // padding to 512-byte boundary
            let remainder = file_size % 512;
            if remainder != 0 {
                size += 512 - remainder;
            }
        }
    }
    size += 1024; // EOF marker (two 512B zero blocks)
    size
}

/// 从 `EntryEnum` 构建 ustar header 的 Bytes
///
/// 自动判断条目类型（目录/symlink/普通文件）并设置对应的 `type_flag` 和 size。
/// `tar_internal_path` 是条目在 tar 内的相对路径。
pub(crate) fn build_header_for_entry(entry: &EntryEnum, tar_internal_path: &str, link_target: &str) -> Bytes {
    let (type_flag, size) = if entry.get_is_dir() {
        (b'5', 0u64)
    } else if entry.get_is_symlink() {
        (b'2', 0u64)
    } else {
        (b'0', entry.get_size())
    };

    // 目录路径需要以 '/' 结尾
    let path = if entry.get_is_dir() && !tar_internal_path.ends_with('/') {
        format!("{tar_internal_path}/")
    } else {
        tar_internal_path.to_string()
    };

    let header = build_ustar_header(
        &path,
        size,
        entry.get_mtime(),
        entry.get_mode().unwrap_or(0),
        entry.get_uid().unwrap_or(0),
        entry.get_gid().unwrap_or(0),
        type_flag,
        link_target,
    );

    Bytes::copy_from_slice(&header)
}

/// 生成 512 字节对齐的 padding
///
/// 如果 `file_size` 不是 512 的倍数，返回对应长度的零填充 Bytes。
/// 如果已对齐则返回 None。
pub(crate) fn tar_padding(file_size: u64) -> Option<Bytes> {
    let remainder = file_size % 512;
    if remainder == 0 {
        return None;
    }
    let padding_len = 512 - remainder;
    let padding = BytesMut::zeroed(padding_len as usize).freeze();
    Some(padding)
}

/// 生成 tar EOF marker（两个 512B 全零块）
pub(crate) fn tar_eof_marker() -> Bytes {
    Bytes::from_static(&[0u8; 1024])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_ustar_header_basic() {
        let header = build_ustar_header("test.txt", 1024, 1_700_000_000_000_000_000, 0o644, 1000, 1000, b'0', "");

        // magic
        assert_eq!(&header[257..263], b"ustar\0");
        // version
        assert_eq!(&header[263..265], b"00");
        // type_flag
        assert_eq!(header[156], b'0');
        // name starts with "test.txt"
        assert_eq!(&header[0..8], b"test.txt");

        // checksum 字段非空
        assert_ne!(&header[148..156], &[0u8; 8]);
    }

    #[test]
    fn test_build_ustar_header_directory() {
        let header = build_ustar_header("mydir/", 0, 1_700_000_000_000_000_000, 0o755, 0, 0, b'5', "");
        assert_eq!(header[156], b'5');
        // size should be zero for directories
        // size field (124..136): "00000000000\0"
        let size_field = std::str::from_utf8(&header[124..135]).ok();
        assert!(size_field.is_some());
        assert_eq!(
            u64::from_str_radix(size_field.map(|s| s.trim_start_matches('0')).unwrap_or("0"), 8).unwrap_or(0),
            0
        );
    }

    #[test]
    fn test_checksum_correctness() {
        let header = build_ustar_header("hello.txt", 5, 1_000_000_000_000_000_000, 0o644, 0, 0, b'0', "");

        // 验证 checksum：将 checksum 字段视为空格，求所有字节的和
        let mut check_header = header;
        check_header[148..156].copy_from_slice(b"        ");
        let expected: u64 = check_header.iter().map(|&b| b as u64).sum();

        // 从 header 读取写入的 checksum 值
        let cksum_str = std::str::from_utf8(&header[148..154]).expect("valid utf8");
        let stored_cksum = u64::from_str_radix(cksum_str.trim_start_matches('0'), 8).unwrap_or(0);

        assert_eq!(stored_cksum, expected);
    }

    #[test]
    fn test_calculate_tar_size_empty() {
        let entries: Vec<Arc<EntryEnum>> = vec![];
        assert_eq!(calculate_tar_size(&entries), 1024); // only EOF marker
    }

    #[test]
    fn test_tar_padding() {
        assert!(tar_padding(512).is_none());
        assert!(tar_padding(1024).is_none());

        let padding = tar_padding(100).expect("should have padding");
        assert_eq!(padding.len(), 412); // 512 - 100

        let padding = tar_padding(513).expect("should have padding");
        assert_eq!(padding.len(), 511); // 1024 - 513
    }

    #[test]
    fn test_tar_eof_marker() {
        let eof = tar_eof_marker();
        assert_eq!(eof.len(), 1024);
        assert!(eof.iter().all(|&b| b == 0));
    }
}
