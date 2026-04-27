//! 时间转换原语。
//!
//! 全局以 i64 纳秒（自 Unix 纪元）为内部时间表示。各协议封装类型
//! （`nfs_rs::Time`、`smb` 的 `FileTime`、`aws_sdk_s3` 的 `DateTime`）
//! 在各自模块保留薄封装，但都委托到这里的原语，避免常量与公式的散落。

use std::time::{SystemTime, UNIX_EPOCH};

use filetime::FileTime;

pub(crate) const NANOS_PER_SEC: i64 = 1_000_000_000;

/// Windows FILETIME 纪元 (1601-01-01) 与 Unix 纪元 (1970-01-01) 之间的差，
/// 以 100ns 为单位。
pub(crate) const FILETIME_UNIX_EPOCH_DIFF: i64 = 116_444_736_000_000_000;

#[inline]
#[must_use]
pub fn combine_secs_nanos(secs: i64, nsecs: u32) -> i64 {
    secs * NANOS_PER_SEC + i64::from(nsecs)
}

#[inline]
#[must_use]
pub fn nanos_to_secs(nanos: i64) -> i64 {
    nanos / NANOS_PER_SEC
}

#[inline]
#[must_use]
#[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
pub fn nanos_subsec(nanos: i64) -> u32 {
    (nanos % NANOS_PER_SEC) as u32
}

#[inline]
#[must_use]
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
pub fn system_time_to_nanos(st: SystemTime) -> i64 {
    st.duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as i64).unwrap_or(0)
}

#[inline]
#[must_use]
pub fn now_nanos() -> i64 {
    system_time_to_nanos(SystemTime::now())
}

#[inline]
#[must_use]
pub fn now_secs() -> i64 {
    nanos_to_secs(now_nanos())
}

/// 纳秒 → `filetime` crate 的 `FileTime`（Unix 纪元，秒 + 纳秒分量）。
/// 名称带 `_local` 以区别于使用 Windows 纪元的 [`smb_filetime_to_nanos`]。
#[inline]
#[must_use]
pub fn nanos_to_filetime_local(nanos: i64) -> FileTime {
    FileTime::from_unix_time(nanos_to_secs(nanos), nanos_subsec(nanos))
}

/// SMB 原始 FILETIME 值（u64 强转 i64，100ns 自 1601）→ 纳秒。
#[inline]
#[must_use]
pub fn smb_filetime_to_nanos(raw: i64) -> i64 {
    (raw - FILETIME_UNIX_EPOCH_DIFF) * 100
}

/// 纳秒 → SMB 原始 FILETIME 值（100ns 自 1601）。返回 i64，调用方按需 `as u64`。
#[inline]
#[must_use]
pub fn nanos_to_smb_filetime(nanos: i64) -> i64 {
    nanos / 100 + FILETIME_UNIX_EPOCH_DIFF
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn combine_split_roundtrip() {
        let secs = 1_700_000_000_i64;
        let nsec = 123_456_789_u32;
        let nanos = combine_secs_nanos(secs, nsec);
        assert_eq!(nanos, 1_700_000_000_123_456_789);
        assert_eq!(nanos_to_secs(nanos), secs);
        assert_eq!(nanos_subsec(nanos), nsec);
    }

    #[test]
    fn smb_filetime_roundtrip() {
        let ns: i64 = 1_700_000_000_000_000_000;
        let raw = nanos_to_smb_filetime(ns);
        let back = smb_filetime_to_nanos(raw);
        assert_eq!(ns, back);
    }

    #[test]
    fn filetime_local_roundtrip() {
        let ns: i64 = 1_700_000_000_123_456_700;
        let ft = nanos_to_filetime_local(ns);
        assert_eq!(ft.unix_seconds(), nanos_to_secs(ns));
        assert_eq!(ft.nanoseconds(), nanos_subsec(ns));
    }

    #[test]
    fn system_time_to_nanos_epoch() {
        let nanos = system_time_to_nanos(UNIX_EPOCH);
        assert_eq!(nanos, 0);
    }

    #[test]
    fn now_is_positive() {
        assert!(now_nanos() > 0);
        assert!(now_secs() > 0);
    }

    #[test]
    fn now_secs_and_nanos_agree() {
        let before = now_secs();
        let nanos = now_nanos();
        let after = now_secs();
        // nanos_to_secs(nanos) must fall within [before, after] (same-second or adjacent second)
        let secs_from_nanos = nanos_to_secs(nanos);
        assert!(secs_from_nanos >= before && secs_from_nanos <= after + 1);
    }
}
