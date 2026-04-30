//! Storage URL 凭据脱敏（通用辅助函数）。
//!
//! Storage URL 可能在 userinfo 段包含 access_key:secret_key（S3）、
//! username:password（CIFS、NFS over RPC-AUTH）等敏感信息。本模块提供
//! 一个标准 URL 解析驱动的脱敏函数 [`redact_storage_url`]，供：
//!
//! - **外部消费者**（terrasync、integrity-check 等）在自己的日志/审计层
//!   屏蔽 storage URL；
//! - **本 crate 内部新增的日志/错误路径**优先使用此函数。
//!
//! 注意：CIFS 已有 `cifs::redact_smb_url`（**保留 username** 仅屏蔽密码，
//! 用于现有错误诊断兼容性）；切换到本通用函数会丢失 username，需评估后再换。

/// 屏蔽 URL 中的 `user:password@` 部分为 `***:***@`。
///
/// 解析失败（非标准 URL，如本地路径 `/foo/bar`）时**原样返回**——
/// 本地路径不含凭据，无需屏蔽。
///
/// 例：
/// - `s3://AKIA...:secret@bucket.host/p` → `s3://***:***@bucket.host/p`
/// - `smb://user:pwd@host/share` → `smb://***:***@host/share`
/// - `nfs://server:port/export:/prefix?uid=1000` → 原样返回（无 userinfo）
/// - `/local/path` → 原样返回
pub fn redact_storage_url(url: &str) -> String {
    let Ok(mut parsed) = url::Url::parse(url) else {
        return url.to_string();
    };
    if parsed.username().is_empty() && parsed.password().is_none() {
        return url.to_string();
    }
    let _ = parsed.set_username("***");
    let _ = parsed.set_password(Some("***"));
    parsed.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_s3_credentials() {
        let out = redact_storage_url("s3://AKIA1234:secretXYZ@bucket.host:9000/prefix");
        assert!(out.contains("***:***@"));
        assert!(!out.contains("AKIA1234"));
        assert!(!out.contains("secretXYZ"));
    }

    #[test]
    fn redacts_cifs_credentials() {
        let out = redact_storage_url("smb://user:pwd@host/share/path");
        assert!(out.contains("***:***@"));
        assert!(!out.contains("pwd"));
    }

    #[test]
    fn passthrough_when_no_userinfo() {
        let url = "nfs://server:2049/export:/prefix?uid=1000&gid=1000";
        assert_eq!(redact_storage_url(url), url);
    }

    #[test]
    fn passthrough_local_path() {
        assert_eq!(redact_storage_url("/local/path"), "/local/path");
        assert_eq!(redact_storage_url("relative/path"), "relative/path");
    }
}
