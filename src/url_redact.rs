//! Storage URL 凭据脱敏（通用辅助函数）。
//!
//! Storage URL 可能在 userinfo 段包含 `access_key:secret_key`（S3）、
//! username:password（CIFS）等敏感信息。本模块提供按「最后一个 `@`」切分的
//! 脱敏函数 [`redact_storage_url`]（不依赖标准 URL 解析，秘钥里的 `/` `#` `?` 骗不过它），供：
//!
//! - **外部消费者**（terrasync、integrity-check 等）在自己的日志/审计层
//!   屏蔽 storage URL；
//! - **本 crate 内部新增的日志/错误路径**优先使用此函数。

/// 屏蔽 URL 中的 `user:password@` 部分为 `***:***@`。
///
/// 凭据段按「`://` 之后的**最后一个** `@`」切分，与 `s3::extract_s3_credentials` 读取 AK/SK 的规则
/// 一致：S3 SK 常含 `/`、`+`、`=`，标准 URL 解析器会把 `/` 当成 authority 的结束，看不到真正的
/// userinfo（旧实现因此在解析失败或误拆时原样返回，泄漏 SK）。
///
/// 没有 `@` 的 URL 与本地路径原样返回。路径、查询串或 S3 key 里带 `@` 而本身没有凭据的 URL
/// （`nfs://h/exp/a@b`、`nfs://h/e?owner=a@b`、`s3://AK:SK@b.h/p/img@2x.png`）也会被屏蔽到最后一个
/// `@` 为止 —— 宁可多遮，不可漏遮；代价是日志里看不到真实主机、只剩 `@` 之后的片段。
/// 输出不再经 URL 库规范化（例如 scheme 大小写原样保留）。
///
/// 例：
/// - `s3://AKIA...:secret@bucket.host/p` → `s3://***:***@bucket.host/p`
/// - `s3://AKIA...:wJal/rX+K=@bucket.host/p` → `s3://***:***@bucket.host/p`
/// - `smb://user:pwd@host/share` → `smb://***:***@host/share`
/// - `nfs://server:port/export:/prefix?uid=1000` → 原样返回（无 `@`）
/// - `/local/path` → 原样返回
#[must_use]
pub fn redact_storage_url(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };
    let rest = &url[scheme_end + 3..];
    match rest.rfind('@') {
        Some(at) => format!("{}://***:***@{}", &url[..scheme_end], &rest[at + 1..]),
        None => url.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_s3_credentials() {
        for url in [
            "s3://AKIA1234:secretXYZ@bucket.host:9000/prefix",
            "s3+sg+https://AKIA1234:secretXYZ@bucket.storagegrid.example/prefix",
            "s3+dxn://AKIA1234:secretXYZ@bucket.dxn.example/prefix",
        ] {
            let out = redact_storage_url(url);
            assert!(out.contains("***:***@"));
            assert!(!out.contains("AKIA1234") && !out.contains("secretXYZ"));
        }
    }

    /// S3 secret keys are base64-like and often contain `/`, `+` or `=`; the crate takes them as
    /// written (`extract_s3_credentials` splits at the last `@`), so no URL parser sees the
    /// userinfo where it is.
    #[test]
    fn redacts_secrets_that_a_url_parser_cannot_see() {
        for url in [
            "s3://AKIA1234:wJalr/XUtnFEMI+K7MDENG=@bucket.host:9000/prefix",
            "s3://AKIA1234:12/secretXYZ@bucket.host/prefix",
            "s3+https://AKIA1234:a+b=c@bucket.host/prefix",
        ] {
            let out = redact_storage_url(url);
            assert!(
                !out.contains("AKIA1234")
                    && !out.contains("wJalr")
                    && !out.contains("secretXYZ")
                    && !out.contains("a+b=c"),
                "{out}"
            );
            assert!(
                out.ends_with("@bucket.host:9000/prefix") || out.ends_with("@bucket.host/prefix"),
                "{out}"
            );
        }
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
    fn at_sign_in_a_path_is_over_redacted_rather_than_trusted() {
        assert_eq!(redact_storage_url("nfs://h/exp/a@b"), "nfs://***:***@b");
        assert_eq!(
            redact_storage_url("nfs://srv:2049/export:/p?owner=a@b"),
            "nfs://***:***@b"
        );
    }

    /// A URL parser ends the userinfo at `#` (fragment) or `?` (query), which leaked these too.
    #[test]
    fn redacts_passwords_with_fragment_and_query_characters() {
        for url in ["smb://user:pa#ss@host/share", "smb://user:p?w@host/share"] {
            assert_eq!(redact_storage_url(url), "smb://***:***@host/share", "{url}");
        }
    }

    #[test]
    fn redacts_username_only_userinfo_and_every_s3_profile() {
        assert_eq!(
            redact_storage_url("hdfs://user@nn:8020/p"),
            "hdfs://***:***@nn:8020/p"
        );
        assert_eq!(
            redact_storage_url("s3+hcp://AK:S/K+=@ns.tenant.hcp.example/p"),
            "s3+hcp://***:***@ns.tenant.hcp.example/p"
        );
    }

    #[test]
    fn passthrough_local_path() {
        assert_eq!(redact_storage_url("/local/path"), "/local/path");
        assert_eq!(redact_storage_url("relative/path"), "relative/path");
    }
}
