//! The `smb://` input boundary. Backend paths are decoded names, never URL text.

use percent_encoding::percent_decode_str;
use url::{Host, Url};

use crate::error::StorageError;

pub(crate) struct ParsedSmbUrl {
    pub(crate) server: String,
    pub(crate) share: String,
    pub(crate) root: Option<String>,
    pub(crate) username: String,
    pub(crate) password: String,
}

pub(crate) fn parse_smb_url(value: &str) -> Result<ParsedSmbUrl, StorageError> {
    let parsed =
        Url::parse(value).map_err(|_| StorageError::UrlParseError("invalid SMB URL".into()))?;
    if parsed.scheme() != "smb" || parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(StorageError::UrlParseError(
            "SMB URL must use smb:// without a query or fragment".into(),
        ));
    }
    let mut server = match parsed.host() {
        Some(Host::Domain(host)) => host.to_owned(),
        Some(Host::Ipv4(address)) => address.to_string(),
        Some(Host::Ipv6(address)) => format!("[{address}]"),
        None => return Err(StorageError::UrlParseError("SMB URL has no server".into())),
    };
    if let Some(port) = parsed.port() {
        server.push(':');
        server.push_str(&port.to_string());
    }

    // Read the original path: Url::parse normalizes dot segments before path() is returned.
    let after_scheme = value
        .split_once("://")
        .map(|(_, rest)| rest)
        .ok_or_else(|| StorageError::UrlParseError("invalid SMB URL".into()))?;
    let (_, raw_path) = after_scheme
        .split_once('/')
        .ok_or_else(|| StorageError::UrlParseError("SMB URL has no share".into()))?;
    let raw_path = raw_path.split(['?', '#']).next().unwrap_or_default();
    let mut segments = raw_path.split('/');
    let share = decode_segment(segments.next().unwrap_or_default())?;
    if share.is_empty() {
        return Err(StorageError::UrlParseError("SMB URL has no share".into()));
    }
    let root = segments
        .filter(|segment| !segment.is_empty())
        .map(decode_segment)
        .collect::<Result<Vec<_>, _>>()?
        .join("/");
    let username = decode_component(parsed.username())?;
    let password = parsed
        .password()
        .map(decode_component)
        .transpose()?
        .unwrap_or_default();
    Ok(ParsedSmbUrl {
        server,
        share,
        root: (!root.is_empty()).then_some(root),
        username,
        password,
    })
}

fn decode_component(value: &str) -> Result<String, StorageError> {
    percent_decode_str(value)
        .decode_utf8()
        .map(std::borrow::Cow::into_owned)
        .map_err(|_| StorageError::UrlParseError("SMB URL contains invalid UTF-8".into()))
}

fn decode_segment(value: &str) -> Result<String, StorageError> {
    let decoded = decode_component(value)?;
    if matches!(decoded.as_str(), "." | "..") || decoded.contains(['/', '\\', '\0']) {
        return Err(StorageError::UrlParseError(
            "SMB URL contains an invalid path segment".into(),
        ));
    }
    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use super::parse_smb_url;
    use crate::error::StorageError;

    #[test]
    fn raw_and_encoded_names_resolve_to_the_same_config() -> Result<(), StorageError> {
        for (raw, encoded, expected) in [
            ("测试", "%E6%B5%8B%E8%AF%95", "测试"),
            ("a b", "a%20b", "a b"),
            ("100%", "100%25", "100%"),
        ] {
            let raw = parse_smb_url(&format!("smb://user:secret@host/share/{raw}"))?;
            let encoded = parse_smb_url(&format!("smb://user:secret@host/share/{encoded}"))?;
            assert_eq!(raw.root.as_deref(), Some(expected));
            assert_eq!(encoded.root, raw.root);
            assert_eq!(encoded.username, "user");
            assert_eq!(encoded.password, "secret");
        }
        Ok(())
    }

    #[test]
    fn parses_share_authority_and_credentials() -> Result<(), StorageError> {
        let url = parse_smb_url("smb://u%40x:p%40ss@[::1]:1445/%E5%85%B1%E4%BA%AB/a%23b")?;
        assert_eq!(url.server, "[::1]:1445");
        assert_eq!(url.share, "共享");
        assert_eq!(url.root.as_deref(), Some("a#b"));
        assert_eq!(url.username, "u@x");
        assert_eq!(url.password, "p@ss");
        Ok(())
    }

    #[test]
    fn refuses_ambiguous_or_unsafe_paths() {
        for path in ["a%2Fb", "a%5Cb", "%2e%2e", "%FF", "a%00b", "../x"] {
            assert!(
                parse_smb_url(&format!("smb://host/share/{path}")).is_err(),
                "{path}"
            );
        }
        for url in [
            "smb://host",
            "smb://host/",
            "smb://host/share?x=1",
            "smb://host/share#x",
        ] {
            assert!(parse_smb_url(url).is_err(), "{url}");
        }
    }
}
